use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{Bytes, BytesMut, BufMut};
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::signal;

// Pre-encoded static responses (zero allocation)
const RESP_OK: &[u8] = b"+OK\r\n";
const RESP_PONG: &[u8] = b"+PONG\r\n";
const RESP_NULL: &[u8] = b"$-1\r\n";
const RESP_ZERO: &[u8] = b":0\r\n";
const RESP_ONE: &[u8] = b":1\r\n";
const RESP_CLOSE: &str = "__CLOSE__";

// Zero-allocation RESP encoding helpers
#[inline(always)]
fn resp_write_bulk_string(buf: &mut BytesMut, data: &[u8]) {
    buf.reserve(1 + 20 + 2 + data.len() + 2);
    buf.put_u8(b'$');
    let mut itoa_buf = itoa::Buffer::new();
    let len_str = itoa_buf.format(data.len());
    buf.put_slice(len_str.as_bytes());
    buf.put_slice(b"\r\n");
    buf.put_slice(data);
    buf.put_slice(b"\r\n");
}

#[inline(always)]
fn resp_write_integer(buf: &mut BytesMut, val: i64) {
    buf.reserve(1 + 20 + 2);
    buf.put_u8(b':');
    let mut itoa_buf = itoa::Buffer::new();
    let s = itoa_buf.format(val);
    buf.put_slice(s.as_bytes());
    buf.put_slice(b"\r\n");
}

#[inline(always)]
fn resp_write_array_header(buf: &mut BytesMut, count: usize) {
    buf.reserve(1 + 20 + 2);
    buf.put_u8(b'*');
    let mut itoa_buf = itoa::Buffer::new();
    let s = itoa_buf.format(count);
    buf.put_slice(s.as_bytes());
    buf.put_slice(b"\r\n");
}

#[inline(always)]
fn resp_write_simple_string(buf: &mut BytesMut, s: &[u8]) {
    buf.reserve(1 + s.len() + 2);
    buf.put_u8(b'+');
    buf.put_slice(s);
    buf.put_slice(b"\r\n");
}

#[inline(always)]
fn resp_write_error(buf: &mut BytesMut, msg: &[u8]) {
    buf.reserve(1 + msg.len() + 2);
    buf.put_u8(b'-');
    buf.put_slice(msg);
    buf.put_slice(b"\r\n");
}

use crate::config::Config;
use crate::jobs::JobQueue;
use crate::pubsub::PubSubBroadcaster;
use crate::storage::{StorageEngine, StorageValue};
use crate::streams::StreamEngine;

pub struct Server {
    config: Config,
    storage: Arc<StorageEngine>,
    broadcaster: Arc<PubSubBroadcaster>,
    streams: Arc<StreamEngine>,
    jobs: Arc<JobQueue>,
    running: Arc<AtomicBool>,
    active_connections: Arc<AtomicU64>,
}

#[derive(Clone)]
struct ServerContext {
    config: Config,
    storage: Arc<StorageEngine>,
    broadcaster: Arc<PubSubBroadcaster>,
    #[allow(dead_code)]
    streams: Arc<StreamEngine>,
    jobs: Arc<JobQueue>,
    client_id: u64,
    subscribed_channels: Arc<parking_lot::RwLock<Vec<Vec<u8>>>>,
    pubsub_receivers: Arc<parking_lot::RwLock<Vec<broadcast::Receiver<Vec<u8>>>>>,
}

impl ServerContext {
    fn is_subscribed(&self) -> bool {
        !self.subscribed_channels.read().is_empty()
    }
}

impl Server {
    pub async fn new(config: Config) -> Self {
        let max_mem = config.parse_max_memory();
        // CPU cores × 4 shards for near-linear scaling
        let num_shards = (std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4) * 4)
            .max(64); // at least 64 shards
        let storage = Arc::new(StorageEngine::with_config(
            num_shards,
            max_mem,
            crate::storage::EvictionPolicy::Lru,
        ));
        let broadcaster = Arc::new(PubSubBroadcaster::new());
        let streams = Arc::new(StreamEngine::new());
        let jobs = Arc::new(JobQueue::new());

        tracing::info!("Using {} shards ({} CPU cores)", num_shards, std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1));

        Self {
            config,
            storage,
            broadcaster,
            streams,
            jobs,
            running: Arc::new(AtomicBool::new(true)),
            active_connections: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let addr: SocketAddr = format!(
            "{}:{}",
            self.config.server.bind, self.config.server.port
        )
        .parse()?;

        let listener = TcpListener::bind(addr).await?;
        tracing::info!("Listening on {}", addr);

        let broadcaster = self.broadcaster.clone();
        let streams = self.streams.clone();
        let jobs = self.jobs.clone();
        let storage = self.storage.clone();
        let config = self.config.clone();
        let active = self.active_connections.clone();

        let signal_running = self.running.clone();
        tokio::spawn(async move {
            signal::ctrl_c().await.ok();
            tracing::info!("Received shutdown signal, stopping server...");
            signal_running.store(false, Ordering::SeqCst);
        });

        loop {
            if !self.running.load(Ordering::SeqCst) {
                tracing::info!("Server shutting down gracefully");
                break;
            }

            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            let current = active.load(Ordering::Relaxed);
                            let max_conn = config.server.max_connections;
                            if current >= max_conn as u64 {
                                tracing::warn!("Max connections ({}) reached, rejecting {}", max_conn, peer_addr);
                                drop(stream);
                                continue;
                            }
                            tracing::trace!("New connection from {}", peer_addr);
                            active.fetch_add(1, Ordering::Relaxed);

                            let sub_broadcaster = broadcaster.clone();
                            let client_id = sub_broadcaster.next_subscriber_id();
                            let ctx = ServerContext {
                                config: config.clone(),
                                storage: storage.clone(),
                                broadcaster: sub_broadcaster,
                                streams: streams.clone(),
                                jobs: jobs.clone(),
                                client_id,
                                subscribed_channels: Arc::new(parking_lot::RwLock::new(Vec::new())),
                                pubsub_receivers: Arc::new(parking_lot::RwLock::new(Vec::new())),
                            };

                            let running = self.running.clone();
                            let active = active.clone();
                            tokio::spawn(async move {
                                Self::handle_connection(stream, ctx, running).await;
                                active.fetch_sub(1, Ordering::Relaxed);
                            });
                        }
                        Err(e) => {
                            tracing::error!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = signal::ctrl_c() => {
                    tracing::info!("Received shutdown signal, stopping server...");
                    self.running.store(false, Ordering::SeqCst);
                }
            }
        }

        Ok(())
    }

    async fn handle_connection(
        stream: TcpStream,
        ctx: ServerContext,
        running: Arc<AtomicBool>,
    ) {
        use tokio::io::AsyncReadExt;
        use bytes::BytesMut;

        let _ = stream.set_nodelay(true);

        let mut stream = BufWriter::with_capacity(131072, stream);
        let mut buf = Vec::with_capacity(131072);
        let mut read_pos: usize = 0;
        let mut resp_buf = BytesMut::with_capacity(131072);
        // Cache subscribed state to avoid RwLock on every iteration
        let mut is_subscribed = false;

        while running.load(Ordering::SeqCst) {
            if read_pos > 0 && read_pos >= buf.len() * 85 / 100 {
                let remaining = buf.len() - read_pos;
                if remaining > 0 {
                    buf.copy_within(read_pos.., 0);
                }
                buf.truncate(remaining);
                read_pos = 0;
            }

            if buf.len() < buf.capacity() {
                let spare = buf.spare_capacity_mut();
                let write_slice = unsafe {
                    std::slice::from_raw_parts_mut(
                        spare.as_mut_ptr() as *mut u8,
                        spare.len(),
                    )
                };
                let read_result = match tokio::time::timeout(
                    std::time::Duration::from_secs(60),
                    stream.get_mut().read(write_slice),
                ).await {
                    Ok(result) => result,
                    Err(_) => {
                        tracing::debug!(client_id = ctx.client_id, "Connection timed out");
                        break;
                    }
                };
                match read_result {
                    Ok(0) => {
                        tracing::trace!(client_id = ctx.client_id, "Client disconnected");
                        break;
                    }
                    Ok(n) => {
                        debug_assert!(n <= spare.len());
                        unsafe { buf.set_len(buf.len() + n); }
                    }
                    Err(e) => {
                        tracing::error!(client_id = ctx.client_id, error = %e, "Read error");
                        break;
                    }
                }
            } else {
                buf.reserve(131072);
            }

            loop {
                let available = &buf[read_pos..];
                if available.is_empty() {
                    break;
                }

                if available[0] != b'*' {
                    if let Some(end) = find_crlf(available) {
                        let line_bytes = &available[..end];
                        let line = std::str::from_utf8(line_bytes).unwrap_or("").trim();
                        read_pos += end + 2;
                        if line.is_empty() {
                            continue;
                        }
                        let args = parse_inline(line);
                        if args.is_empty() {
                            continue;
                        }
                        // Check for CLOSE before buffering
                        if args[0].eq_ignore_ascii_case("QUIT") {
                            ctx.broadcaster.unsubscribe_all(ctx.client_id);
                            return;
                        }
                        let response = Self::process_command(&args, &ctx).await;
                        if response == RESP_CLOSE {
                            ctx.broadcaster.unsubscribe_all(ctx.client_id);
                            return;
                        }
                        resp_buf.extend_from_slice(response.as_bytes());
                    } else {
                        break;
                    }
                } else {
                    // Zero-copy RESP parsing
                    match try_parse_resp_frame_zero_copy(available) {
                        Some((consumed, args)) => {
                            read_pos += consumed;
                            if args.is_empty() {
                                continue;
                            }
                            // Ultra-fast: write response directly to buffer
                            Self::write_command_response(args.as_slice(), &ctx, &mut resp_buf);
                            // Check if we need to close
                            if resp_buf.ends_with(b"__CLOSE__\r\n") {
                                resp_buf.truncate(resp_buf.len() - 11);
                                ctx.broadcaster.unsubscribe_all(ctx.client_id);
                                return;
                            }
                        }
                        None => {
                            break;
                        }
                    }
                }
            }

            if !resp_buf.is_empty() {
                if let Err(e) = stream.write_all(&resp_buf).await {
                    tracing::debug!(client_id = ctx.client_id, error = %e, "Write error");
                    ctx.broadcaster.unsubscribe_all(ctx.client_id);
                    return;
                }
                resp_buf.clear();
                let _ = stream.flush().await;
            }

            // Update cached subscribed state
            if !is_subscribed {
                is_subscribed = ctx.is_subscribed();
            }

            if is_subscribed {
                let msgs_to_send: Vec<Vec<u8>> = {
                    let mut receivers = ctx.pubsub_receivers.write();
                    let mut msgs = Vec::new();
                    for rx in receivers.iter_mut() {
                        while let Ok(msg) = rx.try_recv() {
                            msgs.push(msg);
                        }
                    }
                    msgs
                };
                if !msgs_to_send.is_empty() {
                    for msg in msgs_to_send {
                        resp_buf.extend_from_slice(b"*3\r\n$7\r\nmessage\r\n");
                        let mut itoa_buf = itoa::Buffer::new();
                        resp_buf.extend_from_slice(b"$");
                        resp_buf.extend_from_slice(itoa_buf.format(msg.len()).as_bytes());
                        resp_buf.extend_from_slice(b"\r\n");
                        resp_buf.extend_from_slice(&msg);
                        resp_buf.extend_from_slice(b"\r\n");
                    }
                    let _ = stream.write_all(&resp_buf).await;
                    resp_buf.clear();
                    let _ = stream.flush().await;
                }
            }
        }

        ctx.broadcaster.unsubscribe_all(ctx.client_id);
    }

    async fn process_command(args: &[String], ctx: &ServerContext) -> String {
        // Fast path: byte-level comparison for hot commands (avoids to_ascii_uppercase allocation)
        if let Some(cmd_bytes) = args[0].as_bytes().first() {
            match cmd_bytes {
                b'P' | b'p' => {
                    if args[0].len() == 4 && args[0].eq_ignore_ascii_case("PING") {
                        return if args.len() > 1 {
                            format!("+{}\r\n", args[1])
                        } else {
                            "+PONG\r\n".into()
                        };
                    }
                }
                b'S' | b's' => {
                    if args[0].len() == 3 && args[0].eq_ignore_ascii_case("SET") {
                        if args.len() < 3 {
                            return "-ERR wrong number of arguments for 'SET' command\r\n".into();
                        }
                        let key = args[1].as_bytes();
                        let val = args[2].as_bytes();
                        let mut ttl = None;
                        let mut i = 3;
                        while i < args.len() {
                            match args[i].to_ascii_uppercase().as_str() {
                                "EX" => {
                                    if let Some(secs) = args.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
                                        ttl = Some(std::time::Duration::from_secs(secs));
                                    }
                                    i += 2;
                                }
                                "PX" => {
                                    if let Some(ms) = args.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
                                        ttl = Some(std::time::Duration::from_millis(ms));
                                    }
                                    i += 2;
                                }
                                _ => i += 1,
                            }
                        }
                        if let Some(t) = ttl {
                            ctx.storage.set_with_ttl(key.to_vec(), StorageValue::String(Bytes::copy_from_slice(val)), t);
                        } else {
                            ctx.storage.set(key.to_vec(), StorageValue::String(Bytes::copy_from_slice(val)));
                        }
                        return "+OK\r\n".into();
                    }
                    if args[0].len() == 3 && args[0].eq_ignore_ascii_case("GET") {
                        if args.len() < 2 {
                            return "-ERR wrong number of arguments for 'GET' command\r\n".into();
                        }
                        let key = args[1].as_bytes();
                        return match ctx.storage.get(key) {
                            Some(StorageValue::String(s)) => {
                                format!("${}\r\n{}\r\n", s.len(), String::from_utf8_lossy(&s))
                            }
                            Some(_) => {
                                "-ERR WRONGTYPE Operation against a key\r\n".into()
                            }
                            None => {
                                "$-1\r\n".into()
                            }
                        };
                    }
                }
                b'D' | b'd' => {
                    if args[0].len() == 3 && args[0].eq_ignore_ascii_case("DEL") {
                        if args.len() < 2 {
                            return "-ERR wrong number of arguments for 'DEL' command\r\n".into();
                        }
                        let mut count = 0;
                        for key in &args[1..] {
                            if ctx.storage.del(key.as_bytes()) {
                                count += 1;
                            }
                        }
                        return format!(":{}\r\n", count);
                    }
                }
                _ => {}
            }
        }

        // Slow path: allocate uppercase for less common commands
        let cmd_upper = args[0].to_ascii_uppercase();

        match cmd_upper.as_str() {
            "PING" => {
                if args.len() > 1 {
                    format!("+{}\r\n", args[1])
                } else {
                    "+PONG\r\n".into()
                }
            }
            "ECHO" => {
                if args.len() < 2 {
                    "-ERR wrong number of arguments for 'ECHO' command\r\n".into()
                } else {
                    format!("${}\r\n{}\r\n", args[1].len(), args[1])
                }
            }
            "QUIT" => {
                "__CLOSE__".into()
            }
            "COMMAND" | "COMMAND DOCS" | "COMMAND COUNT" | "COMMAND INFO" => {
                "+OK\r\n".into()
            }
            "DBSIZE" => {
                let size = ctx.storage.dbsize();
                format!(":{}\r\n", size)
            }
            "INFO" => {
                let section = args.get(1).map(|s| s.as_str()).unwrap_or("server");
                let body = match section {
                    "server" => {
                        format!(
                            "# Server\r\nredis_version:7.0.0-crabkv\r\ntcp_port:{}\r\n",
                            ctx.config.server.port
                        )
                    }
                    "clients" => "# Clients\r\nconnected_clients:1\r\n".into(),
                    "memory" => {
                        let max = ctx.config.parse_max_memory();
                        let used = ctx.storage.memory_usage();
                        format!("# Memory\r\nmax_memory:{}\r\nused_memory:{}\r\n", max, used)
                    }
                    _ => "# Server\r\nredis_version:7.0.0-crabkv\r\n".into(),
                };
                format!("${}\r\n{}\r\n", body.len(), body)
            }
            "SET" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'SET' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let val = args[2].as_bytes();
                let mut ttl = None;
                let mut i = 3;
                while i < args.len() {
                    match args[i].to_uppercase().as_str() {
                        "EX" => {
                            if let Some(secs) = args.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
                                ttl = Some(std::time::Duration::from_secs(secs));
                            }
                            i += 2;
                        }
                        "PX" => {
                            if let Some(ms) = args.get(i + 1).and_then(|s| s.parse::<u64>().ok()) {
                                ttl = Some(std::time::Duration::from_millis(ms));
                            }
                            i += 2;
                        }
                        _ => i += 1,
                    }
                }
                if let Some(t) = ttl {
                    ctx.storage.set_with_ttl(key.to_vec(), StorageValue::String(Bytes::copy_from_slice(val)), t);
                } else {
                    ctx.storage.set(key.to_vec(), StorageValue::String(Bytes::copy_from_slice(val)));
                }
                "+OK\r\n".into()
            }
            "GET" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'GET' command\r\n".into();
                }
                let key = args[1].as_bytes();
                match ctx.storage.get(key) {
                    Some(StorageValue::String(s)) => {
                        format!("${}\r\n{}\r\n", s.len(), String::from_utf8_lossy(&s))
                    }
                    Some(_) => {
                        "-ERR WRONGTYPE Operation against a key\r\n".into()
                    }
                    None => {
                        "$-1\r\n".into()
                    }
                }
            }
            "DEL" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'DEL' command\r\n".into();
                }
                let mut count = 0;
                for key in &args[1..] {
                    if ctx.storage.del(key.as_bytes()) {
                        count += 1;
                    }
                }
                format!(":{}\r\n", count)
            }
            "EXISTS" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'EXISTS' command\r\n".into();
                }
                let mut count = 0;
                for key in &args[1..] {
                    if ctx.storage.exists(key.as_bytes()) {
                        count += 1;
                    }
                }
                format!(":{}\r\n", count)
            }
            "SUBSCRIBE" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'SUBSCRIBE' command\r\n".into();
                }
                let mut response = String::new();
                for channel in &args[1..] {
                    let rx = ctx
                        .broadcaster
                        .subscribe_client(ctx.client_id, channel.as_bytes());
                    {
                        let mut subs = ctx.subscribed_channels.write();
                        subs.push(channel.as_bytes().to_vec());
                    }
                    {
                        let mut receivers = ctx.pubsub_receivers.write();
                        receivers.push(rx);
                    }

                    response.push_str(&format!(
                        "*3\r\n$9\r\nsubscribe\r\n${}\r\n{}\r\n:1\r\n",
                        channel.len(),
                        channel
                    ));
                }
                response
            }
            "UNSUBSCRIBE" => {
                for channel in &args[1..] {
                    ctx.broadcaster
                        .unsubscribe(ctx.client_id, channel.as_bytes());
                }
                "*0\r\n".into()
            }
            "PUBLISH" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'PUBLISH' command\r\n".into();
                }
                let count = ctx
                    .broadcaster
                    .publish(args[1].as_bytes(), args[2].as_bytes());
                format!(":{}\r\n", count)
            }
            "PSUBSCRIBE" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'PSUBSCRIBE' command\r\n".into();
                }
                let mut response = String::new();
                for pattern in &args[1..] {
                    let rx = ctx
                        .broadcaster
                        .psubscribe_client(ctx.client_id, pattern.as_bytes());
                    {
                        let mut subs = ctx.subscribed_channels.write();
                        subs.push(pattern.as_bytes().to_vec());
                    }
                    {
                        let mut receivers = ctx.pubsub_receivers.write();
                        receivers.push(rx);
                    }

                    response.push_str(&format!(
                        "*3\r\n$10\r\npsubscribe\r\n${}\r\n{}\r\n:1\r\n",
                        pattern.len(),
                        pattern
                    ));
                }
                response
            }
            "PUNSUBSCRIBE" => {
                for pattern in &args[1..] {
                    ctx.broadcaster
                        .unsubscribe(ctx.client_id, pattern.as_bytes());
                }
                "*0\r\n".into()
            }
            "LPUSH" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'LPUSH' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let vals: Vec<Vec<u8>> = args[2..].iter().map(|a| a.as_bytes().to_vec()).collect();
                let result = ctx.storage.lpush(key, vals);
                match result {
                    Ok(n) => format!(":{}\r\n", n),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "RPUSH" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'RPUSH' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let vals: Vec<Vec<u8>> = args[2..].iter().map(|a| a.as_bytes().to_vec()).collect();
                let result = ctx.storage.rpush(key, vals);
                match result {
                    Ok(n) => format!(":{}\r\n", n),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "LPOP" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'LPOP' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.lpop(key);
                match result {
                    Ok(Some(v)) => format!("${}\r\n{}\r\n", v.len(), String::from_utf8_lossy(&v)),
                    Ok(None) => "$-1\r\n".into(),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "RPOP" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'RPOP' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.rpop(key);
                match result {
                    Ok(Some(v)) => format!("${}\r\n{}\r\n", v.len(), String::from_utf8_lossy(&v)),
                    Ok(None) => "$-1\r\n".into(),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "LLEN" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'LLEN' command\r\n".into();
                }
                let key = args[1].as_bytes();
                format!(":{}\r\n", ctx.storage.llen(key))
            }
            "LRANGE" => {
                if args.len() < 4 {
                    return "-ERR wrong number of arguments for 'LRANGE' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let start = args[2].parse::<i64>().unwrap_or(0);
                let stop = args[3].parse::<i64>().unwrap_or(-1);
                match ctx.storage.lrange(key, start, stop) {
                    Ok(items) => {
                        let mut resp = format!("*{}\r\n", items.len());
                        for item in &items {
                            resp.push_str(&format!("${}\r\n{}\r\n", item.len(), String::from_utf8_lossy(item)));
                        }
                        resp
                    }
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "HSET" => {
                if args.len() < 4 {
                    return "-ERR wrong number of arguments for 'HSET' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let field = args[2].as_bytes().to_vec();
                let value = args[3].as_bytes().to_vec();
                let result = ctx.storage.hset(key, field, value);
                match result {
                    Ok(_) => ":1\r\n".into(),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "HGET" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'HGET' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let field = args[2].as_bytes();
                let result = ctx.storage.hget(key, field);
                match result {
                    Ok(Some(v)) => format!("${}\r\n{}\r\n", v.len(), String::from_utf8_lossy(&v)),
                    Ok(None) => "$-1\r\n".into(),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "HGETALL" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'HGETALL' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.hgetall(key);
                match result {
                    Ok(pairs) => {
                        let mut resp = format!("*{}\r\n", pairs.len() * 2);
                        for (k, v) in &pairs {
                            resp.push_str(&format!("${}\r\n{}\r\n", k.len(), String::from_utf8_lossy(k)));
                            resp.push_str(&format!("${}\r\n{}\r\n", v.len(), String::from_utf8_lossy(v)));
                        }
                        resp
                    }
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "HDEL" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'HDEL' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let mut count = 0;
                for field in &args[2..] {
                    if ctx.storage.hdel(key, field.as_bytes()).unwrap_or(false) {
                        count += 1;
                    }
                }
                format!(":{}\r\n", count)
            }
            "HLEN" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'HLEN' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.hgetall(key);
                match result {
                    Ok(map) => format!(":{}\r\n", map.len()),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "SADD" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'SADD' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let members: Vec<Vec<u8>> = args[2..].iter().map(|a| a.as_bytes().to_vec()).collect();
                let result = ctx.storage.sadd(key, members);
                match result {
                    Ok(n) => format!(":{}\r\n", n),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "SMEMBERS" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'SMEMBERS' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.smembers(key);
                match result {
                    Ok(members) => {
                        let mut resp = format!("*{}\r\n", members.len());
                        for m in &members {
                            resp.push_str(&format!("${}\r\n{}\r\n", m.len(), String::from_utf8_lossy(m)));
                        }
                        resp
                    }
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "SREM" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'SREM' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let mut count = 0;
                for member in &args[2..] {
                    if ctx.storage.sismember(key, member.as_bytes()).unwrap_or(false) {
                        count += 1;
                    }
                }
                format!(":{}\r\n", count)
            }
            "SCARD" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'SCARD' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.smembers(key);
                match result {
                    Ok(members) => format!(":{}\r\n", members.len()),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "SISMEMBER" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'SISMEMBER' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let member = args[2].as_bytes();
                let result = ctx.storage.sismember(key, member);
                match result {
                    Ok(true) => ":1\r\n".into(),
                    Ok(false) => ":0\r\n".into(),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "ZADD" => {
                if args.len() < 4 {
                    return "-ERR wrong number of arguments for 'ZADD' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let score: i64 = args[2].parse().unwrap_or(0);
                let member = args[3].as_bytes().to_vec();
                let result = ctx.storage.zadd(key, score, member);
                match result {
                    Ok(_) => ":1\r\n".into(),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "ZRANGE" => {
                if args.len() < 4 {
                    return "-ERR wrong number of arguments for 'ZRANGE' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let len = ctx.storage.zlen(key);
                let start = args[2].parse::<i64>().unwrap_or(0);
                let stop = args[3].parse::<i64>().unwrap_or(-1);
                let actual_start = if start < 0 { (len as i64 + start).max(0) as usize } else { start as usize };
                let actual_stop = if stop < 0 { (len as i64 + stop).max(0) as usize } else { stop as usize };
                let withscores = args.get(4).map(|s| s.to_uppercase() == "WITHSCORES").unwrap_or(false);
                match ctx.storage.zrange(key, actual_start, actual_stop) {
                    Ok(items) => {
                        if withscores {
                            let mut resp = format!("*{}\r\n", items.len() * 2);
                            for (score, member) in &items {
                                resp.push_str(&format!("${}\r\n{}\r\n", member.len(), String::from_utf8_lossy(member)));
                                resp.push_str(&format!(":{}\r\n", score));
                            }
                            resp
                        } else {
                            let mut resp = format!("*{}\r\n", items.len());
                            for (_score, member) in &items {
                                resp.push_str(&format!("${}\r\n{}\r\n", member.len(), String::from_utf8_lossy(member)));
                            }
                            resp
                        }
                    }
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "ZSCORE" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'ZSCORE' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let member = args[2].as_bytes();
                let result = ctx.storage.zscore(key, member);
                match result {
                    Ok(Some(score)) => format!(":{}\r\n", score),
                    Ok(None) => "$-1\r\n".into(),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "ZRANK" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'ZRANK' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let member = args[2].as_bytes();
                match ctx.storage.zrange(key, 0, usize::MAX) {
                    Ok(items) => {
                        let mut rank = None;
                        for (i, (_score, m)) in items.iter().enumerate() {
                            if m == member {
                                rank = Some(i);
                                break;
                            }
                        }
                        match rank {
                            Some(i) => format!(":{}\r\n", i),
                            None => "$-1\r\n".into(),
                        }
                    }
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "ZREM" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'ZREM' command\r\n".into();
                }
                ":0\r\n".into()
            }
            "ZCARD" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'ZCARD' command\r\n".into();
                }
                let key = args[1].as_bytes();
                format!(":{}\r\n", ctx.storage.zlen(key))
            }
            "INCR" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'INCR' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.incr(key, 1);
                match result {
                    Ok(n) => format!(":{}\r\n", n),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "DECR" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'DECR' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.incr(key, -1);
                match result {
                    Ok(n) => format!(":{}\r\n", n),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "INCRBY" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'INCRBY' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let amount: i64 = args[2].parse().unwrap_or(0);
                let result = ctx.storage.incr(key, amount);
                match result {
                    Ok(n) => format!(":{}\r\n", n),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "DECRBY" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'DECRBY' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let amount: i64 = args[2].parse().unwrap_or(0);
                let result = ctx.storage.incr(key, -amount);
                match result {
                    Ok(n) => format!(":{}\r\n", n),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "EXPIRE" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'EXPIRE' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let secs: u64 = args[2].parse().unwrap_or(0);
                let result = ctx.storage.expire(key, std::time::Duration::from_secs(secs));
                if result { ":1\r\n".into() } else { ":0\r\n".into() }
            }
            "TTL" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'TTL' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.ttl(key);
                match result {
                    Some(d) => format!(":{}\r\n", d.as_secs()),
                    None => ":-2\r\n".into(),
                }
            }
            "PERSIST" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'PERSIST' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.persist(key);
                if result { ":1\r\n".into() } else { ":0\r\n".into() }
            }
            "KEYS" => {
                "*0\r\n".into()
            }
            "TYPE" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'TYPE' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.r#type(key);
                match result {
                    Some(dt) => format!("+{:?}\r\n", dt).to_lowercase().into(),
                    None => "+none\r\n".into(),
                }
            }
            "FLUSHDB" | "FLUSHALL" => {
                ctx.storage.flush_all();
                "+OK\r\n".into()
            },
            "SAVE" | "BGSAVE" => {
                "+OK\r\n".into()
            }
            "LASTSAVE" => {
                ":0\r\n".into()
            }
            "TIME" => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                format!("*2\r\n:{}\r\n:{}\r\n", now.as_secs(), now.subsec_micros())
            }
            "CLIENT" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'CLIENT' command\r\n".into();
                }
                match args[1].to_uppercase().as_str() {
                    "ID" => {
                        format!(":{}\r\n", ctx.client_id)
                    }
                    "LIST" => {
                        "+OK\r\n".into()
                    }
                    "SETNAME" => {
                        "+OK\r\n".into()
                    }
                    "GETNAME" => {
                        "$-1\r\n".into()
                    }
                    _ => {
                        "-ERR unknown CLIENT subcommand\r\n".into()
                    }
                }
            }
            "CONFIG" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'CONFIG' command\r\n".into();
                }
                match args[1].to_uppercase().as_str() {
                    "GET" => {
                        "*0\r\n".into()
                    }
                    "SET" => {
                        "+OK\r\n".into()
                    }
                    _ => {
                        "-ERR unknown CONFIG subcommand\r\n".into()
                    }
                }
            }
            "CLUSTER" => {
                if !ctx.config.cluster.enabled {
                    return "-ERR cluster mode is not enabled\r\n".into();
                }
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'CLUSTER' command\r\n".into();
                }
                match args[1].to_uppercase().as_str() {
                    "INFO" => {
                        "+cluster_enabled:1\r\n".into()
                    }
                    "NODES" => {
                        "+OK\r\n".into()
                    }
                    _ => {
                        "-ERR unknown CLUSTER subcommand\r\n".into()
                    }
                }
            }
            "XADD" => {
                if args.len() < 4 {
                    return "-ERR wrong number of arguments for 'XADD' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let mut fields = ahash::AHashMap::new();
                let mut i = 2;
                if args.len() > 3 && args[2] == "*" {
                    i = 3;
                }
                while i + 1 < args.len() {
                    fields.insert(args[i].as_bytes().to_vec(), args[i + 1].as_bytes().to_vec());
                    i += 2;
                }
                let result = ctx.storage.xadd(key, fields);
                match result {
                    Ok(id) => format!(":{}\r\n", id),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "XLEN" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'XLEN' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.xrange(key, 0, u64::MAX);
                match result {
                    Ok(entries) => format!(":{}\r\n", entries.len()),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "XRANGE" | "XREVRANGE" => {
                if args.len() < 4 {
                    return "-ERR wrong number of arguments for 'XRANGE' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let start: u64 = args[2].parse().unwrap_or(0);
                let end: u64 = args[3].parse().unwrap_or(u64::MAX);
                match ctx.storage.xrange(key, start, end) {
                    Ok(entries) => {
                        let mut resp = format!("*{}\r\n", entries.len());
                        for entry in &entries {
                            resp.push_str(&format!(
                                "*2\r\n:{}\r\n*{}\r\n",
                                entry.id,
                                entry.fields.len() * 2
                            ));
                            for (k, v) in &entry.fields {
                                resp.push_str(&format!("${}\r\n{}\r\n", k.len(), String::from_utf8_lossy(k)));
                                resp.push_str(&format!("${}\r\n{}\r\n", v.len(), String::from_utf8_lossy(v)));
                            }
                        }
                        resp
                    }
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "XREAD" => {
                let mut count = 10;
                let mut stream_idx = 1;
                if args.len() > 3 && args[1].to_uppercase() == "COUNT" {
                    if let Ok(c) = args[2].parse::<usize>() {
                        count = c;
                    }
                    stream_idx = 3;
                }
                if stream_idx >= args.len() {
                    return "*0\r\n".into();
                }
                let mut resp = String::new();
                let mut found_any = false;
                for i in stream_idx..args.len() {
                    let key = args[i].as_bytes();
                    if let Ok(entries) = ctx.storage.xrange(key, 0, u64::MAX) {
                        let limited: Vec<_> = entries.into_iter().take(count).collect();
                        if !limited.is_empty() {
                            found_any = true;
                            resp.push_str(&format!("*1\r\n*{}\r\n", limited.len()));
                            for entry in &limited {
                                resp.push_str(&format!(
                                    "*2\r\n:{}\r\n*{}\r\n",
                                    entry.id,
                                    entry.fields.len() * 2
                                ));
                                for (k, v) in &entry.fields {
                                    resp.push_str(&format!("${}\r\n{}\r\n", k.len(), String::from_utf8_lossy(k)));
                                    resp.push_str(&format!("${}\r\n{}\r\n", v.len(), String::from_utf8_lossy(v)));
                                }
                            }
                        }
                    }
                }
                if found_any { resp } else { "*0\r\n".into() }
            }
            "XACK" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'XACK' command\r\n".into();
                }
                ":0\r\n".into()
            }
            "XGROUP" => {
                "+OK\r\n".into()
            }
            "XINFO" => {
                "+OK\r\n".into()
            }
            "XCLAIM" | "XAUTOCLAIM" => {
                "*0\r\n".into()
            }
            "XTRIM" => {
                ":0\r\n".into()
            }
            "XDEL" => {
                ":0\r\n".into()
            }
            "XCREATE" => {
                "+OK\r\n".into()
            }
            "XQUEUE" => {
                "*0\r\n".into()
            }
            "JOBADD" => {
                if args.len() < 4 {
                    return "-ERR wrong number of arguments for 'JOBADD' command\r\n".into();
                }
                let queue = args[1].as_str();
                let payload = args[2].as_bytes().to_vec();
                let priority: i32 = args[3].parse().unwrap_or(0);
                let id = ctx.jobs.add_job(&queue, payload, priority, None);
                format!("+{}\r\n", id)
            }
            "JOBPOP" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'JOBPOP' command\r\n".into();
                }
                let queue = args[1].as_str();
                let result = ctx.jobs.pop_job(&queue);
                match result {
                    Some(job) => {
                        let payload = String::from_utf8_lossy(&job.payload).to_string();
                        format!(
                            "*4\r\n${}\r\n{}\r\n${}\r\n{}\r\n:{}\r\n:{}\r\n",
                            job.id.len(),
                            job.id,
                            payload.len(),
                            payload,
                            job.priority,
                            job.attempts
                        )
                    }
                    None => "*0\r\n".into(),
                }
            }
            "JOBACK" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'JOBACK' command\r\n".into();
                }
                let result = ctx.jobs.ack_job(&args[1], &args[2]);
                if result { "+OK\r\n".into() } else { "-ERR job not found\r\n".into() }
            }
            "JOBFAIL" => {
                if args.len() < 4 {
                    return "-ERR wrong number of arguments for 'JOBFAIL' command\r\n".into();
                }
                let result = ctx.jobs.fail_job(&args[1], &args[2], &args[3]);
                if result { "+OK\r\n".into() } else { "-ERR job not found\r\n".into() }
            }
            "JOBRETRY" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'JOBRETRY' command\r\n".into();
                }
                let result = ctx.jobs.retry_job(&args[1], &args[2]);
                if result { "+OK\r\n".into() } else { "-ERR job not found\r\n".into() }
            }
            "JOBSTATS" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'JOBSTATS' command\r\n".into();
                }
                let queue = args[1].as_str();
                let (pending, processing, dead) = ctx.jobs.queue_stats(&queue);
                format!("*3\r\n:{}\r\n:{}\r\n:{}\r\n", pending, processing, dead)
            }
            "JOBDEAD" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'JOBDEAD' command\r\n".into();
                }
                let queue = args[1].as_str();
                let letters = ctx.jobs.get_dead_letters(&queue);
                if letters.is_empty() {
                    "*0\r\n".into()
                } else {
                    format!(":{}\r\n", letters.len())
                }
            }
            "XWORKERS" => {
                "*0\r\n".into()
            }
            "XREPLY" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'XREPLY' command\r\n".into();
                }
                "+OK\r\n".into()
            }
            "XWATCH" => {
                "*0\r\n".into()
            }
            _ => {
                format!("-ERR unknown command '{}'\r\n", args[0])
            }
        }
    }

    /// Zero-copy command processor - works directly with byte slices from the read buffer.
    /// This eliminates all String allocations in the hot path.
    /// Ultra-fast command handler that writes directly to BytesMut buffer.
    /// Zero String allocations for the most common commands.
    #[inline(always)]
    fn write_command_response(args: &[&[u8]], ctx: &ServerContext, buf: &mut BytesMut) {
        if args.is_empty() {
            resp_write_error(buf, b"ERR empty command");
            return;
        }

        let cmd = args[0];

        match cmd.len() {
            3 => {
                if cmd.eq_ignore_ascii_case(b"SET") {
                    if args.len() < 3 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'SET' command");
                        return;
                    }
                    let key = args[1];
                    let val = args[2];
                    let mut ttl = None;
                    let mut i = 3;
                    while i < args.len() {
                        if args[i].eq_ignore_ascii_case(b"EX") {
                            if let Some(secs) = args.get(i + 1).and_then(|s| std::str::from_utf8(s).ok()?.parse::<u64>().ok()) {
                                ttl = Some(std::time::Duration::from_secs(secs));
                            }
                            i += 2;
                        } else if args[i].eq_ignore_ascii_case(b"PX") {
                            if let Some(ms) = args.get(i + 1).and_then(|s| std::str::from_utf8(s).ok()?.parse::<u64>().ok()) {
                                ttl = Some(std::time::Duration::from_millis(ms));
                            }
                            i += 2;
                        } else {
                            i += 1;
                        }
                    }
                    if let Some(t) = ttl {
                        ctx.storage.set_with_ttl(key.to_vec(), StorageValue::String(Bytes::copy_from_slice(val)), t);
                    } else {
                        ctx.storage.set(key.to_vec(), StorageValue::String(Bytes::copy_from_slice(val)));
                    }
                    buf.put_slice(RESP_OK);
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"GET") {
                    if args.len() < 2 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'GET' command");
                        return;
                    }
                    match ctx.storage.get(args[1]) {
                        Some(StorageValue::String(s)) => {
                            resp_write_bulk_string(buf, &s);
                        }
                        Some(_) => {
                            resp_write_error(buf, b"ERR WRONGTYPE Operation against a key");
                        }
                        None => {
                            buf.put_slice(RESP_NULL);
                        }
                    }
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"DEL") {
                    if args.len() < 2 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'DEL' command");
                        return;
                    }
                    let mut count = 0u64;
                    for key in &args[1..] {
                        if ctx.storage.del(key) {
                            count += 1;
                        }
                    }
                    resp_write_integer(buf, count as i64);
                    return;
                }
            }
            4 => {
                if cmd.eq_ignore_ascii_case(b"PING") {
                    if args.len() > 1 {
                        resp_write_simple_string(buf, args[1]);
                    } else {
                        buf.put_slice(RESP_PONG);
                    }
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"ECHO") {
                    if args.len() < 2 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'ECHO' command");
                        return;
                    }
                    resp_write_bulk_string(buf, args[1]);
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"QUIT") {
                    buf.put_slice(b"__CLOSE__");
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"INCR") || cmd.eq_ignore_ascii_case(b"DECR") {
                    if args.len() < 2 {
                        resp_write_error(buf, b"ERR wrong number of arguments");
                        return;
                    }
                    let amount = if cmd[0] == b'I' || cmd[0] == b'i' { 1 } else { -1 };
                    match ctx.storage.incr(args[1], amount) {
                        Ok(n) => resp_write_integer(buf, n),
                        Err(e) => {
                            resp_write_error(buf, e.as_bytes());
                        }
                    }
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"ZADD") {
                    if args.len() < 4 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'ZADD' command");
                        return;
                    }
                    if let Some(score) = std::str::from_utf8(args[2]).ok().and_then(|s| s.parse::<i64>().ok()) {
                        match ctx.storage.zadd(args[1], score, args[3].to_vec()) {
                            Ok(_) => buf.put_slice(RESP_ONE),
                            Err(e) => resp_write_error(buf, e.as_bytes()),
                        }
                    } else {
                        resp_write_error(buf, b"ERR score is not a valid integer");
                    }
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"ZRANGE") {
                    if args.len() < 4 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'ZRANGE' command");
                        return;
                    }
                    let len = ctx.storage.zlen(args[1]);
                    let start_raw = std::str::from_utf8(args[2]).unwrap_or("0").parse::<i64>().unwrap_or(0);
                    let stop_raw = std::str::from_utf8(args[3]).unwrap_or("-1").parse::<i64>().unwrap_or(-1);
                    let actual_start = if start_raw < 0 { (len as i64 + start_raw).max(0) as usize } else { start_raw as usize };
                    let actual_stop = if stop_raw < 0 { (len as i64 + stop_raw).max(0) as usize } else { stop_raw as usize };
                    let withscores = args.get(4).map(|s| s.eq_ignore_ascii_case(b"WITHSCORES")).unwrap_or(false);
                    match ctx.storage.zrange(args[1], actual_start, actual_stop) {
                        Ok(items) => {
                            if withscores {
                                resp_write_array_header(buf, items.len() * 2);
                                for (score, member) in &items {
                                    resp_write_bulk_string(buf, member);
                                    let mut itoa_buf = itoa::Buffer::new();
                                    let s = itoa_buf.format(*score);
                                    buf.put_slice(b":");
                                    buf.put_slice(s.as_bytes());
                                    buf.put_slice(b"\r\n");
                                }
                            } else {
                                resp_write_array_header(buf, items.len());
                                for (_score, member) in &items {
                                    resp_write_bulk_string(buf, member);
                                }
                            }
                        }
                        Err(e) => resp_write_error(buf, e.as_bytes()),
                    }
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"MSET") {
                    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'MSET' command");
                        return;
                    }
                    let mut i = 1;
                    while i + 1 < args.len() {
                        ctx.storage.set(args[i].to_vec(), StorageValue::String(Bytes::copy_from_slice(args[i + 1])));
                        i += 2;
                    }
                    buf.put_slice(RESP_OK);
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"MGET") {
                    if args.len() < 2 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'MGET' command");
                        return;
                    }
                    resp_write_array_header(buf, args.len() - 1);
                    for key in &args[1..] {
                        match ctx.storage.get(key) {
                            Some(StorageValue::String(s)) => resp_write_bulk_string(buf, &s),
                            Some(_) => resp_write_error(buf, b"ERR WRONGTYPE Operation against a key"),
                            None => buf.put_slice(RESP_NULL),
                        }
                    }
                    return;
                }
            }
            5 => {
                if cmd.eq_ignore_ascii_case(b"EXISTS") {
                    if args.len() < 2 {
                        resp_write_error(buf, b"ERR wrong number of arguments");
                        return;
                    }
                    let mut count = 0u64;
                    for key in &args[1..] {
                        if ctx.storage.exists(key) {
                            count += 1;
                        }
                    }
                    resp_write_integer(buf, count as i64);
                    return;
                }
            }
            6 => {
                if cmd.eq_ignore_ascii_case(b"DBSIZE") {
                    resp_write_integer(buf, ctx.storage.dbsize() as i64);
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"INCRBY") || cmd.eq_ignore_ascii_case(b"DECRBY") {
                    if args.len() < 3 {
                        resp_write_error(buf, b"ERR wrong number of arguments");
                        return;
                    }
                    let amount: i64 = std::str::from_utf8(args[2])
                        .map(|s| s.parse().unwrap_or(0))
                        .unwrap_or(0);
                    let amount = if cmd[0] == b'I' || cmd[0] == b'i' { amount } else { -amount };
                    match ctx.storage.incr(args[1], amount) {
                        Ok(n) => resp_write_integer(buf, n),
                        Err(e) => {
                            resp_write_error(buf, e.as_bytes());
                        }
                    }
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"ZCARD") {
                    if args.len() < 2 {
                        resp_write_error(buf, b"ERR wrong number of arguments");
                        return;
                    }
                    resp_write_integer(buf, ctx.storage.zlen(args[1]) as i64);
                    return;
                }
            }
            7 => {
                if cmd.eq_ignore_ascii_case(b"LPUSH") || cmd.eq_ignore_ascii_case(b"RPUSH") {
                    if args.len() < 3 {
                        resp_write_error(buf, b"ERR wrong number of arguments");
                        return;
                    }
                    let vals: Vec<Vec<u8>> = args[2..].iter().map(|a| a.to_vec()).collect();
                    let result = if cmd[0] == b'L' || cmd[0] == b'l' {
                        ctx.storage.lpush(args[1], vals)
                    } else {
                        ctx.storage.rpush(args[1], vals)
                    };
                    match result {
                        Ok(n) => resp_write_integer(buf, n as i64),
                        Err(e) => {
                            resp_write_error(buf, e.as_bytes());
                        }
                    }
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"FLUSHALL") || cmd.eq_ignore_ascii_case(b"FLUSHDB") {
                    ctx.storage.flush_all();
                    buf.put_slice(RESP_OK);
                    return;
                }
            }
            8 => {
                if cmd.eq_ignore_ascii_case(b"LPOP") || cmd.eq_ignore_ascii_case(b"RPOP") {
                    if args.len() < 2 {
                        resp_write_error(buf, b"ERR wrong number of arguments");
                        return;
                    }
                    let result = if cmd[0] == b'L' || cmd[0] == b'l' {
                        ctx.storage.lpop(args[1])
                    } else {
                        ctx.storage.rpop(args[1])
                    };
                    match result {
                        Ok(Some(v)) => resp_write_bulk_string(buf, &v),
                        Ok(None) => buf.put_slice(RESP_NULL),
                        Err(e) => {
                            resp_write_error(buf, e.as_bytes());
                        }
                    }
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"PUBLISH") {
                    if args.len() < 3 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'PUBLISH' command");
                        return;
                    }
                    let count = ctx.broadcaster.publish(args[1], args[2]);
                    resp_write_integer(buf, count as i64);
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"LLEN") {
                    if args.len() < 2 {
                        resp_write_error(buf, b"ERR wrong number of arguments");
                        return;
                    }
                    resp_write_integer(buf, ctx.storage.llen(args[1]) as i64);
                    return;
                }
            }
            9 => {
                if cmd.eq_ignore_ascii_case(b"HSET") {
                    if args.len() < 4 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'HSET' command");
                        return;
                    }
                    match ctx.storage.hset(args[1], args[2].to_vec(), args[3].to_vec()) {
                        Ok(_) => buf.put_slice(RESP_ONE),
                        Err(e) => {
                            resp_write_error(buf, e.as_bytes());
                        }
                    }
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"HGET") {
                    if args.len() < 3 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'HGET' command");
                        return;
                    }
                    match ctx.storage.hget(args[1], args[2]) {
                        Ok(Some(v)) => resp_write_bulk_string(buf, &v),
                        Ok(None) => buf.put_slice(RESP_NULL),
                        Err(e) => {
                            resp_write_error(buf, e.as_bytes());
                        }
                    }
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"SADD") {
                    if args.len() < 3 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'SADD' command");
                        return;
                    }
                    let members: Vec<Vec<u8>> = args[2..].iter().map(|a| a.to_vec()).collect();
                    match ctx.storage.sadd(args[1], members) {
                        Ok(n) => resp_write_integer(buf, n as i64),
                        Err(e) => {
                            resp_write_error(buf, e.as_bytes());
                        }
                    }
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"LRANGE") {
                    if args.len() < 4 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'LRANGE' command");
                        return;
                    }
                    let start = std::str::from_utf8(args[2]).unwrap_or("0").parse::<i64>().unwrap_or(0);
                    let stop = std::str::from_utf8(args[3]).unwrap_or("-1").parse::<i64>().unwrap_or(-1);
                    match ctx.storage.lrange(args[1], start, stop) {
                        Ok(items) => {
                            resp_write_array_header(buf, items.len());
                            for item in &items {
                                resp_write_bulk_string(buf, item);
                            }
                        }
                        Err(e) => {
                            resp_write_error(buf, e.as_bytes());
                        }
                    }
                    return;
                }
            }
            10 => {
                if cmd.eq_ignore_ascii_case(b"HGETALL") {
                    if args.len() < 2 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'HGETALL' command");
                        return;
                    }
                    match ctx.storage.hgetall(args[1]) {
                        Ok(pairs) => {
                            resp_write_array_header(buf, pairs.len() * 2);
                            for (k, v) in &pairs {
                                resp_write_bulk_string(buf, k);
                                resp_write_bulk_string(buf, v);
                            }
                        }
                        Err(e) => {
                            resp_write_error(buf, e.as_bytes());
                        }
                    }
                    return;
                }
            }
            12 => {
                if cmd.eq_ignore_ascii_case(b"SISMEMBER") {
                    if args.len() < 3 {
                        resp_write_error(buf, b"ERR wrong number of arguments");
                        return;
                    }
                    match ctx.storage.sismember(args[1], args[2]) {
                        Ok(true) => buf.put_slice(RESP_ONE),
                        Ok(false) => buf.put_slice(RESP_ZERO),
                        Err(e) => {
                            resp_write_error(buf, e.as_bytes());
                        }
                    }
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"SMEMBERS") {
                    if args.len() < 2 {
                        resp_write_error(buf, b"ERR wrong number of arguments");
                        return;
                    }
                    match ctx.storage.smembers(args[1]) {
                        Ok(members) => {
                            resp_write_array_header(buf, members.len());
                            for m in &members {
                                resp_write_bulk_string(buf, m);
                            }
                        }
                        Err(e) => {
                            resp_write_error(buf, e.as_bytes());
                        }
                    }
                    return;
                }
            }
            _ => {}
        }

        // Fallback to string-based command processor for less common commands
        let args_str: Vec<String> = args.iter().map(|a| {
            match std::str::from_utf8(a) {
                Ok(s) => s.to_owned(),
                Err(_) => String::from_utf8_lossy(a).into_owned(),
            }
        }).collect();
        let response = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(Self::process_command(&args_str, ctx))
        });
        buf.extend_from_slice(response.as_bytes());
    }
}

fn parse_inline(line: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut quote_char = '\0';

    for ch in line.chars() {
        if in_quotes {
            if ch == quote_char {
                in_quotes = false;
            } else {
                current.push(ch);
            }
        } else {
            match ch {
                ' ' | '\t' => {
                    if !current.is_empty() {
                        args.push(current.clone());
                        current.clear();
                    }
                }
                '\'' | '"' => {
                    in_quotes = true;
                    quote_char = ch;
                }
                _ => {
                    current.push(ch);
                }
            }
        }
    }

    if !current.is_empty() {
        args.push(current);
    }

    args
}

#[inline(always)]
fn find_crlf(buf: &[u8]) -> Option<usize> {
    let pos = memchr::memchr(b'\r', buf)?;
    if pos + 1 < buf.len() && buf[pos + 1] == b'\n' {
        Some(pos)
    } else {
        None
    }
}

const MAX_RESP_ARGS: usize = 4096;

struct ZeroCopyArgs<'a> {
    buf: smallvec::SmallVec<[&'a [u8]; 16]>,
}

impl<'a> ZeroCopyArgs<'a> {
    fn with_capacity(cap: usize) -> Self {
        ZeroCopyArgs {
            buf: smallvec::SmallVec::with_capacity(cap.min(MAX_RESP_ARGS)),
        }
    }

    #[inline(always)]
    fn push(&mut self, slice: &'a [u8]) {
        if self.buf.len() < MAX_RESP_ARGS {
            self.buf.push(slice);
        }
    }

    #[inline(always)]
    fn as_slice(&self) -> &[&'a [u8]] {
        &self.buf
    }

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

/// Zero-copy RESP frame parser. Returns byte slices pointing into the original buffer.
/// Returns (consumed_bytes, arg_slices) with stack-allocated args (no heap allocation).
#[inline(always)]
fn try_parse_resp_frame_zero_copy<'a>(buf: &'a [u8]) -> Option<(usize, ZeroCopyArgs<'a>)> {
    if buf.len() < 4 || buf[0] != b'*' {
        return None;
    }

    let first_end = find_crlf(buf)?;
    let count_str = std::str::from_utf8(&buf[1..first_end]).ok()?;
    let count: usize = count_str.parse().ok()?;

    if count > MAX_RESP_ARGS {
        return None;
    }

    let mut pos = first_end + 2;
    let mut args = ZeroCopyArgs::with_capacity(count);

    for _ in 0..count {
        if pos >= buf.len() || buf[pos] != b'$' {
            return None;
        }
        let len_end = find_crlf(&buf[pos..])? + pos;
        let len_str = std::str::from_utf8(&buf[pos + 1..len_end]).ok()?;
        let len: usize = len_str.parse().ok()?;
        pos = len_end + 2;

        let end = pos.checked_add(len)?.checked_add(2)?;
        if end > buf.len() {
            return None;
        }

        args.push(&buf[pos..pos + len]);
        pos = end;
    }

    Some((pos, args))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_inline_simple() {
        assert_eq!(parse_inline("PING"), vec!["PING"]);
    }

    #[test]
    fn parse_inline_multiple_args() {
        assert_eq!(parse_inline("SET foo bar"), vec!["SET", "foo", "bar"]);
    }

    #[test]
    fn parse_inline_with_double_quotes() {
        assert_eq!(
            parse_inline(r#"SET "hello world" baz"#),
            vec!["SET", "hello world", "baz"]
        );
    }

    #[test]
    fn parse_inline_with_single_quotes() {
        assert_eq!(
            parse_inline("SET 'hello world' baz"),
            vec!["SET", "hello world", "baz"]
        );
    }

    #[test]
    fn parse_inline_empty() {
        assert!(parse_inline("").is_empty());
    }

    #[test]
    fn parse_inline_extra_spaces() {
        assert_eq!(
            parse_inline("  SET   foo   bar  "),
            vec!["SET", "foo", "bar"]
        );
    }

    #[test]
    fn parse_resp_simple() {
        let input = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let (_, args) = try_parse_resp_frame_zero_copy(input).unwrap();
        let strs: Vec<&str> = args.as_slice().iter().map(|s| std::str::from_utf8(s).unwrap()).collect();
        assert_eq!(strs, vec!["SET", "foo", "bar"]);
    }

    #[test]
    fn parse_resp_single_arg() {
        let input = b"*1\r\n$4\r\nPING\r\n";
        let (_, args) = try_parse_resp_frame_zero_copy(input).unwrap();
        let strs: Vec<&str> = args.as_slice().iter().map(|s| std::str::from_utf8(s).unwrap()).collect();
        assert_eq!(strs, vec!["PING"]);
    }

    #[test]
    fn parse_resp_empty_bulk_string() {
        let input = b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n";
        let (_, args) = try_parse_resp_frame_zero_copy(input).unwrap();
        let strs: Vec<&str> = args.as_slice().iter().map(|s| std::str::from_utf8(s).unwrap()).collect();
        assert_eq!(strs, vec!["GET", ""]);
    }

    #[test]
    fn parse_resp_invalid_header() {
        assert!(try_parse_resp_frame_zero_copy(b"$3\r\nSET\r\n").is_none());
    }

    #[test]
    fn parse_resp_wrong_length() {
        assert!(try_parse_resp_frame_zero_copy(b"*1\r\n$5\r\nSE\r\n").is_none());
    }

    #[test]
    fn parse_resp_truncated() {
        assert!(try_parse_resp_frame_zero_copy(b"*3\r\n$3\r\nSET\r\n").is_none());
    }

    #[tokio::test]
    async fn ping_command() {
        let ctx = test_context();
        assert_eq!(
            Server::process_command(&["PING".into()], &ctx).await,
            "+PONG\r\n"
        );
    }

    #[tokio::test]
    async fn ping_with_arg() {
        let ctx = test_context();
        assert_eq!(
            Server::process_command(&["PING".into(), "hello".into()], &ctx).await,
            "+hello\r\n"
        );
    }

    #[tokio::test]
    async fn echo_command() {
        let ctx = test_context();
        assert_eq!(
            Server::process_command(&["ECHO".into(), "foobar".into()], &ctx).await,
            "$6\r\nfoobar\r\n"
        );
    }

    #[tokio::test]
    async fn echo_missing_arg() {
        let ctx = test_context();
        let resp = Server::process_command(&["ECHO".into()], &ctx).await;
        assert!(resp.starts_with("-ERR"));
    }

    #[tokio::test]
    async fn set_command() {
        let ctx = test_context();
        assert_eq!(
            Server::process_command(&["SET".into(), "k".into(), "v".into()], &ctx).await,
            "+OK\r\n"
        );
    }

    #[tokio::test]
    async fn set_missing_args() {
        let ctx = test_context();
        let resp = Server::process_command(&["SET".into(), "k".into()], &ctx).await;
        assert!(resp.starts_with("-ERR"));
    }

    #[tokio::test]
    async fn get_returns_nil() {
        let ctx = test_context();
        assert_eq!(
            Server::process_command(&["GET".into(), "k".into()], &ctx).await,
            "$-1\r\n"
        );
    }

    #[tokio::test]
    async fn unknown_command() {
        let ctx = test_context();
        let resp = Server::process_command(&["FOOBAR".into()], &ctx).await;
        assert!(resp.starts_with("-ERR unknown command"));
    }

    #[tokio::test]
    async fn quit_command() {
        let ctx = test_context();
        assert_eq!(
            Server::process_command(&["QUIT".into()], &ctx).await,
            "__CLOSE__"
        );
    }

    #[tokio::test]
    async fn publish_command() {
        let ctx = test_context();
        assert_eq!(
            Server::process_command(
                &["PUBLISH".into(), "ch".into(), "msg".into()],
                &ctx
            )
            .await,
            ":0\r\n"
        );
    }

    #[tokio::test]
    async fn subscribe_command() {
        let ctx = test_context();
        let resp = Server::process_command(
            &["SUBSCRIBE".into(), "ch1".into(), "ch2".into()],
            &ctx,
        )
        .await;
        assert!(resp.contains("subscribe"));
        assert!(resp.contains("ch1"));
        assert!(resp.contains("ch2"));
    }

    #[tokio::test]
    async fn client_id() {
        let ctx = test_context();
        let resp = Server::process_command(&["CLIENT".into(), "ID".into()], &ctx).await;
        assert_eq!(resp, ":1\r\n");
    }

    #[tokio::test]
    async fn time_command() {
        let ctx = test_context();
        let resp = Server::process_command(&["TIME".into()], &ctx).await;
        assert!(resp.starts_with("*2\r\n:"));
    }

    #[tokio::test]
    async fn jobadd_command() {
        let ctx = test_context();
        let resp = Server::process_command(
            &["JOBADD".into(), "q".into(), "payload".into(), "0".into()],
            &ctx,
        )
        .await;
        assert!(resp.starts_with('+'));
    }

    #[tokio::test]
    async fn jobpop_empty_queue() {
        let ctx = test_context();
        assert_eq!(
            Server::process_command(&["JOBPOP".into(), "q".into()], &ctx).await,
            "*0\r\n"
        );
    }

    fn test_context() -> ServerContext {
        ServerContext {
            config: Config::default(),
            storage: Arc::new(StorageEngine::with_config(16, 1024 * 1024, crate::storage::EvictionPolicy::Lru)),
            broadcaster: Arc::new(PubSubBroadcaster::new()),
            streams: Arc::new(StreamEngine::new()),
            jobs: Arc::new(JobQueue::new()),
            client_id: 1,
            subscribed_channels: Arc::new(parking_lot::RwLock::new(Vec::new())),
            pubsub_receivers: Arc::new(parking_lot::RwLock::new(Vec::new())),
        }
    }
}
