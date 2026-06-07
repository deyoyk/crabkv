use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{BytesMut, BufMut};
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
const RESP_EMPTY_ARRAY: &[u8] = b"*0\r\n";
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

    fn debug(&self, msg: impl std::fmt::Display) {
        if self.config.server.debug {
            tracing::debug!(client_id = self.client_id, "{}", msg);
        }
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
                            tracing::debug!("New connection from {}", peer_addr);
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

        let mut stream = BufWriter::with_capacity(65536, stream);
        let mut buf = Vec::with_capacity(65536);
        let mut read_pos: usize = 0;
        let mut resp_buf = BytesMut::with_capacity(65536);
        // Cache subscribed state to avoid RwLock on every iteration
        let mut is_subscribed = false;

        while running.load(Ordering::SeqCst) {
            if read_pos > 0 && read_pos >= buf.len() / 2 {
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
                match stream.get_mut().read(write_slice).await {
                    Ok(0) => {
                        tracing::debug!(client_id = ctx.client_id, "Client disconnected");
                        break;
                    }
                    Ok(n) => {
                        unsafe { buf.set_len(buf.len() + n); }
                    }
                    Err(e) => {
                        tracing::error!(client_id = ctx.client_id, error = %e, "Read error");
                        break;
                    }
                }
            } else {
                buf.reserve(65536);
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
                        Some((consumed, arg_slices)) => {
                            read_pos += consumed;
                            if arg_slices.is_empty() {
                                continue;
                            }
                            // Ultra-fast: write response directly to buffer
                            Self::write_command_response(&arg_slices, &ctx, &mut resp_buf);
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
                        ctx.debug("PING -> PONG");
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
                        ctx.storage.set(key.to_vec(), StorageValue::String(val.to_vec()));
                        if let Some(t) = ttl {
                            ctx.storage.expire(key, t);
                        }
                        ctx.debug(format!("SET {} = {} (ttl={:?})", String::from_utf8_lossy(key), String::from_utf8_lossy(val), ttl));
                        return "+OK\r\n".into();
                    }
                    if args[0].len() == 3 && args[0].eq_ignore_ascii_case("GET") {
                        if args.len() < 2 {
                            return "-ERR wrong number of arguments for 'GET' command\r\n".into();
                        }
                        let key = args[1].as_bytes();
                        return match ctx.storage.get(key) {
                            Some(StorageValue::String(s)) => {
                                ctx.debug(format!("GET {} -> {} bytes", String::from_utf8_lossy(key), s.len()));
                                format!("${}\r\n{}\r\n", s.len(), String::from_utf8_lossy(&s))
                            }
                            Some(_) => {
                                ctx.debug(format!("GET {} -> WRONGTYPE", String::from_utf8_lossy(key)));
                                "-ERR WRONGTYPE Operation against a key\r\n".into()
                            }
                            None => {
                                ctx.debug(format!("GET {} -> nil", String::from_utf8_lossy(key)));
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
                        ctx.debug(format!("DEL {:?} -> {}", &args[1..], count));
                        return format!(":{}\r\n", count);
                    }
                }
                _ => {}
            }
        }

        // Slow path: allocate uppercase for less common commands
        let cmd_upper = args[0].to_ascii_uppercase();
        ctx.debug(format!("command={} args={:?}", cmd_upper, &args[1..]));

        match cmd_upper.as_str() {
            "PING" => {
                ctx.debug("PING -> PONG");
                if args.len() > 1 {
                    format!("+{}\r\n", args[1])
                } else {
                    "+PONG\r\n".into()
                }
            }
            "ECHO" => {
                let result = if args.len() < 2 {
                    "-ERR wrong number of arguments for 'ECHO' command\r\n".into()
                } else {
                    format!("${}\r\n{}\r\n", args[1].len(), args[1])
                };
                ctx.debug(format!("ECHO -> {}", result.len()));
                result
            }
            "QUIT" => {
                ctx.debug("QUIT -> closing connection");
                "__CLOSE__".into()
            }
            "COMMAND" | "COMMAND DOCS" | "COMMAND COUNT" | "COMMAND INFO" => {
                ctx.debug(format!("{} -> OK", cmd_upper));
                "+OK\r\n".into()
            }
            "DBSIZE" => {
                let size = ctx.storage.dbsize();
                ctx.debug(format!("DBSIZE -> {}", size));
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
                ctx.debug(format!("INFO {} -> {} bytes", section, body.len()));
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
                ctx.storage.set(key.to_vec(), StorageValue::String(val.to_vec()));
                if let Some(t) = ttl {
                    ctx.storage.expire(key, t);
                }
                ctx.debug(format!("SET {} = {} (ttl={:?})", String::from_utf8_lossy(key), String::from_utf8_lossy(val), ttl));
                "+OK\r\n".into()
            }
            "GET" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'GET' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = match ctx.storage.get(key) {
                    Some(StorageValue::String(s)) => {
                        ctx.debug(format!("GET {} -> {} bytes", String::from_utf8_lossy(key), s.len()));
                        format!("${}\r\n{}\r\n", s.len(), String::from_utf8_lossy(&s))
                    }
                    Some(_) => {
                        ctx.debug(format!("GET {} -> WRONGTYPE", String::from_utf8_lossy(key)));
                        "-ERR WRONGTYPE Operation against a key\r\n".into()
                    }
                    None => {
                        ctx.debug(format!("GET {} -> nil", String::from_utf8_lossy(key)));
                        "$-1\r\n".into()
                    }
                };
                result
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
                ctx.debug(format!("DEL {:?} -> {}", &args[1..], count));
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
                ctx.debug(format!("EXISTS {:?} -> {}", &args[1..], count));
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
                    ctx.debug(format!("SUBSCRIBE {} (client={})", String::from_utf8_lossy(channel.as_bytes()), ctx.client_id));

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
                    ctx.debug(format!("UNSUBSCRIBE {}", String::from_utf8_lossy(channel.as_bytes())));
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
                ctx.debug(format!("PUBLISH {} -> {} subscribers", String::from_utf8_lossy(args[1].as_bytes()), count));
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
                    ctx.debug(format!("PSUBSCRIBE {} (client={})", String::from_utf8_lossy(pattern.as_bytes()), ctx.client_id));

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
                    ctx.debug(format!("PUNSUBSCRIBE {}", String::from_utf8_lossy(pattern.as_bytes())));
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
                ctx.debug(format!("LPUSH {} ({} values) -> {:?}", String::from_utf8_lossy(key), args.len() - 2, result.as_ref().ok()));
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
                ctx.debug(format!("RPUSH {} ({} values) -> {:?}", String::from_utf8_lossy(key), args.len() - 2, result.as_ref().ok()));
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
                ctx.debug(format!("LPOP {} -> {:?}", String::from_utf8_lossy(key), result.as_ref().ok().and_then(|r| r.as_ref().map(|v| String::from_utf8_lossy(v).to_string()))));
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
                ctx.debug(format!("RPOP {} -> {:?}", String::from_utf8_lossy(key), result.as_ref().ok().and_then(|r| r.as_ref().map(|v| String::from_utf8_lossy(v).to_string()))));
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
                let result = ctx.storage.lrange(key, 0, -1);
                ctx.debug(format!("LLEN {} -> {:?}", String::from_utf8_lossy(key), result.as_ref().ok().map(|v| v.len())));
                match result {
                    Ok(items) => format!(":{}\r\n", items.len()),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "LRANGE" => {
                if args.len() < 4 {
                    return "-ERR wrong number of arguments for 'LRANGE' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let start = args[2].parse::<i64>().unwrap_or(0);
                let stop = args[3].parse::<i64>().unwrap_or(-1);
                ctx.debug(format!("LRANGE {} {} {} -> ...", String::from_utf8_lossy(key), start, stop));
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
                let result = ctx.storage.hset(key, field.clone(), value.clone());
                ctx.debug(format!("HSET {} {} = {} -> {:?}", String::from_utf8_lossy(key), String::from_utf8_lossy(&field), String::from_utf8_lossy(&value), result.as_ref().ok()));
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
                ctx.debug(format!("HGET {} {} -> {:?}", String::from_utf8_lossy(key), String::from_utf8_lossy(field), result.as_ref().ok().and_then(|r| r.as_ref().map(|v| String::from_utf8_lossy(v).to_string()))));
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
                ctx.debug(format!("HGETALL {} -> {:?}", String::from_utf8_lossy(key), result.as_ref().ok().map(|v| v.len())));
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
                ctx.debug(format!("HDEL {} {:?} -> {}", String::from_utf8_lossy(key), &args[2..], count));
                format!(":{}\r\n", count)
            }
            "HLEN" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'HLEN' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.hgetall(key);
                ctx.debug(format!("HLEN {} -> {:?}", String::from_utf8_lossy(key), result.as_ref().ok().map(|v| v.len())));
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
                ctx.debug(format!("SADD {} {:?} -> {:?}", String::from_utf8_lossy(key), &args[2..], result.as_ref().ok()));
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
                ctx.debug(format!("SMEMBERS {} -> {:?}", String::from_utf8_lossy(key), result.as_ref().ok().map(|v| v.len())));
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
                ctx.debug(format!("SREM {} {:?} -> {}", String::from_utf8_lossy(key), &args[2..], count));
                format!(":{}\r\n", count)
            }
            "SCARD" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'SCARD' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.smembers(key);
                ctx.debug(format!("SCARD {} -> {:?}", String::from_utf8_lossy(key), result.as_ref().ok().map(|v| v.len())));
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
                ctx.debug(format!("SISMEMBER {} {} -> {:?}", String::from_utf8_lossy(key), String::from_utf8_lossy(member), result.as_ref().ok()));
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
                let result = ctx.storage.zadd(key, score, member.clone());
                ctx.debug(format!("ZADD {} {} {} -> {:?}", String::from_utf8_lossy(key), score, String::from_utf8_lossy(&member), result.as_ref().ok()));
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
                let start = args[2].parse::<i64>().unwrap_or(0);
                let stop = args[3].parse::<i64>().unwrap_or(-1);
                let len = match ctx.storage.zrange(key, 0, 0) {
                    Ok(_) => 0,
                    Err(_) => 0,
                };
                let actual_start = if start < 0 { (len as i64 + start).max(0) as usize } else { start as usize };
                let actual_stop = if stop < 0 { (len as i64 + stop).max(0) as usize } else { stop as usize };
                let withscores = args.get(4).map(|s| s.to_uppercase() == "WITHSCORES").unwrap_or(false);
                ctx.debug(format!("ZRANGE {} {} {} (withscores={}) -> ...", String::from_utf8_lossy(key), actual_start, actual_stop, withscores));
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
                ctx.debug(format!("ZSCORE {} {} -> {:?}", String::from_utf8_lossy(key), String::from_utf8_lossy(member), result.as_ref().ok().and_then(|r| *r)));
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
                        ctx.debug(format!("ZRANK {} {} -> {:?}", String::from_utf8_lossy(key), String::from_utf8_lossy(member), rank));
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
                ctx.debug(format!("ZREM {} {:?} -> 0 (stub)", String::from_utf8_lossy(args[1].as_bytes()), &args[2..]));
                ":0\r\n".into()
            }
            "ZCARD" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'ZCARD' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.zrange(key, 0, usize::MAX);
                ctx.debug(format!("ZCARD {} -> {:?}", String::from_utf8_lossy(key), result.as_ref().ok().map(|v| v.len())));
                match result {
                    Ok(items) => format!(":{}\r\n", items.len()),
                    Err(e) => format!("-ERR {}\r\n", e),
                }
            }
            "INCR" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'INCR' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.incr(key, 1);
                ctx.debug(format!("INCR {} -> {:?}", String::from_utf8_lossy(key), result.as_ref().ok()));
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
                ctx.debug(format!("DECR {} -> {:?}", String::from_utf8_lossy(key), result.as_ref().ok()));
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
                ctx.debug(format!("INCRBY {} {} -> {:?}", String::from_utf8_lossy(key), amount, result.as_ref().ok()));
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
                ctx.debug(format!("DECRBY {} {} -> {:?}", String::from_utf8_lossy(key), amount, result.as_ref().ok()));
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
                ctx.debug(format!("EXPIRE {} {} -> {}", String::from_utf8_lossy(key), secs, result));
                if result { ":1\r\n".into() } else { ":0\r\n".into() }
            }
            "TTL" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'TTL' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.ttl(key);
                ctx.debug(format!("TTL {} -> {:?}", String::from_utf8_lossy(key), result));
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
                ctx.debug(format!("PERSIST {} -> {}", String::from_utf8_lossy(key), result));
                if result { ":1\r\n".into() } else { ":0\r\n".into() }
            }
            "KEYS" => {
                ctx.debug("KEYS -> *0 (stub)");
                "*0\r\n".into()
            }
            "TYPE" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'TYPE' command\r\n".into();
                }
                let key = args[1].as_bytes();
                let result = ctx.storage.r#type(key);
                ctx.debug(format!("TYPE {} -> {:?}", String::from_utf8_lossy(key), result));
                match result {
                    Some(dt) => format!("+{:?}\r\n", dt).to_lowercase().into(),
                    None => "+none\r\n".into(),
                }
            }
            "FLUSHDB" | "FLUSHALL" => {
                ctx.debug(format!("{} -> OK", cmd_upper));
                ctx.storage.flush_all();
                "+OK\r\n".into()
            },
            "SAVE" | "BGSAVE" => {
ctx.debug(format!("{} -> OK (stub)", cmd_upper));
                "+OK\r\n".into()
            }
            "LASTSAVE" => {
                ctx.debug("LASTSAVE -> 0");
                ":0\r\n".into()
            }
            "TIME" => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                ctx.debug(format!("TIME -> {}.{:06}", now.as_secs(), now.subsec_micros()));
                format!("*2\r\n:{}\r\n:{}\r\n", now.as_secs(), now.subsec_micros())
            }
            "CLIENT" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'CLIENT' command\r\n".into();
                }
                match args[1].to_uppercase().as_str() {
                    "ID" => {
                        ctx.debug(format!("CLIENT ID -> {}", ctx.client_id));
                        format!(":{}\r\n", ctx.client_id)
                    }
                    "LIST" => {
                        ctx.debug("CLIENT LIST -> OK (stub)");
                        "+OK\r\n".into()
                    }
                    "SETNAME" => {
                        ctx.debug("CLIENT SETNAME -> OK (stub)");
                        "+OK\r\n".into()
                    }
                    "GETNAME" => {
                        ctx.debug("CLIENT GETNAME -> nil (stub)");
                        "$-1\r\n".into()
                    }
                    sub => {
                        ctx.debug(format!("CLIENT {} -> ERR unknown subcommand", sub));
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
                        ctx.debug("CONFIG GET -> *0 (stub)");
                        "*0\r\n".into()
                    }
                    "SET" => {
                        ctx.debug("CONFIG SET -> OK (stub)");
                        "+OK\r\n".into()
                    }
                    sub => {
                        ctx.debug(format!("CONFIG {} -> ERR unknown subcommand", sub));
                        "-ERR unknown CONFIG subcommand\r\n".into()
                    }
                }
            }
            "CLUSTER" => {
                if !ctx.config.cluster.enabled {
                    ctx.debug("CLUSTER -> ERR not enabled");
                    return "-ERR cluster mode is not enabled\r\n".into();
                }
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'CLUSTER' command\r\n".into();
                }
                match args[1].to_uppercase().as_str() {
                    "INFO" => {
                        ctx.debug("CLUSTER INFO -> OK (stub)");
                        "+cluster_enabled:1\r\n".into()
                    }
                    "NODES" => {
                        ctx.debug("CLUSTER NODES -> OK (stub)");
                        "+OK\r\n".into()
                    }
                    sub => {
                        ctx.debug(format!("CLUSTER {} -> ERR unknown subcommand", sub));
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
                // Skip the ID argument (use "*" for auto-generation)
                if args.len() > 3 && args[2] == "*" {
                    i = 3;
                }
                while i + 1 < args.len() {
                    fields.insert(args[i].as_bytes().to_vec(), args[i + 1].as_bytes().to_vec());
                    i += 2;
                }
                let result = ctx.storage.xadd(key, fields);
                ctx.debug(format!("XADD {} -> {:?}", String::from_utf8_lossy(key), result.as_ref().ok()));
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
                ctx.debug(format!("XLEN {} -> {:?}", String::from_utf8_lossy(key), result.as_ref().ok().map(|v| v.len())));
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
                ctx.debug(format!("XRANGE {} {} {} -> ...", String::from_utf8_lossy(key), start, end));
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
                // XREAD [COUNT count] STREAMS key [key ...] id [id ...]
                let mut count = 10;
                let mut stream_idx = 1;
                if args.len() > 3 && args[1].to_uppercase() == "COUNT" {
                    if let Ok(c) = args[2].parse::<usize>() {
                        count = c;
                    }
                    stream_idx = 3;
                }
                if stream_idx >= args.len() {
                    ctx.debug("XREAD -> *0 (no streams)");
                    return "*0\r\n".into();
                }
                let keys: Vec<_> = args[stream_idx..].iter().map(|a| String::from_utf8_lossy(a.as_bytes()).to_string()).collect();
                ctx.debug(format!("XREAD count={} streams={:?}", count, keys));
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
                ctx.debug(format!("XACK {} {} -> 0 (stub)", String::from_utf8_lossy(args[1].as_bytes()), String::from_utf8_lossy(args[2].as_bytes())));
                ":0\r\n".into()
            }
            "XGROUP" => {
                ctx.debug(format!("XGROUP -> OK (stub)"));
                "+OK\r\n".into()
            }
            "XINFO" => {
                ctx.debug(format!("XINFO -> OK (stub)"));
                "+OK\r\n".into()
            }
            "XCLAIM" | "XAUTOCLAIM" => {
                ctx.debug(format!("{} -> *0 (stub)", cmd_upper));
                "*0\r\n".into()
            }
            "XTRIM" => {
                ctx.debug(format!("XTRIM -> 0 (stub)"));
                ":0\r\n".into()
            }
            "XDEL" => {
                ctx.debug(format!("XDEL -> 0 (stub)"));
                ":0\r\n".into()
            }
            "XCREATE" => {
                ctx.debug(format!("XCREATE -> OK (stub)"));
                "+OK\r\n".into()
            }
            "XQUEUE" => {
                ctx.debug(format!("XQUEUE -> *0 (stub)"));
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
                ctx.debug(format!("JOBADD {} priority={} -> {}", queue, priority, id));
                format!("+{}\r\n", id)
            }
            "JOBPOP" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'JOBPOP' command\r\n".into();
                }
                let queue = args[1].as_str();
                let result = ctx.jobs.pop_job(&queue);
                ctx.debug(format!("JOBPOP {} -> {:?}", queue, result.as_ref().map(|j| &j.id)));
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
                ctx.debug(format!("JOBACK {} {} -> {}", args[1], args[2], result));
                if result { "+OK\r\n".into() } else { "-ERR job not found\r\n".into() }
            }
            "JOBFAIL" => {
                if args.len() < 4 {
                    return "-ERR wrong number of arguments for 'JOBFAIL' command\r\n".into();
                }
                let result = ctx.jobs.fail_job(&args[1], &args[2], &args[3]);
                ctx.debug(format!("JOBFAIL {} {} -> {}", args[1], args[2], result));
                if result { "+OK\r\n".into() } else { "-ERR job not found\r\n".into() }
            }
            "JOBRETRY" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'JOBRETRY' command\r\n".into();
                }
                let result = ctx.jobs.retry_job(&args[1], &args[2]);
                ctx.debug(format!("JOBRETRY {} {} -> {}", args[1], args[2], result));
                if result { "+OK\r\n".into() } else { "-ERR job not found\r\n".into() }
            }
            "JOBSTATS" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'JOBSTATS' command\r\n".into();
                }
                let queue = args[1].as_str();
                let (pending, processing, dead) = ctx.jobs.queue_stats(&queue);
                ctx.debug(format!("JOBSTATS {} -> pending={}, processing={}, dead={}", queue, pending, processing, dead));
                format!("*3\r\n:{}\r\n:{}\r\n:{}\r\n", pending, processing, dead)
            }
            "JOBDEAD" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'JOBDEAD' command\r\n".into();
                }
                let queue = args[1].as_str();
                let letters = ctx.jobs.get_dead_letters(&queue);
                ctx.debug(format!("JOBDEAD {} -> {} dead letters", queue, letters.len()));
                if letters.is_empty() {
                    "*0\r\n".into()
                } else {
                    format!(":{}\r\n", letters.len())
                }
            }
            "XWORKERS" => {
                ctx.debug("XWORKERS -> *0 (stub)");
                "*0\r\n".into()
            }
            "XREPLY" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'XREPLY' command\r\n".into();
                }
                ctx.debug(format!("XREPLY {} {} -> OK (stub)", String::from_utf8_lossy(args[1].as_bytes()), String::from_utf8_lossy(args[2].as_bytes())));
                "+OK\r\n".into()
            }
            "XWATCH" => {
                ctx.debug("XWATCH -> *0 (stub)");
                "*0\r\n".into()
            }
            _ => {
                ctx.debug(format!("{} -> ERR unknown command", args[0]));
                format!("-ERR unknown command '{}'\r\n", args[0])
            }
        }
    }

    /// Zero-copy command processor - works directly with byte slices from the read buffer.
    /// This eliminates all String allocations in the hot path.
    async fn process_command_zerocopy<'a>(args: &[&'a [u8]], ctx: &ServerContext) -> String {
        if args.is_empty() {
            return "-ERR empty command\r\n".into();
        }

        let cmd = args[0];

        // Ultra-hot path: single byte dispatch for the most common commands
        match cmd.len() {
            3 => {
                if cmd.eq_ignore_ascii_case(b"SET") {
                    if args.len() < 3 {
                        return "-ERR wrong number of arguments for 'SET' command\r\n".into();
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
                    ctx.storage.set(key.to_vec(), StorageValue::String(val.to_vec()));
                    if let Some(t) = ttl {
                        ctx.storage.expire(key, t);
                    }
                    return "+OK\r\n".into();
                }
                if cmd.eq_ignore_ascii_case(b"GET") {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments for 'GET' command\r\n".into();
                    }
                    let key = args[1];
                    return match ctx.storage.get(key) {
                        Some(StorageValue::String(s)) => {
                            let mut resp = String::with_capacity(32 + s.len());
                            resp.push_str(&format!("${}\r\n", s.len()));
                            // Write value bytes directly
                            resp.push_str(&String::from_utf8_lossy(&s));
                            resp.push_str("\r\n");
                            resp
                        }
                        Some(_) => "-ERR WRONGTYPE Operation against a key\r\n".into(),
                        None => "$-1\r\n".into(),
                    };
                }
                if cmd.eq_ignore_ascii_case(b"DEL") {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments for 'DEL' command\r\n".into();
                    }
                    let mut count = 0u64;
                    for key in &args[1..] {
                        if ctx.storage.del(key) {
                            count += 1;
                        }
                    }
                    return format!(":{}\r\n", count);
                }
                if cmd.eq_ignore_ascii_case(b"PNG") || cmd.eq_ignore_ascii_case(b"pin") {
                    // Skip - not a real command
                }
            }
            4 => {
                if cmd.eq_ignore_ascii_case(b"PING") {
                    return if args.len() > 1 {
                        let mut resp = String::with_capacity(4 + args[1].len() + 2);
                        resp.push('+');
                        resp.push_str(&String::from_utf8_lossy(args[1]));
                        resp.push_str("\r\n");
                        resp
                    } else {
                        "+PONG\r\n".into()
                    };
                }
                if cmd.eq_ignore_ascii_case(b"ECHO") {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments for 'ECHO' command\r\n".into();
                    }
                    let mut resp = String::with_capacity(16 + args[1].len());
                    resp.push_str(&format!("${}\r\n", args[1].len()));
                    resp.push_str(&String::from_utf8_lossy(args[1]));
                    resp.push_str("\r\n");
                    return resp;
                }
                if cmd.eq_ignore_ascii_case(b"QUIT") {
                    return "__CLOSE__".into();
                }
                if cmd.eq_ignore_ascii_case(b"INCR") || cmd.eq_ignore_ascii_case(b"DECR") {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments\r\n".into();
                    }
                    let amount = if cmd.eq_ignore_ascii_case(b"INCR") { 1 } else { -1 };
                    let result = ctx.storage.incr(args[1], amount);
                    return match result {
                        Ok(n) => format!(":{}\r\n", n),
                        Err(e) => format!("-ERR {}\r\n", e),
                    };
                }
            }
            5 => {
                if cmd.eq_ignore_ascii_case(b"EXISTS") {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments for 'EXISTS' command\r\n".into();
                    }
                    let mut count = 0u64;
                    for key in &args[1..] {
                        if ctx.storage.exists(key) {
                            count += 1;
                        }
                    }
                    return format!(":{}\r\n", count);
                }
            }
            6 => {
                if cmd.eq_ignore_ascii_case(b"DBSIZE") {
                    return format!(":{}\r\n", ctx.storage.dbsize());
                }
                if cmd.eq_ignore_ascii_case(b"INCRBY") || cmd.eq_ignore_ascii_case(b"DECRBY") {
                    if args.len() < 3 {
                        return "-ERR wrong number of arguments\r\n".into();
                    }
                    let amount: i64 = std::str::from_utf8(args[2])
                        .map(|s| s.parse().unwrap_or(0))
                        .unwrap_or(0);
                    let amount = if cmd.eq_ignore_ascii_case(b"INCRBY") { amount } else { -amount };
                    let result = ctx.storage.incr(args[1], amount);
                    return match result {
                        Ok(n) => format!(":{}\r\n", n),
                        Err(e) => format!("-ERR {}\r\n", e),
                    };
                }
            }
            7 => {
                if cmd.eq_ignore_ascii_case(b"LPUSH") || cmd.eq_ignore_ascii_case(b"RPUSH") {
                    if args.len() < 3 {
                        return "-ERR wrong number of arguments\r\n".into();
                    }
                    let vals: Vec<Vec<u8>> = args[2..].iter().map(|a| a.to_vec()).collect();
                    let result = if cmd.eq_ignore_ascii_case(b"LPUSH") {
                        ctx.storage.lpush(args[1], vals)
                    } else {
                        ctx.storage.rpush(args[1], vals)
                    };
                    return match result {
                        Ok(n) => format!(":{}\r\n", n),
                        Err(e) => format!("-ERR {}\r\n", e),
                    };
                }
                if cmd.eq_ignore_ascii_case(b"FLUSHALL") || cmd.eq_ignore_ascii_case(b"FLUSHDB") {
                    ctx.storage.flush_all();
                    return "+OK\r\n".into();
                }
            }
            8 => {
                if cmd.eq_ignore_ascii_case(b"LPOP") || cmd.eq_ignore_ascii_case(b"RPOP") {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments\r\n".into();
                    }
                    let result = if cmd.eq_ignore_ascii_case(b"LPOP") {
                        ctx.storage.lpop(args[1])
                    } else {
                        ctx.storage.rpop(args[1])
                    };
                    return match result {
                        Ok(Some(v)) => {
                            let mut resp = String::with_capacity(32 + v.len());
                            resp.push_str(&format!("${}\r\n", v.len()));
                            resp.push_str(&String::from_utf8_lossy(&v));
                            resp.push_str("\r\n");
                            resp
                        }
                        Ok(None) => "$-1\r\n".into(),
                        Err(e) => format!("-ERR {}\r\n", e),
                    };
                }
                if cmd.eq_ignore_ascii_case(b"PUBLISH") {
                    if args.len() < 3 {
                        return "-ERR wrong number of arguments for 'PUBLISH' command\r\n".into();
                    }
                    let count = ctx.broadcaster.publish(args[1], args[2]);
                    return format!(":{}\r\n", count);
                }
            }
            9 => {
                if cmd.eq_ignore_ascii_case(b"HSET") || cmd.eq_ignore_ascii_case(b"HGET") {
                    if cmd.eq_ignore_ascii_case(b"HSET") {
                        if args.len() < 4 {
                            return "-ERR wrong number of arguments for 'HSET' command\r\n".into();
                        }
                        let result = ctx.storage.hset(args[1], args[2].to_vec(), args[3].to_vec());
                        return match result {
                            Ok(_) => ":1\r\n".into(),
                            Err(e) => format!("-ERR {}\r\n", e),
                        };
                    } else {
                        if args.len() < 3 {
                            return "-ERR wrong number of arguments for 'HGET' command\r\n".into();
                        }
                        let result = ctx.storage.hget(args[1], args[2]);
                        return match result {
                            Ok(Some(v)) => {
                                let mut resp = String::with_capacity(32 + v.len());
                                resp.push_str(&format!("${}\r\n", v.len()));
                                resp.push_str(&String::from_utf8_lossy(&v));
                                resp.push_str("\r\n");
                                resp
                            }
                            Ok(None) => "$-1\r\n".into(),
                            Err(e) => format!("-ERR {}\r\n", e),
                        };
                    }
                }
                if cmd.eq_ignore_ascii_case(b"SADD") {
                    if args.len() < 3 {
                        return "-ERR wrong number of arguments for 'SADD' command\r\n".into();
                    }
                    let members: Vec<Vec<u8>> = args[2..].iter().map(|a| a.to_vec()).collect();
                    let result = ctx.storage.sadd(args[1], members);
                    return match result {
                        Ok(n) => format!(":{}\r\n", n),
                        Err(e) => format!("-ERR {}\r\n", e),
                    };
                }
                if cmd.eq_ignore_ascii_case(b"LRANGE") {
                    if args.len() < 4 {
                        return "-ERR wrong number of arguments for 'LRANGE' command\r\n".into();
                    }
                    let start = std::str::from_utf8(args[2]).unwrap_or("0").parse::<i64>().unwrap_or(0);
                    let stop = std::str::from_utf8(args[3]).unwrap_or("-1").parse::<i64>().unwrap_or(-1);
                    return match ctx.storage.lrange(args[1], start, stop) {
                        Ok(items) => {
                            let mut resp = format!("*{}\r\n", items.len());
                            for item in &items {
                                resp.push_str(&format!("${}\r\n", item.len()));
                                resp.push_str(&String::from_utf8_lossy(item));
                                resp.push_str("\r\n");
                            }
                            resp
                        }
                        Err(e) => format!("-ERR {}\r\n", e),
                    };
                }
            }
            10 => {
                if cmd.eq_ignore_ascii_case(b"SUBSCRIBE") {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments for 'SUBSCRIBE' command\r\n".into();
                    }
                    let mut response = String::new();
                    for channel in &args[1..] {
                        let rx = ctx.broadcaster.subscribe_client(ctx.client_id, channel);
                        {
                            let mut subs = ctx.subscribed_channels.write();
                            subs.push(channel.to_vec());
                        }
                        {
                            let mut receivers = ctx.pubsub_receivers.write();
                            receivers.push(rx);
                        }
                        let ch_str = String::from_utf8_lossy(channel);
                        response.push_str(&format!(
                            "*3\r\n$9\r\nsubscribe\r\n${}\r\n{}\r\n:1\r\n",
                            channel.len(),
                            ch_str
                        ));
                    }
                    return response;
                }
                if cmd.eq_ignore_ascii_case(b"HGETALL") {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments for 'HGETALL' command\r\n".into();
                    }
                    let result = ctx.storage.hgetall(args[1]);
                    return match result {
                        Ok(pairs) => {
                            let mut resp = format!("*{}\r\n", pairs.len() * 2);
                            for (k, v) in &pairs {
                                resp.push_str(&format!("${}\r\n", k.len()));
                                resp.push_str(&String::from_utf8_lossy(k));
                                resp.push_str("\r\n");
                                resp.push_str(&format!("${}\r\n", v.len()));
                                resp.push_str(&String::from_utf8_lossy(v));
                                resp.push_str("\r\n");
                            }
                            resp
                        }
                        Err(e) => format!("-ERR {}\r\n", e),
                    };
                }
            }
            11 => {
                if cmd.eq_ignore_ascii_case(b"PSUBSCRIBE") {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments for 'PSUBSCRIBE' command\r\n".into();
                    }
                    let mut response = String::new();
                    for pattern in &args[1..] {
                        let rx = ctx.broadcaster.psubscribe_client(ctx.client_id, pattern);
                        {
                            let mut subs = ctx.subscribed_channels.write();
                            subs.push(pattern.to_vec());
                        }
                        {
                            let mut receivers = ctx.pubsub_receivers.write();
                            receivers.push(rx);
                        }
                        let pat_str = String::from_utf8_lossy(pattern);
                        response.push_str(&format!(
                            "*3\r\n$10\r\npsubscribe\r\n${}\r\n{}\r\n:1\r\n",
                            pattern.len(),
                            pat_str
                        ));
                    }
                    return response;
                }
                if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") || cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") {
                    for channel in &args[1..] {
                        ctx.broadcaster.unsubscribe(ctx.client_id, channel);
                    }
                    return "*0\r\n".into();
                }
            }
            12 => {
                if cmd.eq_ignore_ascii_case(b"SISMEMBER") {
                    if args.len() < 3 {
                        return "-ERR wrong number of arguments\r\n".into();
                    }
                    let result = ctx.storage.sismember(args[1], args[2]);
                    return match result {
                        Ok(true) => ":1\r\n".into(),
                        Ok(false) => ":0\r\n".into(),
                        Err(e) => format!("-ERR {}\r\n", e),
                    };
                }
                if cmd.eq_ignore_ascii_case(b"SMEMBERS") {
                    if args.len() < 2 {
                        return "-ERR wrong number of arguments\r\n".into();
                    }
                    let result = ctx.storage.smembers(args[1]);
                    return match result {
                        Ok(members) => {
                            let mut resp = format!("*{}\r\n", members.len());
                            for m in &members {
                                resp.push_str(&format!("${}\r\n{}", m.len(), String::from_utf8_lossy(m)));
                                resp.push_str("\r\n");
                            }
                            resp
                        }
                        Err(e) => format!("-ERR {}\r\n", e),
                    };
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
        Self::process_command(&args_str, ctx).await
    }

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
                    ctx.storage.set(key.to_vec(), StorageValue::String(val.to_vec()));
                    if let Some(t) = ttl {
                        ctx.storage.expire(key, t);
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
                if cmd.eq_ignore_ascii_case(b"SUBSCRIBE") {
                    if args.len() < 2 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'SUBSCRIBE' command");
                        return;
                    }
                    for channel in &args[1..] {
                        let rx = ctx.broadcaster.subscribe_client(ctx.client_id, channel);
                        {
                            let mut subs = ctx.subscribed_channels.write();
                            subs.push(channel.to_vec());
                        }
                        {
                            let mut receivers = ctx.pubsub_receivers.write();
                            receivers.push(rx);
                        }
                        // Write subscribe response
                        resp_write_array_header(buf, 3);
                        resp_write_bulk_string(buf, b"subscribe");
                        resp_write_bulk_string(buf, channel);
                        resp_write_integer(buf, 1);
                    }
                    return;
                }
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
            11 => {
                if cmd.eq_ignore_ascii_case(b"PSUBSCRIBE") {
                    if args.len() < 2 {
                        resp_write_error(buf, b"ERR wrong number of arguments for 'PSUBSCRIBE' command");
                        return;
                    }
                    for pattern in &args[1..] {
                        let rx = ctx.broadcaster.psubscribe_client(ctx.client_id, pattern);
                        {
                            let mut subs = ctx.subscribed_channels.write();
                            subs.push(pattern.to_vec());
                        }
                        {
                            let mut receivers = ctx.pubsub_receivers.write();
                            receivers.push(rx);
                        }
                        resp_write_array_header(buf, 3);
                        resp_write_bulk_string(buf, b"psubscribe");
                        resp_write_bulk_string(buf, pattern);
                        resp_write_integer(buf, 1);
                    }
                    return;
                }
                if cmd.eq_ignore_ascii_case(b"UNSUBSCRIBE") || cmd.eq_ignore_ascii_case(b"PUNSUBSCRIBE") {
                    for channel in &args[1..] {
                        ctx.broadcaster.unsubscribe(ctx.client_id, channel);
                    }
                    buf.put_slice(RESP_EMPTY_ARRAY);
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

fn find_crlf(buf: &[u8]) -> Option<usize> {
    // Use memchr for SIMD-accelerated \r search
    let data = if buf.len() > 1 { &buf[..buf.len() - 1] } else { return None };
    let mut pos = 0;
    while pos < data.len() {
        if let Some(idx) = memchr::memchr(b'\r', &data[pos..]) {
            let abs = pos + idx;
            if abs + 1 < buf.len() && buf[abs + 1] == b'\n' {
                return Some(abs);
            }
            pos = abs + 1;
        } else {
            break;
        }
    }
    None
}

/// Zero-copy RESP frame parser. Returns byte slices pointing into the original buffer.
/// Returns (consumed_bytes, arg_slices).
fn try_parse_resp_frame_zero_copy<'a>(buf: &'a [u8]) -> Option<(usize, Vec<&'a [u8]>)> {
    if buf.is_empty() || buf[0] != b'*' {
        return None;
    }

    let first_end = find_crlf(buf)?;
    let count_str = std::str::from_utf8(&buf[1..first_end]).ok()?;
    let count: usize = count_str.parse().ok()?;

    let mut pos = first_end + 2;
    let mut args: Vec<&'a [u8]> = Vec::with_capacity(count);

    for _ in 0..count {
        if pos >= buf.len() || buf[pos] != b'$' {
            return None;
        }
        let len_end = find_crlf(&buf[pos..])? + pos;
        let len_str = std::str::from_utf8(&buf[pos + 1..len_end]).ok()?;
        let len: usize = len_str.parse().ok()?;
        pos = len_end + 2;

        if pos + len > buf.len() {
            return None;
        }

        // Zero-copy: just return a slice into the original buffer
        args.push(&buf[pos..pos + len]);
        pos += len + 2;
    }

    Some((pos, args))
}

/// Legacy wrapper for backward compatibility
fn try_parse_resp_frame(buf: &[u8]) -> Option<(usize, Vec<String>)> {
    try_parse_resp_frame_zero_copy(buf).map(|(consumed, slices)| {
        let args: Vec<String> = slices.iter().map(|s| {
            match std::str::from_utf8(s) {
                Ok(s) => s.to_owned(),
                Err(_) => String::from_utf8_lossy(s).into_owned(),
            }
        }).collect();
        (consumed, args)
    })
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
        let (_, args) = try_parse_resp_frame(input).unwrap();
        assert_eq!(args, vec!["SET", "foo", "bar"]);
    }

    #[test]
    fn parse_resp_single_arg() {
        let input = b"*1\r\n$4\r\nPING\r\n";
        let (_, args) = try_parse_resp_frame(input).unwrap();
        assert_eq!(args, vec!["PING"]);
    }

    #[test]
    fn parse_resp_empty_bulk_string() {
        let input = b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n";
        let (_, args) = try_parse_resp_frame(input).unwrap();
        assert_eq!(args, vec!["GET", ""]);
    }

    #[test]
    fn parse_resp_invalid_header() {
        assert!(try_parse_resp_frame(b"$3\r\nSET\r\n").is_none());
    }

    #[test]
    fn parse_resp_wrong_length() {
        // $5\r\nSE\r\n is incomplete - needs 5 bytes but only 2 before \r\n
        assert!(try_parse_resp_frame(b"*1\r\n$5\r\nSE\r\n").is_none());
    }

    #[test]
    fn parse_resp_truncated() {
        assert!(try_parse_resp_frame(b"*3\r\n$3\r\nSET\r\n").is_none());
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
