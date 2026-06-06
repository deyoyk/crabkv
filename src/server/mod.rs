use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::signal;

use crate::config::Config;
use crate::jobs::JobQueue;
use crate::pubsub::PubSubBroadcaster;

pub struct Server {
    config: Config,
    broadcaster: Arc<PubSubBroadcaster>,
    jobs: Arc<JobQueue>,
    running: Arc<AtomicBool>,
    active_connections: Arc<AtomicU64>,
}

#[derive(Clone)]
struct ServerContext {
    config: Config,
    broadcaster: Arc<PubSubBroadcaster>,
    jobs: Arc<JobQueue>,
    client_id: u64,
    subscribed_channels: Arc<parking_lot::RwLock<Vec<Vec<u8>>>>,
}

impl Server {
    pub async fn new(config: Config) -> Self {
        let broadcaster = Arc::new(PubSubBroadcaster::new());
        let jobs = Arc::new(JobQueue::new());

        Self {
            config,
            broadcaster,
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
        log::info!("Listening on {}", addr);

        let running = self.running.clone();
        let broadcaster = self.broadcaster.clone();
        let jobs = self.jobs.clone();
        let config = self.config.clone();
        let active = self.active_connections.clone();

        tokio::spawn(async move {
            signal::ctrl_c().await.ok();
            log::info!("Received shutdown signal, stopping server...");
            running.store(false, Ordering::SeqCst);
        });

        loop {
            if !self.running.load(Ordering::SeqCst) {
                log::info!("Server shutting down gracefully");
                break;
            }

            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            log::debug!("New connection from {}", peer_addr);
                            active.fetch_add(1, Ordering::Relaxed);

                            let ctx = ServerContext {
                                config: config.clone(),
                                broadcaster: broadcaster.clone(),
                                jobs: jobs.clone(),
                                client_id: PubSubBroadcaster::new().next_subscriber_id(),
                                subscribed_channels: Arc::new(parking_lot::RwLock::new(Vec::new())),
                            };

                            let running = running.clone();
                            let active = active.clone();
                            tokio::spawn(async move {
                                Self::handle_connection(stream, ctx, running).await;
                                active.fetch_sub(1, Ordering::Relaxed);
                            });
                        }
                        Err(e) => {
                            log::error!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = signal::ctrl_c() => {
                    log::info!("Received shutdown signal, stopping server...");
                    self.running.store(false, Ordering::SeqCst);
                }
            }
        }

        Ok(())
    }

    async fn handle_connection(
        mut stream: TcpStream,
        ctx: ServerContext,
        running: Arc<AtomicBool>,
    ) {
        let (reader, mut writer) = stream.split();
        let mut buf_reader = BufReader::new(reader);
        let mut line = Vec::new();

        if let Err(e) = writer.write_all(b"+OK\r\n").await {
            log::debug!("Failed to send welcome: {}", e);
            return;
        }

        while running.load(Ordering::SeqCst) {
            line.clear();
            match buf_reader.read_until(b'\n', &mut line).await {
                Ok(0) => {
                    log::debug!("Client disconnected (id={})", ctx.client_id);
                    break;
                }
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    let input = if trimmed.ends_with('\r') {
                        &trimmed[..trimmed.len() - 1]
                    } else {
                        trimmed
                    };

                    let args = if input.starts_with('*') {
                        match parse_resp(input.as_bytes()) {
                            Some(args) => args,
                            None => {
                                let _ = writer.write_all(b"-ERR protocol error\r\n").await;
                                continue;
                            }
                        }
                    } else {
                        parse_inline(input)
                    };

                    if args.is_empty() {
                        continue;
                    }

                    let response = Self::process_command(&args, &ctx).await;

                    if response == "__CLOSE__" {
                        break;
                    }

                    if let Some(channels) = &ctx订阅频道() {
                        if !channels.is_empty() {
                            for msg in response.lines() {
                                let _ = writer
                                    .write_all(format!("+{}\r\n", msg).as_bytes())
                                    .await;
                            }
                            continue;
                        }
                    }

                    if let Err(e) = writer.write_all(response.as_bytes()).await {
                        log::debug!("Failed to write response: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    log::error!("Failed to read from client: {}", e);
                    break;
                }
            }
        }

        ctx.broadcaster.unsubscribe_all(ctx.client_id);
    }

    async fn process_command(args: &[String], ctx: &ServerContext) -> String {
        let cmd = args[0].to_uppercase();

        match cmd.as_str() {
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
            "QUIT" => "__CLOSE__".into(),
            "COMMAND" | "COMMAND DOCS" | "COMMAND COUNT" | "COMMAND INFO" => {
                "+OK\r\n".into()
            }
            "DBSIZE" => {
                ":0\r\n".into()
            }
            "INFO" => {
                let section = args.get(1).map(|s| s.as_str()).unwrap_or("server");
                match section {
                    "server" => {
                        format!(
                            "${}\r\n# Server\r\nredis_version:7.0.0-crabkv\r\ntcp_port:{}\r\n\r\n",
                            0, ctx.config.server.port
                        )
                    }
                    "clients" => {
                        format!(
                            "${}\r\n# Clients\r\nconnected_clients:1\r\n\r\n",
                            0
                        )
                    }
                    "memory" => {
                        let max = ctx.config.parse_max_memory();
                        format!(
                            "${}\r\n# Memory\r\nmax_memory:{}\r\n\r\n",
                            0, max
                        )
                    }
                    _ => "+OK\r\n".into(),
                }
            }
            "SET" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'SET' command\r\n".into();
                }
                let key = args[1].clone();
                let value = args[2].clone();
                log::debug!("SET {} = {}", key, value);
                "+OK\r\n".into()
            }
            "GET" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'GET' command\r\n".into();
                }
                "$-1\r\n".into()
            }
            "DEL" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'DEL' command\r\n".into();
                }
                format!(":{}\r\n", args.len() - 1)
            }
            "EXISTS" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'EXISTS' command\r\n".into();
                }
                format!(":{}\r\n", args.len() - 1)
            }
            "SUBSCRIBE" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'SUBSCRIBE' command\r\n".into();
                }
                let mut response = String::new();
                for channel in &args[1..] {
                    let mut rx = ctx
                        .broadcaster
                        .subscribe_client(ctx.client_id, channel.as_bytes());
                    {
                        let mut subs = ctx.subscribed_channels.write();
                        subs.push(channel.as_bytes().to_vec());
                    }

                    response.push_str(&format!(
                        "*3\r\n$9\r\nsubscribe\r\n${}\r\n{}\r\n:1\r\n",
                        channel.len(),
                        channel
                    ));

                    tokio::spawn(async move {
                        loop {
                            match rx.recv().await {
                                Ok(_msg) => {}
                                Err(broadcast::error::RecvError::Closed) => break,
                                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            }
                        }
                    });
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
                let channel = &args[1];
                let message = &args[2];
                let count = ctx
                    .broadcaster
                    .publish(channel.as_bytes(), message.as_bytes());
                format!(":{}\r\n", count)
            }
            "PSUBSCRIBE" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'PSUBSCRIBE' command\r\n".into();
                }
                let mut response = String::new();
                for pattern in &args[1..] {
                    let mut rx = ctx
                        .broadcaster
                        .psubscribe_client(ctx.client_id, pattern.as_bytes());
                    {
                        let mut subs = ctx.subscribed_channels.write();
                        subs.push(pattern.as_bytes().to_vec());
                    }

                    response.push_str(&format!(
                        "*3\r\n$10\r\npsubscribe\r\n${}\r\n{}\r\n:1\r\n",
                        pattern.len(),
                        pattern
                    ));

                    tokio::spawn(async move {
                        loop {
                            match rx.recv().await {
                                Ok(_msg) => {}
                                Err(broadcast::error::RecvError::Closed) => break,
                                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            }
                        }
                    });
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
            "LPUSH" | "RPUSH" => {
                if args.len() < 3 {
                    return format!(
                        "-ERR wrong number of arguments for '{}' command\r\n",
                        cmd
                    );
                }
                format!(":{}\r\n", args.len() - 2)
            }
            "LPOP" | "RPOP" => {
                "$-1\r\n".into()
            }
            "LLEN" => {
                ":0\r\n".into()
            }
            "LRANGE" => {
                "*0\r\n".into()
            }
            "HSET" => {
                if args.len() < 4 {
                    return "-ERR wrong number of arguments for 'HSET' command\r\n".into();
                }
                ":1\r\n".into()
            }
            "HGET" => {
                "$-1\r\n".into()
            }
            "HGETALL" => {
                "*0\r\n".into()
            }
            "HDEL" => {
                ":0\r\n".into()
            }
            "SADD" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'SADD' command\r\n".into();
                }
                format!(":{}\r\n", args.len() - 2)
            }
            "SMEMBERS" => {
                "*0\r\n".into()
            }
            "SREM" => {
                ":0\r\n".into()
            }
            "ZADD" => {
                if args.len() < 4 {
                    return "-ERR wrong number of arguments for 'ZADD' command\r\n".into();
                }
                ":1\r\n".into()
            }
            "ZRANGE" | "ZRANGEBYSCORE" => {
                "*0\r\n".into()
            }
            "ZSCORE" => {
                "$-1\r\n".into()
            }
            "INCR" | "DECR" => {
                if args.len() < 2 {
                    return format!(
                        "-ERR wrong number of arguments for '{}' command\r\n",
                        cmd
                    );
                }
                ":1\r\n".into()
            }
            "INCRBY" | "DECRBY" => {
                if args.len() < 3 {
                    return format!(
                        "-ERR wrong number of arguments for '{}' command\r\n",
                        cmd
                    );
                }
                ":1\r\n".into()
            }
            "EXPIRE" | "TTL" | "PTTL" | "PERSIST" => {
                if args.len() < 2 {
                    return format!(
                        "-ERR wrong number of arguments for '{}' command\r\n",
                        cmd
                    );
                }
                ":1\r\n".into()
            }
            "KEYS" => {
                "*0\r\n".into()
            }
            "TYPE" => {
                "+none\r\n".into()
            }
            "FLUSHDB" | "FLUSHALL" => {
                "+OK\r\n".into()
            }
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
                let secs = now.as_secs();
                let micros = now.subsec_micros();
                format!(
                    "*2\r\n:{}\r\n:{}\r\n",
                    secs, micros
                )
            }
            "CLIENT" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'CLIENT' command\r\n".into();
                }
                match args[1].to_uppercase().as_str() {
                    "ID" => format!(":{}\r\n", ctx.client_id),
                    "LIST" => "+OK\r\n".into(),
                    "SETNAME" => "+OK\r\n".into(),
                    "GETNAME" => "$-1\r\n".into(),
                    _ => "-ERR unknown CLIENT subcommand\r\n".into(),
                }
            }
            "CONFIG" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'CONFIG' command\r\n".into();
                }
                match args[1].to_uppercase().as_str() {
                    "GET" => "*0\r\n".into(),
                    "SET" => "+OK\r\n".into(),
                    _ => "-ERR unknown CONFIG subcommand\r\n".into(),
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
                    "INFO" => "+cluster_enabled:1\r\n".into(),
                    "NODES" => "+OK\r\n".into(),
                    _ => "-ERR unknown CLUSTER subcommand\r\n".into(),
                }
            }
            "XADD" => {
                if args.len() < 4 {
                    return "-ERR wrong number of arguments for 'XADD' command\r\n".into();
                }
                format!("$37\r\n1-00000000000000000000000000000000000\r\n")
            }
            "XLEN" => {
                ":0\r\n".into()
            }
            "XRANGE" | "XREVRANGE" => {
                "*0\r\n".into()
            }
            "XREAD" => {
                "*0\r\n".into()
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
                let queue = &args[1];
                let payload = args[2].as_bytes().to_vec();
                let priority: i32 = args[3].parse().unwrap_or(0);
                let id = ctx.jobs.add_job(queue, payload, priority, None);
                format!("+{}\r\n", id)
            }
            "JOBPOP" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'JOBPOP' command\r\n".into();
                }
                match ctx.jobs.pop_job(&args[1]) {
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
                let success = ctx.jobs.ack_job(&args[1], &args[2]);
                if success {
                    "+OK\r\n".into()
                } else {
                    "-ERR job not found\r\n".into()
                }
            }
            "JOBFAIL" => {
                if args.len() < 4 {
                    return "-ERR wrong number of arguments for 'JOBFAIL' command\r\n".into();
                }
                let success = ctx.jobs.fail_job(&args[1], &args[2], &args[3]);
                if success {
                    "+OK\r\n".into()
                } else {
                    "-ERR job not found\r\n".into()
                }
            }
            "JOBRETRY" => {
                if args.len() < 3 {
                    return "-ERR wrong number of arguments for 'JOBRETRY' command\r\n".into();
                }
                let success = ctx.jobs.retry_job(&args[1], &args[2]);
                if success {
                    "+OK\r\n".into()
                } else {
                    "-ERR job not found\r\n".into()
                }
            }
            "JOBSTATS" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'JOBSTATS' command\r\n".into();
                }
                let (pending, processing, dead) = ctx.jobs.queue_stats(&args[1]);
                format!(
                    "*3\r\n:{}\r\n:{}\r\n:{}\r\n",
                    pending, processing, dead
                )
            }
            "JOBDEAD" => {
                if args.len() < 2 {
                    return "-ERR wrong number of arguments for 'JOBDEAD' command\r\n".into();
                }
                let letters = ctx.jobs.get_dead_letters(&args[1]);
                if letters.is_empty() {
                    "*0\r\n".into()
                } else {
                    let mut response = format!(":{}\r\n", letters.len());
                    for entry in &letters {
                        response.push_str(&format!(
                            "${}\r\n",
                            entry.job.id.len() + entry.reason.len() + 1
                        ));
                    }
                    response
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
                format!(
                    "-ERR unknown command '{}'\r\n",
                    args[0]
                )
            }
        }
    }
}

impl ServerContext {
    fn 订阅频道(&self) -> Option<Vec<Vec<u8>>> {
        Some(self.subscribed_channels.read().clone())
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

fn parse_resp(data: &[u8]) -> Option<Vec<String>> {
    let text = std::str::from_utf8(data).ok()?;
    let mut args = Vec::new();
    let mut lines: Vec<&str> = text.lines().collect();

    if lines.is_empty() {
        return None;
    }

    let mut i = 0;
    let first_line = lines[i].trim();
    if !first_line.starts_with('*') {
        return None;
    }

    let count: usize = first_line[1..].parse().ok()?;
    i += 1;

    for _ in 0..count {
        if i >= lines.len() {
            return None;
        }

        let len_line = lines[i].trim();
        if !len_line.starts_with('$') {
            return None;
        }

        let len: usize = len_line[1..].parse().ok()?;
        i += 1;

        if i >= lines.len() {
            return None;
        }

        let value = lines[i];
        if value.len() < len {
            return None;
        }

        args.push(value[..len].to_string());
        i += 1;
    }

    Some(args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_inline_simple() {
        let args = parse_inline("PING");
        assert_eq!(args, vec!["PING"]);
    }

    #[test]
    fn parse_inline_multiple_args() {
        let args = parse_inline("SET foo bar");
        assert_eq!(args, vec!["SET", "foo", "bar"]);
    }

    #[test]
    fn parse_inline_with_quotes() {
        let args = parse_inline("SET \"hello world\" baz");
        assert_eq!(args, vec!["SET", "hello world", "baz"]);
    }

    #[test]
    fn parse_inline_single_quotes() {
        let args = parse_inline("SET 'hello world' baz");
        assert_eq!(args, vec!["SET", "hello world", "baz"]);
    }

    #[test]
    fn parse_inline_empty() {
        let args = parse_inline("");
        assert!(args.is_empty());
    }

    #[test]
    fn parse_inline_extra_spaces() {
        let args = parse_inline("  SET   foo   bar  ");
        assert_eq!(args, vec!["SET", "foo", "bar"]);
    }

    #[test]
    fn parse_resp_simple() {
        let input = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let args = parse_resp(input).unwrap();
        assert_eq!(args, vec!["SET", "foo", "bar"]);
    }

    #[test]
    fn parse_resp_single_arg() {
        let input = b"*1\r\n$4\r\nPING\r\n";
        let args = parse_resp(input).unwrap();
        assert_eq!(args, vec!["PING"]);
    }

    #[test]
    fn parse_resp_empty_bulk_string() {
        let input = b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n";
        let args = parse_resp(input).unwrap();
        assert_eq!(args, vec!["GET", ""]);
    }

    #[test]
    fn parse_resp_invalid_header() {
        let input = b"$3\r\nSET\r\n";
        assert!(parse_resp(input).is_none());
    }

    #[test]
    fn parse_resp_wrong_length() {
        let input = b"*1\r\n$5\r\nSET\r\n";
        assert!(parse_resp(input).is_none());
    }

    #[tokio::test]
    async fn ping_command() {
        let ctx = create_test_context();
        let args = vec!["PING".to_string()];
        let response = Server::process_command(&args, &ctx).await;
        assert_eq!(response, "+PONG\r\n");
    }

    #[tokio::test]
    async fn ping_with_arg() {
        let ctx = create_test_context();
        let args = vec!["PING".to_string(), "hello".to_string()];
        let response = Server::process_command(&args, &ctx).await;
        assert_eq!(response, "+hello\r\n");
    }

    #[tokio::test]
    async fn echo_command() {
        let ctx = create_test_context();
        let args = vec!["ECHO".to_string(), "foobar".to_string()];
        let response = Server::process_command(&args, &ctx).await;
        assert_eq!(response, "$6\r\nfoobar\r\n");
    }

    #[tokio::test]
    async fn set_command() {
        let ctx = create_test_context();
        let args = vec!["SET".to_string(), "key".to_string(), "value".to_string()];
        let response = Server::process_command(&args, &ctx).await;
        assert_eq!(response, "+OK\r\n");
    }

    #[tokio::test]
    async fn get_returns_nil() {
        let ctx = create_test_context();
        let args = vec!["GET".to_string(), "key".to_string()];
        let response = Server::process_command(&args, &ctx).await;
        assert_eq!(response, "$-1\r\n");
    }

    #[tokio::test]
    async fn unknown_command() {
        let ctx = create_test_context();
        let args = vec!["FOOBAR".to_string()];
        let response = Server::process_command(&args, &ctx).await;
        assert!(response.starts_with("-ERR"));
    }

    #[tokio::test]
    async fn quit_command() {
        let ctx = create_test_context();
        let args = vec!["QUIT".to_string()];
        let response = Server::process_command(&args, &ctx).await;
        assert_eq!(response, "__CLOSE__");
    }

    fn create_test_context() -> ServerContext {
        ServerContext {
            config: Config::default(),
            broadcaster: Arc::new(PubSubBroadcaster::new()),
            jobs: Arc::new(JobQueue::new()),
            client_id: 1,
            subscribed_channels: Arc::new(parking_lot::RwLock::new(Vec::new())),
        }
    }
}
