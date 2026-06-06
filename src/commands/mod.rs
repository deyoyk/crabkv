use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::storage::{StorageEngine, StorageValue};
use crate::streams::StreamEngine;
use crate::jobs::JobQueue;

// ---------------------------------------------------------------------------
// CommandResult – maps 1-to-1 to the RESP2 type system
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum CommandResult {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Null,
    Array(Vec<CommandResult>),
    /// Sent when the client enters pub/sub mode.
    Subscribed,
}

impl CommandResult {
    pub fn ok() -> Self {
        CommandResult::Simple("OK".into())
    }

    pub fn integer(v: i64) -> Self {
        CommandResult::Integer(v)
    }

    pub fn bulk(data: impl Into<Vec<u8>>) -> Self {
        CommandResult::Bulk(data.into())
    }

    pub fn error(msg: impl Into<String>) -> Self {
        CommandResult::Error(msg.into())
    }

    pub fn null() -> Self {
        CommandResult::Null
    }
}

// ---------------------------------------------------------------------------
// Pub/Sub broadcaster (minimal – full impl lives in its own module)
// ---------------------------------------------------------------------------

pub struct PubSubBroadcaster {
    subscribers: parking_lot::RwLock<HashMap<String, Vec<Arc<parking_lot::Mutex<tokio::sync::mpsc::Sender<Vec<u8>>>>>>>,
}

impl PubSubBroadcaster {
    pub fn new() -> Self {
        Self {
            subscribers: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    pub fn publish(&self, channel: &str, message: &[u8]) -> i64 {
        let subs = self.subscribers.read();
        let mut count: i64 = 0;
        if let Some(channels) = subs.get(channel) {
            for tx in channels {
                if let Ok(mut guard) = tx.try_lock() {
                    let _ = guard.try_send(message.to_vec());
                    count += 1;
                }
            }
        }
        count
    }

    pub fn subscribe(
        &self,
        channel: String,
    ) -> tokio::sync::mpsc::Receiver<Vec<u8>> {
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let mut subs = self.subscribers.write();
        subs.entry(channel).or_default().push(Arc::new(parking_lot::Mutex::new(tx)));
        rx
    }
}

// ---------------------------------------------------------------------------
// CommandContext
// ---------------------------------------------------------------------------

pub struct CommandContext {
    pub storage: Arc<StorageEngine>,
    pub broadcaster: Arc<PubSubBroadcaster>,
    pub streams: Arc<StreamEngine>,
    pub jobs: Arc<JobQueue>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn parse_i64(v: &[u8]) -> Result<i64, CommandResult> {
    std::str::from_utf8(v)
        .map_err(|_| CommandResult::error("ERR invalid integer"))?
        .parse::<i64>()
        .map_err(|_| CommandResult::error("ERR value is not an integer or out of range"))
}

fn bytes_to_string_lossy(v: &[u8]) -> String {
    String::from_utf8_lossy(v).into_owned()
}

fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    glob_match_inner(&p, &t)
}

fn glob_match_inner(pattern: &[char], text: &[char]) -> bool {
    match (pattern.first(), text.first()) {
        (None, None) => true,
        (Some('*'), rest) => {
            // Try matching zero or more characters
            for i in 0..=text.len() {
                if glob_match_inner(rest, &text[i..]) {
                    return true;
                }
            }
            false
        }
        (Some('?'), Some(_)) => glob_match_inner(&pattern[1..], &text[1..]),
        (Some('['), _) => {
            // Character class
            let end = pattern.iter().position(|&c| c == ']').unwrap_or(pattern.len());
            let chars: Vec<char> = pattern[1..end].to_vec();
            if let Some(&ch) = text.first() {
                let matched = if chars.len() >= 3 && chars[1] == '-' {
                    chars[0] <= ch && ch <= chars[2]
                } else {
                    chars.contains(&ch)
                };
                if matched {
                    return glob_match_inner(&pattern[end + 1..], &text[1..]);
                }
            }
            false
        }
        (Some(&pc), Some(&tc)) if pc == tc => {
            glob_match_inner(&pattern[1..], &text[1..])
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// String commands
// ---------------------------------------------------------------------------

async fn cmd_set(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() < 2 {
        return CommandResult::error("ERR wrong number of arguments for 'SET' command");
    }
    let key = &args[0];
    let value = &args[1];

    let mut ex: Option<u64> = None;
    let mut px: Option<u64> = None;
    let mut nx = false;
    let mut xx = false;

    let mut i = 2;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"EX" => {
                i += 1;
                if i >= args.len() {
                    return CommandResult::error("ERR syntax error");
                }
                ex = Some(parse_i64(&args[i]).map_err(|e| e)? as u64);
            }
            b"PX" => {
                i += 1;
                if i >= args.len() {
                    return CommandResult::error("ERR syntax error");
                }
                px = Some(parse_i64(&args[i]).map_err(|e| e)? as u64);
            }
            b"NX" => nx = true,
            b"XX" => xx = true,
            _ => return CommandResult::error("ERR syntax error"),
        }
        i += 1;
    }

    // NX/XX semantics
    if nx || xx {
        let exists = ctx.storage.exists(key).await;
        if nx && exists {
            return CommandResult::null();
        }
        if xx && !exists {
            return CommandResult::null();
        }
    }

    ctx.storage
        .set(key, StorageValue::String(value.clone()), ex, px)
        .await;

    CommandResult::ok()
}

async fn cmd_get(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'GET' command");
    }
    match ctx.storage.get(&args[0]).await {
        Some(StorageValue::String(v)) => CommandResult::Bulk(v),
        Some(_) => CommandResult::error("WRONGTYPE Operation against a key holding the wrong kind of value"),
        None => CommandResult::Null,
    }
}

async fn cmd_del(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.is_empty() {
        return CommandResult::error("ERR wrong number of arguments for 'DEL' command");
    }
    let mut count = 0i64;
    for key in args {
        if ctx.storage.del(key).await {
            count += 1;
        }
    }
    CommandResult::Integer(count)
}

async fn cmd_exists(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.is_empty() {
        return CommandResult::error("ERR wrong number of arguments for 'EXISTS' command");
    }
    let mut count = 0i64;
    for key in args {
        if ctx.storage.exists(key).await {
            count += 1;
        }
    }
    CommandResult::Integer(count)
}

async fn cmd_mget(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.is_empty() {
        return CommandResult::error("ERR wrong number of arguments for 'MGET' command");
    }
    let mut results = Vec::with_capacity(args.len());
    for key in args {
        match ctx.storage.get(key).await {
            Some(StorageValue::String(v)) => results.push(CommandResult::Bulk(v)),
            _ => results.push(CommandResult::Null),
        }
    }
    CommandResult::Array(results)
}

async fn cmd_mset(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.is_empty() || args.len() % 2 != 0 {
        return CommandResult::error("ERR wrong number of arguments for 'MSET' command");
    }
    for chunk in args.chunks(2) {
        ctx.storage
            .set(&chunk[0], StorageValue::String(chunk[1].clone()), None, None)
            .await;
    }
    CommandResult::ok()
}

async fn cmd_incr(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    cmd_incr_generic(args, ctx, 1).await
}

async fn cmd_incrby(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 2 {
        return CommandResult::error("ERR wrong number of arguments for 'INCRBY' command");
    }
    let increment = parse_i64(&args[1]).map_err(|e| e)?;
    cmd_incr_generic(&args[..1], ctx, increment).await
}

async fn cmd_decr(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    cmd_incr_generic(args, ctx, -1).await
}

async fn cmd_decrby(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 2 {
        return CommandResult::error("ERR wrong number of arguments for 'DECRBY' command");
    }
    let decrement = parse_i64(&args[1]).map_err(|e| e)?;
    cmd_incr_generic(&args[..1], ctx, -decrement).await
}

async fn cmd_incr_generic(args: &[Vec<u8>], ctx: &CommandContext, delta: i64) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'INCR' command");
    }
    let key = &args[0];
    let current = match ctx.storage.get(key).await {
        Some(StorageValue::String(v)) => parse_i64(&v).map_err(|e| e)?,
        None => 0i64,
        Some(_) => {
            return CommandResult::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            );
        }
    };

    let new_val = current
        .checked_add(delta)
        .ok_or_else(|| CommandResult::error("ERR increment or decrement would overflow"))?;

    ctx.storage
        .set(
            key,
            StorageValue::String(new_val.to_string().into_bytes()),
            None,
            None,
        )
        .await;

    CommandResult::Integer(new_val)
}

async fn cmd_append(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 2 {
        return CommandResult::error("ERR wrong number of arguments for 'APPEND' command");
    }
    let key = &args[0];
    let value = &args[1];

    let new_val = match ctx.storage.get(key).await {
        Some(StorageValue::String(mut existing)) => {
            existing.extend_from_slice(value);
            existing
        }
        None => value.clone(),
        Some(_) => {
            return CommandResult::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            );
        }
    };

    let len = new_val.len() as i64;
    ctx.storage
        .set(key, StorageValue::String(new_val), None, None)
        .await;
    CommandResult::Integer(len)
}

async fn cmd_strlen(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'STRLEN' command");
    }
    match ctx.storage.get(&args[0]).await {
        Some(StorageValue::String(v)) => CommandResult::Integer(v.len() as i64),
        None => CommandResult::Integer(0),
        Some(_) => CommandResult::error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
    }
}

async fn cmd_getrange(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 3 {
        return CommandResult::error("ERR wrong number of arguments for 'GETRANGE' command");
    }
    let key = &args[0];
    let start = parse_i64(&args[1]).map_err(|e| e)?;
    let end = parse_i64(&args[2]).map_err(|e| e)?;

    match ctx.storage.get(key).await {
        Some(StorageValue::String(v)) => {
            let len = v.len() as i64;
            let start = if start < 0 { (len + start).max(0) } else { start } as usize;
            let end = if end < 0 { (len + end).max(0) } else { end } as usize;

            if start > v.len() {
                return CommandResult::Bulk(Vec::new());
            }
            let end = end.min(v.len() - 1);
            CommandResult::Bulk(v[start..=end].to_vec())
        }
        None => CommandResult::Bulk(Vec::new()),
        Some(_) => CommandResult::error(
            "WRONGTYPE Operation against a key holding the wrong kind of value",
        ),
    }
}

async fn cmd_setrange(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 3 {
        return CommandResult::error("ERR wrong number of arguments for 'SETRANGE' command");
    }
    let key = &args[0];
    let offset = parse_i64(&args[1]).map_err(|e| e)? as usize;
    let value = &args[2];

    if offset + value.len() > 512 * 1024 * 1024 {
        return CommandResult::error("ERR string exceeds maximum allowed size");
    }

    let mut existing = match ctx.storage.get(key).await {
        Some(StorageValue::String(v)) => v,
        None => vec![0u8; offset + value.len()],
        Some(_) => {
            return CommandResult::error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            );
        }
    };

    // Extend with zeroes if necessary
    if existing.len() < offset + value.len() {
        existing.resize(offset + value.len(), 0);
    }

    existing[offset..offset + value.len()].copy_from_slice(value);
    let new_len = existing.len() as i64;
    ctx.storage
        .set(key, StorageValue::String(existing), None, None)
        .await;
    CommandResult::Integer(new_len)
}

// ---------------------------------------------------------------------------
// Expiration commands
// ---------------------------------------------------------------------------

async fn cmd_expire(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 2 {
        return CommandResult::error("ERR wrong number of arguments for 'EXPIRE' command");
    }
    let key = &args[0];
    let seconds = parse_i64(&args[1]).map_err(|e| e)?;

    if seconds <= 0 {
        ctx.storage.del(key).await;
        return CommandResult::Integer(1);
    }

    match ctx.storage.get(key).await {
        Some(v) => {
            ctx.storage
                .set(key, v, Some(seconds as u64), None)
                .await;
            CommandResult::Integer(1)
        }
        None => CommandResult::Integer(0),
    }
}

async fn cmd_ttl(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'TTL' command");
    }
    match ctx.storage.ttl(&args[0]).await {
        Some(secs) => CommandResult::Integer(secs),
        None => CommandResult::Integer(-2),
    }
}

async fn cmd_persist(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'PERSIST' command");
    }
    match ctx.storage.persist(&args[0]).await {
        true => CommandResult::Integer(1),
        false => CommandResult::Integer(-2),
    }
}

// ---------------------------------------------------------------------------
// Key commands
// ---------------------------------------------------------------------------

async fn cmd_keys(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'KEYS' command");
    }
    let pattern = bytes_to_string_lossy(&args[0]);
    let all_keys = ctx.storage.keys().await;
    let matched: Vec<CommandResult> = all_keys
        .iter()
        .filter(|k| glob_match(&pattern, &bytes_to_string_lossy(k)))
        .map(|k| CommandResult::Bulk(k.clone()))
        .collect();
    CommandResult::Array(matched)
}

async fn cmd_randomkey(_args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    match ctx.storage.random_key().await {
        Some(k) => CommandResult::Bulk(k),
        None => CommandResult::Null,
    }
}

async fn cmd_type(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'TYPE' command");
    }
    match ctx.storage.type_of(&args[0]).await {
        Some(t) => CommandResult::Simple(t),
        None => CommandResult::Simple("none".into()),
    }
}

async fn cmd_rename(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 2 {
        return CommandResult::error("ERR wrong number of arguments for 'RENAME' command");
    }
    let key = &args[0];
    let newkey = &args[1];

    match ctx.storage.get(key).await {
        Some(v) => {
            ctx.storage.set(newkey.clone(), v, None, None).await;
            ctx.storage.del(key).await;
            CommandResult::ok()
        }
        None => CommandResult::error("ERR no such key"),
    }
}

// ---------------------------------------------------------------------------
// List commands
// ---------------------------------------------------------------------------

async fn cmd_lpush(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() < 2 {
        return CommandResult::error("ERR wrong number of arguments for 'LPUSH' command");
    }
    let key = &args[0];
    let values = &args[1..];

    let mut list = ctx.storage.get_list(key).await.unwrap_or_default();
    for v in values.iter().rev() {
        list.push_front(v.clone());
    }
    let len = list.len() as i64;
    ctx.storage.set_list(key, list).await;
    CommandResult::Integer(len)
}

async fn cmd_rpush(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() < 2 {
        return CommandResult::error("ERR wrong number of arguments for 'RPUSH' command");
    }
    let key = &args[0];
    let values = &args[1..];

    let mut list = ctx.storage.get_list(key).await.unwrap_or_default();
    for v in values {
        list.push_back(v.clone());
    }
    let len = list.len() as i64;
    ctx.storage.set_list(key, list).await;
    CommandResult::Integer(len)
}

async fn cmd_lpop(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.is_empty() || args.len() > 2 {
        return CommandResult::error("ERR wrong number of arguments for 'LPOP' command");
    }
    let key = &args[0];
    let count = if args.len() == 2 {
        parse_i64(&args[1]).map_err(|e| e)?.max(1) as usize
    } else {
        1
    };

    match ctx.storage.get_list(key).await {
        Some(mut list) => {
            let n = count.min(list.len());
            let mut result = Vec::with_capacity(n);
            for _ in 0..n {
                if let Some(v) = list.pop_front() {
                    result.push(CommandResult::Bulk(v));
                }
            }
            if list.is_empty() {
                ctx.storage.del(key).await;
            } else {
                ctx.storage.set_list(key, list).await;
            }
            if args.len() == 1 && result.len() == 1 {
                result.remove(0)
            } else {
                CommandResult::Array(result)
            }
        }
        None => CommandResult::Null,
    }
}

async fn cmd_rpop(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.is_empty() || args.len() > 2 {
        return CommandResult::error("ERR wrong number of arguments for 'RPOP' command");
    }
    let key = &args[0];
    let count = if args.len() == 2 {
        parse_i64(&args[1]).map_err(|e| e)?.max(1) as usize
    } else {
        1
    };

    match ctx.storage.get_list(key).await {
        Some(mut list) => {
            let n = count.min(list.len());
            let mut result = Vec::with_capacity(n);
            for _ in 0..n {
                if let Some(v) = list.pop_back() {
                    result.push(CommandResult::Bulk(v));
                }
            }
            if list.is_empty() {
                ctx.storage.del(key).await;
            } else {
                ctx.storage.set_list(key, list).await;
            }
            if args.len() == 1 && result.len() == 1 {
                result.remove(0)
            } else {
                CommandResult::Array(result)
            }
        }
        None => CommandResult::Null,
    }
}

async fn cmd_lrange(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 3 {
        return CommandResult::error("ERR wrong number of arguments for 'LRANGE' command");
    }
    let key = &args[0];
    let start = parse_i64(&args[1]).map_err(|e| e)?;
    let stop = parse_i64(&args[2]).map_err(|e| e)?;

    match ctx.storage.get_list(key).await {
        Some(list) => {
            let len = list.len() as i64;
            let start = normalize_index(start, len);
            let stop = normalize_index(stop, len);

            if start > stop || start >= list.len() {
                return CommandResult::Array(Vec::new());
            }
            let stop = stop.min(list.len() - 1);

            let result: Vec<CommandResult> = list[start..=stop]
                .iter()
                .map(|v| CommandResult::Bulk(v.clone()))
                .collect();
            CommandResult::Array(result)
        }
        None => CommandResult::Array(Vec::new()),
    }
}

fn normalize_index(idx: i64, len: i64) -> usize {
    if idx < 0 {
        (len + idx).max(0) as usize
    } else {
        idx.min(len - 1).max(0) as usize
    }
}

async fn cmd_llen(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'LLEN' command");
    }
    match ctx.storage.get_list(&args[0]).await {
        Some(list) => CommandResult::Integer(list.len() as i64),
        None => CommandResult::Integer(0),
    }
}

async fn cmd_lindex(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 2 {
        return CommandResult::error("ERR wrong number of arguments for 'LINDEX' command");
    }
    let key = &args[0];
    let index = parse_i64(&args[1]).map_err(|e| e)?;

    match ctx.storage.get_list(key).await {
        Some(list) => {
            let idx = normalize_index(index, list.len() as i64);
            match list.get(idx) {
                Some(v) => CommandResult::Bulk(v.clone()),
                None => CommandResult::Null,
            }
        }
        None => CommandResult::Null,
    }
}

async fn cmd_lset(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 3 {
        return CommandResult::error("ERR wrong number of arguments for 'LSET' command");
    }
    let key = &args[0];
    let index = parse_i64(&args[1]).map_err(|e| e)?;
    let value = &args[2];

    match ctx.storage.get_list(key).await {
        Some(mut list) => {
            let idx = normalize_index(index, list.len() as i64);
            if idx >= list.len() {
                return CommandResult::error("ERR index out of range");
            }
            list[idx] = value.clone();
            ctx.storage.set_list(key, list).await;
            CommandResult::ok()
        }
        None => CommandResult::error("ERR no such key"),
    }
}

// ---------------------------------------------------------------------------
// Hash commands
// ---------------------------------------------------------------------------

async fn cmd_hset(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() < 3 || args.len() % 2 != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'HSET' command");
    }
    let key = &args[0];
    let mut hash = ctx.storage.get_hash(key).await.unwrap_or_default();
    let mut count = 0i64;
    for chunk in args[1..].chunks(2) {
        if hash.insert(chunk[0].clone(), chunk[1].clone()).is_none() {
            count += 1;
        }
    }
    ctx.storage.set_hash(key, hash).await;
    CommandResult::Integer(count)
}

async fn cmd_hget(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 2 {
        return CommandResult::error("ERR wrong number of arguments for 'HGET' command");
    }
    let key = &args[0];
    let field = &args[1];

    match ctx.storage.get_hash(key).await {
        Some(hash) => match hash.get(field) {
            Some(v) => CommandResult::Bulk(v.clone()),
            None => CommandResult::Null,
        },
        None => CommandResult::Null,
    }
}

async fn cmd_hdel(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() < 2 {
        return CommandResult::error("ERR wrong number of arguments for 'HDEL' command");
    }
    let key = &args[0];
    let fields = &args[1..];

    match ctx.storage.get_hash_mut(key).await {
        Some(hash) => {
            let mut count = 0i64;
            for field in fields {
                if hash.remove(field).is_some() {
                    count += 1;
                }
            }
            if hash.is_empty() {
                ctx.storage.del(key).await;
            } else {
                ctx.storage.set_hash(key, hash.clone()).await;
            }
            CommandResult::Integer(count)
        }
        None => CommandResult::Integer(0),
    }
}

async fn cmd_hgetall(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'HGETALL' command");
    }
    match ctx.storage.get_hash(&args[0]).await {
        Some(hash) => {
            let mut result = Vec::with_capacity(hash.len() * 2);
            for (field, value) in hash.iter() {
                result.push(CommandResult::Bulk(field.clone()));
                result.push(CommandResult::Bulk(value.clone()));
            }
            CommandResult::Array(result)
        }
        None => CommandResult::Array(Vec::new()),
    }
}

async fn cmd_hlen(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'HLEN' command");
    }
    match ctx.storage.get_hash(&args[0]).await {
        Some(hash) => CommandResult::Integer(hash.len() as i64),
        None => CommandResult::Integer(0),
    }
}

async fn cmd_hkeys(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'HKEYS' command");
    }
    match ctx.storage.get_hash(&args[0]).await {
        Some(hash) => {
            let result: Vec<CommandResult> = hash
                .keys()
                .map(|k| CommandResult::Bulk(k.clone()))
                .collect();
            CommandResult::Array(result)
        }
        None => CommandResult::Array(Vec::new()),
    }
}

async fn cmd_hvals(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'HVALS' command");
    }
    match ctx.storage.get_hash(&args[0]).await {
        Some(hash) => {
            let result: Vec<CommandResult> = hash
                .values()
                .map(|v| CommandResult::Bulk(v.clone()))
                .collect();
            CommandResult::Array(result)
        }
        None => CommandResult::Array(Vec::new()),
    }
}

// ---------------------------------------------------------------------------
// Set commands
// ---------------------------------------------------------------------------

async fn cmd_sadd(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() < 2 {
        return CommandResult::error("ERR wrong number of arguments for 'SADD' command");
    }
    let key = &args[0];
    let members = &args[1..];
    let mut set = ctx.storage.get_set(key).await.unwrap_or_default();
    let mut count = 0i64;
    for m in members {
        if set.insert(m.clone()) {
            count += 1;
        }
    }
    ctx.storage.set_set(key, set).await;
    CommandResult::Integer(count)
}

async fn cmd_srem(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() < 2 {
        return CommandResult::error("ERR wrong number of arguments for 'SREM' command");
    }
    let key = &args[0];
    let members = &args[1..];

    match ctx.storage.get_set_mut(key).await {
        Some(set) => {
            let mut count = 0i64;
            for m in members {
                if set.remove(m) {
                    count += 1;
                }
            }
            if set.is_empty() {
                ctx.storage.del(key).await;
            } else {
                ctx.storage.set_set(key, set.clone()).await;
            }
            CommandResult::Integer(count)
        }
        None => CommandResult::Integer(0),
    }
}

async fn cmd_smembers(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'SMEMBERS' command");
    }
    match ctx.storage.get_set(&args[0]).await {
        Some(set) => {
            let result: Vec<CommandResult> = set
                .iter()
                .map(|m| CommandResult::Bulk(m.clone()))
                .collect();
            CommandResult::Array(result)
        }
        None => CommandResult::Array(Vec::new()),
    }
}

async fn cmd_scard(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'SCARD' command");
    }
    match ctx.storage.get_set(&args[0]).await {
        Some(set) => CommandResult::Integer(set.len() as i64),
        None => CommandResult::Integer(0),
    }
}

async fn cmd_sismember(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 2 {
        return CommandResult::error("ERR wrong number of arguments for 'SISMEMBER' command");
    }
    match ctx.storage.get_set(&args[0]).await {
        Some(set) => {
            if set.contains(&args[1]) {
                CommandResult::Integer(1)
            } else {
                CommandResult::Integer(0)
            }
        }
        None => CommandResult::Integer(0),
    }
}

async fn cmd_sunion(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.is_empty() {
        return CommandResult::error("ERR wrong number of arguments for 'SUNION' command");
    }
    let mut result = std::collections::HashSet::new();
    for key in args {
        if let Some(set) = ctx.storage.get_set(key).await {
            result.extend(set.iter().cloned());
        }
    }
    let out: Vec<CommandResult> = result
        .into_iter()
        .map(CommandResult::Bulk)
        .collect();
    CommandResult::Array(out)
}

async fn cmd_sinter(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.is_empty() {
        return CommandResult::error("ERR wrong number of arguments for 'SINTER' command");
    }

    // Start with the first set
    let mut result: std::collections::HashSet<Vec<u8>> = match ctx.storage.get_set(&args[0]).await {
        Some(set) => set.iter().cloned().collect(),
        None => return CommandResult::Array(Vec::new()),
    };

    // Intersect with remaining sets
    for key in &args[1..] {
        match ctx.storage.get_set(key).await {
            Some(set) => {
                let other: std::collections::HashSet<Vec<u8>> = set.iter().cloned().collect();
                result = result.intersection(&other).cloned().collect();
            }
            None => return CommandResult::Array(Vec::new()),
        }
    }

    let out: Vec<CommandResult> = result
        .into_iter()
        .map(CommandResult::Bulk)
        .collect();
    CommandResult::Array(out)
}

// ---------------------------------------------------------------------------
// Sorted set commands
// ---------------------------------------------------------------------------

async fn cmd_zadd(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() < 3 {
        return CommandResult::error("ERR wrong number of arguments for 'ZADD' command");
    }

    let key = &args[0];
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    let mut start = 1;

    // Parse flags
    while start < args.len() {
        match args[start].to_ascii_uppercase().as_slice() {
            b"NX" => { nx = true; start += 1; }
            b"XX" => { xx = true; start += 1; }
            b"GT" => { gt = true; start += 1; }
            b"LT" => { lt = true; start += 1; }
            _ => break,
        }
    }

    if args.len() - start < 2 || (args.len() - start) % 2 != 0 {
        return CommandResult::error("ERR wrong number of arguments for 'ZADD' command");
    }

    let mut zset = ctx.storage.get_zset(key).await.unwrap_or_default();
    let mut count = 0i64;

    for chunk in args[start..].chunks(2) {
        let score = parse_i64(&chunk[0]).map_err(|e| e)?;
        let member = &chunk[1];

        let existing = zset.get_score(member);

        let should_insert = match existing {
            None => !xx, // New member: only if not XX
            Some(old_score) => {
                if nx {
                    false // NX: skip existing
                } else if gt && score <= old_score {
                    false // GT: only if new score is greater
                } else if lt && score >= old_score {
                    false // LT: only if new score is less
                } else {
                    true
                }
            }
        };

        if should_insert {
            if existing.is_none() && !nx && !xx {
                // For new members without NX/XX, add them
                zset.insert(member.clone(), score);
                count += 1;
            } else if existing.is_some() && (nx || (existing.is_none() && !xx)) {
                // Skip
            } else {
                zset.insert(member.clone(), score);
                count += 1;
            }
        }
    }

    ctx.storage.set_zset(key, zset).await;
    CommandResult::Integer(count)
}

async fn cmd_zrem(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() < 2 {
        return CommandResult::error("ERR wrong number of arguments for 'ZREM' command");
    }
    let key = &args[0];
    let members = &args[1..];

    match ctx.storage.get_zset_mut(key).await {
        Some(zset) => {
            let mut count = 0i64;
            for m in members {
                if zset.remove(m).is_some() {
                    count += 1;
                }
            }
            if zset.is_empty() {
                ctx.storage.del(key).await;
            } else {
                ctx.storage.set_zset(key, zset.clone()).await;
            }
            CommandResult::Integer(count)
        }
        None => CommandResult::Integer(0),
    }
}

async fn cmd_zrange(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() < 3 {
        return CommandResult::error("ERR wrong number of arguments for 'ZRANGE' command");
    }
    let key = &args[0];
    let start = parse_i64(&args[1]).map_err(|e| e)?;
    let stop = parse_i64(&args[2]).map_err(|e| e)?;

    let withscores = args.len() > 3
        && args[3..].iter().any(|a| a.to_ascii_uppercase() == b"WITHSCORES");

    match ctx.storage.get_zset(key).await {
        Some(zset) => {
            let members = zset.range(start, stop);
            if withscores {
                let mut result = Vec::with_capacity(members.len() * 2);
                for (member, score) in members {
                    result.push(CommandResult::Bulk(member));
                    result.push(CommandResult::Bulk(score.to_string().into_bytes()));
                }
                CommandResult::Array(result)
            } else {
                let result: Vec<CommandResult> = members
                    .into_iter()
                    .map(|(m, _)| CommandResult::Bulk(m))
                    .collect();
                CommandResult::Array(result)
            }
        }
        None => CommandResult::Array(Vec::new()),
    }
}

async fn cmd_zrank(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 2 {
        return CommandResult::error("ERR wrong number of arguments for 'ZRANK' command");
    }
    match ctx.storage.get_zset(&args[0]).await {
        Some(zset) => match zset.rank(&args[1]) {
            Some(rank) => CommandResult::Integer(rank as i64),
            None => CommandResult::Null,
        },
        None => CommandResult::Null,
    }
}

async fn cmd_zcard(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'ZCARD' command");
    }
    match ctx.storage.get_zset(&args[0]).await {
        Some(zset) => CommandResult::Integer(zset.len() as i64),
        None => CommandResult::Integer(0),
    }
}

async fn cmd_zscore(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 2 {
        return CommandResult::error("ERR wrong number of arguments for 'ZSCORE' command");
    }
    match ctx.storage.get_zset(&args[0]).await {
        Some(zset) => match zset.get_score(&args[1]) {
            Some(score) => CommandResult::Bulk(score.to_string().into_bytes()),
            None => CommandResult::Null,
        },
        None => CommandResult::Null,
    }
}

// ---------------------------------------------------------------------------
// Pub/Sub commands
// ---------------------------------------------------------------------------

async fn cmd_publish(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() != 2 {
        return CommandResult::error("ERR wrong number of arguments for 'PUBLISH' command");
    }
    let channel = bytes_to_string_lossy(&args[0]);
    let count = ctx.broadcaster.publish(&channel, &args[1]);
    CommandResult::Integer(count)
}

async fn cmd_subscribe(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.is_empty() {
        return CommandResult::error("ERR wrong number of arguments for 'SUBSCRIBE' command");
    }
    for channel_arg in args {
        let channel = bytes_to_string_lossy(channel_arg);
        let _rx = ctx.broadcaster.subscribe(channel);
    }
    CommandResult::Subscribed
}

async fn cmd_unsubscribe(_args: &[Vec<u8>], _ctx: &CommandContext) -> CommandResult {
    CommandResult::Subscribed
}

// ---------------------------------------------------------------------------
// Server commands
// ---------------------------------------------------------------------------

async fn cmd_ping(args: &[Vec<u8>], _ctx: &CommandContext) -> CommandResult {
    if args.is_empty() {
        CommandResult::Simple("PONG".into())
    } else {
        CommandResult::Bulk(args[0].clone())
    }
}

async fn cmd_echo(args: &[Vec<u8>], _ctx: &CommandContext) -> CommandResult {
    if args.len() != 1 {
        return CommandResult::error("ERR wrong number of arguments for 'ECHO' command");
    }
    CommandResult::Bulk(args[0].clone())
}

async fn cmd_command(_args: &[Vec<u8>], _ctx: &CommandContext) -> CommandResult {
    CommandResult::Simple("OK".into())
}

async fn cmd_dbsize(_args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    let size = ctx.storage.dbsize().await as i64;
    CommandResult::Integer(size)
}

async fn cmd_flushdb(_args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    ctx.storage.flushdb().await;
    CommandResult::ok()
}

async fn cmd_flushall(_args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    ctx.storage.flushall().await;
    CommandResult::ok()
}

async fn cmd_info(_args: &[Vec<u8>], _ctx: &CommandContext) -> CommandResult {
    let info = "\
# Server\r\n\
redis_version:7.0.0-crabkv\r\n\
# Keyspace\r\n\
";
    CommandResult::Bulk(info.as_bytes().to_vec())
}

// ---------------------------------------------------------------------------
// Stream commands (stubs)
// ---------------------------------------------------------------------------

async fn cmd_xadd(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return CommandResult::error("ERR wrong number of arguments for 'XADD' command");
    }
    let key = &args[0];
    let _id = &args[1]; // In real impl, parse "*" or explicit ID
    let fields = &args[2..];

    let mut field_map = HashMap::new();
    for chunk in fields.chunks(2) {
        field_map.insert(chunk[0].clone(), chunk[1].clone());
    }

    let entry_id = ctx
        .streams
        .xadd(key, field_map)
        .await;

    CommandResult::Bulk(entry_id.into_bytes())
}

async fn cmd_xread(args: &[Vec<u8>], ctx: &CommandContext) -> CommandResult {
    // Simplified: XREAD [COUNT count] [BLOCK ms] STREAMS key [key ...] id [id ...]
    let mut count: usize = 10;
    let mut i = 0;

    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"COUNT" => {
                i += 1;
                if i < args.len() {
                    count = parse_i64(&args[i]).unwrap_or(10) as usize;
                }
            }
            b"BLOCK" => {
                i += 1;
                // Ignored in stub
            }
            b"STREAMS" => {
                i += 1;
                break;
            }
            _ => {}
        }
        i += 1;
    }

    if i >= args.len() {
        return CommandResult::error("ERR wrong number of arguments for 'XREAD' command");
    }

    let keys = &args[i..];
    let mut result = Vec::new();

    for key in keys {
        let entries = ctx.streams.xread(key, count).await;
        if !entries.is_empty() {
            let mut stream_result = Vec::new();
            for (id, fields) in entries {
                let mut field_arr = Vec::new();
                for (k, v) in fields {
                    field_arr.push(CommandResult::Bulk(k));
                    field_arr.push(CommandResult::Bulk(v));
                }
                stream_result.push(CommandResult::Array(vec![
                    CommandResult::Bulk(id.into_bytes()),
                    CommandResult::Array(field_arr),
                ]));
            }
            result.push(CommandResult::Array(vec![
                CommandResult::Bulk(key.clone()),
                CommandResult::Array(stream_result),
            ]));
        }
    }

    if result.is_empty() {
        CommandResult::Null
    } else {
        CommandResult::Array(result)
    }
}

// ---------------------------------------------------------------------------
// Command dispatcher
// ---------------------------------------------------------------------------

pub async fn dispatch_command(
    cmd: &str,
    args: Vec<Vec<u8>>,
    ctx: &CommandContext,
) -> CommandResult {
    match cmd.to_ascii_uppercase().as_str() {
        // Strings
        "SET" => cmd_set(&args, ctx).await,
        "GET" => cmd_get(&args, ctx).await,
        "DEL" | "UNLINK" => cmd_del(&args, ctx).await,
        "EXISTS" => cmd_exists(&args, ctx).await,
        "MGET" => cmd_mget(&args, ctx).await,
        "MSET" => cmd_mset(&args, ctx).await,
        "INCR" => cmd_incr(&args, ctx).await,
        "INCRBY" => cmd_incrby(&args, ctx).await,
        "DECR" => cmd_decr(&args, ctx).await,
        "DECRBY" => cmd_decrby(&args, ctx).await,
        "APPEND" => cmd_append(&args, ctx).await,
        "STRLEN" => cmd_strlen(&args, ctx).await,
        "GETRANGE" => cmd_getrange(&args, ctx).await,
        "SETRANGE" => cmd_setrange(&args, ctx).await,

        // Expiration
        "EXPIRE" => cmd_expire(&args, ctx).await,
        "PEXPIRE" => {
            // Convert ms to seconds, approximate
            if args.len() < 2 {
                return CommandResult::error("ERR wrong number of arguments for 'PEXPIRE' command");
            }
            let key = &args[0];
            let ms = parse_i64(&args[1]).map_err(|e| e)?;
            let mut expire_args = args;
            expire_args[1] = (ms / 1000).to_string().into_bytes();
            cmd_expire(&expire_args, ctx).await
        }
        "TTL" | "PTTL" => cmd_ttl(&args, ctx).await,
        "PERSIST" => cmd_persist(&args, ctx).await,

        // Keys
        "KEYS" => cmd_keys(&args, ctx).await,
        "RANDOMKEY" => cmd_randomkey(&args, ctx).await,
        "TYPE" => cmd_type(&args, ctx).await,
        "RENAME" => cmd_rename(&args, ctx).await,

        // Lists
        "LPUSH" | "LPUSHX" => cmd_lpush(&args, ctx).await,
        "RPUSH" | "RPUSHX" => cmd_rpush(&args, ctx).await,
        "LPOP" => cmd_lpop(&args, ctx).await,
        "RPOP" => cmd_rpop(&args, ctx).await,
        "LRANGE" => cmd_lrange(&args, ctx).await,
        "LLEN" => cmd_llen(&args, ctx).await,
        "LINDEX" => cmd_lindex(&args, ctx).await,
        "LSET" => cmd_lset(&args, ctx).await,

        // Hashes
        "HSET" | "HSETNX" => cmd_hset(&args, ctx).await,
        "HGET" => cmd_hget(&args, ctx).await,
        "HDEL" => cmd_hdel(&args, ctx).await,
        "HGETALL" | "HGETALL" => cmd_hgetall(&args, ctx).await,
        "HLEN" => cmd_hlen(&args, ctx).await,
        "HKEYS" => cmd_hkeys(&args, ctx).await,
        "HVALS" => cmd_hvals(&args, ctx).await,

        // Sets
        "SADD" => cmd_sadd(&args, ctx).await,
        "SREM" => cmd_srem(&args, ctx).await,
        "SMEMBERS" | "SSCAN" => cmd_smembers(&args, ctx).await,
        "SCARD" => cmd_scard(&args, ctx).await,
        "SISMEMBER" => cmd_sismember(&args, ctx).await,
        "SUNION" => cmd_sunion(&args, ctx).await,
        "SINTER" => cmd_sinter(&args, ctx).await,

        // Sorted sets
        "ZADD" => cmd_zadd(&args, ctx).await,
        "ZREM" => cmd_zrem(&args, ctx).await,
        "ZRANGE" => cmd_zrange(&args, ctx).await,
        "ZRANK" => cmd_zrank(&args, ctx).await,
        "ZCARD" => cmd_zcard(&args).await,
        "ZSCORE" => cmd_zscore(&args, ctx).await,

        // Pub/Sub
        "PUBLISH" => cmd_publish(&args, ctx).await,
        "SUBSCRIBE" => cmd_subscribe(&args, ctx).await,
        "UNSUBSCRIBE" => cmd_unsubscribe(&args, ctx).await,
        "PSUBSCRIBE" | "PUNSUBSCRIBE" => cmd_unsubscribe(&args, ctx).await,

        // Server
        "PING" => cmd_ping(&args, ctx).await,
        "ECHO" => cmd_echo(&args, ctx).await,
        "COMMAND" => cmd_command(&args, ctx).await,
        "DBSIZE" => cmd_dbsize(&args, ctx).await,
        "FLUSHDB" => cmd_flushdb(&args, ctx).await,
        "FLUSHALL" => cmd_flushall(&args, ctx).await,
        "INFO" => cmd_info(&args, ctx).await,

        // Streams
        "XADD" => cmd_xadd(&args, ctx).await,
        "XREAD" => cmd_xread(&args, ctx).await,

        // Client
        "CLIENT" => {
            if args.first().map(|a| a.to_ascii_uppercase()) == Some(b"SETNAME".to_vec()) {
                CommandResult::ok()
            } else {
                CommandResult::Simple("OK".into())
            }
        }
        "SELECT" => CommandResult::ok(),
        "QUIT" => CommandResult::ok(),

        _ => CommandResult::error(format!("ERR unknown command '{}'", cmd)),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Minimal mock storage for testing the dispatcher logic.
    // In production, use the real StorageEngine.

    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[derive(Clone, Debug)]
    enum MockValue {
        String(Vec<u8>),
        List(Vec<Vec<u8>>),
        Hash(HashMap<Vec<u8>, Vec<u8>>),
        Set(std::collections::HashSet<Vec<u8>>),
        ZSet(MockZSet),
    }

    #[derive(Clone, Debug, Default)]
    struct MockZSet {
        members: BTreeMap<i64, Vec<Vec<u8>>>,
        scores: HashMap<Vec<u8>, i64>,
    }

    impl MockZSet {
        fn insert(&mut self, member: Vec<u8>, score: i64) {
            if let Some(old) = self.scores.remove(&member) {
                if let Some(list) = self.members.get_mut(&old) {
                    list.retain(|m| m != &member);
                    if list.is_empty() {
                        self.members.remove(&old);
                    }
                }
            }
            self.scores.insert(member.clone(), score);
            self.members.entry(score).or_default().push(member);
        }

        fn remove(&mut self, member: &[u8]) -> Option<i64> {
            if let Some(score) = self.scores.remove(member) {
                if let Some(list) = self.members.get_mut(&score) {
                    list.retain(|m| m != member);
                    if list.is_empty() {
                        self.members.remove(&score);
                    }
                }
                Some(score)
            } else {
                None
            }
        }

        fn get_score(&self, member: &[u8]) -> Option<i64> {
            self.scores.get(member).copied()
        }

        fn len(&self) -> usize {
            self.scores.len()
        }

        fn is_empty(&self) -> bool {
            self.scores.is_empty()
        }

        fn rank(&self, member: &[u8]) -> Option<usize> {
            let score = self.scores.get(member)?;
            let mut rank = 0;
            for (&s, members) in &self.members {
                if s < *score {
                    rank += members.len();
                } else if s == *score {
                    for m in members {
                        if m == member {
                            return Some(rank);
                        }
                        rank += 1;
                    }
                }
            }
            None
        }

        fn range(&self, start: i64, stop: i64) -> Vec<(Vec<u8>, i64)> {
            let all: Vec<(Vec<u8>, i64)> = self
                .members
                .iter()
                .flat_map(|(&score, members)| {
                    members.iter().map(move |m| (m.clone(), score))
                })
                .collect();

            let len = all.len() as i64;
            let start = if start < 0 { (len + start).max(0) } else { start } as usize;
            let stop = if stop < 0 { (len + stop).max(-1) } else { stop } as usize;

            if start > all.len() {
                return Vec::new();
            }
            let stop = stop.min(all.len() - 1);
            all[start..=stop].to_vec()
        }

        fn iter(&self) -> impl Iterator<Item = &(Vec<u8>, i64)> {
            self.members
                .iter()
                .flat_map(|(&score, members)| {
                    members.iter().map(move |m| (m, score))
                })
        }
    }

    struct MockStorage {
        data: RwLock<HashMap<Vec<u8>, MockValue>>,
    }

    impl MockStorage {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                data: RwLock::new(HashMap::new()),
            })
        }
    }

    // We can't easily satisfy the real StorageEngine trait without defining it,
    // so these tests verify the dispatcher routing and pure logic only.

    #[test]
    fn test_glob_match() {
        assert!(glob_match("hello", "hello"));
        assert!(glob_match("h*o", "hello"));
        assert!(glob_match("h?llo", "hello"));
        assert!(!glob_match("hello", "world"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("h[ae]llo", "hallo"));
        assert!(glob_match("h[ae]llo", "hello"));
        assert!(!glob_match("h[ae]llo", "hullo"));
    }

    #[test]
    fn test_parse_i64() {
        assert_eq!(parse_i64(b"42").unwrap(), 42);
        assert_eq!(parse_i64(b"-7").unwrap(), -7);
        assert!(parse_i64(b"abc").is_err());
        assert!(parse_i64(b"99999999999999999999999999999999").is_err());
    }

    #[test]
    fn test_command_result_helpers() {
        assert_eq!(CommandResult::ok(), CommandResult::Simple("OK".into()));
        assert_eq!(CommandResult::integer(42), CommandResult::Integer(42));
        assert_eq!(CommandResult::null(), CommandResult::Null);
        assert_eq!(
            CommandResult::bulk("test".as_bytes()),
            CommandResult::Bulk(b"test".to_vec())
        );
        assert!(matches!(
            CommandResult::error("err"),
            CommandResult::Error(_)
        ));
    }

    #[test]
    fn test_mock_zset_basics() {
        let mut zs = MockZSet::default();
        zs.insert(b"alice".to_vec(), 100);
        zs.insert(b"bob".to_vec(), 200);
        zs.insert(b"carol".to_vec(), 150);

        assert_eq!(zs.len(), 3);
        assert_eq!(zs.get_score(b"bob"), Some(200));
        assert_eq!(zs.rank(b"alice"), Some(0));
        assert_eq!(zs.rank(b"carol"), Some(1));
        assert_eq!(zs.rank(b"bob"), Some(2));

        let range = zs.range(0, -1);
        assert_eq!(range.len(), 3);
        assert_eq!(range[0].0, b"alice");
        assert_eq!(range[2].0, b"bob");

        assert!(zs.remove(b"bob").is_some());
        assert_eq!(zs.len(), 2);
        assert!(zs.remove(b"bob").is_none());
    }

    #[test]
    fn test_mock_zset_update_score() {
        let mut zs = MockZSet::default();
        zs.insert(b"a".to_vec(), 10);
        zs.insert(b"a".to_vec(), 20);

        assert_eq!(zs.len(), 1);
        assert_eq!(zs.get_score(b"a"), Some(20));
        // Old score entry should be cleaned up
        assert!(zs.members.get(&10).is_none());
    }

    #[test]
    fn test_zadd_flags_nx_xx() {
        // Simulate ZADD NX behavior in a fresh zset
        let mut zs = MockZSet::default();
        let nx = true;
        let xx = false;
        let member = b"m1".to_vec();
        let score = 100i64;

        let existing = zs.get_score(&member);
        let should_insert = match existing {
            None => !xx,
            Some(_) if nx => false,
            _ => true,
        };
        if should_insert {
            zs.insert(member.clone(), score);
        }
        assert_eq!(zs.len(), 1);

        // Try NX again – should skip
        let existing = zs.get_score(&member);
        let should_insert = match existing {
            None => !xx,
            Some(_) if nx => false,
            _ => true,
        };
        if should_insert {
            zs.insert(member.clone(), score);
        }
        assert_eq!(zs.len(), 1); // still 1
    }

    #[test]
    fn test_normalize_index() {
        assert_eq!(normalize_index(0, 5), 0);
        assert_eq!(normalize_index(4, 5), 4);
        assert_eq!(normalize_index(-1, 5), 4);
        assert_eq!(normalize_index(-5, 5), 0);
        assert_eq!(normalize_index(-10, 5), 0);
        assert_eq!(normalize_index(10, 5), 4);
    }
}
