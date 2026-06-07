use std::sync::Arc;
use std::time::Duration;

use ahash::AHashMap;

use crate::jobs::JobQueue;
use crate::protocol::RespValue;
use crate::pubsub::PubSubBroadcaster;
use crate::storage::{StorageEngine, StorageValue};
use crate::streams::StreamEngine;

pub struct CommandContext {
    pub storage: Arc<StorageEngine>,
    pub broadcaster: Arc<PubSubBroadcaster>,
    pub streams: Arc<StreamEngine>,
    pub jobs: Arc<JobQueue>,
}

fn extract_arg<'a>(args: &'a [RespValue], idx: usize) -> Result<Vec<u8>, RespValue> {
    args.get(idx)
        .and_then(|v| v.as_bytes().map(|b| b.to_vec()))
        .ok_or_else(|| RespValue::Error("ERR wrong number of arguments".to_string()))
}

fn extract_str<'a>(args: &'a [RespValue], idx: usize) -> Result<String, RespValue> {
    args.get(idx)
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .ok_or_else(|| RespValue::Error("ERR wrong number of arguments".to_string()))
}

fn extract_i64(args: &[RespValue], idx: usize) -> Result<i64, RespValue> {
    args.get(idx)
        .and_then(|v| v.as_i64())
        .ok_or_else(|| RespValue::Error("ERR invalid integer".to_string()))
}

pub fn dispatch_command(ctx: &CommandContext, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return RespValue::Error("ERR empty command".to_string());
    }

    let cmd = match &args[0] {
        RespValue::BulkString(s) => String::from_utf8_lossy(s).to_uppercase(),
        RespValue::SimpleString(s) => s.to_uppercase(),
        _ => return RespValue::Error("ERR invalid command format".to_string()),
    };

    match cmd.as_str() {
        // ── String commands ──────────────────────────────────────
        "SET" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments for 'SET' command".to_string());
            }
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            let val = match extract_arg(&args, 2) {
                Ok(v) => v,
                Err(e) => return e,
            };

            let mut ttl = None;
            let mut i = 3;
            while i < args.len() {
                let opt = extract_str(&args, i).unwrap_or_default().to_uppercase();
                match opt.as_str() {
                    "EX" => {
                        if let Ok(secs) = extract_i64(&args, i + 1) {
                            ttl = Some(Duration::from_secs(secs as u64));
                        }
                        i += 2;
                    }
                    "PX" => {
                        if let Ok(ms) = extract_i64(&args, i + 1) {
                            ttl = Some(Duration::from_millis(ms as u64));
                        }
                        i += 2;
                    }
                    _ => i += 1,
                }
            }

            match ttl {
                Some(d) => {
                    ctx.storage.set_with_ttl(key, StorageValue::String(val.into()), d);
                }
                None => {
                    ctx.storage.set(key, StorageValue::String(val.into()));
                }
            }
            RespValue::SimpleString("OK".to_string())
        }

        "GET" => {
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            match ctx.storage.get(&key) {
                Some(StorageValue::String(s)) => RespValue::BulkString(s.to_vec()),
                Some(_) => RespValue::Error("ERR wrong type for key".to_string()),
                None => RespValue::Null,
            }
        }

        "DEL" => {
            let mut count = 0i64;
            for i in 1..args.len() {
                if let Ok(key) = extract_arg(&args, i) {
                    if ctx.storage.del(&key) {
                        count += 1;
                    }
                }
            }
            RespValue::Integer(count)
        }

        "EXISTS" => {
            let mut count = 0i64;
            for i in 1..args.len() {
                if let Ok(key) = extract_arg(&args, i) {
                    if ctx.storage.exists(&key) {
                        count += 1;
                    }
                }
            }
            RespValue::Integer(count)
        }

        "INCR" => {
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            match ctx.storage.incr(&key, 1) {
                Ok(n) => RespValue::Integer(n),
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "INCRBY" => {
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            let amount = match extract_i64(&args, 2) {
                Ok(a) => a,
                Err(e) => return e,
            };
            match ctx.storage.incr(&key, amount) {
                Ok(n) => RespValue::Integer(n),
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "DECR" => {
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            match ctx.storage.decr(&key, 1) {
                Ok(n) => RespValue::Integer(n),
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "TTL" => {
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            match ctx.storage.ttl(&key) {
                Some(d) => RespValue::Integer(d.as_secs() as i64),
                None => {
                    if ctx.storage.exists(&key) {
                        RespValue::Integer(-1)
                    } else {
                        RespValue::Integer(-2)
                    }
                }
            }
        }

        "PERSIST" => {
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            if ctx.storage.persist(&key) {
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }

        "MSET" => {
            if args.len() < 3 || args.len() % 2 != 1 {
                return RespValue::Error("ERR wrong number of arguments for 'MSET' command".to_string());
            }
            let mut pairs = Vec::new();
            let mut i = 1;
            while i < args.len() {
                let key = match extract_arg(&args, i) {
                    Ok(k) => k,
                    Err(e) => return e,
                };
                let val = match extract_arg(&args, i + 1) {
                    Ok(v) => v,
                    Err(e) => return e,
                };
                pairs.push((key, StorageValue::String(val.into())));
                i += 2;
            }
            ctx.storage.mset(pairs);
            RespValue::SimpleString("OK".to_string())
        }

        "MGET" => {
            let mut results = Vec::new();
            for i in 1..args.len() {
                if let Ok(key) = extract_arg(&args, i) {
                    match ctx.storage.get(&key) {
                        Some(StorageValue::String(s)) => results.push(RespValue::BulkString(s.to_vec())),
                        _ => results.push(RespValue::Null),
                    }
                } else {
                    results.push(RespValue::Null);
                }
            }
            RespValue::Array(results)
        }

        // ── List commands ────────────────────────────────────────
        "LPUSH" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            let values: Vec<Vec<u8>> = args[2..]
                .iter()
                .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
                .collect();
            match ctx.storage.lpush(&key, values) {
                Ok(n) => RespValue::Integer(n as i64),
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "RPUSH" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            let values: Vec<Vec<u8>> = args[2..]
                .iter()
                .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
                .collect();
            match ctx.storage.rpush(&key, values) {
                Ok(n) => RespValue::Integer(n as i64),
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "LPOP" => {
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            match ctx.storage.lpop(&key) {
                Ok(Some(v)) => RespValue::BulkString(v),
                Ok(None) => RespValue::Null,
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "RPOP" => {
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            match ctx.storage.rpop(&key) {
                Ok(Some(v)) => RespValue::BulkString(v),
                Ok(None) => RespValue::Null,
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "LRANGE" => {
            if args.len() < 4 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            let start = extract_i64(&args, 2).unwrap_or(0);
            let stop = extract_i64(&args, 3).unwrap_or(-1);
            match ctx.storage.lrange(&key, start, stop) {
                Ok(items) => {
                    RespValue::Array(items.into_iter().map(RespValue::BulkString).collect())
                }
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "LLEN" => {
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            match ctx.storage.lrange(&key, 0, -1) {
                Ok(items) => RespValue::Integer(items.len() as i64),
                Err(_) => RespValue::Integer(0),
            }
        }

        // ── Hash commands ────────────────────────────────────────
        "HSET" => {
            if args.len() < 4 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            let field = match extract_arg(&args, 2) {
                Ok(f) => f,
                Err(e) => return e,
            };
            let value = match extract_arg(&args, 3) {
                Ok(v) => v,
                Err(e) => return e,
            };
            match ctx.storage.hset(&key, field, value) {
                Ok(new) => RespValue::Integer(if new { 1 } else { 0 }),
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "HGET" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            let field = match extract_arg(&args, 2) {
                Ok(f) => f,
                Err(e) => return e,
            };
            match ctx.storage.hget(&key, &field) {
                Ok(Some(v)) => RespValue::BulkString(v),
                Ok(None) => RespValue::Null,
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "HDEL" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            let field = match extract_arg(&args, 2) {
                Ok(f) => f,
                Err(e) => return e,
            };
            match ctx.storage.hdel(&key, &field) {
                Ok(deleted) => RespValue::Integer(if deleted { 1 } else { 0 }),
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "HGETALL" => {
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            match ctx.storage.hgetall(&key) {
                Ok(map) => {
                    let mut items = Vec::with_capacity(map.len() * 2);
                    for (k, v) in map {
                        items.push(RespValue::BulkString(k));
                        items.push(RespValue::BulkString(v));
                    }
                    RespValue::Array(items)
                }
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        // ── Set commands ─────────────────────────────────────────
        "SADD" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            let members: Vec<Vec<u8>> = args[2..]
                .iter()
                .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
                .collect();
            match ctx.storage.sadd(&key, members) {
                Ok(n) => RespValue::Integer(n as i64),
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "SISMEMBER" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            let member = match extract_arg(&args, 2) {
                Ok(m) => m,
                Err(e) => return e,
            };
            match ctx.storage.sismember(&key, &member) {
                Ok(has) => RespValue::Integer(if has { 1 } else { 0 }),
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "SMEMBERS" => {
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            match ctx.storage.smembers(&key) {
                Ok(members) => {
                    let mut items: Vec<RespValue> =
                        members.into_iter().map(RespValue::BulkString).collect();
                    items.sort_by(|a, b| {
                        let a_bytes = a.as_bytes().unwrap_or(&[]);
                        let b_bytes = b.as_bytes().unwrap_or(&[]);
                        a_bytes.cmp(b_bytes)
                    });
                    RespValue::Array(items)
                }
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "SREM" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            let member = match extract_arg(&args, 2) {
                Ok(m) => m,
                Err(e) => return e,
            };
            match ctx.storage.sismember(&key, &member) {
                Ok(true) => {
                    // Rebuild set without the member
                    if let Ok(members) = ctx.storage.smembers(&key) {
                        let remaining: Vec<Vec<u8>> = members
                            .into_iter()
                            .filter(|m| m != &member)
                            .collect();
                        ctx.storage.sadd(&key, remaining).unwrap_or(0);
                        RespValue::Integer(1)
                    } else {
                        RespValue::Integer(0)
                    }
                }
                Ok(false) => RespValue::Integer(0),
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        // ── Sorted Set commands ──────────────────────────────────
        "ZADD" => {
            if args.len() < 4 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            let score = match extract_i64(&args, 2) {
                Ok(s) => s,
                Err(e) => return e,
            };
            let member = match extract_arg(&args, 3) {
                Ok(m) => m,
                Err(e) => return e,
            };
            match ctx.storage.zadd(&key, score, member) {
                Ok(new) => RespValue::Integer(if new { 1 } else { 0 }),
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "ZSCORE" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            let member = match extract_arg(&args, 2) {
                Ok(m) => m,
                Err(e) => return e,
            };
            match ctx.storage.zscore(&key, &member) {
                Ok(Some(score)) => RespValue::BulkString(score.to_string().into_bytes()),
                Ok(None) => RespValue::Null,
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "ZRANGE" => {
            if args.len() < 4 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let key = match extract_arg(&args, 1) {
                Ok(k) => k,
                Err(e) => return e,
            };
            let start = extract_i64(&args, 2).unwrap_or(0) as usize;
            let stop = extract_i64(&args, 3).unwrap_or(-1);
            let stop = if stop < 0 {
                // Negative stop: count from end. We'll use a large number and let zrange handle it.
                usize::MAX
            } else {
                stop as usize
            };
            match ctx.storage.zrange(&key, start, stop) {
                Ok(items) => {
                    RespValue::Array(
                        items
                            .into_iter()
                            .map(|(score, member)| {
                                RespValue::Array(vec![
                                    RespValue::BulkString(member),
                                    RespValue::BulkString(score.to_string().into_bytes()),
                                ])
                            })
                            .collect(),
                    )
                }
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        // ── Pub/Sub ──────────────────────────────────────────────
        "PUBLISH" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let channel = match extract_arg(&args, 1) {
                Ok(c) => c,
                Err(e) => return e,
            };
            let message = match extract_arg(&args, 2) {
                Ok(m) => m,
                Err(e) => return e,
            };
            let count = ctx.broadcaster.publish(&channel, &message);
            RespValue::Integer(count as i64)
        }

        // ── Streams ──────────────────────────────────────────────
        "XADD" => {
            if args.len() < 4 || args.len() % 2 != 0 {
                return RespValue::Error("ERR wrong number of arguments for XADD".to_string());
            }
            let stream = match extract_arg(&args, 1) {
                Ok(s) => s,
                Err(e) => return e,
            };
            let mut fields = AHashMap::new();
            let mut i = 2;
            while i < args.len() {
                let field = match extract_arg(&args, i) {
                    Ok(f) => f,
                    Err(e) => return e,
                };
                let value = match extract_arg(&args, i + 1) {
                    Ok(v) => v,
                    Err(e) => return e,
                };
                fields.insert(field, value);
                i += 2;
            }
            match ctx.streams.xadd(&ctx.storage, &stream, fields) {
                Ok(id) => RespValue::BulkString(id.to_string().into_bytes()),
                Err(msg) => RespValue::Error(format!("ERR {}", msg)),
            }
        }

        "XREAD" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments for XREAD".to_string());
            }
            let mut count = None;
            let mut stream_idx = 1;

            let subcmd = extract_str(&args, 1).unwrap_or_default().to_uppercase();
            if subcmd == "COUNT" {
                if let Ok(n) = extract_i64(&args, 2) {
                    count = Some(n as usize);
                }
                stream_idx = 3;
            }

            let stream_names: Vec<Vec<u8>> = args[stream_idx..]
                .iter()
                .filter_map(|a| a.as_bytes().map(|b| b.to_vec()))
                .collect();

            let stream_refs: Vec<&[u8]> = stream_names.iter().map(|s| s.as_slice()).collect();
            let results = ctx.streams.xread(&ctx.storage, &stream_refs, count);

            if results.is_empty() {
                return RespValue::Null;
            }

            let items: Vec<RespValue> = results
                .into_iter()
                .map(|(name, entries)| {
                    let entry_vals: Vec<RespValue> = entries
                        .into_iter()
                        .map(|e| {
                            let mut fields = Vec::with_capacity(e.fields.len() * 2);
                            for (k, v) in e.fields {
                                fields.push(RespValue::BulkString(k));
                                fields.push(RespValue::BulkString(v));
                            }
                            RespValue::Array(vec![
                                RespValue::BulkString(e.id.to_string().into_bytes()),
                                RespValue::Array(fields),
                            ])
                        })
                        .collect();
                    RespValue::Array(vec![
                        RespValue::BulkString(name),
                        RespValue::Array(entry_vals),
                    ])
                })
                .collect();
            RespValue::Array(items)
        }

        "XLEN" => {
            let stream = match extract_arg(&args, 1) {
                Ok(s) => s,
                Err(e) => return e,
            };
            RespValue::Integer(ctx.streams.xlen(&ctx.storage, &stream) as i64)
        }

        // ── Job Queue ────────────────────────────────────────────
        "QUEUE.ADD" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let queue = extract_str(&args, 1).unwrap_or_else(|_| "default".to_string());
            let payload = match extract_arg(&args, 2) {
                Ok(p) => p,
                Err(e) => return e,
            };
            let priority = extract_i64(&args, 3).unwrap_or(0) as i32;
            let id = ctx.jobs.add_job(&queue, payload, priority, None);
            RespValue::BulkString(id.into_bytes())
        }

        "QUEUE.POP" => {
            let queue = extract_str(&args, 1).unwrap_or_else(|_| "default".to_string());
            match ctx.jobs.pop_job(&queue) {
                Some(job) => {
                    let mut items = Vec::new();
                    items.push(RespValue::BulkString(job.id.into_bytes()));
                    items.push(RespValue::BulkString(job.payload));
                    RespValue::Array(items)
                }
                None => RespValue::Null,
            }
        }

        "QUEUE.ACK" => {
            if args.len() < 3 {
                return RespValue::Error("ERR wrong number of arguments".to_string());
            }
            let queue = extract_str(&args, 1).unwrap_or_else(|_| "default".to_string());
            let job_id = extract_str(&args, 2).unwrap_or_default();
            if ctx.jobs.ack_job(&queue, &job_id) {
                RespValue::Integer(1)
            } else {
                RespValue::Integer(0)
            }
        }

        // ── Utility ──────────────────────────────────────────────
        "PING" => RespValue::SimpleString("PONG".to_string()),
        "DBSIZE" => RespValue::Integer(ctx.storage.dbsize() as i64),
        "FLUSHALL" => {
            ctx.storage.flush_all();
            RespValue::SimpleString("OK".to_string())
        }

        _ => RespValue::Error(format!("ERR unknown command '{}'", cmd)),
    }
}
