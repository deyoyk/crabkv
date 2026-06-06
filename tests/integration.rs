use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crabkv::commands::{dispatch_command, CommandContext};
use crabkv::jobs::JobQueue;
use crabkv::protocol::{RespParser, RespValue};
use crabkv::pubsub::PubSubBroadcaster;
use crabkv::storage::{EvictionPolicy, StorageEngine, StorageValue};
use crabkv::streams::StreamEngine;

fn setup() -> CommandContext {
    CommandContext {
        storage: Arc::new(StorageEngine::with_config(
            16,
            1024 * 1024,
            EvictionPolicy::Lru,
        )),
        broadcaster: Arc::new(PubSubBroadcaster::new()),
        streams: Arc::new(StreamEngine::new()),
        jobs: Arc::new(JobQueue::new()),
    }
}

fn bulk(s: &str) -> RespValue {
    RespValue::BulkString(s.as_bytes().to_vec())
}

fn int(n: i64) -> RespValue {
    RespValue::Integer(n)
}

fn cmd_args(args: &[&str]) -> Vec<RespValue> {
    args.iter().map(|a| bulk(a)).collect()
}

// ── String operations ──────────────────────────────────────────────

#[tokio::test]
async fn test_string_set_get() {
    let ctx = setup();

    let result = dispatch_command(&ctx, &cmd_args(&["SET", "name", "crabkv"]));
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    let result = dispatch_command(&ctx, &cmd_args(&["GET", "name"]));
    assert_eq!(result, bulk("crabkv"));
}

#[tokio::test]
async fn test_string_get_nonexistent() {
    let ctx = setup();
    let result = dispatch_command(&ctx, &cmd_args(&["GET", "missing"]));
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_string_set_overwrite() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["SET", "k", "v1"]));
    dispatch_command(&ctx, &cmd_args(&["SET", "k", "v2"]));
    let result = dispatch_command(&ctx, &cmd_args(&["GET", "k"]));
    assert_eq!(result, bulk("v2"));
}

#[tokio::test]
async fn test_string_del() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["SET", "k", "v"]));

    let result = dispatch_command(&ctx, &cmd_args(&["DEL", "k"]));
    assert_eq!(result, int(1));

    let result = dispatch_command(&ctx, &cmd_args(&["DEL", "k"]));
    assert_eq!(result, int(0));

    let result = dispatch_command(&ctx, &cmd_args(&["GET", "k"]));
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_string_del_multiple() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["SET", "a", "1"]));
    dispatch_command(&ctx, &cmd_args(&["SET", "b", "2"]));
    dispatch_command(&ctx, &cmd_args(&["SET", "c", "3"]));

    let result = dispatch_command(&ctx, &cmd_args(&["DEL", "a", "b", "c"]));
    assert_eq!(result, int(3));

    let result = dispatch_command(&ctx, &cmd_args(&["DEL", "a"]));
    assert_eq!(result, int(0));
}

#[tokio::test]
async fn test_string_exists() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["SET", "k", "v"]));

    let result = dispatch_command(&ctx, &cmd_args(&["EXISTS", "k"]));
    assert_eq!(result, int(1));

    let result = dispatch_command(&ctx, &cmd_args(&["EXISTS", "nope"]));
    assert_eq!(result, int(0));
}

#[tokio::test]
async fn test_string_incr_decr() {
    let ctx = setup();

    let result = dispatch_command(&ctx, &cmd_args(&["INCR", "counter"]));
    assert_eq!(result, int(1));

    let result = dispatch_command(&ctx, &cmd_args(&["INCR", "counter"]));
    assert_eq!(result, int(2));

    let result = dispatch_command(&ctx, &cmd_args(&["DECR", "counter"]));
    assert_eq!(result, int(1));

    let result = dispatch_command(&ctx, &cmd_args(&["DECR", "counter"]));
    assert_eq!(result, int(0));

    let result = dispatch_command(&ctx, &cmd_args(&["DECR", "counter"]));
    assert_eq!(result, int(-1));
}

#[tokio::test]
async fn test_string_incrby() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["SET", "k", "10"]));

    let result = dispatch_command(&ctx, &cmd_args(&["INCRBY", "k", "5"]));
    assert_eq!(result, int(15));

    let result = dispatch_command(&ctx, &cmd_args(&["INCRBY", "k", "-3"]));
    assert_eq!(result, int(12));
}

#[tokio::test]
async fn test_string_mset_mget() {
    let ctx = setup();

    let result = dispatch_command(&ctx, &cmd_args(&["MSET", "k1", "v1", "k2", "v2", "k3", "v3"]));
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));

    let result = dispatch_command(&ctx, &cmd_args(&["MGET", "k1", "k2", "k3"]));
    assert_eq!(
        result,
        RespValue::Array(vec![bulk("v1"), bulk("v2"), bulk("v3")])
    );
}

#[tokio::test]
async fn test_string_mget_partial_miss() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["SET", "a", "1"]));

    let result = dispatch_command(&ctx, &cmd_args(&["MGET", "a", "b"]));
    assert_eq!(
        result,
        RespValue::Array(vec![bulk("1"), RespValue::Null])
    );
}

// ── Expiration ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_expiration_set_with_ttl() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["SET", "k", "v", "EX", "10"]));

    let result = dispatch_command(&ctx, &cmd_args(&["TTL", "k"]));
    match result {
        RespValue::Integer(ttl) => assert!(ttl > 0 && ttl <= 10),
        _ => panic!("expected integer TTL"),
    }
}

#[tokio::test]
async fn test_expiration_ttl_no_expiry() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["SET", "k", "v"]));

    let result = dispatch_command(&ctx, &cmd_args(&["TTL", "k"]));
    assert_eq!(result, int(-1));
}

#[tokio::test]
async fn test_expiration_persist() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["SET", "k", "v", "EX", "10"]));

    let result = dispatch_command(&ctx, &cmd_args(&["PERSIST", "k"]));
    assert_eq!(result, int(1));

    let result = dispatch_command(&ctx, &cmd_args(&["TTL", "k"]));
    assert_eq!(result, int(-1));
}

#[tokio::test]
async fn test_expiration_ttl_nonexistent() {
    let ctx = setup();
    let result = dispatch_command(&ctx, &cmd_args(&["TTL", "nope"]));
    assert_eq!(result, int(-2));
}

#[tokio::test]
async fn test_expiration_persist_nonexistent() {
    let ctx = setup();
    let result = dispatch_command(&ctx, &cmd_args(&["PERSIST", "nope"]));
    assert_eq!(result, int(0));
}

// ── List operations ────────────────────────────────────────────────

#[tokio::test]
async fn test_list_lpush_rpush() {
    let ctx = setup();

    let result = dispatch_command(&ctx, &cmd_args(&["RPUSH", "lst", "a"]));
    assert_eq!(result, int(1));

    let result = dispatch_command(&ctx, &cmd_args(&["RPUSH", "lst", "b"]));
    assert_eq!(result, int(2));

    let result = dispatch_command(&ctx, &cmd_args(&["LPUSH", "lst", "z"]));
    assert_eq!(result, int(3));
}

#[tokio::test]
async fn test_list_lpop_rpop() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["RPUSH", "lst", "a"]));
    dispatch_command(&ctx, &cmd_args(&["RPUSH", "lst", "b"]));
    dispatch_command(&ctx, &cmd_args(&["RPUSH", "lst", "c"]));

    let result = dispatch_command(&ctx, &cmd_args(&["LPOP", "lst"]));
    assert_eq!(result, bulk("a"));

    let result = dispatch_command(&ctx, &cmd_args(&["RPOP", "lst"]));
    assert_eq!(result, bulk("c"));

    let result = dispatch_command(&ctx, &cmd_args(&["LPOP", "lst"]));
    assert_eq!(result, bulk("b"));

    let result = dispatch_command(&ctx, &cmd_args(&["LPOP", "lst"]));
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_list_lrange() {
    let ctx = setup();
    for ch in &["a", "b", "c", "d", "e"] {
        dispatch_command(&ctx, &cmd_args(&["RPUSH", "lst", ch]));
    }

    let result = dispatch_command(&ctx, &cmd_args(&["LRANGE", "lst", "0", "2"]));
    assert_eq!(
        result,
        RespValue::Array(vec![bulk("a"), bulk("b"), bulk("c")])
    );

    let result = dispatch_command(&ctx, &cmd_args(&["LRANGE", "lst", "2", "-1"]));
    assert_eq!(
        result,
        RespValue::Array(vec![bulk("c"), bulk("d"), bulk("e")])
    );
}

#[tokio::test]
async fn test_list_llen() {
    let ctx = setup();
    assert_eq!(
        dispatch_command(&ctx, &cmd_args(&["LLEN", "lst"])),
        int(0)
    );

    dispatch_command(&ctx, &cmd_args(&["RPUSH", "lst", "a"]));
    dispatch_command(&ctx, &cmd_args(&["RPUSH", "lst", "b"]));

    assert_eq!(
        dispatch_command(&ctx, &cmd_args(&["LLEN", "lst"])),
        int(2)
    );
}

#[tokio::test]
async fn test_list_empty_lpop() {
    let ctx = setup();
    let result = dispatch_command(&ctx, &cmd_args(&["LPOP", "empty"]));
    assert_eq!(result, RespValue::Null);
}

// ── Hash operations ────────────────────────────────────────────────

#[tokio::test]
async fn test_hash_hset_hget() {
    let ctx = setup();

    let result = dispatch_command(&ctx, &cmd_args(&["HSET", "user:1", "name", "Alice"]));
    assert_eq!(result, int(1));

    let result = dispatch_command(&ctx, &cmd_args(&["HSET", "user:1", "age", "30"]));
    assert_eq!(result, int(1));

    let result = dispatch_command(&ctx, &cmd_args(&["HSET", "user:1", "name", "Bob"]));
    assert_eq!(result, int(0));

    let result = dispatch_command(&ctx, &cmd_args(&["HGET", "user:1", "name"]));
    assert_eq!(result, bulk("Bob"));

    let result = dispatch_command(&ctx, &cmd_args(&["HGET", "user:1", "age"]));
    assert_eq!(result, bulk("30"));
}

#[tokio::test]
async fn test_hash_hdel() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["HSET", "h", "f1", "v1"]));
    dispatch_command(&ctx, &cmd_args(&["HSET", "h", "f2", "v2"]));

    let result = dispatch_command(&ctx, &cmd_args(&["HDEL", "h", "f1"]));
    assert_eq!(result, int(1));

    let result = dispatch_command(&ctx, &cmd_args(&["HGET", "h", "f1"]));
    assert_eq!(result, RespValue::Null);

    let result = dispatch_command(&ctx, &cmd_args(&["HDEL", "h", "f1"]));
    assert_eq!(result, int(0));
}

#[tokio::test]
async fn test_hash_hgetall() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["HSET", "h", "k1", "v1"]));
    dispatch_command(&ctx, &cmd_args(&["HSET", "h", "k2", "v2"]));

    let result = dispatch_command(&ctx, &cmd_args(&["HGETALL", "h"]));
    match result {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 4);
            let mut pairs: Vec<_> = items
                .chunks(2)
                .map(|c| {
                    let k = match &c[0] {
                        RespValue::BulkString(s) => s.clone(),
                        _ => panic!("expected bulk string key"),
                    };
                    let v = match &c[1] {
                        RespValue::BulkString(s) => s.clone(),
                        _ => panic!("expected bulk string val"),
                    };
                    (k, v)
                })
                .collect();
            pairs.sort();
            assert_eq!(pairs[0], (b"k1".to_vec(), b"v1".to_vec()));
            assert_eq!(pairs[1], (b"k2".to_vec(), b"v2".to_vec()));
        }
        _ => panic!("expected array"),
    }
}

#[tokio::test]
async fn test_hash_hget_nonexistent() {
    let ctx = setup();
    let result = dispatch_command(&ctx, &cmd_args(&["HGET", "nohash", "field"]));
    assert_eq!(result, RespValue::Null);
}

// ── Set operations ─────────────────────────────────────────────────

#[tokio::test]
async fn test_set_sadd_sismember() {
    let ctx = setup();

    let result = dispatch_command(&ctx, &cmd_args(&["SADD", "tags", "rust"]));
    assert_eq!(result, int(1));

    let result = dispatch_command(&ctx, &cmd_args(&["SADD", "tags", "redis"]));
    assert_eq!(result, int(1));

    let result = dispatch_command(&ctx, &cmd_args(&["SADD", "tags", "rust"]));
    assert_eq!(result, int(0));

    let result = dispatch_command(&ctx, &cmd_args(&["SISMEMBER", "tags", "rust"]));
    assert_eq!(result, int(1));

    let result = dispatch_command(&ctx, &cmd_args(&["SISMEMBER", "tags", "go"]));
    assert_eq!(result, int(0));
}

#[tokio::test]
async fn test_set_smembers() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["SADD", "s", "x"]));
    dispatch_command(&ctx, &cmd_args(&["SADD", "s", "y"]));
    dispatch_command(&ctx, &cmd_args(&["SADD", "s", "z"]));

    let result = dispatch_command(&ctx, &cmd_args(&["SMEMBERS", "s"]));
    match result {
        RespValue::Array(members) => {
            assert_eq!(members.len(), 3);
            let mut sorted = members.clone();
            sorted.sort_by(|a, b| {
                let a_str = a.as_str().unwrap_or("");
                let b_str = b.as_str().unwrap_or("");
                a_str.cmp(b_str)
            });
            assert_eq!(sorted, vec![bulk("x"), bulk("y"), bulk("z")]);
        }
        _ => panic!("expected array"),
    }
}

#[tokio::test]
async fn test_set_smembers_empty() {
    let ctx = setup();
    let result = dispatch_command(&ctx, &cmd_args(&["SMEMBERS", "empty"]));
    assert_eq!(result, RespValue::Array(vec![]));
}

// ── Sorted Set operations ──────────────────────────────────────────

#[tokio::test]
async fn test_sorted_set_zadd_zrange() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["ZADD", "zs", "1", "a"]));
    dispatch_command(&ctx, &cmd_args(&["ZADD", "zs", "3", "c"]));
    dispatch_command(&ctx, &cmd_args(&["ZADD", "zs", "2", "b"]));

    let result = dispatch_command(&ctx, &cmd_args(&["ZRANGE", "zs", "0", "-1"]));
    match result {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 3);
            let member = match &items[0] {
                RespValue::Array(pair) => match &pair[0] {
                    RespValue::BulkString(s) => s.clone(),
                    _ => panic!("expected bulk string"),
                },
                _ => panic!("expected array"),
            };
            assert_eq!(member, b"a");
        }
        _ => panic!("expected array"),
    }
}

#[tokio::test]
async fn test_sorted_set_zscore() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["ZADD", "zs", "42", "item"]));

    let result = dispatch_command(&ctx, &cmd_args(&["ZSCORE", "zs", "item"]));
    assert_eq!(result, bulk("42"));

    let result = dispatch_command(&ctx, &cmd_args(&["ZSCORE", "zs", "missing"]));
    assert_eq!(result, RespValue::Null);
}

// ── PubSub ─────────────────────────────────────────────────────────

#[tokio::test]
async fn test_pubsub_publish_no_subscribers() {
    let ctx = setup();
    let result = dispatch_command(&ctx, &cmd_args(&["PUBLISH", "channel", "hello"]));
    assert_eq!(result, int(0));
}

#[tokio::test]
async fn test_pubsub_subscribe_and_publish() {
    let ctx = setup();
    let mut rx = ctx.broadcaster.subscribe(b"news");

    let result = dispatch_command(&ctx, &cmd_args(&["PUBLISH", "news", "breaking"]));
    assert_eq!(result, int(1));

    let msg = rx.try_recv().unwrap();
    assert_eq!(msg, b"breaking".to_vec());
}

#[tokio::test]
async fn test_pubsub_multiple_subscribers() {
    let ctx = setup();
    let mut rx1 = ctx.broadcaster.subscribe(b"chat");
    let mut rx2 = ctx.broadcaster.subscribe(b"chat");
    let mut rx3 = ctx.broadcaster.subscribe(b"chat");

    let result = dispatch_command(&ctx, &cmd_args(&["PUBLISH", "chat", "hi"]));
    assert_eq!(result, int(3));

    assert_eq!(rx1.try_recv().unwrap(), b"hi".to_vec());
    assert_eq!(rx2.try_recv().unwrap(), b"hi".to_vec());
    assert_eq!(rx3.try_recv().unwrap(), b"hi".to_vec());
}

#[tokio::test]
async fn test_pubsub_channel_isolation() {
    let ctx = setup();
    let mut rx_a = ctx.broadcaster.subscribe(b"ch_a");
    let mut rx_b = ctx.broadcaster.subscribe(b"ch_b");

    dispatch_command(&ctx, &cmd_args(&["PUBLISH", "ch_a", "msg_a"]));

    assert_eq!(rx_a.try_recv().unwrap(), b"msg_a".to_vec());
    assert!(rx_b.try_recv().is_err());
}

#[tokio::test]
async fn test_pubsub_pattern_subscribe() {
    let ctx = setup();
    let mut rx = ctx.broadcaster.psubscribe(b"logs.*");

    let _ = ctx.broadcaster.publish_pattern(b"logs.*", b"entry1");
    assert_eq!(rx.try_recv().unwrap(), b"entry1".to_vec());
}

#[tokio::test]
async fn test_pubsub_publish_matching_glob() {
    let ctx = setup();
    let mut rx1 = ctx.broadcaster.subscribe(b"orders.us");
    let mut rx2 = ctx.broadcaster.subscribe(b"orders.eu");
    let mut rx3 = ctx.broadcaster.subscribe(b"metrics.latency");

    let total = ctx.broadcaster.publish_matching(b"orders.*", b"data");
    assert_eq!(total, 2);
    assert_eq!(rx1.try_recv().unwrap(), b"data".to_vec());
    assert_eq!(rx2.try_recv().unwrap(), b"data".to_vec());
    assert!(rx3.try_recv().is_err());
}

// ── Streams ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_streams_xadd_xread() {
    let ctx = setup();

    let result = dispatch_command(
        &ctx,
        &cmd_args(&["XADD", "mystream", "field1", "val1", "field2", "val2"]),
    );
    match &result {
        RespValue::BulkString(id) => {
            let id_str = std::str::from_utf8(id).unwrap();
            assert!(id_str.parse::<u64>().is_ok());
        }
        _ => panic!("expected bulk string id"),
    }

    let result = dispatch_command(&ctx, &cmd_args(&["XREAD", "STREAMS", "mystream"]));
    assert_ne!(result, RespValue::Null);
}

#[tokio::test]
async fn test_streams_xlen() {
    let ctx = setup();
    assert_eq!(
        dispatch_command(&ctx, &cmd_args(&["XLEN", "mystream"])),
        int(0)
    );

    dispatch_command(&ctx, &cmd_args(&["XADD", "mystream", "k", "v"]));
    dispatch_command(&ctx, &cmd_args(&["XADD", "mystream", "k", "v"]));

    assert_eq!(
        dispatch_command(&ctx, &cmd_args(&["XLEN", "mystream"])),
        int(2)
    );
}

#[tokio::test]
async fn test_streams_xread_nonexistent() {
    let ctx = setup();
    let result = dispatch_command(&ctx, &cmd_args(&["XREAD", "STREAMS", "empty"]));
    assert_eq!(result, RespValue::Null);
}

// ── Job Queue ──────────────────────────────────────────────────────

#[tokio::test]
async fn test_job_queue_add_pop_ack() {
    let ctx = setup();

    let result = dispatch_command(&ctx, &cmd_args(&["QUEUE.ADD", "tasks", "payload_data"]));
    let job_id = match &result {
        RespValue::BulkString(id) => std::str::from_utf8(id).unwrap().to_string(),
        _ => panic!("expected job id"),
    };

    let result = dispatch_command(&ctx, &cmd_args(&["QUEUE.POP", "tasks"]));
    match &result {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            let id = match &items[0] {
                RespValue::BulkString(s) => std::str::from_utf8(s).unwrap(),
                _ => panic!("expected id"),
            };
            assert_eq!(id, job_id);
            let payload = match &items[1] {
                RespValue::BulkString(s) => s,
                _ => panic!("expected payload"),
            };
            assert_eq!(payload, b"payload_data");
        }
        _ => panic!("expected array with job id and payload"),
    }

    let result = dispatch_command(&ctx, &cmd_args(&["QUEUE.ACK", "tasks", &job_id]));
    assert_eq!(result, int(1));
}

#[tokio::test]
async fn test_job_queue_pop_empty() {
    let ctx = setup();
    let result = dispatch_command(&ctx, &cmd_args(&["QUEUE.POP", "empty_queue"]));
    assert_eq!(result, RespValue::Null);
}

#[tokio::test]
async fn test_job_queue_priority() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["QUEUE.ADD", "q", "low", "10"]));
    dispatch_command(&ctx, &cmd_args(&["QUEUE.ADD", "q", "high", "1"]));
    dispatch_command(&ctx, &cmd_args(&["QUEUE.ADD", "q", "mid", "5"]));

    let result = dispatch_command(&ctx, &cmd_args(&["QUEUE.POP", "q"]));
    match &result {
        RespValue::Array(items) => {
            let payload = match &items[1] {
                RespValue::BulkString(s) => s,
                _ => panic!("expected payload"),
            };
            assert_eq!(payload, b"high");
        }
        _ => panic!("expected array"),
    }
}

#[tokio::test]
async fn test_job_queue_ack_unknown_job() {
    let ctx = setup();
    let result = dispatch_command(&ctx, &cmd_args(&["QUEUE.ACK", "q", "nonexistent"]));
    assert_eq!(result, int(0));
}

// ── RESP Protocol Parsing ──────────────────────────────────────────

#[tokio::test]
async fn test_resp_protocol_parse() {
    let mut parser = RespParser::new();

    parser.feed(b"+OK\r\n");
    let frame = parser.parse_frame().unwrap().unwrap();
    assert_eq!(frame, RespValue::SimpleString("OK".to_string()));

    parser.feed(b"-ERR bad\r\n");
    let frame = parser.parse_frame().unwrap().unwrap();
    assert_eq!(frame, RespValue::Error("ERR bad".to_string()));

    parser.feed(b":42\r\n");
    let frame = parser.parse_frame().unwrap().unwrap();
    assert_eq!(frame, RespValue::Integer(42));

    parser.feed(b"$5\r\nhello\r\n");
    let frame = parser.parse_frame().unwrap().unwrap();
    assert_eq!(frame, RespValue::BulkString(b"hello".to_vec()));

    parser.feed(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    let frame = parser.parse_frame().unwrap().unwrap();
    assert_eq!(
        frame,
        RespValue::Array(vec![
            RespValue::BulkString(b"foo".to_vec()),
            RespValue::BulkString(b"bar".to_vec()),
        ])
    );
}

#[tokio::test]
async fn test_resp_partial_frame() {
    let mut parser = RespParser::new();
    parser.feed(b"+OK");
    assert_eq!(parser.parse_frame().unwrap(), None);
    parser.feed(b"\r\n");
    assert_eq!(
        parser.parse_frame().unwrap(),
        Some(RespValue::SimpleString("OK".to_string()))
    );
}

#[tokio::test]
async fn test_resp_null_bulk_string() {
    let mut parser = RespParser::new();
    parser.feed(b"$-1\r\n");
    let frame = parser.parse_frame().unwrap().unwrap();
    assert_eq!(frame, RespValue::Null);
}

#[tokio::test]
async fn test_resp_nested_array() {
    let mut parser = RespParser::new();
    parser.feed(b"*2\r\n*1\r\n$3\r\nfoo\r\n:1\r\n");
    let frame = parser.parse_frame().unwrap().unwrap();
    assert_eq!(
        frame,
        RespValue::Array(vec![
            RespValue::Array(vec![RespValue::BulkString(b"foo".to_vec())]),
            RespValue::Integer(1),
        ])
    );
}

// ── Error handling ─────────────────────────────────────────────────

#[tokio::test]
async fn test_empty_command() {
    let ctx = setup();
    let result = dispatch_command(&ctx, &[]);
    match result {
        RespValue::Error(msg) => assert!(msg.contains("empty command")),
        _ => panic!("expected error"),
    }
}

#[tokio::test]
async fn test_unknown_command() {
    let ctx = setup();
    let result = dispatch_command(&ctx, &cmd_args(&["NONEXISTENT"]));
    match result {
        RespValue::Error(msg) => assert!(msg.contains("unknown command")),
        _ => panic!("expected error"),
    }
}

#[tokio::test]
async fn test_wrong_arg_count_set() {
    let ctx = setup();
    let result = dispatch_command(&ctx, &cmd_args(&["SET"]));
    match result {
        RespValue::Error(msg) => assert!(msg.contains("wrong number of arguments")),
        _ => panic!("expected error"),
    }
}

// ── PING / DBSIZE / FLUSHALL ──────────────────────────────────────

#[tokio::test]
async fn test_ping() {
    let ctx = setup();
    let result = dispatch_command(&ctx, &cmd_args(&["PING"]));
    assert_eq!(result, RespValue::SimpleString("PONG".to_string()));
}

#[tokio::test]
async fn test_dbsize() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["SET", "a", "1"]));
    dispatch_command(&ctx, &cmd_args(&["SET", "b", "2"]));
    let result = dispatch_command(&ctx, &cmd_args(&["DBSIZE"]));
    assert_eq!(result, int(2));
}

#[tokio::test]
async fn test_flushall() {
    let ctx = setup();
    dispatch_command(&ctx, &cmd_args(&["SET", "a", "1"]));
    dispatch_command(&ctx, &cmd_args(&["SET", "b", "2"]));
    let result = dispatch_command(&ctx, &cmd_args(&["FLUSHALL"]));
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    let result = dispatch_command(&ctx, &cmd_args(&["DBSIZE"]));
    assert_eq!(result, int(0));
}
