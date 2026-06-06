use criterion::{black_box, criterion_group, criterion_main, Criterion};
use crabkv::storage::{StorageEngine, StorageValue, EvictionPolicy};
use std::sync::Arc;
use std::thread;

fn engine() -> StorageEngine {
    StorageEngine::with_config(256, 1024 * 1024 * 1024, EvictionPolicy::Lru)
}

fn bench_set(c: &mut Criterion) {
    let e = engine();
    let val = StorageValue::String(b"value_that_is_about_1_kilobyte_in_length_padded_with_extra_data_to_reach_the_target_size!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!".to_vec());
    c.bench_function("SET 1KB", |b| {
        b.iter(|| {
            e.set(black_box(b"key".to_vec()), black_box(val.clone()));
        })
    });
}

fn bench_get(c: &mut Criterion) {
    let e = engine();
    e.set(b"key".to_vec(), StorageValue::String(b"hello world".to_vec()));
    c.bench_function("GET 1KB", |b| {
        b.iter(|| {
            black_box(e.get(black_box(b"key")));
        })
    });
}

fn bench_mset(c: &mut Criterion) {
    let e = engine();
    let pairs: Vec<(Vec<u8>, StorageValue)> = (0..1000)
        .map(|i| {
            let key = format!("key_{:06}", i).into_bytes();
            (key, StorageValue::String(b"val_XXXXXXXXXXXXXXXX".to_vec()))
        })
        .collect();

    c.bench_function("MSET 1000 keys", |b| {
        b.iter(|| {
            e.mset(black_box(pairs.clone()));
        })
    });
}

fn bench_mget(c: &mut Criterion) {
    let e = engine();
    let keys: Vec<Vec<u8>> = (0..1000)
        .map(|i| format!("key_{:06}", i).into_bytes())
        .collect();
    for k in &keys {
        e.set(k.clone(), StorageValue::String(b"hello".to_vec()));
    }

    c.bench_function("MGET 1000 keys", |b| {
        b.iter(|| {
            black_box(e.mget(black_box(&keys)));
        })
    });
}

fn bench_incr(c: &mut Criterion) {
    let e = engine();
    e.set(b"counter".to_vec(), StorageValue::String(b"0".to_vec()));
    c.bench_function("INCR", |b| {
        b.iter(|| {
            black_box(e.incr(black_box(b"counter"), 1).unwrap());
        })
    });
}

fn bench_concurrent_set(c: &mut Criterion) {
    let e = Arc::new(engine());
    c.bench_function("concurrent SET 4 threads", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..4)
                .map(|t| {
                    let e = e.clone();
                    thread::spawn(move || {
                        for i in 0..250 {
                            let key = format!("t{}k{}", t, i).into_bytes();
                            e.set(key, StorageValue::String(b"value".to_vec()));
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        })
    });
}

fn bench_concurrent_get(c: &mut Criterion) {
    let e = Arc::new(engine());
    for i in 0..1000 {
        let key = format!("key_{}", i).into_bytes();
        e.set(key, StorageValue::String(b"value".to_vec()));
    }
    c.bench_function("concurrent GET 4 threads", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..4)
                .map(|t| {
                    let e = e.clone();
                    thread::spawn(move || {
                        for i in 0..250 {
                            let key = format!("key_{}", t * 250 + i).into_bytes();
                            black_box(e.get(&key));
                        }
                    })
                })
                .collect();
            for h in handles {
                h.join().unwrap();
            }
        })
    });
}

fn bench_list_push(c: &mut Criterion) {
    let e = engine();
    c.bench_function("RPUSH single element", |b| {
        b.iter(|| {
            e.rpush(
                black_box(b"mylist".to_vec()),
                black_box(vec![b"element_value".to_vec()]),
            )
            .unwrap();
        })
    });
}

fn bench_hash_set_get(c: &mut Criterion) {
    let e = engine();
    e.hset(b"myhash".to_vec(), b"field".to_vec(), b"val".to_vec())
        .unwrap();

    let mut group = c.benchmark_group("hash");
    group.bench_function("HSET", |b| {
        b.iter(|| {
            e.hset(
                black_box(b"myhash".to_vec()),
                black_box(b"field".to_vec()),
                black_box(b"newval".to_vec()),
            )
            .unwrap();
        })
    });
    group.bench_function("HGET", |b| {
        b.iter(|| {
            black_box(e.hget(black_box(b"myhash"), black_box(b"field")));
        })
    });
    group.finish();
}

fn bench_set_add(c: &mut Criterion) {
    let e = engine();
    c.bench_function("SADD", |b| {
        b.iter(|| {
            e.sadd(
                black_box(b"myset".to_vec()),
                black_box(vec![b"member".to_vec()]),
            )
            .unwrap();
        })
    });
}

fn bench_zadd(c: &mut Criterion) {
    let e = engine();
    c.bench_function("ZADD", |b| {
        b.iter(|| {
            e.zadd(
                black_box(b"myzset".to_vec()),
                black_box(1),
                black_box(b"member".to_vec()),
            )
            .unwrap();
        })
    });
}

criterion_group!(
    benches,
    bench_set,
    bench_get,
    bench_mset,
    bench_mget,
    bench_incr,
    bench_concurrent_set,
    bench_concurrent_get,
    bench_list_push,
    bench_hash_set_get,
    bench_set_add,
    bench_zadd
);
criterion_main!(benches);
