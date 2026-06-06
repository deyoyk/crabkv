use criterion::{black_box, criterion_group, criterion_main, Criterion};
use crabkv::protocol::RespParser;

fn bench_parse_simple_string(c: &mut Criterion) {
    c.bench_function("parse simple string", |b| {
        b.iter(|| {
            let mut parser = RespParser::new();
            parser.feed(black_box(b"+OK\r\n"));
            black_box(parser.parse_frame().unwrap());
        })
    });
}

fn bench_parse_bulk_string(c: &mut Criterion) {
    let data = format!("${}\r\n{}\r\n", 1000, "x".repeat(1000));
    c.bench_function("parse 1KB bulk string", |b| {
        b.iter(|| {
            let mut parser = RespParser::new();
            parser.feed(black_box(data.as_bytes()));
            black_box(parser.parse_frame().unwrap());
        })
    });
}

fn bench_parse_array(c: &mut Criterion) {
    let cmd = "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
    c.bench_function("parse 3-element array", |b| {
        b.iter(|| {
            let mut parser = RespParser::new();
            parser.feed(black_box(cmd.as_bytes()));
            black_box(parser.parse_frame().unwrap());
        })
    });
}

fn bench_parse_integer(c: &mut Criterion) {
    c.bench_function("parse integer", |b| {
        b.iter(|| {
            let mut parser = RespParser::new();
            parser.feed(black_box(b":12345\r\n"));
            black_box(parser.parse_frame().unwrap());
        })
    });
}

fn bench_parse_large_array(c: &mut Criterion) {
    let mut cmd = String::from("*100\r\n");
    for i in 0..100 {
        let val = format!("${}\r\nval{}\r\n", format!("{}", i).len() + 3, i);
        cmd.push_str(&val);
    }
    c.bench_function("parse 100-element array", |b| {
        b.iter(|| {
            let mut parser = RespParser::new();
            parser.feed(black_box(cmd.as_bytes()));
            black_box(parser.parse_frame().unwrap());
        })
    });
}

fn bench_parse_error(c: &mut Criterion) {
    c.bench_function("parse error", |b| {
        b.iter(|| {
            let mut parser = RespParser::new();
            parser.feed(black_box(b"-ERR something went wrong\r\n"));
            black_box(parser.parse_frame().unwrap());
        })
    });
}

criterion_group!(
    benches,
    bench_parse_simple_string,
    bench_parse_bulk_string,
    bench_parse_array,
    bench_parse_integer,
    bench_parse_large_array,
    bench_parse_error
);
criterion_main!(benches);
