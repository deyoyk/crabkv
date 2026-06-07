#!/usr/bin/env python3

import socket
import time
import threading
import argparse
from concurrent.futures import ThreadPoolExecutor

SERVERS = {
    "CrabKV": ("127.0.0.1", 6380),
    "Redis": ("127.0.0.1", 6379)
}

TESTS = [
    ("PING", lambda i: ["PING"]),
    ("SET", lambda i: ["SET", f"key:{i}", f"value:{i}"]),
    ("GET", lambda i: ["GET", f"key:{i}"]),
    ("DEL", lambda i: ["DEL", f"key:{i}"]),
    ("EXISTS", lambda i: ["EXISTS", f"key:{i}"]),
    ("INCR", lambda i: ["INCR", f"counter:{i % 100}"]),
    ("HSET", lambda i: ["HSET", "hash", f"field{i}", f"value{i}"]),
    ("HGET", lambda i: ["HGET", "hash", f"field{i}"]),
    ("LPUSH", lambda i: ["LPUSH", "list", f"value{i}"]),
    ("LRANGE", lambda i: ["LRANGE", "list", "0", "50"]),
    ("SADD", lambda i: ["SADD", "set", f"member{i}"]),
    ("SISMEMBER", lambda i: ["SISMEMBER", "set", f"member{i}"]),
    ("ZADD", lambda i: ["ZADD", "zset", str(i), f"member{i}"]),
    ("ZRANGE", lambda i: ["ZRANGE", "zset", "0", "50"]),
]


def encode_resp(args):
    cmd = f"*{len(args)}\r\n"

    for arg in args:
        arg = str(arg)
        cmd += f"${len(arg)}\r\n{arg}\r\n"

    return cmd.encode()


class RESPReader:
    __slots__ = ('sock', 'buf', 'pos')

    def __init__(self, sock):
        self.sock = sock
        self.buf = b''
        self.pos = 0

    def _fill(self, n=4096):
        data = self.sock.recv(n)
        if data:
            self.buf = self.buf[self.pos:] + data
            self.pos = 0
        return len(data)

    def _readline(self):
        while True:
            start = self.pos
            buf = self.buf
            idx = buf.find(b'\r\n', start)
            if idx != -1:
                self.pos = idx + 2
                return buf[start:idx]
            if self._fill() == 0:
                return buf[start:]

    def read(self):
        line = self._readline()
        if not line:
            return None

        prefix = line[0:1]

        if prefix in (b'+', b'-', b':'):
            return

        if prefix == b'$':
            size = int(line[1:])
            if size >= 0:
                self._skip(size + 2)
            return

        if prefix == b'*':
            count = int(line[1:])
            for _ in range(max(count, 0)):
                self.read()
            return

    def _skip(self, n):
        avail = len(self.buf) - self.pos
        if n <= avail:
            self.pos += n
            return
        n -= avail
        self.buf = b''
        self.pos = 0
        while n > 0:
            data = self.sock.recv(min(4096, n))
            if not data:
                break
            n -= len(data)


def read_response(sock, reader=None):
    if reader is None:
        reader = RESPReader(sock)
    reader.read()


def worker(host, port, cmd_builder, operations):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    sock.connect((host, port))

    reader = RESPReader(sock)
    start = time.perf_counter()

    for i in range(operations):
        sock.sendall(encode_resp(cmd_builder(i)))
        read_response(sock, reader)

    elapsed = time.perf_counter() - start

    sock.close()

    return operations / elapsed


def benchmark_server(name, host, port, operations, workers):
    print(f"\n{name} ({host}:{port})")

    results = {}

    for test_name, builder in TESTS:

        try:
            with ThreadPoolExecutor(max_workers=workers) as executor:

                futures = [
                    executor.submit(
                        worker,
                        host,
                        port,
                        builder,
                        operations // workers,
                    )
                    for _ in range(workers)
                ]

                throughputs = [f.result() for f in futures]

            total = sum(throughputs)

            results[test_name] = total

            print(f"{test_name:<12} {total:>12,.0f} ops/s")

        except Exception as e:
            results[test_name] = None
            print(f"{test_name:<12} ERROR ({e})")

    return results


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-n",
        "--operations",
        type=int,
        default=10000,
        help="Operations per benchmark",
    )

    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=8,
        help="Worker threads",
    )

    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
    )

    parser.add_argument(
        "--crabkv-port",
        type=int,
        default=6380,
    )

    args = parser.parse_args()

    SERVERS["Redis"] = ("127.0.0.1", args.redis_port)
    SERVERS["CrabKV"] = ("127.0.0.1", args.crabkv_port)

    all_results = {}

    print("=" * 70)
    print("Redis vs CrabKV Benchmark")
    print("=" * 70)

    for name, (host, port) in SERVERS.items():
        all_results[name] = benchmark_server(
            name,
            host,
            port,
            args.operations,
            args.workers,
        )

    print("\n" + "=" * 90)
    print("COMPARISON")
    print("=" * 90)

    print(
        f"{'Command':<15}"
        f"{'Redis':>15}"
        f"{'CrabKV':>15}"
        f"{'Ratio':>12}"
        f"{'Winner':>12}"
    )

    print("-" * 90)

    redis_total = 0
    crab_total = 0
    count = 0

    for cmd, _ in TESTS:
        r = all_results["Redis"].get(cmd)
        c = all_results["CrabKV"].get(cmd)

        if r is None or c is None:
            continue

        ratio = (c / r) * 100

        winner = "CrabKV" if c > r else "Redis"

        redis_total += r
        crab_total += c
        count += 1

        print(
            f"{cmd:<15}"
            f"{r:>15,.0f}"
            f"{c:>15,.0f}"
            f"{ratio:>11.1f}%"
            f"{winner:>12}"
        )

    print("-" * 90)

    if count:
        avg_ratio = (crab_total / redis_total) * 100

        print(f"\nRedis Total : {redis_total:,.0f} ops/s")
        print(f"CrabKV Total: {crab_total:,.0f} ops/s")
        print(f"CrabKV Relative Performance: {avg_ratio:.2f}%")

        if avg_ratio > 100:
            print("CrabKV wins overall!")

        elif avg_ratio >= 90:
            print("CrabKV is within 10% of Redis.")

        else:
            print("Redis is faster overall.")


if __name__ == "__main__":
    main()