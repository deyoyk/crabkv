#!/usr/bin/env python3

import socket
import time
import os
import argparse
import multiprocessing
import statistics

_INT_CACHE = {}

def resp_int_str(n):
    if n not in _INT_CACHE:
        s = str(n).encode()
        _INT_CACHE[n] = b'$' + str(len(s)).encode() + b'\r\n' + s + b'\r\n'
    return _INT_CACHE[n]

def resp_bulk(s):
    if isinstance(s, str):
        s = s.encode()
    return b'$' + str(len(s)).encode() + b'\r\n' + s + b'\r\n'

def resp_cmd(*args):
    parts = [b'*' + str(len(args)).encode() + b'\r\n']
    for arg in args:
        if isinstance(arg, int):
            parts.append(resp_int_str(arg))
        elif isinstance(arg, bytes):
            parts.append(b'$' + str(len(arg)).encode() + b'\r\n' + arg + b'\r\n')
        else:
            s = str(arg).encode()
            parts.append(b'$' + str(len(s)).encode() + b'\r\n' + s + b'\r\n')
    return b''.join(parts)

PING = resp_cmd('PING')
SET_CMD_TPL = lambda k, v: resp_cmd('SET', k, v)
GET_CMD_TPL = lambda k: resp_cmd('GET', k)
DEL_CMD_TPL = lambda k: resp_cmd('DEL', k)
MSET_CMD_TPL = lambda pairs: b''.join(
    [b'*' + str(1 + len(pairs) * 2).encode() + b'\r\n', b'$4\r\nMSET\r\n'] +
    [b for k, v in pairs for b in [resp_bulk(k), resp_bulk(v)]]
)
MGET_CMD_TPL = lambda keys: b''.join(
    [b'*' + str(1 + len(keys)).encode() + b'\r\n', b'$4\r\nMGET\r\n'] +
    [resp_bulk(k) for k in keys]
)

def count_resp_responses(data, expected):
    count = 0
    pos = 0
    length = len(data)

    while count < expected and pos < length:
        prefix = data[pos:pos + 1]

        if prefix in (b'+', b'-', b':'):
            nl = data.find(b'\r\n', pos + 1)
            if nl == -1:
                break
            pos = nl + 2
            count += 1

        elif prefix == b'$':
            nl = data.find(b'\r\n', pos + 1)
            if nl == -1:
                break

            bulk_len = int(data[pos + 1:nl])

            if bulk_len == -1:
                pos = nl + 2
                count += 1
                continue

            payload_end = nl + 2 + bulk_len

            if payload_end + 2 > length:
                break

            pos = payload_end + 2
            count += 1

        elif prefix == b'*':
            nl = data.find(b'\r\n', pos + 1)

            if nl == -1:
                break

            arr_count = int(data[pos + 1:nl])
            pos = nl + 2

            if arr_count == -1:
                count += 1
                continue

            for _ in range(arr_count):
                if pos >= length:
                    break

                ep = data[pos:pos + 1]

                if ep in (b'+', b'-', b':'):
                    nl2 = data.find(b'\r\n', pos + 1)
                    if nl2 == -1:
                        break
                    pos = nl2 + 2

                elif ep == b'$':
                    nl2 = data.find(b'\r\n', pos + 1)
                    if nl2 == -1:
                        break

                    bl = int(data[pos + 1:nl2])

                    if bl == -1:
                        pos = nl2 + 2
                    else:
                        pe = nl2 + 2 + bl

                        if pe + 2 > length:
                            break

                        pos = pe + 2

                else:
                    break

            count += 1

        else:
            break

    return count, pos


class PipelinedClient:
    def __init__(self, host, port, pipeline_size=100):
        self.host = host
        self.port = port
        self.pipeline_size = pipeline_size
        self.sock = None
        self.buf = b''
        self.connect()

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.sock.settimeout(30)

        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024 * 1024)
        except Exception:
            pass

        self.sock.connect((self.host, self.port))

    def close(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass

            self.sock = None

    def flush_pipeline(self, cmds, expected):
        self.sock.sendall(b''.join(cmds))

        while True:
            try:
                data = self.sock.recv(65536)

                if not data:
                    break

                self.buf += data

            except socket.timeout:
                break

            count, consumed = count_resp_responses(self.buf, expected)

            if count >= expected:
                self.buf = self.buf[consumed:]
                return count

        count, consumed = count_resp_responses(self.buf, expected)
        self.buf = self.buf[consumed:]

        return count

    def drain_to(self, target_count):
        while True:
            count, consumed = count_resp_responses(self.buf, target_count)

            if count >= target_count:
                self.buf = self.buf[consumed:]
                return

            try:
                data = self.sock.recv(65536)

                if not data:
                    break

                self.buf += data

            except socket.timeout:
                break


def bench_ping_pipeline(args):
    host, port, ops, pipeline_size = args

    client = PipelinedClient(host, port, pipeline_size)
    cmds = [PING for _ in range(pipeline_size)]

    batch = ops // pipeline_size

    start = time.perf_counter()

    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)

    elapsed = time.perf_counter() - start

    client.close()

    return batch * pipeline_size, elapsed


def bench_set_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args

    client = PipelinedClient(host, port, pipeline_size)

    batch = ops // pipeline_size

    start = time.perf_counter()

    for b in range(batch):
        actual_cmds = [
            SET_CMD_TPL(
                f'pw:{worker_id}:{b * pipeline_size + i}',
                f'v{i}'
            )
            for i in range(pipeline_size)
        ]

        client.flush_pipeline(actual_cmds, pipeline_size)

    elapsed = time.perf_counter() - start

    client.close()

    return batch * pipeline_size, elapsed


def bench_get_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args

    client = PipelinedClient(host, port, pipeline_size)

    populate = [
        SET_CMD_TPL(f'pg:{worker_id}:{i}', f'v{i}')
        for i in range(pipeline_size)
    ]

    client.flush_pipeline(populate, pipeline_size)

    cmds = [
        GET_CMD_TPL(f'pg:{worker_id}:{i}')
        for i in range(pipeline_size)
    ]

    batch = ops // pipeline_size

    start = time.perf_counter()

    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)

    elapsed = time.perf_counter() - start

    client.close()

    return batch * pipeline_size, elapsed


def bench_setget_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args

    client = PipelinedClient(host, port, pipeline_size)

    half = pipeline_size // 2
    batch = ops // pipeline_size

    start = time.perf_counter()

    for b in range(batch):
        cmds = []

        for i in range(half):
            k = f'psg:{worker_id}:{b * half + i}'

            cmds.append(SET_CMD_TPL(k, f'v{i}'))
            cmds.append(GET_CMD_TPL(k))

        client.flush_pipeline(cmds, half * 2)

    elapsed = time.perf_counter() - start

    client.close()

    return batch * pipeline_size, elapsed


def bench_mset_mget_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args

    client = PipelinedClient(host, port, pipeline_size)

    batch = ops // pipeline_size

    start = time.perf_counter()

    for b in range(batch):
        pairs = [
            (f'pm:{worker_id}:{b * pipeline_size + i}', f'val{i}')
            for i in range(pipeline_size)
        ]

        mget_keys = [k for k, _ in pairs]

        cmds = [
            MSET_CMD_TPL(pairs),
            MGET_CMD_TPL(mget_keys)
        ]

        client.flush_pipeline(cmds, 2)

    elapsed = time.perf_counter() - start

    client.close()

    return batch * pipeline_size, elapsed


def bench_incr_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args

    client = PipelinedClient(host, port, pipeline_size)

    init_cmd = SET_CMD_TPL(f'pcnt:{worker_id}', '0')

    client.flush_pipeline([init_cmd], 1)

    incr_cmd = resp_cmd('INCR', f'pcnt:{worker_id}')
    cmds = [incr_cmd] * pipeline_size

    batch = ops // pipeline_size

    start = time.perf_counter()

    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)

    elapsed = time.perf_counter() - start

    client.close()

    return batch * pipeline_size, elapsed


def run_benchmark(name, worker_fn, host, port, total_ops, pipeline_size, num_workers):
    ops_per_worker = total_ops // num_workers

    pool_args = [
        (host, port, ops_per_worker, pipeline_size, i)
        for i in range(num_workers)
    ]

    if 'ping' in name.lower():
        pool_args = [
            (host, port, ops_per_worker, pipeline_size)
            for _ in range(num_workers)
        ]

    start = time.perf_counter()

    with multiprocessing.Pool(num_workers) as pool:
        results = pool.map(worker_fn, pool_args)

    wall_time = time.perf_counter() - start

    total_done = sum(r[0] for r in results)
    worker_times = [r[1] for r in results]

    throughput = total_done / wall_time if wall_time > 0 else 0

    print(
        f"  {name:<35} "
        f"Ops: {total_done:>10,}  |  "
        f"Throughput: {throughput:>12,.0f} ops/s  |  "
        f"Wall: {wall_time:>6.2f}s  |  "
        f"Workers: {num_workers}  |  "
        f"Pipeline: {pipeline_size}"
    )

    return throughput


def main():
    parser = argparse.ArgumentParser(
        description="CrabKV Ultra-High-Throughput Stress Test"
    )

    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)

    parser.add_argument(
        "--ops",
        "-n",
        type=int,
        default=1_000_000,
        help="Total ops per test (default: 1M)"
    )

    parser.add_argument(
        "--pipeline",
        "-p",
        type=int,
        default=256,
        help="Pipeline depth (default: 256)"
    )

    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=None,
        help="Worker processes (default: CPU count)"
    )

    parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick mode: fewer ops for fast validation"
    )

    args = parser.parse_args()

    num_workers = args.workers or min(os.cpu_count() or 4, 16)
    total_ops = args.ops
    pipeline_size = args.pipeline

    if args.quick:
        total_ops = min(total_ops, 100_000)
        pipeline_size = min(pipeline_size, 64)

    print("=" * 80)
    print("  CrabKV Ultra-High-Throughput Stress Test")
    print("=" * 80)
    print(f"  Target:     {args.host}:{args.port}")
    print(f"  Total Ops:  {total_ops:,}")
    print(f"  Pipeline:   {pipeline_size}")
    print(f"  Workers:    {num_workers}")
    print(f"  OS CPUs:    {os.cpu_count()}")
    print()

    try:
        c = PipelinedClient(args.host, args.port, 1)
        c.flush_pipeline([PING], 1)
        c.close()

        print("  Connected!\n")

    except Exception as e:
        print(f"  ERROR: Cannot connect to {args.host}:{args.port} - {e}")
        print("  Make sure CrabKV is running: cargo run --release")
        return

    tests = [
        ("PING (pipelined)", bench_ping_pipeline),
        ("SET (pipelined)", bench_set_pipeline),
        ("GET (pipelined)", bench_get_pipeline),
        ("SET+GET pair (pipelined)", bench_setget_pipeline),
        ("MSET+MGET 100 keys", bench_mset_mget_pipeline),
        ("INCR (pipelined)", bench_incr_pipeline),
    ]

    throughputs = []

    for name, fn in tests:
        try:
            t = run_benchmark(
                name,
                fn,
                args.host,
                args.port,
                total_ops,
                pipeline_size,
                num_workers
            )

            throughputs.append(t)

        except Exception as e:
            print(f"  {name}: ERROR - {e}")

    print()
    print("=" * 80)

    if throughputs:
        best = max(throughputs)

        print(f"  Best throughput: {best:>12,.0f} ops/s")

        if best >= 1_000_000:
            print("  TARGET REACHED: 1M+ ops/s!")
        else:
            print(f"  Gap to 1M: {(1_000_000 - best):>12,.0f} ops/s")
            print(f"  Try: --pipeline 512 --workers {num_workers * 2}")

    print("=" * 80)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()