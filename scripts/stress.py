#!/usr/bin/env python3

import socket
import time
import os
import argparse
import multiprocessing

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
EXISTS_CMD_TPL = lambda k: resp_cmd('EXISTS', k)

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

    def send_recv(self, cmd):
        self.sock.sendall(cmd)
        data = self.sock.recv(65536)
        self.buf += data
        count, consumed = count_resp_responses(self.buf, 1)
        self.buf = self.buf[consumed:]
        return count


# =============================================================================
# STRING COMMANDS
# =============================================================================

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
        cmds = [
            SET_CMD_TPL(f'pw:{worker_id}:{b * pipeline_size + i}', f'v{i}')
            for i in range(pipeline_size)
        ]
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_get_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [SET_CMD_TPL(f'pg:{worker_id}:{i}', f'v{i}') for i in range(pipeline_size)]
    client.flush_pipeline(populate, pipeline_size)
    cmds = [GET_CMD_TPL(f'pg:{worker_id}:{i}') for i in range(pipeline_size)]
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
        cmds = [MSET_CMD_TPL(pairs), MGET_CMD_TPL(mget_keys)]
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


def bench_del_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    batch = ops // pipeline_size
    start = time.perf_counter()
    for b in range(batch):
        populate = [
            SET_CMD_TPL(f'pdel:{worker_id}:{b * pipeline_size + i}', f'v{i}')
            for i in range(pipeline_size)
        ]
        client.flush_pipeline(populate, pipeline_size)
        deletes = [
            DEL_CMD_TPL(f'pdel:{worker_id}:{b * pipeline_size + i}')
            for i in range(pipeline_size)
        ]
        client.flush_pipeline(deletes, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_exists_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [
        SET_CMD_TPL(f'pexists:{worker_id}:{i}', f'v{i}')
        for i in range(pipeline_size)
    ]
    client.flush_pipeline(populate, pipeline_size)
    cmds = [
        EXISTS_CMD_TPL(f'pexists:{worker_id}:{i % pipeline_size}')
        for i in range(pipeline_size)
    ]
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_incrby_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    init_cmd = SET_CMD_TPL(f'pincrby:{worker_id}', '0')
    client.flush_pipeline([init_cmd], 1)
    incrby_cmd = resp_cmd('INCRBY', f'pincrby:{worker_id}', '10')
    cmds = [incrby_cmd] * pipeline_size
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_decr_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    init_cmd = SET_CMD_TPL(f'pdecr:{worker_id}', str(ops + 1000))
    client.flush_pipeline([init_cmd], 1)
    decr_cmd = resp_cmd('DECR', f'pdecr:{worker_id}')
    cmds = [decr_cmd] * pipeline_size
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_append_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    init_cmd = SET_CMD_TPL(f'pappend:{worker_id}', 'base')
    client.flush_pipeline([init_cmd], 1)
    append_cmd = resp_cmd('APPEND', f'pappend:{worker_id}', 'x')
    cmds = [append_cmd] * pipeline_size
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_setex_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    batch = ops // pipeline_size
    start = time.perf_counter()
    for b in range(batch):
        cmds = [
            resp_cmd('SET', f'psetex:{worker_id}:{b * pipeline_size + i}', f'v{i}', 'EX', '3600')
            for i in range(pipeline_size)
        ]
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


# =============================================================================
# LIST COMMANDS
# =============================================================================

def bench_lpush_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    batch = ops // pipeline_size
    start = time.perf_counter()
    for b in range(batch):
        cmds = [
            resp_cmd('LPUSH', f'plist:{worker_id}', f'val{i}')
            for i in range(pipeline_size)
        ]
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_rpush_lpop_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    batch = ops // pipeline_size
    start = time.perf_counter()
    for b in range(batch):
        cmds = []
        for i in range(pipeline_size):
            cmds.append(resp_cmd('RPUSH', f'prlist:{worker_id}', f'v{i}'))
            cmds.append(resp_cmd('LPOP', f'prlist:{worker_id}'))
        client.flush_pipeline(cmds, pipeline_size * 2)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_lrange_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [
        resp_cmd('RPUSH', f'plrlist:{worker_id}', f'v{i}')
        for i in range(100)
    ]
    client.flush_pipeline(populate, 100)
    cmds = [resp_cmd('LRANGE', f'plrlist:{worker_id}', '0', '49')] * pipeline_size
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_llen_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [
        resp_cmd('RPUSH', f'pllen:{worker_id}', f'v{i}')
        for i in range(50)
    ]
    client.flush_pipeline(populate, 50)
    cmds = [resp_cmd('LLEN', f'pllen:{worker_id}')] * pipeline_size
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


# =============================================================================
# HASH COMMANDS
# =============================================================================

def bench_hset_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    batch = ops // pipeline_size
    start = time.perf_counter()
    for b in range(batch):
        cmds = [
            resp_cmd('HSET', f'phash:{worker_id}', f'field{i}', f'val{i}')
            for i in range(pipeline_size)
        ]
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_hget_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [
        resp_cmd('HSET', f'phget:{worker_id}', f'field{i}', f'val{i}')
        for i in range(pipeline_size)
    ]
    client.flush_pipeline(populate, pipeline_size)
    cmds = [
        resp_cmd('HGET', f'phget:{worker_id}', f'field{i}')
        for i in range(pipeline_size)
    ]
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_hgetall_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [
        resp_cmd('HSET', f'phgetall:{worker_id}', f'field{i}', f'val{i}')
        for i in range(20)
    ]
    client.flush_pipeline(populate, 20)
    cmds = [resp_cmd('HGETALL', f'phgetall:{worker_id}')] * pipeline_size
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_hset_hget_pair_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    half = pipeline_size // 2
    batch = ops // pipeline_size
    start = time.perf_counter()
    for b in range(batch):
        cmds = []
        for i in range(half):
            k = f'phsg:{worker_id}'
            cmds.append(resp_cmd('HSET', k, f'f{i}', f'v{i}'))
            cmds.append(resp_cmd('HGET', k, f'f{i}'))
        client.flush_pipeline(cmds, half * 2)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_hdel_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [
        resp_cmd('HSET', f'phdel:{worker_id}', f'field{i}', f'val{i}')
        for i in range(pipeline_size * 2)
    ]
    client.flush_pipeline(populate, pipeline_size * 2)
    cmds = [
        resp_cmd('HDEL', f'phdel:{worker_id}', f'field{i}')
        for i in range(pipeline_size)
    ]
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


# =============================================================================
# SET COMMANDS
# =============================================================================

def bench_sadd_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    batch = ops // pipeline_size
    start = time.perf_counter()
    for b in range(batch):
        cmds = [
            resp_cmd('SADD', f'pset:{worker_id}', f'member{i}')
            for i in range(pipeline_size)
        ]
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_sismember_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [
        resp_cmd('SADD', f'psism:{worker_id}', f'm{i}')
        for i in range(pipeline_size)
    ]
    client.flush_pipeline(populate, pipeline_size)
    cmds = [
        resp_cmd('SISMEMBER', f'psism:{worker_id}', f'm{i % pipeline_size}')
        for i in range(pipeline_size)
    ]
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_scard_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [
        resp_cmd('SADD', f'pscard:{worker_id}', f'm{i}')
        for i in range(100)
    ]
    client.flush_pipeline(populate, 100)
    cmds = [resp_cmd('SCARD', f'pscard:{worker_id}')] * pipeline_size
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_smembers_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [
        resp_cmd('SADD', f'psmem:{worker_id}', f'm{i}')
        for i in range(50)
    ]
    client.flush_pipeline(populate, 50)
    cmds = [resp_cmd('SMEMBERS', f'psmem:{worker_id}')] * pipeline_size
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


# =============================================================================
# SORTED SET COMMANDS
# =============================================================================

def bench_zadd_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    batch = ops // pipeline_size
    start = time.perf_counter()
    for b in range(batch):
        cmds = [
            resp_cmd('ZADD', f'pzset:{worker_id}', str(i), f'member{i}')
            for i in range(pipeline_size)
        ]
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_zrange_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [
        resp_cmd('ZADD', f'pzrange:{worker_id}', str(i), f'm{i}')
        for i in range(100)
    ]
    client.flush_pipeline(populate, 100)
    cmds = [resp_cmd('ZRANGE', f'pzrange:{worker_id}', '0', '49')] * pipeline_size
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_zscore_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [
        resp_cmd('ZADD', f'pzscore:{worker_id}', str(i * 10), f'm{i}')
        for i in range(pipeline_size)
    ]
    client.flush_pipeline(populate, pipeline_size)
    cmds = [
        resp_cmd('ZSCORE', f'pzscore:{worker_id}', f'm{i}')
        for i in range(pipeline_size)
    ]
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


# =============================================================================
# STREAM COMMANDS
# =============================================================================

def bench_xadd_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    batch = ops // pipeline_size
    start = time.perf_counter()
    for b in range(batch):
        cmds = [
            resp_cmd('XADD', f'pstream:{worker_id}', '*', 'field', f'val{i}')
            for i in range(pipeline_size)
        ]
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_xrange_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [
        resp_cmd('XADD', f'pxrange:{worker_id}', '*', 'data', f'v{i}')
        for i in range(100)
    ]
    client.flush_pipeline(populate, 100)
    cmds = [resp_cmd('XRANGE', f'pxrange:{worker_id}', '-', '+')] * pipeline_size
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_xadd_xlen_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    half = pipeline_size // 2
    batch = ops // pipeline_size
    start = time.perf_counter()
    for b in range(batch):
        cmds = []
        for i in range(half):
            cmds.append(resp_cmd('XADD', f'pxlen:{worker_id}', '*', 'k', f'v{i}'))
            cmds.append(resp_cmd('XLEN', f'pxlen:{worker_id}'))
        client.flush_pipeline(cmds, half * 2)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


# =============================================================================
# KEY MANAGEMENT / TTL COMMANDS
# =============================================================================

def bench_expire_ttl_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [
        SET_CMD_TPL(f'pttl:{worker_id}:{i}', f'v{i}')
        for i in range(pipeline_size)
    ]
    client.flush_pipeline(populate, pipeline_size)
    cmds = []
    for i in range(pipeline_size):
        cmds.append(resp_cmd('EXPIRE', f'pttl:{worker_id}:{i}', '3600'))
        cmds.append(resp_cmd('TTL', f'pttl:{worker_id}:{i}'))
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size * 2)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_persist_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [
        resp_cmd('SET', f'ppersist:{worker_id}:{i}', f'v{i}', 'EX', '3600')
        for i in range(pipeline_size)
    ]
    client.flush_pipeline(populate, pipeline_size)
    cmds = [resp_cmd('PERSIST', f'ppersist:{worker_id}:{i}') for i in range(pipeline_size)]
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_type_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [SET_CMD_TPL(f'ptype:{worker_id}:{i}', f'v{i}') for i in range(pipeline_size)]
    client.flush_pipeline(populate, pipeline_size)
    cmds = [resp_cmd('TYPE', f'ptype:{worker_id}:{i}') for i in range(pipeline_size)]
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


# =============================================================================
# MIXED / REAL-WORLD WORKLOADS
# =============================================================================

def bench_read_heavy_mixed(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [SET_CMD_TPL(f'pread:{worker_id}:{i}', f'v{i}') for i in range(pipeline_size)]
    client.flush_pipeline(populate, pipeline_size)
    batch = ops // pipeline_size
    start = time.perf_counter()
    for b in range(batch):
        cmds = []
        for i in range(pipeline_size):
            idx = (b * pipeline_size + i) % pipeline_size
            if i % 10 == 0:
                cmds.append(SET_CMD_TPL(f'pread:{worker_id}:{idx}', f'new{b}'))
            else:
                cmds.append(GET_CMD_TPL(f'pread:{worker_id}:{idx}'))
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_hash_heavy_mixed(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    populate = [
        resp_cmd('HSET', f'phmixed:{worker_id}', f'field{i}', f'val{i}')
        for i in range(50)
    ]
    client.flush_pipeline(populate, 50)
    batch = ops // pipeline_size
    start = time.perf_counter()
    for b in range(batch):
        cmds = []
        for i in range(pipeline_size):
            idx = (b * pipeline_size + i) % 50
            if i % 5 == 0:
                cmds.append(resp_cmd('HSET', f'phmixed:{worker_id}', f'field{idx}', f'updated{b}'))
            else:
                cmds.append(resp_cmd('HGET', f'phmixed:{worker_id}', f'field{idx}'))
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_counter_mixed(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    init = [SET_CMD_TPL(f'pcounter:{worker_id}:{i}', '0') for i in range(pipeline_size)]
    client.flush_pipeline(init, pipeline_size)
    batch = ops // pipeline_size
    start = time.perf_counter()
    for b in range(batch):
        cmds = []
        for i in range(pipeline_size):
            if i % 3 == 0:
                cmds.append(resp_cmd('INCR', f'pcounter:{worker_id}:{i}'))
            elif i % 3 == 1:
                cmds.append(resp_cmd('INCRBY', f'pcounter:{worker_id}:{i}', '5'))
            else:
                cmds.append(GET_CMD_TPL(f'pcounter:{worker_id}:{i}'))
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_dbsize_pipeline(args):
    host, port, ops, pipeline_size, worker_id = args
    client = PipelinedClient(host, port, pipeline_size)
    cmds = [resp_cmd('DBSIZE')] * pipeline_size
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


def bench_echo_pipeline(args):
    host, port, ops, pipeline_size = args
    client = PipelinedClient(host, port, pipeline_size)
    cmds = [resp_cmd('ECHO', 'hello')] * pipeline_size
    batch = ops // pipeline_size
    start = time.perf_counter()
    for _ in range(batch):
        client.flush_pipeline(cmds, pipeline_size)
    elapsed = time.perf_counter() - start
    client.close()
    return batch * pipeline_size, elapsed


# =============================================================================
# RUNNER
# =============================================================================

def run_benchmark(name, worker_fn, host, port, total_ops, pipeline_size, num_workers, needs_worker_id=True):
    ops_per_worker = total_ops // num_workers

    if needs_worker_id:
        pool_args = [
            (host, port, ops_per_worker, pipeline_size, i)
            for i in range(num_workers)
        ]
    else:
        pool_args = [
            (host, port, ops_per_worker, pipeline_size)
            for _ in range(num_workers)
        ]

    start = time.perf_counter()

    with multiprocessing.Pool(num_workers) as pool:
        results = pool.map(worker_fn, pool_args)

    wall_time = time.perf_counter() - start
    total_done = sum(r[0] for r in results)
    throughput = total_done / wall_time if wall_time > 0 else 0

    print(
        f"  {name:<40} "
        f"{throughput:>12,.0f} ops/s  "
        f"Wall: {wall_time:>5.2f}s"
    )
    return throughput


def main():
    parser = argparse.ArgumentParser(description="CrabKV Comprehensive Stress Test")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--ops", "-n", type=int, default=1_000_000)
    parser.add_argument("--pipeline", "-p", type=int, default=256)
    parser.add_argument("--workers", "-w", type=int, default=None)
    parser.add_argument("--quick", action="store_true", help="Quick validation mode")
    parser.add_argument("--group", "-g", default="all",
                        choices=["all", "string", "list", "hash", "set", "zset", "stream", "keys", "mixed"],
                        help="Run specific test group")
    args = parser.parse_args()

    num_workers = args.workers or min(os.cpu_count() or 4, 16)
    total_ops = args.ops
    pipeline_size = args.pipeline

    if args.quick:
        total_ops = min(total_ops, 100_000)
        pipeline_size = min(pipeline_size, 64)

    print("=" * 90)
    print("  CrabKV Comprehensive Stress Test (A-Z Feature Coverage)")
    print("=" * 90)
    print(f"  Target:     {args.host}:{args.port}")
    print(f"  Total Ops:  {total_ops:,}")
    print(f"  Pipeline:   {pipeline_size}")
    print(f"  Workers:    {num_workers}")
    print(f"  CPUs:       {os.cpu_count()}")
    print()

    try:
        c = PipelinedClient(args.host, args.port, 1)
        c.flush_pipeline([PING], 1)
        c.close()
        print("  Connected!\n")
    except Exception as e:
        print(f"  ERROR: Cannot connect - {e}")
        return

    all_tests = {
        "string": [
            ("PING",                       bench_ping_pipeline,           False),
            ("SET",                        bench_set_pipeline,            True),
            ("GET",                        bench_get_pipeline,            True),
            ("SET+GET pair",               bench_setget_pipeline,         True),
            ("MSET+MGET",                  bench_mset_mget_pipeline,      True),
            ("INCR",                       bench_incr_pipeline,           True),
            ("DECR",                       bench_decr_pipeline,           True),
            ("INCRBY",                     bench_incrby_pipeline,         True),
            ("DEL",                        bench_del_pipeline,            True),
            ("EXISTS",                     bench_exists_pipeline,         True),
            ("APPEND",                     bench_append_pipeline,         True),
            ("SET EX (TTL)",               bench_setex_pipeline,          True),
            ("ECHO",                       bench_echo_pipeline,           False),
        ],
        "list": [
            ("LPUSH",                      bench_lpush_pipeline,          True),
            ("RPUSH+LPOP",                 bench_rpush_lpop_pipeline,     True),
            ("LRANGE (50 items)",          bench_lrange_pipeline,         True),
            ("LLEN",                       bench_llen_pipeline,           True),
        ],
        "hash": [
            ("HSET",                       bench_hset_pipeline,           True),
            ("HGET",                       bench_hget_pipeline,           True),
            ("HGETALL",                    bench_hgetall_pipeline,        True),
            ("HSET+HGET pair",             bench_hset_hget_pair_pipeline, True),
            ("HDEL",                       bench_hdel_pipeline,           True),
        ],
        "set": [
            ("SADD",                       bench_sadd_pipeline,           True),
            ("SISMEMBER",                  bench_sismember_pipeline,      True),
            ("SCARD",                      bench_scard_pipeline,          True),
            ("SMEMBERS",                   bench_smembers_pipeline,       True),
        ],
        "zset": [
            ("ZADD",                       bench_zadd_pipeline,           True),
            ("ZRANGE (50 items)",          bench_zrange_pipeline,         True),
            ("ZSCORE",                     bench_zscore_pipeline,         True),
        ],
        "stream": [
            ("XADD",                       bench_xadd_pipeline,           True),
            ("XRANGE (all)",               bench_xrange_pipeline,         True),
            ("XADD+XLEN pair",             bench_xadd_xlen_pipeline,      True),
        ],
        "keys": [
            ("EXPIRE+TTL pair",            bench_expire_ttl_pipeline,     True),
            ("PERSIST",                    bench_persist_pipeline,        True),
            ("TYPE",                       bench_type_pipeline,           True),
            ("DBSIZE",                     bench_dbsize_pipeline,         True),
        ],
        "mixed": [
            ("Read-heavy (90/10 R/W)",     bench_read_heavy_mixed,        True),
            ("Hash mixed (80/20 R/W)",     bench_hash_heavy_mixed,        True),
            ("Counter mixed (INCR+GET)",   bench_counter_mixed,           True),
        ],
    }

    group = args.group
    if group == "all":
        groups_to_run = ["string", "list", "hash", "set", "zset", "stream", "keys", "mixed"]
    else:
        groups_to_run = [group]

    all_throughputs = []

    for grp in groups_to_run:
        tests = all_tests.get(grp, [])
        if not tests:
            continue

        print(f"  --- {grp.upper()} COMMANDS ---")

        for name, fn, needs_id in tests:
            try:
                t = run_benchmark(
                    name, fn, args.host, args.port,
                    total_ops, pipeline_size, num_workers,
                    needs_worker_id=needs_id
                )
                all_throughputs.append((f"{grp}/{name}", t))
            except Exception as e:
                print(f"  {name:<40} ERROR: {e}")

        print()

    print("=" * 90)
    print("  SUMMARY (sorted by throughput)")
    print("=" * 90)

    all_throughputs.sort(key=lambda x: x[1], reverse=True)

    for name, t in all_throughputs[:10]:
        marker = " *" if t >= 1_000_000 else ""
        print(f"  {name:<45} {t:>12,.0f} ops/s{marker}")

    if all_throughputs:
        best_name, best = all_throughputs[0]
        print()
        print(f"  Best: {best_name} @ {best:,.0f} ops/s")
        if best >= 1_000_000:
            print("  TARGET REACHED: 1M+ ops/s!")
        else:
            print(f"  Gap to 1M: {1_000_000 - best:,.0f} ops/s")

    print("=" * 90)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
