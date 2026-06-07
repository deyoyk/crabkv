# CrabKV

A high-performance, Redis-compatible key-value server written in Rust.

## Quick Start

### One-liner install (Linux / macOS)

```bash
curl -fsSL https://raw.githubusercontent.com/deyoyk/crabkv/main/install.sh | bash
```

### Custom port

```bash
curl -fsSL https://raw.githubusercontent.com/deyoyk/crabkv/main/install.sh | bash -s -- --port 6380
```

### Install with autostart

```bash
curl -fsSL https://raw.githubusercontent.com/deyoyk/crabkv/main/install.sh | bash -s -- --autostart
```

### Windows (PowerShell)

```powershell
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/deyoyk/crabkv/main/install.ps1" -OutFile "install.ps1"; .\install.ps1
```

## Install Options

| Flag | Description | Default |
|------|-------------|---------|
| `-p, --port` | Port to listen on | `6379` |
| `-d, --dir` | Install directory | `~/.local/bin` |
| `-a, --autostart` | Set up OS-level autostart | off |
| `-v, --version` | Specific version to install | `latest` |

## Connect

```bash
redis-cli -p 6379
```

Compatible with any Redis client library (ioredis, redis-py, Jedis, etc.).

## Build from Source

```bash
git clone https://github.com/deyoyk/crabkv.git
cd crabkv
cargo build --release
./target/release/crabkv -p 6379
```

## Features

- RESP protocol (Redis serialization protocol)
- String, List, Hash, Set, Sorted Set, Stream commands
- TTL / expiration
- Pipelining
- Pub/Sub
- Persistence (AOF)
- Job queues
- Metrics endpoint

## License

MIT OR Apache-2.0
