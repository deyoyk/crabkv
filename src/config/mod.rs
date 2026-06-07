use std::fs;

use clap::Parser;
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(
    name = "crabkv",
    about = "CrabKV - A next-generation distributed data platform"
)]
pub struct Cli {
    #[arg(short, long, default_value = "0.0.0.0")]
    pub bind: String,

    #[arg(short, long, default_value_t = 6379)]
    pub port: u16,

    #[arg(long)]
    pub config: Option<String>,

    #[arg(long)]
    pub max_memory: Option<String>,

    #[arg(long)]
    pub cluster: bool,

    #[arg(long)]
    pub metrics_port: Option<u16>,

    #[arg(long)]
    pub debug: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub memory: MemoryConfig,
    pub cluster: ClusterConfig,
    pub metrics: MetricsConfig,
    pub replication: ReplicationConfig,
    pub persistence: PersistenceConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub bind: String,
    pub port: u16,
    pub max_connections: usize,
    #[serde(default)]
    pub debug: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MemoryConfig {
    pub max_memory: String,
    pub eviction_policy: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub nodes: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ReplicationConfig {
    pub mode: String,
    pub peers: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PersistenceConfig {
    pub enabled: bool,
    pub mode: String,
    pub dir: String,
    pub aof_fsync: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                bind: "0.0.0.0".into(),
                port: 6379,
                max_connections: 10000,
                debug: true,
            },
            memory: MemoryConfig {
                max_memory: "8GB".into(),
                eviction_policy: "allkeys-lru".into(),
            },
            cluster: ClusterConfig {
                enabled: false,
                nodes: Vec::new(),
            },
            metrics: MetricsConfig {
                enabled: true,
                port: 9090,
            },
            replication: ReplicationConfig {
                mode: "standalone".into(),
                peers: Vec::new(),
            },
            persistence: PersistenceConfig {
                enabled: true,
                mode: "aof".into(),
                dir: "./data".into(),
                aof_fsync: "everysec".into(),
            },
        }
    }
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn apply_cli(&mut self, cli: &Cli) {
        self.server.bind = cli.bind.clone();
        self.server.port = cli.port;

        if let Some(ref mem) = cli.max_memory {
            self.memory.max_memory = mem.clone();
        }

        if cli.cluster {
            self.cluster.enabled = true;
        }

        if let Some(mp) = cli.metrics_port {
            self.metrics.port = mp;
        }

        if cli.debug {
            self.server.debug = true;
        }
    }

    pub fn parse_max_memory(&self) -> usize {
        parse_memory_string(&self.memory.max_memory)
    }
}

fn parse_memory_string(s: &str) -> usize {
    let trimmed = s.trim().to_uppercase();

    let (num_part, unit_part) = if let Some(pos) = trimmed.find(|c: char| c.is_alphabetic()) {
        (&trimmed[..pos], &trimmed[pos..])
    } else {
        (trimmed.as_str(), "")
    };

    let num: f64 = num_part.parse().unwrap_or(0.0);

    match unit_part {
        "B" | "" => num as usize,
        "K" | "KB" => (num * 1024.0) as usize,
        "M" | "MB" => (num * 1024.0 * 1024.0) as usize,
        "G" | "GB" => (num * 1024.0 * 1024.0 * 1024.0) as usize,
        "T" | "TB" => (num * 1024.0 * 1024.0 * 1024.0 * 1024.0) as usize,
        _ => (num * 1024.0 * 1024.0 * 1024.0) as usize,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = Config::default();
        assert_eq!(config.server.bind, "0.0.0.0");
        assert_eq!(config.server.port, 6379);
        assert_eq!(config.server.max_connections, 10000);
        assert_eq!(config.memory.max_memory, "8GB");
        assert!(!config.cluster.enabled);
        assert_eq!(config.metrics.port, 9090);
    }

    #[test]
    fn parse_memory_gb() {
        let mut config = Config::default();
        config.memory.max_memory = "8GB".into();
        assert_eq!(config.parse_max_memory(), 8 * 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_memory_mb() {
        let mut config = Config::default();
        config.memory.max_memory = "512MB".into();
        assert_eq!(config.parse_max_memory(), 512 * 1024 * 1024);
    }

    #[test]
    fn parse_memory_kb() {
        let mut config = Config::default();
        config.memory.max_memory = "256KB".into();
        assert_eq!(config.parse_max_memory(), 256 * 1024);
    }

    #[test]
    fn parse_memory_bytes() {
        let mut config = Config::default();
        config.memory.max_memory = "1024B".into();
        assert_eq!(config.parse_max_memory(), 1024);
    }

    #[test]
    fn parse_memory_tb() {
        let mut config = Config::default();
        config.memory.max_memory = "2TB".into();
        assert_eq!(config.parse_max_memory(), 2 * 1024 * 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_memory_decimal() {
        let mut config = Config::default();
        config.memory.max_memory = "1.5GB".into();
        assert_eq!(
            config.parse_max_memory(),
            (1.5 * 1024.0 * 1024.0 * 1024.0) as usize
        );
    }

    #[test]
    fn parse_memory_default_unit_is_gb() {
        let mut config = Config::default();
        config.memory.max_memory = "4GB".into();
        assert_eq!(config.parse_max_memory(), 4 * 1024 * 1024 * 1024);
    }

    #[test]
    fn parse_memory_case_insensitive() {
        let mut config = Config::default();
        config.memory.max_memory = "2gb".into();
        assert_eq!(config.parse_max_memory(), 2 * 1024 * 1024 * 1024);
    }

    #[test]
    fn apply_cli_overrides() {
        let mut config = Config::default();
        let cli = Cli {
            bind: "127.0.0.1".into(),
            port: 6380,
            config: None,
            max_memory: Some("4GB".into()),
            cluster: true,
            metrics_port: Some(8080),
            debug: false,
        };
        config.apply_cli(&cli);
        assert_eq!(config.server.bind, "127.0.0.1");
        assert_eq!(config.server.port, 6380);
        assert_eq!(config.memory.max_memory, "4GB");
        assert!(config.cluster.enabled);
        assert_eq!(config.metrics.port, 8080);
    }

    #[test]
    fn apply_cli_partial_overrides() {
        let mut config = Config::default();
        let cli = Cli {
            bind: "127.0.0.1".into(),
            port: 6380,
            config: None,
            max_memory: None,
            cluster: false,
            metrics_port: None,
            debug: false,
        };
        config.apply_cli(&cli);
        assert_eq!(config.server.bind, "127.0.0.1");
        assert_eq!(config.server.port, 6380);
        assert_eq!(config.memory.max_memory, "8GB");
        assert!(!config.cluster.enabled);
    }
}
