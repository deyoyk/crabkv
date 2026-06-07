use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug)]
pub struct Metrics {
    pub total_commands: AtomicU64,
    pub total_gets: AtomicU64,
    pub total_sets: AtomicU64,
    pub total_hits: AtomicU64,
    pub total_misses: AtomicU64,
    pub connected_clients: AtomicU64,
    pub total_connections: AtomicU64,
    pub total_keys: AtomicU64,
    pub memory_used_bytes: AtomicU64,
    start_time: Instant,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            total_commands: AtomicU64::new(0),
            total_gets: AtomicU64::new(0),
            total_sets: AtomicU64::new(0),
            total_hits: AtomicU64::new(0),
            total_misses: AtomicU64::new(0),
            connected_clients: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            total_keys: AtomicU64::new(0),
            memory_used_bytes: AtomicU64::new(0),
            start_time: Instant::now(),
        })
    }

    pub fn record_command(&self, command: &str, _duration_us: u64) {
        self.total_commands.fetch_add(1, Ordering::Relaxed);
        match command.to_uppercase().as_str() {
            "GET" => { self.total_gets.fetch_add(1, Ordering::Relaxed); }
            "SET" => { self.total_sets.fetch_add(1, Ordering::Relaxed); }
            _ => {}
        }
    }

    pub fn hit(&self) {
        self.total_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn miss(&self) {
        self.total_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> String {
        let hits = self.total_hits.load(Ordering::Relaxed);
        let misses = self.total_misses.load(Ordering::Relaxed);
        let hit_rate = if hits + misses > 0 {
            hits as f64 / (hits + misses) as f64 * 100.0
        } else {
            0.0
        };

        format!(
            "# Stats\n\
             total_commands:{}\n\
             total_gets:{}\n\
             total_sets:{}\n\
             hits:{}\n\
             misses:{}\n\
             hit_rate:{:.2}%\n\
             connected_clients:{}\n\
             total_connections:{}\n\
             total_keys:{}\n\
             used_memory:{}\n\
             uptime_seconds:{}\n",
            self.total_commands.load(Ordering::Relaxed),
            self.total_gets.load(Ordering::Relaxed),
            self.total_sets.load(Ordering::Relaxed),
            hits, misses, hit_rate,
            self.connected_clients.load(Ordering::Relaxed),
            self.total_connections.load(Ordering::Relaxed),
            self.total_keys.load(Ordering::Relaxed),
            self.memory_used_bytes.load(Ordering::Relaxed),
            self.start_time.elapsed().as_secs(),
        )
    }

    pub fn to_prometheus(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("# HELP crabkv_commands_total Total commands processed\n"));
        out.push_str(&format!("# TYPE crabkv_commands_total counter\n"));
        out.push_str(&format!("crabkv_commands_total {}\n", self.total_commands.load(Ordering::Relaxed)));
        out.push_str(&format!("# HELP crabkv_connected_clients Currently connected clients\n"));
        out.push_str(&format!("# TYPE crabkv_connected_clients gauge\n"));
        out.push_str(&format!("crabkv_connected_clients {}\n", self.connected_clients.load(Ordering::Relaxed)));
        out.push_str(&format!("# HELP crabkv_memory_used_bytes Memory used in bytes\n"));
        out.push_str(&format!("# TYPE crabkv_memory_used_bytes gauge\n"));
        out.push_str(&format!("crabkv_memory_used_bytes {}\n", self.memory_used_bytes.load(Ordering::Relaxed)));
        out
    }
}

pub struct MetricsServer {
    metrics: Arc<Metrics>,
    port: u16,
}

impl MetricsServer {
    pub fn new(metrics: Arc<Metrics>, port: u16) -> Self {
        Self { metrics, port }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let metrics = self.metrics.clone();
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", self.port)).await?;
        tracing::info!("Metrics server listening on port {}", self.port);

        loop {
            let (mut stream, _) = listener.accept().await?;
            let metrics = metrics.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n{}",
                    metrics.to_prometheus()
                );
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_basics() {
        let m = Metrics::new();
        m.record_command("GET", 100);
        m.hit();
        m.miss();
        assert_eq!(m.total_gets.load(Ordering::Relaxed), 1);
        let stats = m.get_stats();
        assert!(stats.contains("total_gets:1"));
    }

    #[test]
    fn test_prometheus_format() {
        let m = Metrics::new();
        m.record_command("SET", 50);
        let prom = m.to_prometheus();
        assert!(prom.contains("crabkv_commands_total"));
    }
}
