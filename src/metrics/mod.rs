use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

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
    pub uptime_seconds: AtomicU64,
    pub pubsub_channels: AtomicU64,
    pub pubsub_subscribers: AtomicU64,
    pub stream_entries: AtomicU64,
    pub jobs_pending: AtomicU64,
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
            uptime_seconds: AtomicU64::new(0),
            pubsub_channels: AtomicU64::new(0),
            pubsub_subscribers: AtomicU64::new(0),
            stream_entries: AtomicU64::new(0),
            jobs_pending: AtomicU64::new(0),
            start_time: Instant::now(),
        })
    }

    pub fn record_command(&self, command: &str, _duration_us: u64) {
        self.total_commands.fetch_add(1, Ordering::Relaxed);
        match command.to_ascii_lowercase().as_str() {
            "get" => {
                self.total_gets.fetch_add(1, Ordering::Relaxed);
            }
            "set" => {
                self.total_sets.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    pub fn hit(&self) {
        self.total_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn miss(&self) {
        self.total_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn client_connected(&self) {
        self.connected_clients.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub fn client_disconnected(&self) {
        self.connected_clients.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn update_uptime(&self) {
        let secs = self.start_time.elapsed().as_secs();
        self.uptime_seconds.store(secs, Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> String {
        self.update_uptime();
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
             total_hits:{}\n\
             total_misses:{}\n\
             hit_rate:{:.2}%\n\
             connected_clients:{}\n\
             total_connections:{}\n\
             total_keys:{}\n\
             memory_used_bytes:{}\n\
             uptime_seconds:{}\n\
             pubsub_channels:{}\n\
             pubsub_subscribers:{}\n\
             stream_entries:{}\n\
             jobs_pending:{}\n",
            self.total_commands.load(Ordering::Relaxed),
            self.total_gets.load(Ordering::Relaxed),
            self.total_sets.load(Ordering::Relaxed),
            hits,
            misses,
            hit_rate,
            self.connected_clients.load(Ordering::Relaxed),
            self.total_connections.load(Ordering::Relaxed),
            self.total_keys.load(Ordering::Relaxed),
            self.memory_used_bytes.load(Ordering::Relaxed),
            self.uptime_seconds.load(Ordering::Relaxed),
            self.pubsub_channels.load(Ordering::Relaxed),
            self.pubsub_subscribers.load(Ordering::Relaxed),
            self.stream_entries.load(Ordering::Relaxed),
            self.jobs_pending.load(Ordering::Relaxed),
        )
    }

    pub fn to_prometheus(&self) -> String {
        self.update_uptime();
        let hits = self.total_hits.load(Ordering::Relaxed);
        let misses = self.total_misses.load(Ordering::Relaxed);
        let hit_rate = if hits + misses > 0 {
            hits as f64 / (hits + misses) as f64
        } else {
            0.0
        };

        format!(
            "# HELP crabkv_total_commands Total number of commands processed.\n\
             # TYPE crabkv_total_commands counter\n\
             crabkv_total_commands {}\n\
             # HELP crabkv_total_gets Total GET commands.\n\
             # TYPE crabkv_total_gets counter\n\
             crabkv_total_gets {}\n\
             # HELP crabkv_total_sets Total SET commands.\n\
             # TYPE crabkv_total_sets counter\n\
             crabkv_total_sets {}\n\
             # HELP crabkv_total_hits Total cache hits.\n\
             # TYPE crabkv_total_hits counter\n\
             crabkv_total_hits {}\n\
             # HELP crabkv_total_misses Total cache misses.\n\
             # TYPE crabkv_total_misses counter\n\
             crabkv_total_misses {}\n\
             # HELP crabkv_hit_rate Cache hit rate.\n\
             # TYPE crabkv_hit_rate gauge\n\
             crabkv_hit_rate {:.4}\n\
             # HELP crabkv_connected_clients Currently connected clients.\n\
             # TYPE crabkv_connected_clients gauge\n\
             crabkv_connected_clients {}\n\
             # HELP crabkv_total_connections Total connections accepted.\n\
             # TYPE crabkv_total_connections counter\n\
             crabkv_total_connections {}\n\
             # HELP crabkv_total_keys Total number of keys.\n\
             # TYPE crabkv_total_keys gauge\n\
             crabkv_total_keys {}\n\
             # HELP crabkv_memory_used_bytes Memory used in bytes.\n\
             # TYPE crabkv_memory_used_bytes gauge\n\
             crabkv_memory_used_bytes {}\n\
             # HELP crabkv_uptime_seconds Server uptime in seconds.\n\
             # TYPE crabkv_uptime_seconds gauge\n\
             crabkv_uptime_seconds {}\n\
             # HELP crabkv_pubsub_channels Number of pub/sub channels.\n\
             # TYPE crabkv_pubsub_channels gauge\n\
             crabkv_pubsub_channels {}\n\
             # HELP crabkv_pubsub_subscribers Number of pub/sub subscribers.\n\
             # TYPE crabkv_pubsub_subscribers gauge\n\
             crabkv_pubsub_subscribers {}\n\
             # HELP crabkv_stream_entries Number of stream entries.\n\
             # TYPE crabkv_stream_entries gauge\n\
             crabkv_stream_entries {}\n\
             # HELP crabkv_jobs_pending Number of pending jobs.\n\
             # TYPE crabkv_jobs_pending gauge\n\
             crabkv_jobs_pending {}\n",
            self.total_commands.load(Ordering::Relaxed),
            self.total_gets.load(Ordering::Relaxed),
            self.total_sets.load(Ordering::Relaxed),
            hits,
            misses,
            hit_rate,
            self.connected_clients.load(Ordering::Relaxed),
            self.total_connections.load(Ordering::Relaxed),
            self.total_keys.load(Ordering::Relaxed),
            self.memory_used_bytes.load(Ordering::Relaxed),
            self.uptime_seconds.load(Ordering::Relaxed),
            self.pubsub_channels.load(Ordering::Relaxed),
            self.pubsub_subscribers.load(Ordering::Relaxed),
            self.stream_entries.load(Ordering::Relaxed),
            self.jobs_pending.load(Ordering::Relaxed),
        )
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
        let addr = format!("0.0.0.0:{}", self.port);
        let listener = TcpListener::bind(&addr).await?;
        log::info!("Metrics server listening on {}", addr);

        let metrics = self.metrics.clone();

        loop {
            let (stream, _) = listener.accept().await?;
            let metrics = metrics.clone();

            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                    let metrics = metrics.clone();
                    async move {
                        match (req.method(), req.uri().path()) {
                            (&Method::GET, "/metrics") => {
                                let body = metrics.to_prometheus();
                                Ok::<_, hyper::Error>(
                                    Response::builder()
                                        .status(StatusCode::OK)
                                        .header("content-type", "text/plain; version=0.0.4")
                                        .body(Full::new(Bytes::from(body)))
                                        .unwrap(),
                                )
                            }
                            (&Method::GET, "/stats") => {
                                let body = metrics.get_stats();
                                Ok::<_, hyper::Error>(
                                    Response::builder()
                                        .status(StatusCode::OK)
                                        .header("content-type", "text/plain")
                                        .body(Full::new(Bytes::from(body)))
                                        .unwrap(),
                                )
                            }
                            (&Method::GET, "/health") => {
                                Ok::<_, hyper::Error>(
                                    Response::builder()
                                        .status(StatusCode::OK)
                                        .body(Full::new(Bytes::from("ok")))
                                        .unwrap(),
                                )
                            }
                            _ => Ok::<_, hyper::Error>(
                                Response::builder()
                                    .status(StatusCode::NOT_FOUND)
                                    .body(Full::new(Bytes::from("not found")))
                                    .unwrap(),
                            ),
                        }
                    }
                });

                if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                    log::error!("metrics connection error: {}", err);
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_creation() {
        let m = Metrics::new();
        assert_eq!(m.total_commands.load(Ordering::Relaxed), 0);
        assert_eq!(m.connected_clients.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn record_command_increments_totals() {
        let m = Metrics::new();
        m.record_command("get", 100);
        m.record_command("set", 200);
        m.record_command("get", 150);

        assert_eq!(m.total_commands.load(Ordering::Relaxed), 3);
        assert_eq!(m.total_gets.load(Ordering::Relaxed), 2);
        assert_eq!(m.total_sets.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn hit_miss_tracking() {
        let m = Metrics::new();
        m.hit();
        m.hit();
        m.miss();

        assert_eq!(m.total_hits.load(Ordering::Relaxed), 2);
        assert_eq!(m.total_misses.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn client_connect_disconnect() {
        let m = Metrics::new();
        m.client_connected();
        m.client_connected();
        assert_eq!(m.connected_clients.load(Ordering::Relaxed), 2);
        assert_eq!(m.total_connections.load(Ordering::Relaxed), 2);

        m.client_disconnected();
        assert_eq!(m.connected_clients.load(Ordering::Relaxed), 1);
        assert_eq!(m.total_connections.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn get_stats_contains_all_fields() {
        let m = Metrics::new();
        m.record_command("get", 50);
        m.hit();
        m.total_keys.store(42, Ordering::Relaxed);
        m.memory_used_bytes.store(1024, Ordering::Relaxed);

        let stats = m.get_stats();
        assert!(stats.contains("total_commands:1"));
        assert!(stats.contains("total_keys:42"));
        assert!(stats.contains("memory_used_bytes:1024"));
    }

    #[test]
    fn prometheus_output_format() {
        let m = Metrics::new();
        m.record_command("get", 50);
        m.hit();

        let prom = m.to_prometheus();
        assert!(prom.contains("# HELP crabkv_total_commands"));
        assert!(prom.contains("# TYPE crabkv_total_commands counter"));
        assert!(prom.contains("crabkv_total_commands 1"));
        assert!(prom.contains("crabkv_hit_rate"));
    }

    #[test]
    fn prometheus_hit_rate_zero_when_no_ops() {
        let m = Metrics::new();
        let prom = m.to_prometheus();
        assert!(prom.contains("crabkv_hit_rate 0.0000"));
    }

    #[test]
    fn prometheus_hit_rate_one_on_all_hits() {
        let m = Metrics::new();
        for _ in 0..10 {
            m.hit();
        }
        let prom = m.to_prometheus();
        assert!(prom.contains("crabkv_hit_rate 1.0000"));
    }
}
