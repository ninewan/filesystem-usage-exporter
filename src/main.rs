use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use axum::{
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use tokio::sync::{Mutex, RwLock};
use tokio_cron_scheduler::{Job, JobScheduler};

/// 配置文件结构
#[derive(Debug, Clone, serde::Deserialize)]
struct Config {
    #[serde(default = "default_user")]
    user: String,
    #[serde(default = "default_schedule")]
    schedule: String,
    #[serde(default = "default_listen")]
    listen: String,
    servers: Vec<Server>,
}

fn default_user() -> String {
    "root".into()
}
fn default_schedule() -> String {
    "0 0 2 * * *".into()
}
fn default_listen() -> String {
    "0.0.0.0:9101".into()
}

#[derive(Debug, Clone, serde::Deserialize)]
struct Server {
    ip: String,
    name: String,
}

/// 单块磁盘的采集结果
#[derive(Debug, Clone)]
struct DiskMetric {
    host: String,
    name: String,
    device: String,
    mountpoint: String,
    size_bytes: u64,
    used_bytes: u64,
    available_bytes: u64,
    usage_percent: f64,
}

type SharedMetrics = Arc<RwLock<Vec<DiskMetric>>>;
type SharedTimestamp = Arc<RwLock<i64>>;

#[derive(Clone)]
struct AppState {
    metrics: SharedMetrics,
    last_check: SharedTimestamp,
    refresh_lock: Arc<Mutex<()>>,
    config: Arc<Config>,
}

/// 当前时间的秒级 Unix 时间戳
fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 解析 `df -P` 输出，等价于用户给出的 awk 逻辑
fn parse_df(stdout: &str, ip: &str, name: &str) -> Vec<DiskMetric> {
    let mut metrics = Vec::new();
    for line in stdout.lines().skip(1) {
        let tokens: Vec<&str> = line.split_whitespace().collect();
        if tokens.len() < 6 {
            continue;
        }
        let device = tokens[0];
        // 仅保留以 / 开头的文件系统（与 awk 中 $1 ~ /^\// 一致）
        if !device.starts_with('/') {
            continue;
        }
        let size: u64 = tokens[1].parse().unwrap_or(0);
        let used: u64 = tokens[2].parse().unwrap_or(0);
        let avail: u64 = tokens[3].parse().unwrap_or(0);
        let cap: f64 = tokens[4].trim_end_matches('%').parse().unwrap_or(0.0);
        // 挂载点可能含空格，取剩余部分
        let mountpoint = tokens[5..].join(" ");

        metrics.push(DiskMetric {
            host: ip.to_string(),
            name: name.to_string(),
            device: device.to_string(),
            mountpoint,
            // df -P 的单位是 1K 块，乘 1024 转字节
            size_bytes: size * 1024,
            used_bytes: used * 1024,
            available_bytes: avail * 1024,
            usage_percent: cap,
        });
    }
    metrics
}

/// 通过 ssh 到单台机器执行 `df -P` 并解析
async fn collect_one(user: &str, server: &Server) -> Result<Vec<DiskMetric>> {
    let target = format!("{}@{}", user, server.ip);
    let output = tokio::process::Command::new("ssh")
        .arg("-o")
        .arg("BatchMode=yes")
        .arg("-o")
        .arg("StrictHostKeyChecking=accept-new")
        .arg("-o")
        .arg("ConnectTimeout=10")
        .arg(&target)
        .arg("df -P")
        .output()
        .await?;

    if !output.status.success() {
        anyhow::bail!(
            "ssh {} failed: {}",
            server.ip,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(parse_df(&stdout, &server.ip, &server.name))
}

/// 并发采集所有服务器
async fn collect_all(config: Arc<Config>) -> Vec<DiskMetric> {
    let mut handles = Vec::new();
    for server in config.servers.iter().cloned() {
        let user = config.user.clone();
        handles.push(tokio::spawn(async move {
            match collect_one(&user, &server).await {
                Ok(m) => {
                    tracing::info!("collected {} disks from {}", m.len(), server.ip);
                    m
                }
                Err(e) => {
                    tracing::error!("collect {} failed: {e}", server.ip);
                    Vec::new()
                }
            }
        }));
    }

    let mut all = Vec::new();
    for h in handles {
        if let Ok(m) = h.await {
            all.extend(m);
        }
    }
    all
}

/// 采集并写入共享内存
async fn collect_and_store(config: Arc<Config>, state: AppState) {
    let ts = now_ts();
    let data = collect_all(config).await;
    tracing::info!("total disks collected: {}", data.len());
    *state.metrics.write().await = data;
    *state.last_check.write().await = ts;
}

/// 转义 Prometheus label 值
fn esc(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// 渲染为 Prometheus 文本格式
fn render_metrics(data: &[DiskMetric], last_check: i64) -> String {
    let mut out = String::new();

    out.push_str("# HELP yishu_filesystem_size_bytes Total size of disk in bytes\n");
    out.push_str("# TYPE yishu_filesystem_size_bytes gauge\n");
    for m in data {
        out.push_str(&format!(
            "yishu_filesystem_size_bytes{{host=\"{}\",name=\"{}\",device=\"{}\",mountpoint=\"{}\"}} {}\n",
            esc(&m.host), esc(&m.name), esc(&m.device), esc(&m.mountpoint), m.size_bytes
        ));
    }

    out.push_str("# HELP yishu_filesystem_used_bytes Used bytes of disk\n");
    out.push_str("# TYPE yishu_filesystem_used_bytes gauge\n");
    for m in data {
        out.push_str(&format!(
            "yishu_filesystem_used_bytes{{host=\"{}\",name=\"{}\",device=\"{}\",mountpoint=\"{}\"}} {}\n",
            esc(&m.host), esc(&m.name), esc(&m.device), esc(&m.mountpoint), m.used_bytes
        ));
    }

    out.push_str("# HELP yishu_filesystem_available_bytes Available bytes of disk\n");
    out.push_str("# TYPE yishu_filesystem_available_bytes gauge\n");
    for m in data {
        out.push_str(&format!(
            "yishu_filesystem_available_bytes{{host=\"{}\",name=\"{}\",device=\"{}\",mountpoint=\"{}\"}} {}\n",
            esc(&m.host), esc(&m.name), esc(&m.device), esc(&m.mountpoint), m.available_bytes
        ));
    }

    out.push_str("# HELP yishu_filesystem_usage_percent Disk usage percentage\n");
    out.push_str("# TYPE yishu_filesystem_usage_percent gauge\n");
    for m in data {
        out.push_str(&format!(
            "yishu_filesystem_usage_percent{{host=\"{}\",name=\"{}\",device=\"{}\",mountpoint=\"{}\"}} {}\n",
            esc(&m.host), esc(&m.name), esc(&m.device), esc(&m.mountpoint), m.usage_percent
        ));
    }

    out.push_str("# HELP yishu_filesystem_check_timestamp Last collection time in seconds since epoch\n");
    out.push_str("# TYPE yishu_filesystem_check_timestamp gauge\n");
    out.push_str(&format!("yishu_filesystem_check_timestamp {}\n", last_check));

    out
}

/// /metrics handler
async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let data = state.metrics.read().await;
    let last_check = *state.last_check.read().await;
    let body = render_metrics(&data, last_check);
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

/// /refresh handler：立即触发一次采集并刷新内存数据
async fn refresh(State(state): State<AppState>) -> Response {
    // 用 Mutex 串行化，避免并发请求重复 ssh 打爆目标机器
    let _guard = match state.refresh_lock.try_lock() {
        Ok(g) => g,
        Err(_) => {
            return (
                StatusCode::CONFLICT,
                "a refresh is already in progress\n",
            )
                .into_response();
        }
    };

    let before = *state.last_check.read().await;
    collect_and_store(state.config.clone(), state.clone()).await;
    let after = *state.last_check.read().await;
    let count = state.metrics.read().await.len();

    tracing::info!(
        "manual refresh done: disks={}, ts={} (was {})",
        count,
        after,
        before
    );

    (
        StatusCode::OK,
        format!("ok refreshed disks={} timestamp={}\n", count, after),
    )
        .into_response()
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.yaml".to_string());
    let raw = std::fs::read_to_string(&path)?;
    let config: Config = serde_yaml::from_str(&raw)?;
    tracing::info!("loaded {} servers from {}", config.servers.len(), path);

    let config = Arc::new(config);
    let state = AppState {
        metrics: Arc::new(RwLock::new(Vec::new())),
        last_check: Arc::new(RwLock::new(0)),
        refresh_lock: Arc::new(Mutex::new(())),
        config: config.clone(),
    };

    // 启动时立即采集一次，确保 /metrics 一开始就有值
    collect_and_store(config.clone(), state.clone()).await;

    // 每天定时采集（写死 Asia/Shanghai 时区，与运行环境解耦）
    let sched = JobScheduler::new().await?;
    let cfg = config.clone();
    let st = state.clone();
    sched
        .add(
            Job::new_async_tz(
                config.schedule.as_str(),
                chrono_tz::Asia::Shanghai,
                move |_uuid, _l| {
                    let cfg = cfg.clone();
                    let st = st.clone();
                    Box::pin(async move {
                        collect_and_store(cfg, st).await;
                    })
                },
            )?,
        )
        .await?;
    sched.start().await?;
    tracing::info!(
        "scheduled daily collection (Asia/Shanghai): {}",
        config.schedule
    );

    // HTTP 服务
    let app = Router::new()
        .route("/metrics", get(metrics))
        .route("/refresh", get(refresh))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind(&config.listen).await?;
    tracing::info!(
        "listening on http://{}/metrics and http://{}/refresh",
        config.listen, config.listen
    );
    axum::serve(listener, app).await?;

    Ok(())
}
