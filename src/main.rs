use std::{net::SocketAddr, sync::Arc, time::Duration};

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use base64::Engine as _;
use serde::Deserialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, Mutex},
    time::{sleep, timeout},
};
use tracing::{error, info, warn};

use vex_v5_serial::{
    commands::file::{ProgramData, UploadProgram},
    Connection,
    protocol::{
        cdc2::{Cdc2CommandPacket, Cdc2ReplyPacket},
        Encode, FixedString,
    },
    serial::{find_devices, SerialConnection, SerialDevice},
};

// ========== Program Execution Packets ==========

/// Volume ID for program execution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum VolumeId {
    User = 1,
    System = 15,
}

impl Encode for VolumeId {
    fn encode(&self, data: &mut [u8]) {
        data[0] = *self as u8;
    }

    fn size(&self) -> usize {
        1
    }
}

/// Options for program execution (0x00 = run, 0x80 = stop)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecuteOptions {
    pub run: bool,
}

impl ExecuteOptions {
    pub fn run() -> Self {
        Self { run: true }
    }

    pub fn stop() -> Self {
        Self { run: false }
    }
}

impl Encode for ExecuteOptions {
    fn encode(&self, data: &mut [u8]) {
        data[0] = if self.run { 0x00 } else { 0x80 };
    }

    fn size(&self) -> usize {
        1
    }
}

/// FILE_LOAD command payload (CDC2 extended 0x18)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileLoadPayload {
    pub vid: VolumeId,
    pub options: ExecuteOptions,
    pub filename: FixedString<24>,
}

impl Encode for FileLoadPayload {
    fn encode(&self, data: &mut [u8]) {
        let mut offset = 0;

        self.vid.encode(&mut data[offset..]);
        offset += self.vid.size();

        self.options.encode(&mut data[offset..]);
        offset += self.options.size();

        self.filename.encode(&mut data[offset..]);
    }

    fn size(&self) -> usize {
        self.vid.size() + self.options.size() + self.filename.size()
    }
}

/// FILE_LOAD command packet (0x56, 0x18)
pub type FileLoadPacket = Cdc2CommandPacket<0x56, 0x18, FileLoadPayload>;

/// FILE_LOAD reply
pub type FileLoadReplyPacket = Cdc2ReplyPacket<0x56, 0x18, ()>;

// ========== Main Application ==========

#[derive(Clone)]
struct AppState {
    cmd: Arc<Mutex<CmdConn>>,
}

struct CmdConn {
    system_port_path: Option<String>,
    conn: Option<SerialConnection>,
}

impl CmdConn {
    fn new() -> Self {
        Self {
            system_port_path: None,
            conn: None,
        }
    }
}

#[derive(Deserialize)]
struct UploadRequest {
    slot: u8,
    name: String,
    description: String,
    #[serde(default = "default_icon")]
    icon: String,
    #[serde(default = "default_program_type")]
    program_type: String,
    #[serde(default)]
    compress: bool,
    #[serde(default = "default_after_upload")]
    after_upload: String,
    monolith_b64: String,
}

fn default_icon() -> String {
    "USER902x.bmp".to_string()
}
fn default_program_type() -> String {
    "user".to_string()
}
fn default_after_upload() -> String {
    "do_nothing".to_string()
}

#[derive(Deserialize)]
struct RunRequest {
    slot: u8,
}



fn normalize_slot(slot: u8) -> Result<u8, &'static str> {
    match slot {
        0..=7 => Ok(slot),
        1..=8 => Ok(slot - 1),
        _ => Err("slot must be 0-7 (0-indexed) or 1-8 (1-indexed)"),
    }
}

fn parse_after_upload(s: &str) -> vex_v5_serial::protocol::cdc2::file::FileExitAction {
    use vex_v5_serial::protocol::cdc2::file::FileExitAction;
    match s {
        "run" => FileExitAction::RunProgram,
        "halt" => FileExitAction::Halt,
        "show_run_screen" => FileExitAction::ShowRunScreen,
        _ => FileExitAction::DoNothing,
    }
}

fn discover_brain_ports() -> Result<(String, String), String> {
    let devices = find_devices().map_err(|e| format!("find_devices failed: {e:?}"))?;
    let brain = devices
        .into_iter()
        .find(|d| matches!(d, SerialDevice::Brain { .. }))
        .ok_or("no V5 Brain found over USB")?;

    match brain {
        SerialDevice::Brain {
            system_port,
            user_port,
        } => Ok((system_port, user_port)),
        _ => Err("unexpected device type".to_string()),
    }
}

async fn ensure_cmd_connected(cmd: &Arc<Mutex<CmdConn>>) -> Result<(), String> {
    let (system_port, _user_port) = discover_brain_ports()?;

    let mut g = cmd.lock().await;
    let needs_open = match (&g.system_port_path, &g.conn) {
        (Some(p), Some(_)) if p == &system_port => false,
        _ => true,
    };

    if !needs_open {
        return Ok(());
    }

    info!("Opening Brain system port for commands: {}", system_port);

    let device = SerialDevice::Unknown {
        system_port: system_port.clone(),
    };

    let conn = SerialConnection::open(device, Duration::from_secs(5))
        .map_err(|e| format!("SerialConnection::open(system) failed: {e:?}"))?;

    g.system_port_path = Some(system_port);
    g.conn = Some(conn);

    Ok(())
}

async fn upload_handler(
    State(state): State<AppState>,
    Json(req): Json<UploadRequest>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    ensure_cmd_connected(&state.cmd)
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e))?;

    let slot0 = normalize_slot(req.slot).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;

    let bin = base64::engine::general_purpose::STANDARD
        .decode(req.monolith_b64.as_bytes())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("invalid base64: {e}")))?;

    let cmd = UploadProgram {
        name: req.name,
        description: req.description,
        icon: req.icon,
        program_type: req.program_type,
        slot: slot0,
        compress: req.compress,
        data: ProgramData::Monolith(bin),
        after_upload: parse_after_upload(&req.after_upload),
        ini_callback: None,
        bin_callback: None,
        lib_callback: None,
    };

    let mut g = state.cmd.lock().await;
    let conn = g
        .conn
        .as_mut()
        .ok_or_else(|| (StatusCode::SERVICE_UNAVAILABLE, "not connected".to_string()))?;

    info!("Uploading program to slot {} (0-indexed)", slot0);

    match timeout(Duration::from_secs(120), conn.execute_command(cmd)).await {
        Ok(Ok(())) => Ok((StatusCode::OK, "upload complete".to_string())),
        Ok(Err(e)) => Err((StatusCode::BAD_GATEWAY, format!("upload failed: {e}"))),
        Err(_) => Err((StatusCode::GATEWAY_TIMEOUT, "upload timed out".to_string())),
    }
}

async fn run_handler_native(
    State(state): State<AppState>,
    slot: u8,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    ensure_cmd_connected(&state.cmd)
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e))?;

    let filename = format!("slot_{}.bin", slot);
    let filename_fixed = FixedString::new(&filename)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("invalid filename: {e}")))?;

    let payload = FileLoadPayload {
        vid: VolumeId::User,
        options: ExecuteOptions::run(),
        filename: filename_fixed,
    };

    let packet = FileLoadPacket::new(payload);

    let mut g = state.cmd.lock().await;
    let conn = g
        .conn
        .as_mut()
        .ok_or_else(|| (StatusCode::SERVICE_UNAVAILABLE, "not connected".to_string()))?;

    info!("Running program in slot {}", slot);

    match timeout(
        Duration::from_secs(5),
        conn.handshake::<FileLoadReplyPacket>(Duration::from_secs(2), 0, packet),
    )
    .await
    {
        Ok(Ok(_)) => Ok((StatusCode::OK, format!("Program in slot {} started", slot))),
        Ok(Err(e)) => Err((StatusCode::BAD_GATEWAY, format!("run failed: {e}"))),
        Err(_) => Err((StatusCode::GATEWAY_TIMEOUT, "run timed out".to_string())),
    }
}



async fn run_handler(
    state: State<AppState>,
    Json(req): Json<RunRequest>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    let slot = normalize_slot(req.slot).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    run_handler_native(state, slot).await
}

async fn stop_handler_native(
    State(state): State<AppState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    ensure_cmd_connected(&state.cmd)
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE, e))?;

    // For stop, filename might not matter but we'll send it anyway like PROS does
    // Using slot 0 as a generic placeholder since slot doesn't matter for stopping.
    let filename = format!("slot_{}.bin", 0);
    let filename_fixed = FixedString::new(&filename)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("invalid filename: {e}")))?;

    let payload = FileLoadPayload {
        vid: VolumeId::User,
        options: ExecuteOptions::stop(),
        filename: filename_fixed,
    };

    let packet = FileLoadPacket::new(payload);

    let mut g = state.cmd.lock().await;
    let conn = g
        .conn
        .as_mut()
        .ok_or_else(|| (StatusCode::SERVICE_UNAVAILABLE, "not connected".to_string()))?;

    info!("Stopping program");

    match timeout(
        Duration::from_secs(5),
        conn.handshake::<FileLoadReplyPacket>(Duration::from_secs(2), 0, packet),
    )
    .await
    {
        Ok(Ok(_)) => Ok((StatusCode::OK, "Program stopped".to_string())),
        Ok(Err(e)) => Err((StatusCode::BAD_GATEWAY, format!("stop failed: {e}"))),
        Err(_) => Err((StatusCode::GATEWAY_TIMEOUT, "stop timed out".to_string())),
    }
}



async fn stop_handler(
    state: State<AppState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    stop_handler_native(state).await
}

async fn open_user_port_stream() -> Result<tokio_serial::SerialStream, String> {
    let (_system, user) = discover_brain_ports()?;
    info!("Opening Brain user port for stdio: {}", user);

    tokio_serial::SerialStream::open(
        &tokio_serial::new(user, 115200)
            .parity(tokio_serial::Parity::None)
            .stop_bits(tokio_serial::StopBits::One)
            .timeout(Duration::from_secs(5)),
    )
    .map_err(|e| format!("open user port failed: {e}"))
}

async fn stdio_pump_task(
    stdout_tx: broadcast::Sender<Vec<u8>>,
    mut stdin_rx: mpsc::Receiver<Vec<u8>>,
) {
    let mut backoff = Duration::from_millis(200);
    let backoff_max = Duration::from_secs(3);

    loop {
        let stream = match open_user_port_stream().await {
            Ok(s) => {
                backoff = Duration::from_millis(200);
                s
            }
            Err(e) => {
                warn!("stdio: no user port yet ({e}); retrying in {:?}", backoff);
                sleep(backoff).await;
                backoff = (backoff * 2).min(backoff_max);
                continue;
            }
        };

        let (mut r, mut w) = tokio::io::split(stream);
        let mut buf = [0u8; 16 * 1024];

        loop {
            tokio::select! {
                read = r.read(&mut buf) => {
                    match read {
                        Ok(0) => {
                            warn!("stdio: user port EOF (0 bytes), reconnecting");
                            break;
                        }
                        Ok(n) => {
                            let _ = stdout_tx.send(buf[..n].to_vec());
                        }
                        Err(e) => {
                            warn!("stdio: read error {e}, reconnecting");
                            break;
                        }
                    }
                }

                maybe = stdin_rx.recv() => {
                    let Some(bytes) = maybe else {
                        warn!("stdio: stdin channel closed; exiting stdio task");
                        return;
                    };
                    if let Err(e) = w.write_all(&bytes).await {
                        warn!("stdio: write error {e}, reconnecting");
                        break;
                    }
                }
            }
        }

        sleep(Duration::from_millis(100)).await;
    }
}

async fn tcp_stdio_server(
    addr: SocketAddr,
    stdout_tx: broadcast::Sender<Vec<u8>>,
    stdin_tx: mpsc::Sender<Vec<u8>>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Stdio TCP server (duplex) on {}", addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        info!("Stdio client connected: {}", peer);

        let mut stdout_rx = stdout_tx.subscribe();
        let stdin_tx = stdin_tx.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_stdio_client(socket, &mut stdout_rx, stdin_tx).await {
                warn!("client {} ended: {}", peer, e);
            }
            info!("Stdio client disconnected: {}", peer);
        });
    }
}

async fn handle_stdio_client(
    socket: TcpStream,
    stdout_rx: &mut broadcast::Receiver<Vec<u8>>,
    stdin_tx: mpsc::Sender<Vec<u8>>,
) -> anyhow::Result<()> {
    let (mut r, mut w) = socket.into_split();

    w.write_all(b"[v5-serial] connected; Brain user stdio (duplex)\n")
        .await?;

    let mut inbuf = [0u8; 4096];

    loop {
        tokio::select! {
            read = r.read(&mut inbuf) => {
                let n = read?;
                if n == 0 { break; }
                let _ = stdin_tx.send(inbuf[..n].to_vec()).await;
            }

            msg = stdout_rx.recv() => {
                match msg {
                    Ok(chunk) => {
                        let wr = timeout(Duration::from_secs(5), w.write_all(&chunk)).await;
                        match wr {
                            Ok(Ok(())) => {}
                            _ => break,
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        let _ = w.write_all(format!("\n[v5-serial] lagged; skipped {skipped} chunks\n").as_bytes()).await;
                    }
                    Err(_) => break,
                }
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()))
        .init();

    info!("V5 Brain uploader + stdio TCP service starting…");

    let (stdout_tx, _stdout_rx) = broadcast::channel::<Vec<u8>>(2048);
    let (stdin_tx, stdin_rx) = mpsc::channel::<Vec<u8>>(2048);

    let cmd = Arc::new(Mutex::new(CmdConn::new()));

    tokio::spawn({
        let stdout_tx = stdout_tx.clone();
        async move {
            stdio_pump_task(stdout_tx, stdin_rx).await;
        }
    });

    tokio::spawn({
        let stdout_tx = stdout_tx.clone();
        let stdin_tx = stdin_tx.clone();
        async move {
            let addr: SocketAddr = "0.0.0.0:8903".parse().unwrap();
            if let Err(e) = tcp_stdio_server(addr, stdout_tx, stdin_tx).await {
                error!("tcp stdio server crashed: {e}");
            }
        }
    });

    let state = AppState { cmd };

    let app = Router::new()
        .route("/upload", post(upload_handler))
        .route("/run", post(run_handler))
        .route("/stop", post(stop_handler))
        .with_state(state);

    let http_addr: SocketAddr = "0.0.0.0:80".parse().unwrap();
    info!("HTTP on http://{}/upload, /run, /stop", http_addr);
    info!("TCP stdio on tcp://0.0.0.0:8903 (use: nc localhost 8903)");

    axum::serve(tokio::net::TcpListener::bind(http_addr).await?, app).await?;
    Ok(())
}
