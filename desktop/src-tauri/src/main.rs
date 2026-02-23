use std::fs;
use std::net::{Ipv4Addr, SocketAddrV4, TcpStream};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use tauri::{Manager, WindowEvent};

const BACKEND_HOST: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
const BACKEND_PORT: u16 = 8000;
const BACKEND_URL_SPAWNED: &str = "http://127.0.0.1:8000/?spawned=1";
const BACKEND_URL_EXTERNAL: &str = "http://127.0.0.1:8000/?spawned=0&external_backend=1";
const NO_BROWSER_ENV: &str = "MTG_ENGINE_NO_BROWSER";
const DB_PATH_ENV: &str = "MTG_ENGINE_DB_PATH";
const UI_DIST_ENV: &str = "MTG_ENGINE_UI_DIST_DIR";
const IMAGE_CACHE_ENV: &str = "MTG_ENGINE_IMAGE_CACHE_DIR";
const MISSING_DIST_MESSAGE: &str = "UI not built. Run cd ui_harness && npm run build";
const MISSING_DB_MESSAGE: &str = "Baseline DB missing. Run cd desktop && npm run sync:db";
const UI_DIST_VERSION_FILE: &str = "ui_dist_version.txt";

struct ManagedBackend {
    spawned_by_app: bool,
    child: Option<Child>,
}

struct BackendState {
    managed: Mutex<ManagedBackend>,
}

fn repo_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    match manifest_dir.parent().and_then(|parent| parent.parent()) {
        Some(root) => root.to_path_buf(),
        None => manifest_dir,
    }
}

fn preferred_python_path(repo_root: &Path) -> PathBuf {
    if cfg!(windows) {
        repo_root.join(".venv").join("Scripts").join("python.exe")
    } else {
        repo_root.join(".venv").join("bin").join("python")
    }
}

fn is_backend_port_open() -> bool {
    let socket = SocketAddrV4::new(BACKEND_HOST, BACKEND_PORT);
    TcpStream::connect_timeout(&socket.into(), Duration::from_millis(200)).is_ok()
}

fn is_backend_health_ready() -> bool {
    let socket = SocketAddrV4::new(BACKEND_HOST, BACKEND_PORT);
    let mut stream = match TcpStream::connect_timeout(&socket.into(), Duration::from_millis(200)) {
        Ok(connection) => connection,
        Err(_) => return false,
    };

    let _ = stream.set_read_timeout(Some(Duration::from_millis(400)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(400)));

    let request = b"GET /health HTTP/1.1\r\nHost: 127.0.0.1:8000\r\nConnection: close\r\n\r\n";
    if stream.write_all(request).is_err() {
        return false;
    }

    if stream.flush().is_err() {
        return false;
    }

    let mut response_prefix = [0_u8; 256];
    let read_bytes = match stream.read(&mut response_prefix) {
        Ok(size) if size > 0 => size,
        _ => return false,
    };

    let response_head = String::from_utf8_lossy(&response_prefix[..read_bytes]);
    response_head.starts_with("HTTP/1.1 200") || response_head.starts_with("HTTP/1.0 200")
}

fn show_missing_dist_window(app: &tauri::App) -> Result<(), String> {
    tauri::WebviewWindowBuilder::new(
        app,
        "missing-dist",
        tauri::WebviewUrl::App("missing_dist.html".into()),
    )
    .title("MTG Engine")
    .inner_size(620.0, 300.0)
    .resizable(false)
    .build()
    .map(|_| ())
    .map_err(|error| format!("Failed to open missing dist notice window: {error}"))
}

fn copy_dir_recursive(source_dir: &Path, target_dir: &Path) -> Result<(), String> {
    if !target_dir.exists() {
        fs::create_dir_all(target_dir)
            .map_err(|error| format!("Failed to create directory {}: {error}", target_dir.display()))?;
    }

    let entries = fs::read_dir(source_dir)
        .map_err(|error| format!("Failed to read directory {}: {error}", source_dir.display()))?;

    for entry_result in entries {
        let entry = entry_result
            .map_err(|error| format!("Failed to read directory entry in {}: {error}", source_dir.display()))?;
        let entry_path = entry.path();
        let destination = target_dir.join(entry.file_name());

        if entry_path.is_dir() {
            copy_dir_recursive(&entry_path, &destination)?;
            continue;
        }

        fs::copy(&entry_path, &destination).map_err(|error| {
            format!(
                "Failed to copy file {} to {}: {error}",
                entry_path.display(),
                destination.display()
            )
        })?;
    }

    Ok(())
}

fn resolve_bundled_ui_paths(app: &tauri::App, repo_root: &Path) -> Result<(PathBuf, PathBuf), String> {
    let mut candidates: Vec<PathBuf> = Vec::new();

    if let Ok(resource_dir) = app.path().resource_dir() {
        candidates.push(resource_dir);
    }

    candidates.push(repo_root.join("desktop").join("resources"));

    for candidate in candidates {
        let ui_dist_dir = candidate.join("ui_dist");
        let marker_path = candidate.join(UI_DIST_VERSION_FILE);
        if ui_dist_dir.is_dir() && marker_path.is_file() {
            return Ok((ui_dist_dir, marker_path));
        }
    }

    Err(MISSING_DIST_MESSAGE.to_string())
}

fn ensure_ui_dist_in_app_data(app: &tauri::App, repo_root: &Path) -> Result<PathBuf, String> {
    let app_data_dir = app
        .path()
        .app_data_dir()
        .map_err(|error| format!("Failed to resolve app data directory: {error}"))?;
    let target_ui_dist_dir = app_data_dir.join("ui_dist");
    let target_marker_path = target_ui_dist_dir.join(UI_DIST_VERSION_FILE);

    let (source_ui_dist_dir, source_marker_path) = resolve_bundled_ui_paths(app, repo_root)?;
    let source_marker = fs::read_to_string(&source_marker_path).map_err(|error| {
        format!(
            "Failed to read UI dist marker {}: {error}",
            source_marker_path.display()
        )
    })?;

    let target_marker = fs::read_to_string(&target_marker_path).ok();
    let needs_copy = !target_ui_dist_dir.is_dir() || target_marker.as_deref() != Some(source_marker.as_str());

    if needs_copy {
        if target_ui_dist_dir.is_dir() {
            fs::remove_dir_all(&target_ui_dist_dir).map_err(|error| {
                format!(
                    "Failed to clear existing UI dist directory {}: {error}",
                    target_ui_dist_dir.display()
                )
            })?;
        }

        fs::create_dir_all(&target_ui_dist_dir).map_err(|error| {
            format!(
                "Failed to create target UI dist directory {}: {error}",
                target_ui_dist_dir.display()
            )
        })?;

        copy_dir_recursive(&source_ui_dist_dir, &target_ui_dist_dir)?;
        fs::write(&target_marker_path, source_marker.as_bytes()).map_err(|error| {
            format!(
                "Failed to write target UI marker {}: {error}",
                target_marker_path.display()
            )
        })?;
    }

    let target_index = target_ui_dist_dir.join("index.html");
    if !target_index.is_file() {
        return Err(MISSING_DIST_MESSAGE.to_string());
    }

    Ok(target_ui_dist_dir)
}

fn resolve_bundled_db_path(app: &tauri::App, repo_root: &Path) -> Result<PathBuf, String> {
    let mut candidates: Vec<PathBuf> = Vec::new();

    if let Ok(resource_dir) = app.path().resource_dir() {
        candidates.push(resource_dir.join("mtg.sqlite"));
    }

    candidates.push(repo_root.join("desktop").join("resources").join("mtg.sqlite"));

    for candidate in candidates {
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    Err(MISSING_DB_MESSAGE.to_string())
}

fn ensure_baseline_db_in_app_data(app: &tauri::App, repo_root: &Path) -> Result<PathBuf, String> {
    let app_data_dir = app
        .path()
        .app_data_dir()
        .map_err(|error| format!("Failed to resolve app data directory for baseline DB: {error}"))?;
    let target_db_path = app_data_dir.join("mtg.sqlite");

    if target_db_path.is_file() {
        return Ok(target_db_path);
    }

    let source_db_path = resolve_bundled_db_path(app, repo_root)?;
    fs::create_dir_all(&app_data_dir).map_err(|error| {
        format!(
            "Failed to create app data directory {}: {error}",
            app_data_dir.display()
        )
    })?;
    fs::copy(&source_db_path, &target_db_path).map_err(|error| {
        format!(
            "Failed to copy baseline DB from {} to {}: {error}",
            source_db_path.display(),
            target_db_path.display()
        )
    })?;

    if !target_db_path.is_file() {
        return Err(format!(
            "Baseline DB copy did not produce {}",
            target_db_path.display()
        ));
    }

    Ok(target_db_path)
}

fn ensure_image_cache_dirs_in_app_data(app: &tauri::App) -> Result<PathBuf, String> {
    let app_data_dir = app
        .path()
        .app_data_dir()
        .map_err(|error| format!("Failed to resolve app data directory for image cache: {error}"))?;
    let cache_root = app_data_dir.join("card_images");

    for size in ["normal", "small"] {
        let size_dir = cache_root.join(size);
        fs::create_dir_all(&size_dir).map_err(|error| {
            format!(
                "Failed to create image cache directory {}: {error}",
                size_dir.display()
            )
        })?;
    }

    Ok(cache_root)
}

fn wait_for_existing_backend_ready(timeout: Duration) -> bool {
    let started_at = Instant::now();
    while started_at.elapsed() < timeout {
        if is_backend_port_open() && is_backend_health_ready() {
            return true;
        }
        thread::sleep(Duration::from_millis(250));
    }
    false
}

fn wait_for_backend_ready(child: &mut Child, timeout: Duration) -> Result<(), String> {
    let started_at = Instant::now();
    while started_at.elapsed() < timeout {
        if is_backend_port_open() && is_backend_health_ready() {
            return Ok(());
        }

        match child.try_wait() {
            Ok(Some(status)) => {
                return Err(format!(
                    "Backend process exited before readiness check completed (status: {status})"
                ));
            }
            Ok(None) => {}
            Err(error) => {
                return Err(format!(
                    "Failed while waiting for backend readiness: {error}"
                ));
            }
        }

        thread::sleep(Duration::from_millis(250));
    }

    Err("Backend did not become ready on port 8000 within 20 seconds".to_string())
}

fn spawn_backend_process(
    repo_root: &Path,
    ui_dist_dir: &Path,
    db_path: &Path,
    image_cache_dir: &Path,
) -> Result<Child, String> {
    let launch_prod_path = repo_root.join("launch_prod.py");
    if !launch_prod_path.is_file() {
        return Err(format!(
            "Missing launch_prod.py at {}",
            launch_prod_path.display()
        ));
    }

    let preferred_python = preferred_python_path(repo_root);
    let mut command = if preferred_python.is_file() {
        let mut cmd = Command::new(preferred_python);
        cmd.arg(&launch_prod_path);
        cmd
    } else {
        let mut cmd = Command::new("python");
        cmd.arg(&launch_prod_path);
        cmd
    };

    command
        .current_dir(repo_root)
        .env(NO_BROWSER_ENV, "1")
        .env(DB_PATH_ENV, db_path.as_os_str())
        .env(UI_DIST_ENV, ui_dist_dir.as_os_str())
        .env(IMAGE_CACHE_ENV, image_cache_dir.as_os_str())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    command
        .spawn()
        .map_err(|error| format!("Failed to start backend process: {error}"))
}

fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> bool {
    let started_at = Instant::now();
    while started_at.elapsed() < timeout {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => thread::sleep(Duration::from_millis(100)),
            Err(_) => return false,
        }
    }

    false
}

fn try_graceful_terminate(child: &mut Child) {
    let pid = child.id().to_string();

    #[cfg(windows)]
    {
        let _ = Command::new("taskkill").args(["/PID", pid.as_str()]).status();
    }

    #[cfg(not(windows))]
    {
        let _ = Command::new("kill").args(["-TERM", pid.as_str()]).status();
    }
}

fn force_kill(child: &mut Child) {
    let pid = child.id().to_string();

    #[cfg(windows)]
    {
        let _ = Command::new("taskkill")
            .args(["/PID", pid.as_str(), "/T", "/F"])
            .status();
    }

    let _ = child.kill();
    let _ = child.wait();
}

fn terminate_child_with_timeout(child: &mut Child) {
    try_graceful_terminate(child);
    if wait_for_child_exit(child, Duration::from_secs(5)) {
        return;
    }

    force_kill(child);
}

fn shutdown_backend_if_spawned(state: &BackendState) {
    let mut managed_guard = state
        .managed
        .lock()
        .expect("Backend state mutex poisoned during shutdown");

    if !managed_guard.spawned_by_app {
        return;
    }

    if let Some(mut child) = managed_guard.child.take() {
        terminate_child_with_timeout(&mut child);
    }
}

fn initialize_backend_state(
    root: &Path,
    ui_dist_dir: &Path,
    db_path: &Path,
    image_cache_dir: &Path,
) -> Result<ManagedBackend, String> {
    if is_backend_port_open() {
        if !wait_for_existing_backend_ready(Duration::from_secs(20)) {
            return Err("Backend port is open but /health did not return HTTP 200 within 20 seconds".to_string());
        }
        return Ok(ManagedBackend {
            spawned_by_app: false,
            child: None,
        });
    }

    let mut child = spawn_backend_process(root, ui_dist_dir, db_path, image_cache_dir)?;

    if let Err(error) = wait_for_backend_ready(&mut child, Duration::from_secs(20)) {
        terminate_child_with_timeout(&mut child);
        return Err(error);
    }

    Ok(ManagedBackend {
        spawned_by_app: true,
        child: Some(child),
    })
}

fn main() {
    let app = tauri::Builder::default()
        .manage(BackendState {
            managed: Mutex::new(ManagedBackend {
                spawned_by_app: false,
                child: None,
            }),
        })
        .setup(|app| {
            let root = repo_root();
            let ui_dist_dir = match ensure_ui_dist_in_app_data(app, &root) {
                Ok(path) => path,
                Err(error) => {
                    eprintln!("{error}");
                    if let Err(window_error) = show_missing_dist_window(app) {
                        eprintln!("{window_error}");
                        eprintln!("{MISSING_DIST_MESSAGE}");
                    }
                    return Ok(());
                }
            };

            let db_path = match ensure_baseline_db_in_app_data(app, &root) {
                Ok(path) => path,
                Err(error) => {
                    eprintln!("{error}");
                    std::process::exit(1);
                }
            };

            let image_cache_dir = match ensure_image_cache_dirs_in_app_data(app) {
                Ok(path) => path,
                Err(error) => {
                    eprintln!("{error}");
                    std::process::exit(1);
                }
            };

            let mut managed_backend = match initialize_backend_state(&root, &ui_dist_dir, &db_path, &image_cache_dir) {
                Ok(state) => state,
                Err(error) => {
                    eprintln!("Failed to initialize backend for desktop app: {error}");
                    std::process::exit(1);
                }
            };

            let target_url = if managed_backend.spawned_by_app {
                BACKEND_URL_SPAWNED
            } else {
                BACKEND_URL_EXTERNAL
            }
                .parse()
                .expect("Desktop backend URL must be a valid URL");

            if let Err(window_error) = tauri::WebviewWindowBuilder::new(app, "main", tauri::WebviewUrl::External(target_url))
                .title("MTG Engine")
                .inner_size(1440.0, 920.0)
                .build()
            {
                if managed_backend.spawned_by_app {
                    if let Some(mut child) = managed_backend.child.take() {
                        terminate_child_with_timeout(&mut child);
                    }
                }
                return Err(window_error.into());
            }

            let backend_state = app.state::<BackendState>();
            let mut managed_guard = backend_state
                .managed
                .lock()
                .expect("Backend state mutex poisoned during startup");
            *managed_guard = managed_backend;

            Ok(())
        })
        .on_window_event(|window, event| {
            if matches!(event, WindowEvent::CloseRequested { .. }) {
                let state = window.state::<BackendState>();
                shutdown_backend_if_spawned(&state);
            }
        })
        .build(tauri::generate_context!());

    match app {
        Ok(app) => {
            app.run(|app_handle, event| {
                if matches!(event, tauri::RunEvent::ExitRequested { .. }) {
                    let state = app_handle.state::<BackendState>();
                    shutdown_backend_if_spawned(&state);
                }
            });
        }
        Err(error) => {
            eprintln!("Failed to build desktop application: {error}");
            std::process::exit(1);
        }
    }
}
