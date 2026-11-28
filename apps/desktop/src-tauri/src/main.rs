mod dashboard;
mod project_ops;

use std::path::PathBuf;

use serde::Serialize;
use tauri::{Emitter, Manager, RunEvent, Theme, WindowEvent};
use tauri_plugin_dialog::DialogExt;
use tokio::sync::Mutex;

const WINDOW_VISIBLE: &str = "roost://window-visible";

struct Bridge {
    state_dir: PathBuf,
    active: Mutex<()>,
    operation: Mutex<Option<PreparedLaunch>>,
}

struct PreparedLaunch {
    id: roost_core::ProjectId,
    ticket: uuid::Uuid,
    php: PathBuf,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum BridgeError {
    Offline,
    Disconnected,
    Incompatible,
    Unsupported,
    Timeout,
    AccessDenied,
    InvalidResponse,
    Unavailable,
    Busy,
}

#[tauri::command]
async fn dashboard_snapshot(
    bridge: tauri::State<'_, Bridge>,
    page: u32,
    selected_id: Option<roost_core::ProjectId>,
) -> Result<dashboard::Dashboard, BridgeError> {
    let _active = bridge.active.try_lock().map_err(|_| BridgeError::Busy)?;
    dashboard::read(&bridge.state_dir, page, selected_id).await
}

#[tauri::command]
async fn choose_project_directory(app: tauri::AppHandle) -> Result<Option<PathBuf>, BridgeError> {
    let (send, receive) = tokio::sync::oneshot::channel();
    app.dialog()
        .file()
        .set_title("Add Laravel project")
        .pick_folder(move |path| {
            let _ = send.send(path);
        });
    receive
        .await
        .map_err(|_| BridgeError::Unavailable)?
        .map(|path| path.into_path().map_err(|_| BridgeError::Unavailable))
        .transpose()
}

#[tauri::command]
async fn resolve_project_php(
    bridge: tauri::State<'_, Bridge>,
    id: roost_core::ProjectId,
    provider: Option<roost_core::PhpProvider>,
) -> Result<uuid::Uuid, BridgeError> {
    let mut launch = bridge.operation.try_lock().map_err(|_| BridgeError::Busy)?;
    let resolved = project_ops::resolve(&bridge.state_dir, id, provider).await?;
    let ticket = uuid::Uuid::new_v4();
    *launch = Some(PreparedLaunch {
        id,
        ticket,
        php: resolved.path,
    });
    Ok(ticket)
}

#[tauri::command]
async fn discover_project_queues(
    bridge: tauri::State<'_, Bridge>,
    id: roost_core::ProjectId,
    provider: Option<roost_core::PhpProvider>,
) -> Result<project_ops::Discovery, BridgeError> {
    let _active = bridge.operation.try_lock().map_err(|_| BridgeError::Busy)?;
    project_ops::discover(&bridge.state_dir, id, provider).await
}

#[tauri::command]
async fn project_action(
    bridge: tauri::State<'_, Bridge>,
    action: project_ops::Action,
    key: uuid::Uuid,
) -> Result<project_ops::Receipt, project_ops::ActionError> {
    let launch = bridge
        .operation
        .try_lock()
        .map_err(|_| project_ops::ActionError::Connection {
            fault: BridgeError::Busy,
        })?;
    let php = match action.launch() {
        Some((id, ticket)) => Some(
            launch
                .as_ref()
                .filter(|prepared| prepared.id == id && prepared.ticket == ticket)
                .ok_or(project_ops::ActionError::Rejected {
                    reason: "expired_launch",
                })?
                .php
                .clone(),
        ),
        None => None,
    };
    project_ops::execute(&bridge.state_dir, action, key, php).await
}

fn main() {
    let state_dir = std::env::var_os("ROOST_DESKTOP_STATE_DIR")
        .map(PathBuf::from)
        .map_or_else(roost_ipc::state_dir, Ok)
        .expect("Roost requires an absolute state directory");
    assert!(
        state_dir.is_absolute(),
        "Roost requires an absolute state directory"
    );
    let app = tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .manage(Bridge {
            state_dir,
            active: Mutex::new(()),
            operation: Mutex::new(None),
        })
        .setup(|app| {
            // Apply an app-local appearance for visual smoke runs. System appearance is the default.
            let appearance = match std::env::var("ROOST_DESKTOP_APPEARANCE").as_deref() {
                Ok("light") => Some(Theme::Light),
                Ok("dark") => Some(Theme::Dark),
                _ => None,
            };
            if let Some(window) = app.get_webview_window("main") {
                window.set_theme(appearance)?;
            }
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            dashboard_snapshot,
            choose_project_directory,
            resolve_project_php,
            discover_project_queues,
            project_action
        ])
        .on_window_event(|window, event| {
            if let WindowEvent::CloseRequested { api, .. } = event {
                api.prevent_close();
                let _ = window.emit(WINDOW_VISIBLE, false);
                let _ = window.hide();
            }
        })
        .build(tauri::generate_context!())
        .expect("could not start the Roost desktop");
    app.run(|app, event| {
        if let RunEvent::Reopen { .. } = event
            && let Some(window) = app.get_webview_window("main")
        {
            let _ = window.show();
            let _ = window.set_focus();
            let _ = window.emit(WINDOW_VISIBLE, true);
        }
    });
}
