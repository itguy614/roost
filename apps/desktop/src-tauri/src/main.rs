mod dashboard;

use std::path::PathBuf;

use serde::Serialize;
use tauri::{Emitter, Manager, RunEvent, Theme, WindowEvent};
use tokio::sync::Mutex;

const WINDOW_VISIBLE: &str = "roost://window-visible";

struct Bridge {
    state_dir: PathBuf,
    active: Mutex<()>,
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
) -> Result<dashboard::Dashboard, BridgeError> {
    let _active = bridge.active.try_lock().map_err(|_| BridgeError::Busy)?;
    dashboard::read(&bridge.state_dir, page).await
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
        .manage(Bridge {
            state_dir,
            active: Mutex::new(()),
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
        .invoke_handler(tauri::generate_handler![dashboard_snapshot])
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
