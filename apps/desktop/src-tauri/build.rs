fn main() {
    tauri_build::try_build(tauri_build::Attributes::new().app_manifest(
        tauri_build::AppManifest::new().commands(&[
            "dashboard_snapshot",
            "choose_project_directory",
            "resolve_project_php",
            "discover_project_queues",
            "project_action",
            "reconcile_project_action",
        ]),
    ))
    .expect("desktop build configuration is valid");
}
