use std::process::Command;

use uuid::Uuid;

#[test]
fn reports_missing_daemon_socket_with_startup_hint() {
    let state = std::env::temp_dir().join(format!("roost-missing-{}", Uuid::new_v4()));
    let output = Command::new(env!("CARGO_BIN_EXE_roost"))
        .arg("--state-dir")
        .arg(&state)
        .args(["provider", "list"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains(&*state.join("roost.sock").to_string_lossy()));
    assert!(error.contains("Start roost-daemon"));
    assert!(error.contains("pass the same directory"));
}
