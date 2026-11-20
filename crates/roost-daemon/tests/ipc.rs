use std::{
    fs,
    os::unix::{
        fs::{MetadataExt, PermissionsExt},
        net::UnixListener,
    },
    path::PathBuf,
};

use roost_daemon::Daemon;
use roost_ipc::{Client, VERSION, endpoint::SOCKET_NAME};
use uuid::Uuid;

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        // Keep the socket pathname within macOS's sun_path limit.
        let root = PathBuf::from(format!("/tmp/roost-ipc-{}", Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        Self(root)
    }

    fn state(&self) -> PathBuf {
        self.0.join("state")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn protects_private_endpoint_and_recovers_a_stale_socket() {
    let fixture = Fixture::new();
    let state = fixture.state();
    let socket = state.join(SOCKET_NAME);
    fs::create_dir(&state).unwrap();
    fs::set_permissions(&state, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(Daemon::bind(&state).await.is_err());
    fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(&socket, "unrelated file").unwrap();
    assert!(Daemon::bind(&state).await.is_err());
    assert_eq!(fs::read_to_string(&socket).unwrap(), "unrelated file");
    fs::remove_file(&socket).unwrap();

    let live = UnixListener::bind(&socket).unwrap();
    assert!(Daemon::bind(&state).await.is_err());
    drop(live);
    let daemon = Daemon::bind(&state).await.unwrap();
    let task = tokio::spawn(async move { daemon.run().await });
    let mut client = Client::connect(&state).await.unwrap();
    assert_eq!(client.status().await.unwrap().protocol_version, VERSION);
    assert!(Daemon::bind(&state).await.is_err());
    for path in [
        &socket,
        &state.join("roost.sqlite3"),
        &state.join("daemon.lock"),
    ] {
        assert_eq!(fs::symlink_metadata(path).unwrap().mode() & 0o777, 0o600);
    }
    drop(client);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(!socket.exists());
}
