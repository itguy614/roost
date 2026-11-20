use std::{
    fs::{self, DirBuilder},
    io,
    os::unix::fs::{DirBuilderExt, FileTypeExt, MetadataExt},
    path::{Path, PathBuf},
};

use tokio::net::UnixStream;

pub const SOCKET_NAME: &str = "roost.sock";

pub fn effective_uid() -> u32 {
    // SAFETY: geteuid takes no arguments and has no memory preconditions.
    unsafe { libc::geteuid() }
}

pub fn private_directory(path: &Path, create: bool) -> io::Result<PathBuf> {
    let name = path
        .file_name()
        .filter(|_| path.is_absolute())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "state directory must be an absolute path with a directory name",
            )
        })?;
    let parent = fs::canonicalize(path.parent().expect("named absolute path has a parent"))?;
    let uid = effective_uid();
    for ancestor in parent.ancestors().collect::<Vec<_>>().into_iter().rev() {
        let meta = fs::symlink_metadata(ancestor)?;
        let trusted_owner = meta.uid() == uid || meta.uid() == 0;
        let trusted_mode = meta.mode() & 0o022 == 0 || meta.mode() & 0o1000 != 0;
        if !meta.is_dir() || !trusted_owner || !trusted_mode {
            return Err(permission("state directory has an untrusted parent"));
        }
    }
    let path = parent.join(name);
    if create {
        match DirBuilder::new().mode(0o700).create(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    let meta = fs::symlink_metadata(&path)?;
    if !meta.is_dir() || meta.uid() != uid || meta.mode() & 0o777 != 0o700 {
        return Err(permission(
            "state directory must be owned by the current user with mode 0700",
        ));
    }
    Ok(path)
}

pub fn owned_socket(path: &Path) -> io::Result<fs::Metadata> {
    let meta = fs::symlink_metadata(path)?;
    if !meta.file_type().is_socket() || meta.uid() != effective_uid() {
        return Err(permission(
            "IPC endpoint must be a socket owned by the current user",
        ));
    }
    Ok(meta)
}

pub fn verify_socket(path: &Path) -> io::Result<()> {
    if owned_socket(path)?.mode() & 0o777 != 0o600 {
        return Err(permission("IPC socket must have mode 0600"));
    }
    Ok(())
}

pub fn verify_peer(stream: &UnixStream) -> io::Result<()> {
    if stream.peer_cred()?.uid() != effective_uid() {
        return Err(permission("IPC peer must have the same effective user ID"));
    }
    Ok(())
}

fn permission(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, message)
}
