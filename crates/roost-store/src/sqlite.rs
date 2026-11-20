//! Minimal system SQLite bindings. Statements never escape a Connection call.
use std::{
    ffi::{CStr, CString, c_char, c_int},
    os::unix::ffi::OsStrExt,
    path::Path,
    ptr::{self, NonNull},
};

#[repr(C)]
struct Db {
    _private: [u8; 0],
}
#[repr(C)]
struct Stmt {
    _private: [u8; 0],
}

#[link(name = "sqlite3")]
unsafe extern "C" {
    fn sqlite3_open_v2(
        path: *const c_char,
        db: *mut *mut Db,
        flags: c_int,
        vfs: *const c_char,
    ) -> c_int;
    fn sqlite3_close(db: *mut Db) -> c_int;
    fn sqlite3_errmsg(db: *mut Db) -> *const c_char;
    fn sqlite3_extended_errcode(db: *mut Db) -> c_int;
    fn sqlite3_busy_timeout(db: *mut Db, ms: c_int) -> c_int;
    fn sqlite3_exec(
        db: *mut Db,
        sql: *const c_char,
        callback: Option<
            unsafe extern "C" fn(
                *mut std::ffi::c_void,
                c_int,
                *mut *mut c_char,
                *mut *mut c_char,
            ) -> c_int,
        >,
        context: *mut std::ffi::c_void,
        error: *mut *mut c_char,
    ) -> c_int;
    fn sqlite3_prepare_v2(
        db: *mut Db,
        sql: *const c_char,
        len: c_int,
        stmt: *mut *mut Stmt,
        tail: *mut *const c_char,
    ) -> c_int;
    fn sqlite3_bind_text(
        stmt: *mut Stmt,
        index: c_int,
        data: *const c_char,
        len: c_int,
        destroy: Option<unsafe extern "C" fn(*mut std::ffi::c_void)>,
    ) -> c_int;
    fn sqlite3_step(stmt: *mut Stmt) -> c_int;
    fn sqlite3_column_text(stmt: *mut Stmt, column: c_int) -> *const u8;
    fn sqlite3_finalize(stmt: *mut Stmt) -> c_int;
    fn sqlite3_changes(db: *mut Db) -> c_int;
}

const OK: c_int = 0;
const ROW: c_int = 100;
const DONE: c_int = 101;
const OPEN_READWRITE: c_int = 0x0000_0002;
const OPEN_CREATE: c_int = 0x0000_0004;
const OPEN_NOFOLLOW: c_int = 0x0100_0000;

#[derive(Debug, thiserror::Error)]
#[error("SQLite error {code}: {message}")]
pub struct Error {
    pub code: c_int,
    message: String,
}

impl Error {
    fn input(message: &str) -> Self {
        Self {
            code: -1,
            message: message.into(),
        }
    }
}

pub struct Connection(NonNull<Db>);

impl Connection {
    pub fn open(path: &Path, create: bool) -> Result<Self, Error> {
        let path = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| Error::input("NUL in database path"))?;
        let mut db = ptr::null_mut();
        let flags = OPEN_READWRITE | OPEN_NOFOLLOW | if create { OPEN_CREATE } else { 0 };
        // SQLite may return a handle even if opening fails. Own it before checking the result.
        let code = unsafe { sqlite3_open_v2(path.as_ptr(), &mut db, flags, ptr::null()) };
        let connection = Self(
            NonNull::new(db).ok_or_else(|| Error::input("could not allocate a database handle"))?,
        );
        connection.check(code)?;
        // SAFETY: the handle is live for the duration of the call.
        connection.check(unsafe { sqlite3_busy_timeout(connection.0.as_ptr(), 3000) })?;
        connection.exec(c"PRAGMA foreign_keys = ON")?;
        Ok(connection)
    }

    pub fn exec(&self, sql: &CStr) -> Result<(), Error> {
        // SAFETY: both pointers are live and neither callbacks nor error strings are requested.
        self.check(unsafe {
            sqlite3_exec(
                self.0.as_ptr(),
                sql.as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        })
    }

    pub fn query(&self, sql: &CStr, params: &[Option<&str>]) -> Result<Vec<String>, Error> {
        let mut stmt = ptr::null_mut();
        // SAFETY: SQLite writes the statement handle to stmt and reads the NUL-terminated SQL.
        self.check(unsafe {
            sqlite3_prepare_v2(
                self.0.as_ptr(),
                sql.as_ptr(),
                -1,
                &mut stmt,
                ptr::null_mut(),
            )
        })?;
        let stmt =
            Statement(NonNull::new(stmt).ok_or_else(|| Error::input("empty SQLite statement"))?);
        for (index, value) in params.iter().enumerate() {
            if let Some(value) = value {
                let len = c_int::try_from(value.len())
                    .map_err(|_| Error::input("SQLite parameter is too long"))?;
                let index = c_int::try_from(index + 1)
                    .map_err(|_| Error::input("too many SQLite parameters"))?;
                // SAFETY: params outlive the statement, including finalization. Unbound slots are NULL.
                self.check(unsafe {
                    sqlite3_bind_text(stmt.0.as_ptr(), index, value.as_ptr().cast(), len, None)
                })?;
            }
        }
        let mut rows = Vec::new();
        loop {
            // SAFETY: this statement is live and only used by this call.
            match unsafe { sqlite3_step(stmt.0.as_ptr()) } {
                ROW => {
                    // SAFETY: column zero is read from a current row and copied before stepping.
                    let value = unsafe { sqlite3_column_text(stmt.0.as_ptr(), 0) };
                    if value.is_null() {
                        return Err(Error::input("missing SQLite result"));
                    }
                    // All queries here return generated JSON, or integer PRAGMA values as text.
                    let value = unsafe { CStr::from_ptr(value.cast()) };
                    rows.push(
                        value
                            .to_str()
                            .map_err(|_| Error::input("invalid UTF-8 in SQLite result"))?
                            .to_owned(),
                    );
                }
                DONE => return Ok(rows),
                code => self.check(code)?,
            }
        }
    }

    pub fn changes(&self) -> c_int {
        // SAFETY: all operations use the single live handle on the owning thread.
        unsafe { sqlite3_changes(self.0.as_ptr()) }
    }

    fn check(&self, code: c_int) -> Result<(), Error> {
        if code == OK {
            return Ok(());
        }
        // SAFETY: the live handle owns the error string, copied before another SQLite call.
        let message = unsafe { CStr::from_ptr(sqlite3_errmsg(self.0.as_ptr())) }
            .to_string_lossy()
            .into_owned();
        let code = unsafe { sqlite3_extended_errcode(self.0.as_ptr()) };
        Err(Error { code, message })
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // SAFETY: the connection owns the handle and no statements escape query.
        unsafe { sqlite3_close(self.0.as_ptr()) };
    }
}

struct Statement(NonNull<Stmt>);

impl Drop for Statement {
    fn drop(&mut self) {
        // SAFETY: the statement uniquely owns this handle.
        unsafe { sqlite3_finalize(self.0.as_ptr()) };
    }
}
