// Built by the integration tests as an executable PHP stand-in.
use std::{
    fs,
    io::{self, Write},
    process::{Command, ExitCode},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

static STOP: AtomicBool = AtomicBool::new(false);

unsafe extern "C" {
    fn signal(number: i32, handler: extern "C" fn(i32)) -> usize;
}

extern "C" fn stop(_: i32) {
    STOP.store(true, Ordering::Relaxed);
}

extern "C" fn ignore(_: i32) {}

// The comparison needs a predictable RSS drop when the load ends. Darwin's
// allocator may keep freed regions resident, so the fixture owns a VM mapping.
struct RssLoad(*mut u8);

impl RssLoad {
    const BYTES: usize = 64 * 1024 * 1024;

    fn new() -> Self {
        const READ_WRITE: i32 = 0x03;
        const PRIVATE_ANONYMOUS: i32 = 0x1002;
        unsafe extern "C" {
            fn mmap(
                address: *mut u8,
                length: usize,
                protection: i32,
                flags: i32,
                fd: i32,
                offset: i64,
            ) -> *mut u8;
        }
        // SAFETY: mmap supplies the address and a private, writable region.
        let pointer = unsafe {
            mmap(
                std::ptr::null_mut(),
                Self::BYTES,
                READ_WRITE,
                PRIVATE_ANONYMOUS,
                -1,
                0,
            )
        };
        assert_ne!(pointer as usize, usize::MAX, "could not map RSS fixture");
        for offset in (0..Self::BYTES).step_by(4096) {
            // SAFETY: each offset lies within the mapping. Touch each page.
            unsafe { pointer.add(offset).write_volatile(1) };
        }
        Self(pointer)
    }
}

impl Drop for RssLoad {
    fn drop(&mut self) {
        unsafe extern "C" {
            fn munmap(address: *mut u8, length: usize) -> i32;
        }
        // SAFETY: this object uniquely owns the mapping of exactly this size.
        assert_eq!(unsafe { munmap(self.0, Self::BYTES) }, 0);
    }
}

fn main() -> ExitCode {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    if arguments == ["descendant"] {
        unsafe { signal(15, ignore) };
        loop {
            thread::sleep(Duration::from_millis(10));
        }
    }
    let scheduler = arguments == ["artisan", "schedule:work"];
    let behavior = if scheduler {
        fs::read_to_string("scheduler-mode").unwrap_or_else(|_| "graceful".into())
    } else {
        assert_eq!(
            arguments,
            [
                "artisan",
                "queue:work",
                "database",
                "--sleep=3",
                "--tries=1",
                "--timeout=60",
                "--queue=critical,default"
            ]
        );
        fs::read_to_string("artisan").unwrap()
    };
    match behavior.as_str() {
        "fail" => return ExitCode::from(7),
        "clean_exit" => return ExitCode::SUCCESS,
        "trace_fail" => {
            let mut stderr = io::stderr().lock();
            stderr.write_all(&[b'x'; 96 * 1024]).unwrap();
            stderr.write_all(b"\nRuntimeException: queue connection unavailable\nDB_PASSWORD=secret-fixture-output\n#0 src/Queue.php(20): connect()").unwrap();
            return ExitCode::from(7);
        }
        _ => {}
    }
    unsafe { signal(15, if behavior == "ignore" { ignore } else { stop }) };
    if behavior == "ignore" {
        let child = Command::new(std::env::current_exe().unwrap())
            .arg("descendant")
            .spawn()
            .unwrap();
        let temporary = format!("descendant-{}.tmp", std::process::id());
        fs::write(&temporary, child.id().to_string()).unwrap();
        fs::rename(temporary, "descendant.pid").unwrap();
    }
    if scheduler {
        println!("scheduler ready");
    } else {
        println!("worker ready");
    }
    eprintln!("Authorization: Bearer secret-fixture-output");
    let pid = std::process::id().to_string();
    let mut memory = None;
    let mut cpu_load = false;
    let mut next_check = std::time::Instant::now();
    while !STOP.load(Ordering::Relaxed) {
        if matches!(behavior.as_str(), "controlled-rss" | "controlled-cpu")
            && std::time::Instant::now() >= next_check
        {
            next_check = std::time::Instant::now() + Duration::from_millis(200);
            cpu_load = behavior == "controlled-cpu"
                && fs::read_to_string("cpu-load-pid").is_ok_and(|target| target == pid);
            let selected = behavior == "controlled-rss"
                && fs::read_to_string("rss-load-pid").is_ok_and(|target| target == pid);
            if selected && memory.is_none() {
                memory = Some(RssLoad::new());
            } else if !selected {
                memory = None;
            }
        }
        if behavior == "soak_crash" && fs::remove_file("exit-once").is_ok() {
            return ExitCode::from(7);
        }
        if behavior == "loaded" || cpu_load {
            let until = std::time::Instant::now() + Duration::from_millis(2);
            while std::time::Instant::now() < until {
                std::hint::spin_loop();
            }
        }
        thread::sleep(Duration::from_millis(10));
    }
    if scheduler {
        thread::sleep(Duration::from_millis(120));
        fs::write(format!("scheduler-stopped-{}", std::process::id()), "").unwrap();
    }
    ExitCode::SUCCESS
}
