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
    while !STOP.load(Ordering::Relaxed) {
        if behavior == "soak_crash" && fs::remove_file("exit-once").is_ok() {
            return ExitCode::from(7);
        }
        if behavior == "loaded" {
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
