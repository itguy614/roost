// Built by the integration tests as an executable PHP stand-in.
use std::{
    fs,
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
    let behavior = fs::read_to_string("artisan").unwrap();
    if behavior == "fail" {
        return ExitCode::from(7);
    }
    unsafe { signal(15, if behavior == "ignore" { ignore } else { stop }) };
    if behavior == "ignore" {
        let child = Command::new(std::env::current_exe().unwrap())
            .arg("descendant")
            .spawn()
            .unwrap();
        fs::write("descendant.pid", child.id().to_string()).unwrap();
    }
    println!("secret-fixture-output");
    eprintln!("secret-fixture-output");
    while !STOP.load(Ordering::Relaxed) {
        thread::sleep(Duration::from_millis(10));
    }
    ExitCode::SUCCESS
}
