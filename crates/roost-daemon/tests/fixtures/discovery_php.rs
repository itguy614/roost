// Executable probe stand-in. The integration suite needs no installed PHP.
use std::{
    env, fs,
    io::{self, Write},
    process::{Command, ExitCode},
    thread,
    time::Duration,
};

fn main() -> ExitCode {
    let arguments: Vec<_> = env::args().skip(1).collect();
    assert_eq!(env::vars().count(), 3);
    assert!(env::var("HOME").is_ok() && env::var("TMPDIR").is_ok());
    assert_eq!(env::var("PATH").unwrap(), "/usr/bin:/bin:/usr/sbin:/sbin");
    assert_eq!(
        arguments[0..4],
        ["-d", "display_errors=stderr", "-d", "log_errors=0"]
    );
    let program = arguments.last().unwrap();
    assert!(program.contains("queue.connections"));
    assert!(std::path::Path::new("artisan").is_file());
    match fs::read_to_string("artisan").unwrap().as_str() {
        "oversize" => {
            if io::stderr().write_all(&vec![b'x'; 80 * 1024]).is_err() {
                return ExitCode::FAILURE;
            }
        }
        "hang" => {
            let descendant = Command::new("/bin/sleep").arg("20").spawn().unwrap();
            let path = format!("descendant-{}.pid", std::process::id());
            fs::write(path, descendant.id().to_string()).unwrap();
            thread::sleep(Duration::from_secs(20));
        }
        "fail" => {
            eprintln!("database://user:private-fixture-value@localhost");
            return ExitCode::FAILURE;
        }
        "invalid" => {
            println!(
                r#"{{"default":null,"connections":[],"omitted_connections":0,"password":"private-fixture-value"}}"#
            );
        }
        _ => {
            eprintln!("database://user:private-fixture-value@localhost");
            println!(
                r#"{{"default":"database","connections":[{{"name":"database","driver":"database"}},{{"name":"email_redis","driver":"redis"}}],"omitted_connections":0}}"#
            );
        }
    }
    ExitCode::SUCCESS
}
