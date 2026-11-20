mod discovery;
mod groups;
mod scheduler;
mod watch;
mod workers;

use std::{ffi::OsString, io, path::PathBuf, process::ExitCode};

use roost_ipc::{AddParams, Client, ClientError, RemoveParams, endpoint::SOCKET_NAME};
use uuid::Uuid;

const USAGE: &str = "Usage:
  roost [--state-dir ABSOLUTE_PATH] daemon status
  roost [--state-dir ABSOLUTE_PATH] project add PATH
  roost [--state-dir ABSOLUTE_PATH] project list
  roost [--state-dir ABSOLUTE_PATH] project remove ID
  roost [--state-dir ABSOLUTE_PATH] project start ID [PHP_PATH | --provider herd|system] [--allow-horizon]
  roost [--state-dir ABSOLUTE_PATH] project stop ID
  roost [--state-dir ABSOLUTE_PATH] project autostart ID on|off
  roost [--state-dir ABSOLUTE_PATH] project restart ID [PHP_PATH | --provider herd|system] [--allow-horizon]
  roost [--state-dir ABSOLUTE_PATH] worker profile create ID NAME CONNECTION [QUEUE ...]
  roost [--state-dir ABSOLUTE_PATH] worker profile create ID NAME CONNECTION --options-json JSON
  roost [--state-dir ABSOLUTE_PATH] worker profile list ID
  roost [--state-dir ABSOLUTE_PATH] worker profile set-code-restart PROFILE_ID never|on-change --revision REV
  roost [--state-dir ABSOLUTE_PATH] watch status ID
  roost [--state-dir ABSOLUTE_PATH] watch enable ID --revision REV [--rules-json JSON]
  roost [--state-dir ABSOLUTE_PATH] watch disable ID --revision REV
  roost [--state-dir ABSOLUTE_PATH] process list ID
  roost [--state-dir ABSOLUTE_PATH] log tail ID
  roost [--state-dir ABSOLUTE_PATH] worker crashes ID
  roost [--state-dir ABSOLUTE_PATH] provider list
  roost [--state-dir ABSOLUTE_PATH] provider detect ID
  roost [--state-dir ABSOLUTE_PATH] provider resolve-php ID [--php PATH | --provider herd|system]
  roost [--state-dir ABSOLUTE_PATH] queue connections ID
  roost [--state-dir ABSOLUTE_PATH] queue discover ID [--php PATH | --provider herd|system]
  roost [--state-dir ABSOLUTE_PATH] queue suggestions ID [NAME ...]
  roost [--state-dir ABSOLUTE_PATH] scheduler configure ID [--revision REV] [--options-json JSON]
  roost [--state-dir ABSOLUTE_PATH] scheduler status ID
  roost [--state-dir ABSOLUTE_PATH] scheduler start ID [PHP_PATH | --provider herd|system]
  roost [--state-dir ABSOLUTE_PATH] scheduler restart ID [PHP_PATH | --provider herd|system]
  roost [--state-dir ABSOLUTE_PATH] scheduler stop ID
  roost [--state-dir ABSOLUTE_PATH] scheduler logs ID
  roost [--state-dir ABSOLUTE_PATH] scheduler crashes ID
  roost [--state-dir ABSOLUTE_PATH] group list
  roost [--state-dir ABSOLUTE_PATH] group get ID
  roost [--state-dir ABSOLUTE_PATH] group create NAME [--options-json JSON]
  roost [--state-dir ABSOLUTE_PATH] group update ID NAME --revision REV --options-json JSON
  roost [--state-dir ABSOLUTE_PATH] group delete ID --revision REV
  roost [--state-dir ABSOLUTE_PATH] group add-project ID PROJECT_ID --revision REV
  roost [--state-dir ABSOLUTE_PATH] group remove-project ID PROJECT_ID --revision REV
  roost [--state-dir ABSOLUTE_PATH] group start|restart ID [--php PATH | --provider herd|system] [--allow-horizon]
  roost [--state-dir ABSOLUTE_PATH] group stop ID";

enum Command {
    Status,
    Add(PathBuf),
    List,
    Remove(Uuid),
    Worker(workers::Command),
    Discovery(discovery::Command),
    Group(groups::Command),
    Scheduler(scheduler::Command),
    Watch(watch::Command),
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(error) => {
            eprintln!("roost: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<ExitCode, Box<dyn std::error::Error>> {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    let (dir, args) = match args.as_slice() {
        [flag, dir, rest @ ..] if flag == "--state-dir" => (Some(PathBuf::from(dir)), rest),
        args => (None, args),
    };
    let command = match args {
        [help] if help == "-h" || help == "--help" => {
            println!("{USAGE}");
            return Ok(ExitCode::SUCCESS);
        }
        [noun, verb] if noun == "daemon" && verb == "status" => Command::Status,
        [noun, verb] if noun == "project" && verb == "list" => Command::List,
        [noun, verb, path] if noun == "project" && verb == "add" => {
            let path = std::path::absolute(path)?;
            if path.to_str().is_none() {
                return Err(io::Error::other("project path must be valid UTF-8").into());
            }
            Command::Add(path)
        }
        [noun, verb, id] if noun == "project" && verb == "remove" => {
            let id = id
                .to_str()
                .and_then(|id| Uuid::parse_str(id).ok())
                .ok_or_else(|| io::Error::other("project ID must be a UUID"))?;
            Command::Remove(id)
        }
        _ => {
            if let Some(command) = discovery::Command::parse(args)? {
                Command::Discovery(command)
            } else if let Some(command) = groups::Command::parse(args)? {
                Command::Group(command)
            } else if let Some(command) = scheduler::Command::parse(args)? {
                Command::Scheduler(command)
            } else if let Some(command) = watch::Command::parse(args)? {
                Command::Watch(command)
            } else {
                Command::Worker(
                    workers::Command::parse(args)?.ok_or_else(|| io::Error::other(USAGE))?,
                )
            }
        }
    };
    let dir = match dir {
        Some(dir) => dir,
        None => roost_ipc::state_dir()?,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let mut client = match Client::connect(&dir).await {
            Ok(client) => client,
            Err(ClientError::Io(error))
                if matches!(error.kind(), io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused) =>
            {
                return Err(io::Error::new(error.kind(), format!(
                    "cannot reach the Roost daemon at {} ({error}). Start roost-daemon in another terminal. If using --state-dir, pass the same directory to both commands.",
                    dir.join(SOCKET_NAME).display(),
                )).into());
            }
            Err(error) => return Err(error.into()),
        };
        let mut failed = false;
        let result = match command {
            Command::Status => serde_json::to_value(client.status().await?)?,
            Command::Add(path) => serde_json::to_value(
                client
                    .add_project(&AddParams {
                        path,
                        idempotency_key: Some(Uuid::new_v4()),
                    })
                    .await?,
            )?,
            Command::List => serde_json::to_value(client.list_projects().await?)?,
            Command::Remove(id) => serde_json::to_value(
                client
                    .remove_project(&RemoveParams {
                        id: roost_core::ProjectId(id),
                        idempotency_key: Some(Uuid::new_v4()),
                    })
                    .await?,
            )?,
            Command::Worker(command) => command.execute(&mut client).await?,
            Command::Discovery(command) => command.execute(&mut client).await?,
            Command::Group(command) => {
                let (result, partial_failure) = command.execute(&mut client).await?;
                failed = partial_failure;
                result
            }
            Command::Scheduler(command) => command.execute(&mut client).await?,
            Command::Watch(command) => command.execute(&mut client).await?,
        };
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(if failed { ExitCode::FAILURE } else { ExitCode::SUCCESS })
    })
}
