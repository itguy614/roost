mod workers;

use std::{ffi::OsString, io, path::PathBuf, process::ExitCode};

use roost_ipc::{AddParams, Client, RemoveParams};
use uuid::Uuid;

const USAGE: &str = "Usage:
  roost [--state-dir ABSOLUTE_PATH] daemon status
  roost [--state-dir ABSOLUTE_PATH] project add PATH
  roost [--state-dir ABSOLUTE_PATH] project list
  roost [--state-dir ABSOLUTE_PATH] project remove ID
  roost [--state-dir ABSOLUTE_PATH] project start ID PHP_PATH [--allow-horizon]
  roost [--state-dir ABSOLUTE_PATH] project stop ID
  roost [--state-dir ABSOLUTE_PATH] project restart ID PHP_PATH [--allow-horizon]
  roost [--state-dir ABSOLUTE_PATH] worker profile create ID NAME CONNECTION [QUEUE ...]
  roost [--state-dir ABSOLUTE_PATH] worker profile create ID NAME CONNECTION --options-json JSON
  roost [--state-dir ABSOLUTE_PATH] worker profile list ID
  roost [--state-dir ABSOLUTE_PATH] process list ID
  roost [--state-dir ABSOLUTE_PATH] log tail ID";

enum Command {
    Status,
    Add(PathBuf),
    List,
    Remove(Uuid),
    Worker(workers::Command),
}

fn main() -> ExitCode {
    if let Err(error) = run() {
        eprintln!("roost: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    let (dir, args) = match args.as_slice() {
        [flag, dir, rest @ ..] if flag == "--state-dir" => (Some(PathBuf::from(dir)), rest),
        args => (None, args),
    };
    let command = match args {
        [help] if help == "-h" || help == "--help" => {
            println!("{USAGE}");
            return Ok(());
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
            Command::Worker(workers::Command::parse(args)?.ok_or_else(|| io::Error::other(USAGE))?)
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
        let mut client = Client::connect(&dir).await?;
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
        };
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    })
}
