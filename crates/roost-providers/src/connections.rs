use std::time::Duration;

use roost_core::{
    Project,
    discovery::{ConnectionSnapshot, QueueConnection, ResolvedPhp},
    unix_timestamp,
};
use serde::Deserialize;

use crate::{Error, Resolver, safe_identifier};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeOutput {
    default: Option<String>,
    connections: Vec<QueueConnection>,
    omitted_connections: u32,
}

pub fn discover_connections(
    resolver: &Resolver,
    project: &Project,
    php: &ResolvedPhp,
) -> Result<ConnectionSnapshot, Error> {
    project.validate_location().map_err(|_| Error::Project)?;
    if !project.path.join("vendor/autoload.php").is_file()
        || !project.path.join("bootstrap/app.php").is_file()
    {
        return Err(Error::BootstrapFiles);
    }
    let mut command = resolver.command(&php.path, &project.path);
    command.args([
        "-d",
        "memory_limit=128M",
        "-d",
        "max_execution_time=2",
        "-r",
        include_str!("connections.php")
            .strip_prefix("<?php\n")
            .expect("embedded probe starts with a PHP opening tag"),
    ]);
    let execution = roost_process::run_probe(
        command,
        &resolver.environment(),
        Duration::from_millis(2400),
        64 * 1024,
    )
    .map_err(Error::DiscoveryProbe)?;
    parse_snapshot(&execution, php)
}

fn parse_snapshot(output: &[u8], php: &ResolvedPhp) -> Result<ConnectionSnapshot, Error> {
    let ProbeOutput {
        default,
        mut connections,
        omitted_connections,
    } = serde_json::from_slice(output).map_err(|_| Error::InvalidOutput)?;
    if connections.len() > 128
        || connections.iter().any(|entry| {
            !safe_identifier(&entry.name)
                || entry
                    .driver
                    .as_ref()
                    .is_some_and(|name| !safe_identifier(name))
        })
    {
        return Err(Error::InvalidOutput);
    }
    connections.sort_unstable_by(|a, b| a.name.cmp(&b.name));
    if connections
        .windows(2)
        .any(|pair| pair[0].name == pair[1].name)
    {
        return Err(Error::InvalidOutput);
    }
    Ok(ConnectionSnapshot {
        default: default.filter(|name| {
            connections
                .iter()
                .any(|connection| &connection.name == name)
        }),
        connections,
        omitted_connections,
        observed_at: unix_timestamp(),
        php_source: php.source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use roost_core::discovery::PhpSource;

    #[test]
    fn accepts_only_symbolic_connection_names_and_documented_fields() {
        let php = ResolvedPhp {
            path: "/php".into(),
            source: PhpSource::Override,
        };
        let data = br#"{"default":"db","connections":[{"name":"redis","driver":"redis"},{"name":"db","driver":"database"}],"omitted_connections":0}"#;
        let parsed = parse_snapshot(data, &php).unwrap();
        assert_eq!(parsed.default.as_deref(), Some("db"));
        assert_eq!(parsed.connections[0].name, "db");
        for data in [
            br#"{"default":"db","connections":[{"name":"db","driver":"database","password":"private"}],"omitted_connections":0}"#.as_slice(),
            br#"{"default":null,"connections":[{"name":"redis://user:password@host","driver":"redis"}],"omitted_connections":0}"#,
            br#"{"default":null,"connections":[{"name":"ghp_secretvalue","driver":"redis"}],"omitted_connections":0}"#,
        ] {
            assert!(matches!(parse_snapshot(data, &php), Err(Error::InvalidOutput)));
        }
    }
}
