use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::{self, Read},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    sync::LazyLock,
    time::{Duration, Instant},
};

use regex::Regex;
use roost_core::{
    WorkerProfile,
    discovery::{ConfiguredQueues, QueueSuggestion, QueueSuggestions, SuggestionSource},
};

use crate::safe_identifier;

const MAX_NAMES: usize = 128;
const MAX_FILE_BYTES: u64 = 128 * 1024;
const MAX_TOTAL_BYTES: u64 = 4 * 1024 * 1024;
const MAX_ENTRIES: usize = 4096;
const MAX_FILES: u32 = 384;
const MAX_DEPTH: u8 = 12;
const MAX_PROFILES: usize = 128;

static TOKEN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
    r#"(?s)/\*.*?(?:\*/|\z)|(?m://[^\n]*|#[^\[\n][^\n]*|#$)|'(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"|\$?[a-zA-Z_][a-zA-Z_0-9]*|::|[^\s]"#
).unwrap()
});

#[derive(Default)]
struct Progress {
    files: u32,
    entries: usize,
    bytes: u64,
    limited: bool,
    dynamic: bool,
    omitted: bool,
}

pub fn suggest_queues(
    root: &Path,
    profiles: &[WorkerProfile],
    explicit: &[String],
) -> QueueSuggestions {
    let mut progress = Progress::default();
    let mut names: BTreeMap<String, BTreeSet<SuggestionSource>> = BTreeMap::new();
    for name in explicit.iter().take(MAX_NAMES) {
        progress.omitted |= !insert(&mut names, name, SuggestionSource::Explicit);
    }
    progress.omitted |= explicit.len() > MAX_NAMES || profiles.len() > MAX_PROFILES;
    let mut queue_budget = 256;
    let configured_profiles = profiles
        .iter()
        .take(MAX_PROFILES)
        .filter_map(|profile| {
            if !safe_identifier(&profile.connection) {
                progress.omitted = true;
                return None;
            }
            let mut queues = Vec::new();
            for name in &profile.queues {
                if queue_budget == 0 {
                    progress.omitted = true;
                    break;
                }
                if safe_identifier(name) {
                    queue_budget -= 1;
                    queues.push(name.clone());
                    progress.omitted |= !insert(&mut names, name, SuggestionSource::WorkerProfile);
                } else {
                    progress.omitted = true;
                }
            }
            Some(ConfiguredQueues {
                profile_id: profile.id,
                connection: profile.connection.clone(),
                queues,
            })
        })
        .collect();

    scan_tree(root, &mut names, &mut progress);
    QueueSuggestions {
        suggestions: names
            .into_iter()
            .map(|(name, sources)| QueueSuggestion {
                name,
                sources: sources.into_iter().collect(),
            })
            .collect(),
        configured_profiles,
        complete: false,
        dynamic_references: progress.dynamic,
        scan_limited: progress.limited,
        files_scanned: progress.files,
        omitted_values: progress.omitted,
    }
}

fn scan_tree(
    root: &Path,
    names: &mut BTreeMap<String, BTreeSet<SuggestionSource>>,
    progress: &mut Progress,
) {
    let until = Instant::now() + Duration::from_millis(350);
    let mut directories = vec![(root.to_path_buf(), 0)];
    while let Some((dir, depth)) = directories.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            progress.limited = true;
            continue;
        };
        for entry in entries {
            if Instant::now() >= until
                || progress.entries >= MAX_ENTRIES
                || progress.files >= MAX_FILES
                || progress.bytes >= MAX_TOTAL_BYTES
            {
                progress.limited = true;
                return;
            }
            progress.entries += 1;
            let Ok(entry) = entry else {
                progress.limited = true;
                continue;
            };
            let Ok(kind) = entry.file_type() else {
                progress.limited = true;
                continue;
            };
            if kind.is_dir() {
                consider_dir(entry.path(), depth, &mut directories, progress);
            } else if kind.is_file()
                && entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "php")
            {
                scan_file(&entry.path(), names, progress);
            }
        }
    }
}

fn consider_dir(
    path: PathBuf,
    depth: u8,
    directories: &mut Vec<(PathBuf, u8)>,
    progress: &mut Progress,
) {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return;
    };
    if name.starts_with('.')
        || [
            "vendor",
            "node_modules",
            "storage",
            "build",
            "dist",
            "cache",
        ]
        .contains(&name)
    {
        return;
    }
    if depth >= MAX_DEPTH {
        progress.limited = true;
    } else {
        directories.push((path, depth + 1));
    }
}

fn scan_file(
    path: &Path,
    names: &mut BTreeMap<String, BTreeSet<SuggestionSource>>,
    progress: &mut Progress,
) {
    let allowance = MAX_FILE_BYTES.min(MAX_TOTAL_BYTES - progress.bytes);
    let mut contents = Vec::new();
    let read = read_source(path, allowance, &mut contents);
    progress.bytes += contents.len() as u64;
    if let (Ok(()), Ok(source)) = (read, std::str::from_utf8(&contents)) {
        progress.files += 1;
        scan_php(source, names, progress);
    } else {
        progress.limited = true;
    }
}

fn read_source(path: &Path, allowance: u64, contents: &mut Vec<u8>) -> io::Result<()> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW)
        .open(path)?;
    let stat = file.metadata()?;
    if !stat.is_file() || stat.len() > allowance {
        return Err(io::Error::other("source limit"));
    }
    file.take(allowance + 1).read_to_end(contents)?;
    if contents.len() as u64 > allowance {
        return Err(io::Error::other("source limit"));
    }
    Ok(())
}

fn scan_php(
    source: &str,
    names: &mut BTreeMap<String, BTreeSet<SuggestionSource>>,
    progress: &mut Progress,
) {
    // Skip heredoc/nowdoc bodies instead of interpreting arbitrary text as PHP tokens.
    if source.contains("<<<") {
        progress.limited = true;
        return;
    }
    let tokens: Vec<_> = TOKEN
        .find_iter(source)
        .map(|token| token.as_str())
        .filter(|token| {
            !token.starts_with("//") && !token.starts_with("/*") && !token.starts_with('#')
        })
        .collect();
    for (index, token) in tokens.iter().enumerate() {
        let rest = &tokens[index + 1..];
        let argument = if token.eq_ignore_ascii_case("onQueue") && rest.first() == Some(&"(") {
            Some((&rest[1..], ")"))
        } else if token == &"$queue" && rest.first() == Some(&"=") {
            Some((&rest[1..], ";"))
        } else if token.eq_ignore_ascii_case("Queue")
            && matches!(rest, ["::", method, "(", ..] if method.eq_ignore_ascii_case("pushOn"))
        {
            Some((&rest[3..], ","))
        } else {
            None
        };
        if let Some((args, terminator)) = argument {
            match args {
                [literal, end, ..] if *end == terminator && unquote(literal).is_some() => {
                    let value = unquote(literal).expect("checked literal");
                    progress.omitted |= !insert(names, value, SuggestionSource::StaticSource);
                }
                _ => progress.dynamic = true,
            }
        }
    }
}

fn unquote(value: &str) -> Option<&str> {
    value
        .strip_prefix('\'')
        .and_then(|name| name.strip_suffix('\''))
        .or_else(|| {
            value
                .strip_prefix('"')
                .and_then(|name| name.strip_suffix('"'))
                .filter(|name| !name.contains('$'))
        })
        .filter(|name| !name.contains('\\'))
}

fn insert(
    names: &mut BTreeMap<String, BTreeSet<SuggestionSource>>,
    name: &str,
    source: SuggestionSource,
) -> bool {
    if !safe_identifier(name) || (names.len() >= MAX_NAMES && !names.contains_key(name)) {
        return false;
    }
    names.entry(name.to_owned()).or_default().insert(source);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_literals_and_reports_dynamic_calls_without_inventing_names() {
        let mut names = BTreeMap::new();
        let mut progress = Progress::default();
        scan_php(
            r#"<?php
               // dispatch()->onQueue('ignored');
               /* Queue::pushOn('also-ignored', $job); */
               $x = "a string with onQueue('nope')";
               $job->onQueue('emails');
               public string $queue = "notifications";
               Queue::pushOn('critical', $job);
               $job->onQueue($tenant);
               $job->onQueue('prefix' . $suffix);
               $job->onQueue("$dynamic");
            "#,
            &mut names,
            &mut progress,
        );
        assert_eq!(
            names.keys().collect::<Vec<_>>(),
            ["critical", "emails", "notifications"]
        );
        assert!(progress.dynamic);
    }
}
