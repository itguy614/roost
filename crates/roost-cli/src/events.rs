use std::{ffi::OsString, io};

use roost_ipc::events::EventHistoryParams;

use crate::discovery::{parse_id, utf8};

pub(super) fn parse(args: &[OsString]) -> io::Result<Option<EventHistoryParams>> {
    let [noun, verb, rest @ ..] = args else {
        return Ok(None);
    };
    if noun != "event" || verb != "history" {
        return Ok(None);
    }
    let mut params = EventHistoryParams {
        project_id: None,
        cursor: None,
        limit: None,
    };
    let mut options = rest.chunks_exact(2);
    for pair in &mut options {
        match utf8(&pair[0])? {
            "--project" if params.project_id.is_none() => {
                params.project_id = Some(parse_id(&pair[1])?)
            }
            "--cursor" if params.cursor.is_none() => params.cursor = Some(utf8(&pair[1])?.into()),
            "--limit" if params.limit.is_none() => {
                params.limit = Some(
                    utf8(&pair[1])?
                        .parse()
                        .map_err(|_| io::Error::other("invalid event history limit"))?,
                )
            }
            _ => {
                return Err(io::Error::other(
                    "unknown or duplicate event history option",
                ));
            }
        }
    }
    if !options.remainder().is_empty() {
        return Err(io::Error::other("event history options require a value"));
    }
    Ok(Some(params))
}
