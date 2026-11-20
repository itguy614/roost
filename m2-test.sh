#!/bin/bash

set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
if [ -n "${ROOST_TEST_APP:-}" ]; then
  app="$ROOST_TEST_APP"
  php_path="${ROOST_TEST_PHP:?Set ROOST_TEST_PHP to the PHP executable for the app}"
  connection="${ROOST_TEST_CONNECTION:?Set ROOST_TEST_CONNECTION to a local queue connection}"
  if [ ! -f "$app/artisan" ]; then
    printf 'No artisan file found in %s\n' "$app" >&2
    exit 1
  fi
  if [ ! -f "$php_path" ] || [ ! -x "$php_path" ]; then
    printf 'PHP executable not found at %s\n' "$php_path" >&2
    exit 1
  fi
  printf 'Smoke test mode: Laravel app\nApp: %s\nPHP: %s\nQueue connection: %s\n' "$app" "$php_path" "$connection"
else
  if [ -n "${ROOT_TEST_APP:-}${ROOT_TEST_PHP:-}${ROOT_TEST_CONNECTION:-}" ]; then
    printf 'Use ROOST_TEST_APP, ROOST_TEST_PHP, and ROOST_TEST_CONNECTION for a Laravel app. The prefix is ROOST_TEST_.\n' >&2
    exit 1
  fi
  if [ -n "${ROOST_TEST_PHP:-}${ROOST_TEST_CONNECTION:-}" ]; then
    printf 'Set ROOST_TEST_APP to run the smoke test against Laravel.\n' >&2
    exit 1
  fi
  printf 'Smoke test mode: Rust worker fixture\n'
fi

target_dir="${CARGO_TARGET_DIR:-target}"
host_target="$(rustc -vV | /usr/bin/sed -n 's/^host: //p')"
cargo build --locked --target-dir "$target_dir" --target "$host_target" -p roost-cli -p roost-daemon
bin_dir="$target_dir/$host_target/debug"
scratch="$(mktemp -d /tmp/roost-m2.XXXXXX)"
daemon_pid=""

cleanup() {
  local status=$?
  trap - EXIT
  if [ -n "$daemon_pid" ]; then
    if jobs -pr | /usr/bin/grep -Fxq "$daemon_pid"; then
      kill -TERM "$daemon_pid" || :
    fi
    wait "$daemon_pid" || :
  fi
  if [ "$status" -eq 0 ]; then
    rm -rf -- "$scratch"
  else
    printf 'Smoke test failed. State and daemon log are in %s\n' "$scratch" >&2
    if [ -f "$scratch/daemon.log" ]; then
      cat "$scratch/daemon.log" >&2
    fi
  fi
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cli() {
  "$bin_dir/roost" --state-dir "$scratch/state" "$@"
}

json_field() {
  /usr/bin/plutil -extract "$1" raw -o - -
}

start_daemon() {
  "$bin_dir/roost-daemon" --state-dir "$scratch/state" >>"$scratch/daemon.log" 2>&1 &
  daemon_pid=$!
  local attempt status
  for ((attempt=0; attempt<40; attempt++)); do
    if status="$(cli daemon status 2>/dev/null)"; then
      test "$(printf '%s' "$status" | json_field pid)" = "$daemon_pid"
      test "$(printf '%s' "$status" | json_field protocol_version)" = "1"
      return 0
    fi
    if ! jobs -pr | /usr/bin/grep -Fxq "$daemon_pid"; then
      return 1
    fi
    sleep 0.25
  done
  return 1
}

stop_daemon() {
  jobs -pr | /usr/bin/grep -Fxq "$daemon_pid"
  kill -TERM "$daemon_pid"
  wait "$daemon_pid"
  daemon_pid=""
  test ! -e "$scratch/state/roost.sock"
}

wait_worker() {
  local expected="$1" previous_pid="${2:-}" attempt status state pid
  for ((attempt=0; attempt<60; attempt++)); do
    status="$(cli process list "$project_id")"
    state="$(printf '%s' "$status" | json_field 0.state 2>/dev/null || :)"
    if [ "$state" = "$expected" ]; then
      if [ "$expected" = "running" ]; then
        pid="$(printf '%s' "$status" | json_field 0.identity.pid)"
        if [ "$pid" != "$previous_pid" ]; then
          printf '%s\n' "$status"
          return 0
        fi
      else
        printf '%s\n' "$status"
        return 0
      fi
    fi
    if [ "$state" = "failed" ]; then
      printf 'Worker failed while waiting for %s:\n%s\n' "$expected" "$status" >&2
      return 1
    fi
    sleep 0.25
  done
  printf 'Timed out waiting for %s:\n%s\n' "$expected" "$status" >&2
  return 1
}

if [ -n "${ROOST_TEST_APP:-}" ]; then
  queue="roost-smoke-$(/usr/bin/uuidgen)"
  options="{\"queues\":[\"$queue\"],\"crash_restart\":\"always\"}"
else
  app="$scratch/app"
  php_path="$scratch/php"
  connection="database"
  options='{"queues":["critical","default"],"crash_restart":"always"}'
  mkdir "$app"
  printf '%s' graceful >"$app/artisan"
  rustc --edition=2024 crates/roost-daemon/tests/fixtures/queue_worker.rs -o "$php_path"
fi

start_daemon
registration="$(cli project add "$app")"
project_id="$(printf '%s' "$registration" | json_field result.id)"
cli worker profile create "$project_id" Smoke "$connection" --options-json "$options"
cli worker profile list "$project_id"

cli project start "$project_id" "$php_path"
running="$(wait_worker running)"
first_pid="$(printf '%s' "$running" | json_field 0.identity.pid)"
printf 'First worker PID %s\n' "$first_pid"

if [ -z "${ROOST_TEST_APP:-}" ]; then
  bytes=""
  for ((attempt=0; attempt<40; attempt++)); do
    summaries="$(cli log tail "$project_id")"
    bytes="$(printf '%s' "$summaries" | json_field 0.bytes 2>/dev/null || :)"
    if [ -n "$bytes" ]; then
      break
    fi
    sleep 0.25
  done
  test "$bytes" -gt 0
fi
cli log tail "$project_id"

cli project restart "$project_id" "$php_path"
replacement="$(wait_worker running "$first_pid")"
printf 'Replacement worker PID %s\n' "$(printf '%s' "$replacement" | json_field 0.identity.pid)"

cli project stop "$project_id"
wait_worker stopped
sleep 2
stopped="$(cli process list "$project_id")"
test "$(printf '%s' "$stopped" | json_field 0.state)" = "stopped"
test "$(printf '%s' "$stopped" | json_field 0.desired_state)" = "stopped"
test "$(printf '%s' "$stopped" | json_field 0.restarts)" = "0"

stop_daemon
start_daemon
projects="$(cli project list)"
test "$(printf '%s' "$projects" | json_field 0.id)" = "$project_id"
test "$(printf '%s' "$projects" | json_field 0.desired_state)" = "stopped"
test "$(cli process list "$project_id")" = '[]'
cli worker profile list "$project_id"
cli project remove "$project_id"
test "$(cli project list)" = '[]'
stop_daemon

printf 'Milestone 2 CLI smoke test passed.\n'
