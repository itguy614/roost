#!/bin/bash
set -euo pipefail

# Run from the repository root after cargo build --workspace --locked.
mode="${1:-lifecycle}"
case "$mode" in
  lifecycle|allow|deny) ;;
  *) printf 'Usage: bash %s lifecycle|allow|deny\n' "$0" >&2; exit 2 ;;
esac

scratch="$(mktemp -d /tmp/roost-notify.XXXXXX)"
state="$scratch/state"
cli="$PWD/target/debug/roost"
daemon="$PWD/target/debug/roost-daemon"
app_root="$scratch"
if [[ -n "${ROOST_SPIKE_APP_PARENT:-}" ]]; then
  app_root="$(mktemp -d "$ROOST_SPIKE_APP_PARENT/RoostSpike.XXXXXX")"
fi
app="$app_root/Roost Notification Spike.app"
bundle="dev.roost.notification-spike.$(uuidgen | tr '[:upper:]' '[:lower:]')"
daemon_pid=""
event=-
if [[ "$mode" != lifecycle ]]; then event="roost-spike-$mode"; fi

stop_daemon() {
  if [[ -n "$daemon_pid" ]] && jobs -pr | /usr/bin/grep -Fxq "$daemon_pid"; then
    kill -TERM "$daemon_pid"
    wait "$daemon_pid"
  fi
  daemon_pid=""
}

cleanup() {
  if [[ -d "$state" ]]; then touch "$state/notification-spike.stop"; fi
  stop_daemon
  printf 'Disposable evidence directory: %s\n' "$scratch"
  if [[ "$app_root" != "$scratch" ]]; then printf 'Disposable bundle directory: %s\n' "$app_root"; fi
}
trap cleanup EXIT

start_daemon() {
  "$daemon" --state-dir "$state" >> "$scratch/daemon.log" 2>&1 &
  daemon_pid=$!
  for ((i=0; i<100; i++)); do
    if "$cli" --state-dir "$state" daemon status >/dev/null 2>&1; then return; fi
    sleep 0.1
  done
  cat "$scratch/daemon.log" >&2
  return 1
}

wait_log() {
  local log="$1" message="$2" attempts="${3:-100}"
  for ((i=0; i<attempts; i++)); do
    if [[ -f "$log" ]] && /usr/bin/grep -Fq "$message" "$log"; then return; fi
    if [[ "$message" == "authorization request granted="* && -f "$log" ]] &&
        /usr/bin/grep -Fq "authorization request failed:" "$log"; then
      cat "$log" >&2
      return 1
    fi
    sleep 0.1
  done
  printf 'Timed out waiting for %s in %s\n' "$message" "$log" >&2
  if [[ -f "$log" ]]; then cat "$log" >&2; fi
  return 1
}

build_bundle() {
  local version="$1"
  local agent=true
  if [[ "${ROOST_SPIKE_REGULAR:-0}" == 1 ]]; then agent=false; fi
  mkdir -p "$app/Contents/MacOS"
  cp "$scratch/NotificationProbe" "$app/Contents/MacOS/NotificationProbe"
  cat > "$app/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>CFBundleExecutable</key><string>NotificationProbe</string>
<key>CFBundleIdentifier</key><string>$bundle</string>
<key>CFBundleName</key><string>Roost Notification Spike</string>
<key>CFBundleDisplayName</key><string>Roost Notification Spike ($mode)</string>
<key>CFBundlePackageType</key><string>APPL</string>
<key>CFBundleShortVersionString</key><string>$version.0</string>
<key>CFBundleVersion</key><string>$version</string>
<key>LSUIElement</key><$agent/>
</dict></plist>
PLIST
  /usr/bin/codesign --force --sign "${ROOST_SPIKE_SIGN_IDENTITY:--}" "$app"
  /usr/bin/codesign --verify --strict "$app"
  /usr/bin/codesign -d -r- "$app" > "$scratch/signature-$version.log" 2>&1
}

launch_helper() {
  local log="$1"
  touch "$log"
  /bin/launchctl asuser "$(id -u)" /usr/bin/open -n -a "$app" \
    --stdout "$log" --stderr "$log" --args "$state" "$cli" "$event"
}

swiftc -parse-as-library -module-cache-path "${ROOST_SPIKE_SWIFT_CACHE:-$scratch/swift-cache}" \
  docs/spikes/m4-notification-probe.swift -o "$scratch/NotificationProbe"
build_bundle 1
start_daemon
if [[ "$mode" == lifecycle ]]; then
  (umask 077; printf '%s' '-:accepted' > "$state/notification-spike.journal")
fi
first="$scratch/v1.log"
launch_helper "$first"
wait_log "$first" "windows=0"
launch_helper "$scratch/duplicate.log"
wait_log "$scratch/duplicate.log" "duplicate helper rejected"

if [[ "$mode" == allow ]]; then
  wait_log "$first" "authorization request granted=true" 900
  wait_log "$first" "OS accepted event=$event"
  wait_log "$first" "Notification Center contains event=$event count=1" 200
elif [[ "$mode" == deny ]]; then
  wait_log "$first" "authorization request granted=false" 900
  wait_log "$first" "skipped submission and committed $event:denied"
  if /usr/bin/grep -Fq "OS accepted" "$first"; then exit 1; fi
fi
wait_log "$first" "daemon connected pid=$daemon_pid"
stop_daemon
wait_log "$first" "daemon disconnected"
start_daemon
wait_log "$first" "daemon connected pid=$daemon_pid"
touch "$state/notification-spike.stop"
wait_log "$first" "helper stopped"
sleep 1
mv "$app" "$scratch/Previous.app"
rm "$state/notification-spike.stop"
build_bundle 2
second="$scratch/v2.log"
launch_helper "$second"
wait_log "$second" "build=2 windows=0"
wait_log "$second" "daemon connected pid=$daemon_pid"
if [[ "$mode" == allow ]]; then
  wait_log "$second" "authorization before=2"
  wait_log "$second" "journal resumed $event:accepted"
  if /usr/bin/grep -Fq "OS accepted" "$second"; then exit 1; fi
elif [[ "$mode" == deny ]]; then
  wait_log "$second" "authorization before=1"
  wait_log "$second" "journal resumed $event:denied"
else
  wait_log "$second" "journal resumed -:accepted"
fi
touch "$state/notification-spike.stop"
wait_log "$second" "helper stopped"
cat "$first" "$scratch/duplicate.log" "$second"
