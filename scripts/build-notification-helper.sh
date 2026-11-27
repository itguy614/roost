#!/bin/bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
app=${1:-"$HOME/Applications/Roost Notifications.app"}
identity=${ROOST_NOTIFICATION_SIGN_IDENTITY:--}
bundle=${ROOST_NOTIFICATION_BUNDLE_ID:-dev.roost.notifications}
build=${ROOST_NOTIFICATION_BUILD:-1}
if [[ "$app" != /*.app || -e "$app" || -L "$app" ]]; then
  printf 'Choose an unused absolute .app path\n' >&2
  exit 2
fi
if [[ ! "$bundle" =~ ^[A-Za-z0-9][A-Za-z0-9.-]+$ || ! "$build" =~ ^[0-9]+$ ]]; then
  printf 'Invalid bundle identifier or build number\n' >&2
  exit 2
fi

package="$root/apps/notification-helper"
scratch="$root/target/notification-swift"
swift build --package-path "$package" --scratch-path "$scratch" -c release
bin=$(swift build --package-path "$package" --scratch-path "$scratch" -c release --show-bin-path)
mkdir -p "$(dirname "$app")"
stage=$(mktemp -d "$(dirname "$app")/RoostNotifyBuild.XXXXXX")
trap 'rm -rf -- "$stage"' EXIT
bundle_path="$stage/Roost Notifications.app"
mkdir -p "$bundle_path/Contents/MacOS"
cp "$bin/RoostNotificationHelper" "$bundle_path/Contents/MacOS/"
cat > "$bundle_path/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>CFBundleIdentifier</key><string>$bundle</string>
<key>CFBundleExecutable</key><string>RoostNotificationHelper</string>
<key>CFBundlePackageType</key><string>APPL</string>
<key>CFBundleName</key><string>Roost Notifications</string>
<key>CFBundleDisplayName</key><string>Roost Notifications</string>
<key>CFBundleShortVersionString</key><string>0.1.0</string>
<key>CFBundleVersion</key><string>$build</string>
<key>LSMinimumSystemVersion</key><string>13.0</string>
<key>LSUIElement</key><true/>
</dict></plist>
PLIST
/usr/bin/plutil -lint "$bundle_path/Contents/Info.plist"
/usr/bin/codesign --force --sign "$identity" "$bundle_path"
/usr/bin/codesign --verify --strict "$bundle_path"
# Claim the destination without overwriting or nesting inside an existing app.
if ! mkdir -- "$app"; then
  printf 'Could not create an unused destination bundle\n' >&2
  exit 2
fi
mv "$bundle_path/Contents" "$app/Contents"
/usr/bin/codesign --verify --strict "$app"
printf '%s\n' "$app"
