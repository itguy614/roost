#!/bin/bash
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
developer=$(xcode-select -p)
args=()
# Some standalone Command Line Tools installs expose Swift Testing outside
# the default compiler and dynamic loader search paths.
if [[ "$developer" == */CommandLineTools && -d "$developer/Library/Developer/Frameworks/Testing.framework" ]]; then
  support="$developer/Library/Developer"
  args=(-Xswiftc -F -Xswiftc "$support/Frameworks"
        -Xlinker -rpath -Xlinker "$support/Frameworks"
        -Xlinker -rpath -Xlinker "$support/usr/lib")
fi
swift test --package-path "$root/apps/notification-helper" --scratch-path "$root/target/notification-swift" \
  --disable-xctest ${args[@]+"${args[@]}"}
