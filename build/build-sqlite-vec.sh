#!/usr/bin/env bash
#
# Refreshes the prebuilt sqlite-vec native binaries shipped by Shiny.DocumentDb.Sqlite.VectorSupport.
#
# Downloads the official sqlite-vec release artifacts (no compilation / NDK required) and lays
# them out under src/Shiny.DocumentDb.Sqlite.VectorSupport/native/:
#   - apple/sqlite_vec.xcframework         iOS device + simulator static libs (requires Xcode)
#   - android/<abi>/libsqlite_vec0.so      Android loadable libs, one per ABI
#   - runtimes/<rid>/native/vec0.*         desktop + Mac Catalyst loadable libs
#
# Usage:  build/build-sqlite-vec.sh [version]   (default version below)
#
set -euo pipefail

VER="${1:-0.1.9}"
BASE="https://github.com/asg017/sqlite-vec/releases/download/v${VER}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/src/Shiny.DocumentDb.Sqlite.VectorSupport/native"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

echo "sqlite-vec v$VER -> $OUT"

fetch() { # asset-suffix  (e.g. static-ios-aarch64)
  curl -fsSL "$BASE/sqlite-vec-${VER}-$1.tar.gz" -o "$TMP/$1.tar.gz"
  mkdir -p "$TMP/$1"
  tar xzf "$TMP/$1.tar.gz" -C "$TMP/$1"
}

# ── Apple: iOS device (arm64) + simulator (arm64 + x86_64) static xcframework ──
if ! command -v xcodebuild >/dev/null 2>&1; then
  echo "WARNING: xcodebuild not found — skipping Apple xcframework (Android + desktop still build)." >&2
else
  fetch static-ios-aarch64
  fetch static-iossimulator-aarch64
  fetch static-iossimulator-x86_64
  lipo -create \
    "$TMP/static-iossimulator-aarch64/libsqlite_vec0.a" \
    "$TMP/static-iossimulator-x86_64/libsqlite_vec0.a" \
    -output "$TMP/libsqlite_vec0-sim.a"
  rm -rf "$OUT/apple/sqlite_vec.xcframework"
  mkdir -p "$OUT/apple"
  xcodebuild -create-xcframework \
    -library "$TMP/static-ios-aarch64/libsqlite_vec0.a" \
    -library "$TMP/libsqlite_vec0-sim.a" \
    -output "$OUT/apple/sqlite_vec.xcframework" >/dev/null
  echo "  apple/sqlite_vec.xcframework"
fi

# ── Android: one loadable .so per ABI (asset-suffix:ndk-abi) ──
for pair in \
  "android-aarch64:arm64-v8a" \
  "android-armv7a:armeabi-v7a" \
  "android-i686:x86" \
  "android-x86_64:x86_64"; do
  suffix="${pair%%:*}"; abi="${pair##*:}"
  fetch "loadable-$suffix"
  mkdir -p "$OUT/android/$abi"
  cp "$TMP/loadable-$suffix/vec0.so" "$OUT/android/$abi/libsqlite_vec0.so"
  echo "  android/$abi/libsqlite_vec0.so"
done

# ── Desktop + Mac Catalyst: loadable libs under runtimes/<rid>/native ──
copy_rt() { # asset-suffix  rid  filename
  fetch "$1"
  mkdir -p "$OUT/runtimes/$2/native"
  cp "$TMP/$1/$3" "$OUT/runtimes/$2/native/$3"
  echo "  runtimes/$2/native/$3"
}
copy_rt loadable-macos-aarch64  osx-arm64    vec0.dylib
copy_rt loadable-macos-x86_64   osx-x64      vec0.dylib
copy_rt loadable-linux-aarch64  linux-arm64  vec0.so
copy_rt loadable-linux-x86_64   linux-x64    vec0.so
copy_rt loadable-windows-x86_64 win-x64      vec0.dll

# Mac Catalyst runs on macOS and can dlopen the macOS dylib.
for rid in maccatalyst-arm64 maccatalyst-x64; do
  mkdir -p "$OUT/runtimes/$rid/native"
  cp "$OUT/runtimes/osx-arm64/native/vec0.dylib" "$OUT/runtimes/$rid/native/vec0.dylib"
  echo "  runtimes/$rid/native/vec0.dylib"
done

echo "done."
