#!/usr/bin/env bash
#
# Refreshes the sqlite-vec native binaries shipped by Shiny.DocumentDb.Sqlite.VectorSupport,
# laying them out under src/Shiny.DocumentDb.Sqlite.VectorSupport/native/:
#   - apple/sqlite_vec.xcframework         iOS device + simulator static libs (requires Xcode)
#   - android/<abi>/libsqlite_vec0.so      Android loadable libs, one per ABI (requires the NDK)
#   - runtimes/<rid>/native/vec0.*         desktop + Mac Catalyst loadable libs
#
# Apple and desktop come straight from the official sqlite-vec release assets. Android is compiled
# here from the sqlite-vec amalgamation instead, because the upstream android-* release assets are
# linked with 4 KB page alignment and Android 15+ requires 16 KB-aligned segments for 64-bit ABIs
# (Play Store requirement as of Nov 2025 — a 4 KB lib fails to load on a 16 KB-page device). The
# compile mirrors upstream's own release workflow (same flags, same vendored SQLite headers) and
# only adds the max-page-size linker flag; the resulting .so exports the identical symbol set.
#
# Usage:  build/build-sqlite-vec.sh [version] [--skip-missing]
#
#   --skip-missing   warn instead of failing when Xcode or the NDK is unavailable. Used by the
#                    MSBuild auto-fetch so a plain `dotnet build` still works on a machine without
#                    mobile toolchains. CI omits it so an incomplete package can never be packed.
#
set -euo pipefail

VER="0.1.9"
SKIP_MISSING=0
for arg in "$@"; do
  case "$arg" in
    --skip-missing) SKIP_MISSING=1 ;;
    *) VER="$arg" ;;
  esac
done

# The SQLite amalgamation upstream vendors for its own builds — only sqlite3.h/sqlite3ext.h are
# used (loadable extensions bind the engine through sqlite3_api_routines at load time, so these
# headers are independent of the e_sqlite3/e_sqlcipher build the app actually ships).
SQLITE_AMALGAMATION="sqlite-amalgamation-3450300"
SQLITE_AMALGAMATION_URL="https://www.sqlite.org/2024/$SQLITE_AMALGAMATION.zip"

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

missing() { # human-readable reason
  if [ "$SKIP_MISSING" = "1" ]; then
    echo "WARNING: $1 — skipping (omit --skip-missing to make this fatal)." >&2
    return 0
  fi
  echo "ERROR: $1" >&2
  exit 1
}

# ── Apple: iOS device (arm64) + simulator (arm64 + x86_64) static xcframework ──
if ! command -v xcodebuild >/dev/null 2>&1; then
  missing "xcodebuild not found; cannot build the Apple xcframework"
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

# ── Android: compile one 16 KB-aligned loadable .so per ABI from the amalgamation ──

# Newest NDK we can find: the explicit env vars first, then the side-by-side installs under the SDK.
find_ndk() {
  for candidate in "${ANDROID_NDK_HOME:-}" "${ANDROID_NDK_ROOT:-}"; do
    if [ -n "$candidate" ] && [ -d "$candidate/toolchains/llvm/prebuilt" ]; then
      echo "$candidate"
      return 0
    fi
  done
  for sdk in "${ANDROID_HOME:-}" "${ANDROID_SDK_ROOT:-}" "$HOME/Library/Android/sdk" "$HOME/Android/Sdk"; do
    [ -n "$sdk" ] && [ -d "$sdk/ndk" ] || continue
    local newest
    newest="$(find "$sdk/ndk" -maxdepth 1 -mindepth 1 -type d | sort -V | tail -1)"
    if [ -n "$newest" ] && [ -d "$newest/toolchains/llvm/prebuilt" ]; then
      echo "$newest"
      return 0
    fi
  done
  return 1
}

if ! NDK="$(find_ndk)"; then
  missing "no Android NDK found (set ANDROID_NDK_HOME, or install one via: sdkmanager --install 'ndk;28.2.13676358')"
else
  # The NDK ships a single darwin-x86_64 host toolchain, so an Apple Silicon host runs it under Rosetta.
  NDK_BIN="$(find "$NDK/toolchains/llvm/prebuilt" -maxdepth 1 -mindepth 1 -type d | head -1)/bin"

  curl -fsSL "$SQLITE_AMALGAMATION_URL" -o "$TMP/sqlite-amalgamation.zip"
  unzip -oq "$TMP/sqlite-amalgamation.zip" -d "$TMP"
  VENDOR="$TMP/$SQLITE_AMALGAMATION"

  fetch amalgamation
  VEC_SRC="$(find "$TMP/amalgamation" -name sqlite-vec.c | head -1)"

  # Flags mirror sqlite-vec's own loadable target and Android release job (-fPIC -shared -O3, API 21),
  # plus the 16 KB max-page-size the upstream prebuilts are missing.
  for pair in \
    "aarch64-linux-android21-clang:arm64-v8a" \
    "armv7a-linux-androideabi21-clang:armeabi-v7a" \
    "i686-linux-android21-clang:x86" \
    "x86_64-linux-android21-clang:x86_64"; do
    cc="${pair%%:*}"; abi="${pair##*:}"
    mkdir -p "$OUT/android/$abi"
    "$NDK_BIN/$cc" \
      -fPIC -shared -O3 -lm \
      -I"$VENDOR" \
      -Wl,-z,max-page-size=16384 -Wl,-z,common-page-size=16384 \
      "$VEC_SRC" -o "$OUT/android/$abi/libsqlite_vec0.so"
    echo "  android/$abi/libsqlite_vec0.so"
  done

  # Gate: every 64-bit ABI must report 16 KB (0x4000) alignment on all PT_LOAD segments. This is the
  # check that would have caught the upstream prebuilts, so it runs on every refresh.
  for abi in arm64-v8a x86_64; do
    so="$OUT/android/$abi/libsqlite_vec0.so"
    bad="$("$NDK_BIN/llvm-readelf" -l "$so" | awk '$1 == "LOAD" && $NF != "0x4000"' | wc -l | tr -d ' ')"
    if [ "$bad" != "0" ]; then
      echo "ERROR: $so has PT_LOAD segments that are not 16 KB aligned (Android 15+ requirement)." >&2
      exit 1
    fi
  done
  echo "  android: 16 KB page alignment verified (arm64-v8a, x86_64)"
fi

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
