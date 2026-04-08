#!/usr/bin/env bash

set -euo pipefail

UPSTREAM_TAG="v1.5.7"
UPSTREAM_COMMIT="f8745da6ff1ad1e7bab384bd1f9d742439278e99"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATCH_FILE="$SCRIPT_DIR/patches/${UPSTREAM_TAG}-qs2.patch"

usage() {
    cat <<EOF
Usage: $(basename "$0") /path/to/local/zstd-checkout

This updater is pinned to:
  tag:    ${UPSTREAM_TAG}
  commit: ${UPSTREAM_COMMIT}

It reproduces qs2's bundled zstd files by:
  1. running upstream build/single_file_libs/create_single_file_library.sh
  2. copying lib/zstd.h
  3. applying ${PATCH_FILE##*/} on top of the generated zstd.c
EOF
}

if [[ $# -ne 1 ]]; then
    usage >&2
    exit 1
fi

UPSTREAM_ROOT="$(cd "$1" && pwd)"

if [[ ! -d "$UPSTREAM_ROOT/.git" ]]; then
    echo "Expected a git checkout at: $UPSTREAM_ROOT" >&2
    exit 1
fi

ACTUAL_COMMIT="$(git -C "$UPSTREAM_ROOT" rev-parse HEAD)"
if [[ "$ACTUAL_COMMIT" != "$UPSTREAM_COMMIT" ]]; then
    echo "Unexpected upstream commit: $ACTUAL_COMMIT" >&2
    echo "Expected commit: $UPSTREAM_COMMIT (${UPSTREAM_TAG})" >&2
    exit 1
fi

if [[ ! -f "$UPSTREAM_ROOT/build/single_file_libs/create_single_file_library.sh" ]]; then
    echo "Missing upstream single-file generator at build/single_file_libs/create_single_file_library.sh" >&2
    exit 1
fi

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/qs2-zstd.XXXXXX")"
cleanup() {
    rm -rf "$tmpdir"
}
trap cleanup EXIT

mkdir -p "$tmpdir/build"
cp -R "$UPSTREAM_ROOT/build/single_file_libs" "$tmpdir/build/"
ln -s "$UPSTREAM_ROOT/lib" "$tmpdir/lib"

(
    cd "$tmpdir/build/single_file_libs"
    ./create_single_file_library.sh
)

cp "$tmpdir/build/single_file_libs/zstd.c" "$tmpdir/zstd.c"
cp "$UPSTREAM_ROOT/lib/zstd.h" "$tmpdir/zstd.h"

if [[ -s "$PATCH_FILE" ]]; then
    patch -d "$tmpdir" -p1 < "$PATCH_FILE"
fi

cp "$tmpdir/zstd.c" "$SCRIPT_DIR/zstd.c"
cp "$tmpdir/zstd.h" "$SCRIPT_DIR/zstd.h"

echo "Updated bundled zstd from ${UPSTREAM_TAG} (${UPSTREAM_COMMIT})"
