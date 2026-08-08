#!/usr/bin/env bash

set -euo pipefail

UPSTREAM_TAG="v0.8.3"
UPSTREAM_COMMIT="e626a72bc2321cd320e953a0ccf1584cad60f363"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATCH_FILE="$SCRIPT_DIR/patches/${UPSTREAM_TAG}-qdata-cpp.patch"

usage() {
    cat <<EOF
Usage: $(basename "$0") /path/to/local/xxHash-checkout

This updater is pinned to:
  tag:    ${UPSTREAM_TAG}
  commit: ${UPSTREAM_COMMIT}

It reproduces qdata-cpp's vendored xxHash files by:
  1. copying xxhash.c and xxhash.h from the pinned upstream checkout
  2. applying ${PATCH_FILE##*/} if it is non-empty
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

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/qs2-xxhash.XXXXXX")"
cleanup() {
    rm -rf "$tmpdir"
}
trap cleanup EXIT

cp "$UPSTREAM_ROOT/xxhash.c" "$tmpdir/xxhash.c"
cp "$UPSTREAM_ROOT/xxhash.h" "$tmpdir/xxhash.h"

if [[ -s "$PATCH_FILE" ]]; then
    patch -d "$tmpdir" -p1 < "$PATCH_FILE"
fi

cp "$tmpdir/xxhash.c" "$SCRIPT_DIR/xxhash.c"
cp "$tmpdir/xxhash.h" "$SCRIPT_DIR/xxhash.h"

echo "Updated vendored xxHash from ${UPSTREAM_TAG} (${UPSTREAM_COMMIT})"
