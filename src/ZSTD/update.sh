#!/usr/bin/env bash

set -euo pipefail

UPSTREAM_TAG="v1.6.0"
UPSTREAM_COMMIT="916e8e70b33317d469a41ee837e47317807815c8"
# GitHub tag archive, as pinned by Homebrew's zstd formula.
UPSTREAM_ARCHIVE_SHA256="289bb207935e5b4f241239f9832e665fa6b9636c3f87f479e125371e13842b8e"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PATCH_FILE="$SCRIPT_DIR/patches/${UPSTREAM_TAG}-qs2.patch"

usage() {
    cat <<EOF
Usage: $(basename "$0") /path/to/zstd-checkout-or-tag-archive

This updater is pinned to:
  tag:    ${UPSTREAM_TAG}
  commit: ${UPSTREAM_COMMIT}
  archive: https://github.com/meta/zstd/archive/refs/tags/${UPSTREAM_TAG}.tar.gz

It reproduces qs2's bundled zstd files by:
  1. running upstream build/single_file_libs/combine.py (Python 3.8 or newer),
     with the arguments of upstream's create_single_file_library.sh
  2. copying lib/zstd.h and lib/zstd_errors.h
  3. applying ${PATCH_FILE##*/} with no fuzz
  4. checking the SHA-256 of the three files before replacing anything
EOF
}

if [[ $# -ne 1 ]]; then
    usage >&2
    exit 1
fi

# Upstream's create_single_file_library.sh falls back to combine.sh on Python
# older than 3.8, which inlines legacy/zstd_legacy.h despite -x and gives a
# different zstd.c. Only combine.py reproduces the bundled file.
if ! python3 -c 'import sys; assert sys.version_info >= (3, 8)' 2>/dev/null; then
    echo "Python 3.8 or newer is required (upstream combine.py)" >&2
    exit 1
fi

# SHA-256 of the bundled files this script must reproduce.
EXPECTED_SHA256="eabbfa9f3a0c5316eb6f189b06330ee31d6a9f47376ef5c163090646bac5ff29  zstd.c
210532c53dfe4f922c3a82d20761dc5d91aeffdb1c70a1394c91fe785cd8dfdc  zstd.h
66a8c3f71d12ea6e797e4f622f31f3f8f81c41b36f48cad4f5de7d8bfb6aac0a  zstd_errors.h"

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$@"
    else
        shasum -a 256 "$@"
    fi
}

tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/qs2-zstd.XXXXXX")"
cleanup() {
    rm -rf "$tmpdir"
}
trap cleanup EXIT

if [[ -f "$1" ]]; then
    archive_sha256="$(sha256 < "$1" | cut -d ' ' -f 1)"
    if [[ "$archive_sha256" != "$UPSTREAM_ARCHIVE_SHA256" ]]; then
        echo "Unexpected SHA-256 for the zstd tag archive: $archive_sha256" >&2
        exit 1
    fi
    UPSTREAM_ROOT="$tmpdir/upstream"
    mkdir "$UPSTREAM_ROOT"
    tar -xzf "$1" --strip-components=1 -C "$UPSTREAM_ROOT"
elif [[ -e "$1/.git" ]]; then
    UPSTREAM_ROOT="$(cd "$1" && pwd)"
    actual_commit="$(git -C "$UPSTREAM_ROOT" rev-parse HEAD)"
    if [[ "$actual_commit" != "$UPSTREAM_COMMIT" ]]; then
        echo "Unexpected upstream commit: $actual_commit" >&2
        echo "Expected commit: $UPSTREAM_COMMIT (${UPSTREAM_TAG})" >&2
        exit 1
    fi
else
    echo "Expected a git checkout or tag archive at: $1" >&2
    exit 1
fi

if [[ ! -f "$UPSTREAM_ROOT/build/single_file_libs/combine.py" ]]; then
    echo "Missing upstream single-file generator at build/single_file_libs/combine.py" >&2
    exit 1
fi

mkdir -p "$tmpdir/build"
cp -R "$UPSTREAM_ROOT/build/single_file_libs" "$tmpdir/build/"
ln -s "$UPSTREAM_ROOT/lib" "$tmpdir/lib"

(
    cd "$tmpdir/build/single_file_libs"
    python3 ./combine.py -r ../../lib -x legacy/zstd_legacy.h -o zstd.c zstd-in.c
)

cp "$tmpdir/build/single_file_libs/zstd.c" "$tmpdir/zstd.c"
cp "$UPSTREAM_ROOT/lib/zstd.h" "$tmpdir/zstd.h"
cp "$UPSTREAM_ROOT/lib/zstd_errors.h" "$tmpdir/zstd_errors.h"

if [[ -s "$PATCH_FILE" ]]; then
    patch -d "$tmpdir" -p1 --fuzz=0 --batch < "$PATCH_FILE"
fi

# A malformed hunk can be skipped without an error, so check the result.
actual_sha256="$(cd "$tmpdir" && sha256 zstd.c zstd.h zstd_errors.h)"
if [[ "$actual_sha256" != "$EXPECTED_SHA256" ]]; then
    echo "Generated files do not match the expected SHA-256:" >&2
    echo "$actual_sha256" >&2
    exit 1
fi

cp "$tmpdir/zstd.c" "$SCRIPT_DIR/zstd.c"
cp "$tmpdir/zstd.h" "$SCRIPT_DIR/zstd.h"
cp "$tmpdir/zstd_errors.h" "$SCRIPT_DIR/zstd_errors.h"

echo "Updated bundled zstd from ${UPSTREAM_TAG} (${UPSTREAM_COMMIT})"
