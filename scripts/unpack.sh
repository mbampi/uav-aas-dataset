#!/bin/sh
# Decompress all .xz data files in place (keeps the .xz) and verify checksums.
set -e
cd "$(dirname "$0")/.."
find data excluded -name '*.xz' -exec xz -dk -f {} +
if command -v sha256sum >/dev/null 2>&1; then sha256sum -c --quiet SHA256SUMS; else shasum -a 256 -c --quiet SHA256SUMS; fi
echo "OK"
