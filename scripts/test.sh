#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "[tests] package unit tests"
"${repo_root}/scripts/test-packages.sh"

echo "[tests] app integration tests"
"${repo_root}/scripts/test-apps.sh"
