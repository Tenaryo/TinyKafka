#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
HOOKS_DIR="$(git -C "$PROJECT_ROOT" rev-parse --git-common-dir)/hooks"
HOOK_PATH="${HOOKS_DIR}/pre-commit"

cp "${PROJECT_ROOT}/scripts/pre-commit" "$HOOK_PATH"
chmod +x "$HOOK_PATH"

echo "Pre-commit hook installed to ${HOOK_PATH}"
echo "To skip hook: SKIP_PRECOMMIT=1 git commit ..."
