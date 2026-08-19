#!/usr/bin/env bash

set -euo pipefail

# This script prepares and inspects the release without publishing it. The
# registry commands require manual 2FA and are documented in
# RELEASE_CHECKLIST.md.
pnpm release:check

(
  cd packages/core
  npm pack --dry-run
)

(
  cd packages/compat
  npm pack --dry-run
)

(
  cd packages/cli
  npm pack --dry-run
)
