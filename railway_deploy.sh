#!/usr/bin/env bash
set -euo pipefail

# railway_deploy.sh
# On Railway (or any host), clone the private tenants repo and build/run its Docker image.
# Requires: GITHUB_TOKEN (or GITHUB_ACCESS_TOKEN) set in the environment.

REPO_URL="https://github.com/voteagora/tenants"
CLONE_DIR="${CLONE_DIR:-./tenants}"

# Prefer GITHUB_TOKEN; fall back to GITHUB_ACCESS_TOKEN
GITHUB_TOKEN="${GITHUB_TOKEN:-${GITHUB_ACCESS_TOKEN:-}}"
if [[ -z "${GITHUB_TOKEN}" ]]; then
  echo "Error: GITHUB_TOKEN or GITHUB_ACCESS_TOKEN must be set to clone the private repo." >&2
  exit 1
fi

# Clone using token (no prompt). Use a shallow clone for speed.
CLONE_URL="https://x-access-token:${GITHUB_TOKEN}@github.com/voteagora/tenants.git"
if [[ -d "${CLONE_DIR}" ]]; then
  echo "Directory ${CLONE_DIR} already exists; pulling latest..."
  git -C "${CLONE_DIR}" pull --ff-only || true
else
  echo "Cloning ${REPO_URL} into ${CLONE_DIR}..."
  git clone --depth 1 "${CLONE_URL}" "${CLONE_DIR}"
fi

sanic app.server --host=0.0.0.0 --port=8000
