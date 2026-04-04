#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${LAGER_MC_REPO_URL:-https://github.com/xoquox/Lagerverwaltung.git}"
BRANCH="${LAGER_MC_BRANCH:-main}"
TARGET_DIR="${LAGER_MC_TARGET_DIR:-${HOME}/Lagerverwaltung}"
BUNDLE_PATH=""
RUN_MIGRATIONS="ask"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--branch <branch>] [--target <dir>] [--bundle <bundle.zip>] [--run-migrations] [--skip-migrations]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --branch)
      BRANCH="${2:-}"
      shift 2
      ;;
    --target)
      TARGET_DIR="${2:-}"
      shift 2
      ;;
    --bundle)
      BUNDLE_PATH="${2:-}"
      shift 2
      ;;
    --run-migrations)
      RUN_MIGRATIONS="yes"
      shift
      ;;
    --skip-migrations)
      RUN_MIGRATIONS="no"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unbekannte Option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -e "${TARGET_DIR}" ]]; then
  echo "Zielverzeichnis existiert bereits: ${TARGET_DIR}" >&2
  exit 1
fi

git clone --branch "${BRANCH}" --depth 1 "${REPO_URL}" "${TARGET_DIR}"

args=()
if [[ -n "${BUNDLE_PATH}" ]]; then
  args+=(--bundle "${BUNDLE_PATH}")
fi
if [[ "${RUN_MIGRATIONS}" == "yes" ]]; then
  args+=(--run-migrations)
elif [[ "${RUN_MIGRATIONS}" == "no" ]]; then
  args+=(--skip-migrations)
fi

exec "${TARGET_DIR}/scripts/install-linux.sh" "${args[@]}"
