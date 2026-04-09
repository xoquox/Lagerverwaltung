#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME="$(basename "$0")"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALLER_PATH="${ROOT_DIR}/scripts/install-standalone-de.sh"

MODE="app-only"
KEEP_ROOT=0
TEST_ROOT="${LAGER_MC_TEST_ROOT:-}"
USE_FIXED_ROOT=0
EXTRA_INSTALLER_ARGS=()
DB_HOST="127.0.0.1"
DB_NAME="lagerdb"
DB_USER="lager"
DB_PASS="lagerpass"

show_help() {
  cat <<EOF
${SCRIPT_NAME}

Startet den deutschen Standalone-Installer in einer isolierten Nix-Testumgebung.

Die Testumgebung verwendet ein eigenes HOME, eigenes XDG-Verzeichnis und ein
separates Zielverzeichnis. So bleibt das echte Benutzerprofil unveraendert.
Zusaetzlich wird eine temporaere PostgreSQL-Testinstanz gestartet.

Verwendung:
  ${SCRIPT_NAME} [optionen] [-- <weitere installer-optionen>]

Optionen:
  --mode <complete|app-only|workstation|update>
      Standard: app-only
  --root <pfad>
      Verwendet ein festes Testverzeichnis statt mktemp.
  --keep
      Testverzeichnis nach dem Lauf nicht loeschen.
  --installer <pfad>
      Alternativen Installer verwenden.
  -h, --help

Beispiele:
  ${SCRIPT_NAME}
  ${SCRIPT_NAME} --mode complete --keep
  ${SCRIPT_NAME} --root /tmp/lager-mc-installer-test --mode app-only

Temporäre PostgreSQL-Testdaten im Installer:
  Host:      127.0.0.1
  Datenbank: lagerdb
  Benutzer:  lager
  Passwort:  lagerpass
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      shift 2
      ;;
    --root)
      TEST_ROOT="${2:-}"
      USE_FIXED_ROOT=1
      shift 2
      ;;
    --keep)
      KEEP_ROOT=1
      shift
      ;;
    --installer)
      INSTALLER_PATH="${2:-}"
      shift 2
      ;;
    --)
      shift
      EXTRA_INSTALLER_ARGS+=("$@")
      break
      ;;
    -h|--help)
      show_help
      exit 0
      ;;
    *)
      EXTRA_INSTALLER_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ ! -f "${INSTALLER_PATH}" ]]; then
  printf 'Installer nicht gefunden: %s\n' "${INSTALLER_PATH}" >&2
  exit 1
fi

case "${MODE}" in
  complete|app-only|workstation|update)
    ;;
  *)
    printf 'Ungueltiger Modus: %s\n' "${MODE}" >&2
    exit 1
    ;;
esac

if [[ -z "${TEST_ROOT}" ]]; then
  TEST_ROOT="$(mktemp -d /tmp/lager-mc-installer-test.XXXXXX)"
fi

if [[ "${USE_FIXED_ROOT}" -eq 1 ]]; then
  mkdir -p "${TEST_ROOT}"
fi

TEST_HOME="${TEST_ROOT}/home"
TEST_WORK="${TEST_ROOT}/work"
TEST_CACHE="${TEST_ROOT}/cache"
TEST_CONFIG="${TEST_ROOT}/config"
TARGET_DIR="${TEST_WORK}/Lagerverwaltung"
INNER_SCRIPT="${TEST_ROOT}/run-inner.sh"
ARGS_FILE="${TEST_ROOT}/installer-args.txt"
LOCAL_ARCHIVE="${TEST_ROOT}/lagerverwaltung-local.tar.gz"
PGDATA_DIR="${TEST_ROOT}/pgdata"
PGSOCKET_DIR="${TEST_ROOT}/pgsocket"
PGLOG_FILE="${TEST_ROOT}/postgres.log"
mkdir -p "${TEST_HOME}" "${TEST_WORK}" "${TEST_CACHE}" "${TEST_CONFIG}" "${PGSOCKET_DIR}"

cleanup() {
  local exit_code=$?
  if [[ "${KEEP_ROOT}" -eq 0 ]]; then
    rm -rf "${TEST_ROOT}"
  else
    printf '\nTestverzeichnis bleibt erhalten:\n  %s\n' "${TEST_ROOT}"
  fi
  exit "${exit_code}"
}
trap cleanup EXIT

: > "${ARGS_FILE}"
for arg in "${EXTRA_INSTALLER_ARGS[@]}"; do
  printf '%s\n' "${arg}" >> "${ARGS_FILE}"
done

tar -czf "${LOCAL_ARCHIVE}" \
  --exclude='.git' \
  --exclude='.venv' \
  --exclude='__pycache__' \
  --exclude='.pytest_cache' \
  --exclude='*.pyc' \
  -C "$(dirname "${ROOT_DIR}")" \
  "$(basename "${ROOT_DIR}")"

cat > "${INNER_SCRIPT}" <<EOF
#!/usr/bin/env bash
set -euo pipefail

find_free_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

wait_for_postgres() {
  local attempts=40
  while (( attempts > 0 )); do
    if pg_isready -h '${DB_HOST}' -p "\${DB_PORT}" -d postgres >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
    attempts=\$((attempts - 1))
  done
  return 1
}

start_temp_postgres() {
  DB_PORT=5432
  if python3 - <<'PY'
import socket
s = socket.socket()
try:
    s.bind(("127.0.0.1", 5432))
except OSError:
    raise SystemExit(1)
finally:
    s.close()
PY
  then
    :
  else
    DB_PORT="\$(find_free_port)"
  fi

  initdb -D '${PGDATA_DIR}' -U '${DB_USER}' --auth=trust >/dev/null
  {
    printf "listen_addresses = '127.0.0.1'\\n"
    printf "port = %s\\n" "\${DB_PORT}"
    printf "unix_socket_directories = '%s'\\n" '${PGSOCKET_DIR}'
  } >> '${PGDATA_DIR}/postgresql.conf'

  pg_ctl -D '${PGDATA_DIR}' -l '${PGLOG_FILE}' start >/dev/null

  if ! wait_for_postgres; then
    printf 'Temporäre PostgreSQL-Instanz konnte nicht gestartet werden.\\n' >&2
    if [[ -f '${PGLOG_FILE}' ]]; then
      printf 'PostgreSQL-Log:\\n' >&2
      sed -n '1,120p' '${PGLOG_FILE}' >&2 || true
    fi
    exit 1
  fi

  createdb -h '${DB_HOST}' -p "\${DB_PORT}" -U '${DB_USER}' '${DB_NAME}' >/dev/null
}

cleanup_inner() {
  if [[ -d '${PGDATA_DIR}' ]]; then
    pg_ctl -D '${PGDATA_DIR}' -m fast stop >/dev/null 2>&1 || true
  fi
}
trap cleanup_inner EXIT

start_temp_postgres

export HOME='${TEST_HOME}'
export XDG_CACHE_HOME='${TEST_CACHE}'
export XDG_CONFIG_HOME='${TEST_CONFIG}'
export XDG_DATA_HOME='${TEST_HOME}/.local/share'
export LAGER_MC_TARGET_DIR='${TARGET_DIR}'
export LAGER_MC_CONNECT_BROWSER_MODE='manual'
export PGHOST='${DB_HOST}'
export PGPORT="\${DB_PORT}"
export PGDATABASE='${DB_NAME}'
export PGUSER='${DB_USER}'
export PGPASSWORD='${DB_PASS}'
mkdir -p "\${HOME}" "\${XDG_CACHE_HOME}" "\${XDG_CONFIG_HOME}" "\${XDG_DATA_HOME}"
printf 'Test-HOME: %s\n' "\${HOME}"
printf 'Test-Ziel: %s\n' '${TARGET_DIR}'
printf 'Temporäre PostgreSQL-Testdaten:\n'
printf '  Host: %s\n' '${DB_HOST}'
printf '  Port: %s\n' "\${DB_PORT}"
printf '  Datenbank: %s\n' '${DB_NAME}'
printf '  Benutzer: %s\n' '${DB_USER}'
printf '  Passwort: %s\n' '${DB_PASS}'
printf '  Archiv: %s\n' 'file://${LOCAL_ARCHIVE}'
installer_args=()
while IFS= read -r line; do
  installer_args+=("\${line}")
done < '${ARGS_FILE}'
exec bash '${INSTALLER_PATH}' --mode '${MODE}' --archive-url 'file://${LOCAL_ARCHIVE}' "\${installer_args[@]}"
EOF
chmod +x "${INNER_SCRIPT}"

printf 'Starte isolierte Installer-Testumgebung.\n'
printf 'Testverzeichnis: %s\n' "${TEST_ROOT}"
printf 'Installer: %s\n' "${INSTALLER_PATH}"
printf 'Modus: %s\n' "${MODE}"
printf 'Zielverzeichnis im Test: %s\n\n' "${TARGET_DIR}"
printf 'Temporäre PostgreSQL-Testdaten fuer den Installer:\n'
printf '  Host: %s\n' "${DB_HOST}"
printf '  Port: %s\n' 'wird im Test automatisch gesetzt'
printf '  Datenbank: %s\n' "${DB_NAME}"
printf '  Benutzer: %s\n' "${DB_USER}"
printf '  Passwort: %s\n\n' "${DB_PASS}"
printf 'Archiv fuer den Installer-Test: %s\n\n' "${LOCAL_ARCHIVE}"

if command -v nix-shell >/dev/null 2>&1; then
  exec nix-shell -p \
    bash curl gnutar python3 git dialog pkg-config gcc \
    postgresql libpq cairo pango gdk-pixbuf glib libffi cups \
    --run "bash '${INNER_SCRIPT}'"
fi

if command -v nix >/dev/null 2>&1; then
  exec nix shell \
    nixpkgs#bash \
    nixpkgs#curl \
    nixpkgs#gnutar \
    nixpkgs#python3 \
    nixpkgs#git \
    nixpkgs#dialog \
    nixpkgs#pkg-config \
    nixpkgs#gcc \
    nixpkgs#postgresql \
    nixpkgs#libpq \
    nixpkgs#cairo \
    nixpkgs#pango \
    nixpkgs#gdk-pixbuf \
    nixpkgs#glib \
    nixpkgs#libffi \
    nixpkgs#cups \
    --command bash "${INNER_SCRIPT}"
fi

printf 'Weder nix-shell noch nix gefunden.\n' >&2
exit 1
