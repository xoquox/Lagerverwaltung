#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${PROJECT_DIR}/.venv"
BIN_DIR="${HOME}/.local/bin"
LAUNCHER="${BIN_DIR}/lager-mc"
APPLICATIONS_DIR="${HOME}/.local/share/applications"
ICONS_DIR="${HOME}/.local/share/icons/hicolor/scalable/apps"
DESKTOP_FILE="${APPLICATIONS_DIR}/lager-mc.desktop"
ICON_TARGET="${ICONS_DIR}/lager-mc.svg"
BUNDLE_PATH=""
RUN_MIGRATIONS="ask"

usage() {
  cat <<EOF
Usage: $(basename "$0") [--bundle <bundle.zip>] [--run-migrations] [--skip-migrations]
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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

detect_package_manager() {
  if command -v dnf >/dev/null 2>&1; then
    echo "dnf"
    return
  fi
  if command -v apt-get >/dev/null 2>&1; then
    echo "apt"
    return
  fi
  if command -v pacman >/dev/null 2>&1; then
    echo "pacman"
    return
  fi
  if command -v zypper >/dev/null 2>&1; then
    echo "zypper"
    return
  fi

  echo "Kein unterstuetzter Paketmanager gefunden (dnf/apt/pacman/zypper)." >&2
  exit 1
}

install_system_dependencies() {
  local manager="$1"
  echo "Installiere Systemabhaengigkeiten mit ${manager} ..."

  case "${manager}" in
    dnf)
      sudo dnf install -y \
        python3 \
        python3-devel \
        python3-pip \
        python3-virtualenv \
        gcc \
        postgresql-devel \
        redhat-rpm-config \
        libusb1 \
        cups \
        cairo \
        pango \
        gdk-pixbuf2 \
        libffi-devel
      ;;
    apt)
      sudo apt-get update
      sudo apt-get install -y \
        python3 \
        python3-dev \
        python3-pip \
        python3-venv \
        build-essential \
        libpq-dev \
        libusb-1.0-0 \
        cups-client \
        libcairo2 \
        libpango-1.0-0 \
        libgdk-pixbuf-2.0-0 \
        libffi-dev \
        shared-mime-info \
        fonts-dejavu-core
      ;;
    pacman)
      sudo pacman -Syu --noconfirm \
        python \
        python-pip \
        python-virtualenv \
        base-devel \
        postgresql-libs \
        postgresql \
        libusb \
        cups \
        cairo \
        pango \
        gdk-pixbuf2 \
        libffi
      ;;
    zypper)
      sudo zypper --non-interactive install \
        python3 \
        python3-devel \
        python3-pip \
        python3-virtualenv \
        gcc \
        make \
        postgresql-devel \
        libusb-1_0-0 \
        cups-client \
        cairo \
        pango \
        gdk-pixbuf \
        libffi-devel
      ;;
    *)
      echo "Nicht unterstuetzter Paketmanager: ${manager}" >&2
      exit 1
      ;;
  esac
}

prompt_bundle_path() {
  local answer
  if [[ -n "${BUNDLE_PATH}" ]]; then
    return
  fi
  read -r -p "Bundle-Archiv fuer lokale Settings vorhanden? Pfad eingeben oder leer lassen: " answer
  BUNDLE_PATH="${answer}"
}

apply_bundle_if_requested() {
  if [[ -z "${BUNDLE_PATH}" ]]; then
    return
  fi
  if [[ ! -f "${BUNDLE_PATH}" ]]; then
    echo "Bundle nicht gefunden: ${BUNDLE_PATH}" >&2
    exit 1
  fi
  echo "Spiele Bundle ein ..."
  "${VENV_DIR}/bin/python" "${PROJECT_DIR}/scripts/apply_local_bundle.py" "${BUNDLE_PATH}"
}

maybe_run_migrations() {
  local should_run="${RUN_MIGRATIONS}"
  if [[ "${should_run}" == "ask" ]]; then
    local answer
    read -r -p "DB-Migration jetzt ausfuehren? [J/n] " answer
    answer="${answer:-J}"
    case "${answer}" in
      n|N)
        should_run="no"
        ;;
      *)
        should_run="yes"
        ;;
    esac
  fi

  if [[ "${should_run}" != "yes" ]]; then
    return
  fi

  echo "Fuehre DB-Migration aus ..."
  "${VENV_DIR}/bin/python" "${PROJECT_DIR}/scripts/run_db_migrations.py"
}

echo "Starte Linux-Installation fuer Lager MC ..."
PKG_MANAGER="$(detect_package_manager)"
install_system_dependencies "${PKG_MANAGER}"

echo "Erzeuge virtuelle Umgebung in ${VENV_DIR} ..."
python3 -m venv "${VENV_DIR}"

echo "Installiere Python-Abhaengigkeiten ..."
"${VENV_DIR}/bin/python" -m ensurepip --upgrade
"${VENV_DIR}/bin/python" -m pip install --upgrade pip wheel
"${VENV_DIR}/bin/python" -m pip install -r "${PROJECT_DIR}/requirements.txt"

prompt_bundle_path
apply_bundle_if_requested
maybe_run_migrations

mkdir -p "${BIN_DIR}"
mkdir -p "${APPLICATIONS_DIR}"
mkdir -p "${ICONS_DIR}"

cat > "${LAUNCHER}" <<EOF
#!/usr/bin/env bash
exec "${VENV_DIR}/bin/python" "${PROJECT_DIR}/lager_mc.py" "\$@"
EOF

chmod +x "${LAUNCHER}"
install -m 0644 "${PROJECT_DIR}/assets/lager-mc.svg" "${ICON_TARGET}"

cat > "${DESKTOP_FILE}" <<EOF
[Desktop Entry]
Version=1.0
Type=Application
Name=Lager MC
Comment=Terminaloberflaeche fuer Lagerverwaltung
Exec=${LAUNCHER}
Icon=${ICON_TARGET}
Terminal=true
Categories=Office;Utility;
Keywords=Lager;Inventar;Shopify;
StartupNotify=true
EOF

echo
echo "Installation abgeschlossen."
echo "Starten mit:"
echo "  ${LAUNCHER}"
echo
echo "Ein Startmenue-Eintrag wurde angelegt:"
echo "  ${DESKTOP_FILE}"
echo
echo "Falls ~/.local/bin noch nicht im PATH ist, einmal neu einloggen oder manuell aufrufen."
