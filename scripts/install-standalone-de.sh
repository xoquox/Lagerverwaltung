#!/usr/bin/env bash
set -euo pipefail

SCRIPT_NAME="$(basename "$0")"
SCRIPT_VERSION="0.1.0"

REPO_ARCHIVE_URL_DEFAULT="${LAGER_MC_ARCHIVE_URL:-https://github.com/xoquox/Lagerverwaltung/archive/refs/heads/main.tar.gz}"
TARGET_DIR_DEFAULT="${LAGER_MC_TARGET_DIR:-${HOME}/Lagerverwaltung}"
SHOPIFY_APP_SCOPES_DEFAULT="${SHOPIFY_APP_SCOPES:-read_customers,read_inventory,read_locations,read_merchant_managed_fulfillment_orders,read_orders,read_products,write_inventory,write_merchant_managed_fulfillment_orders}"
SHOPIFY_APP_REDIRECT_URI_DEFAULT="${SHOPIFY_APP_REDIRECT_URI:-}"
PRIVACY_URL_DEFAULT="${LAGER_MC_PRIVACY_URL:-https://lagerverwaltung.org/de/app-datenschutz.txt}"
TERMS_URL_DEFAULT="${LAGER_MC_TERMS_URL:-https://lagerverwaltung.org/de/agb.txt}"
DOC_URL_DEFAULT="${LAGER_MC_DOC_URL:-https://lagerverwaltung.org/doku.html}"
DOWNLOAD_URL_DEFAULT="${LAGER_MC_DOWNLOAD_URL:-https://lagerverwaltung.org/download.html}"
DB_HOST_DEFAULT="${PGHOST:-localhost}"
DB_PORT_DEFAULT="${PGPORT:-5432}"
DB_NAME_DEFAULT="${PGDATABASE:-lagerdb}"
DB_USER_DEFAULT="${PGUSER:-lager}"
CONNECT_BROWSER_MODE="${LAGER_MC_CONNECT_BROWSER_MODE:-auto}"

MODE="complete"
MODE_SET_BY_ARG=0
TARGET_DIR="${TARGET_DIR_DEFAULT}"
ARCHIVE_URL="${REPO_ARCHIVE_URL_DEFAULT}"
SHOPIFY_APP_SCOPES="${SHOPIFY_APP_SCOPES_DEFAULT}"
SHOPIFY_APP_REDIRECT_URI="${SHOPIFY_APP_REDIRECT_URI_DEFAULT}"
PRIVACY_URL="${PRIVACY_URL_DEFAULT}"
TERMS_URL="${TERMS_URL_DEFAULT}"
DOC_URL="${DOC_URL_DEFAULT}"
DOWNLOAD_URL="${DOWNLOAD_URL_DEFAULT}"
UI_BACKEND="text"
INSTALL_ROOT=""
PACKAGE_MANAGER=""
LOG_FILE="${TMPDIR:-/tmp}/lager-mc-installer-$(date +%Y%m%d-%H%M%S).log"
LAST_PROGRESS_TEXT=""
GUIDED_UI_INSTALL_ATTEMPTED=0
DIALOGRC_FILE=""

append_log_line() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >> "${LOG_FILE}"
}

cleanup_temp_files() {
  if [[ -n "${DIALOGRC_FILE}" && -f "${DIALOGRC_FILE}" ]]; then
    rm -f "${DIALOGRC_FILE}"
  fi
}

trap cleanup_temp_files EXIT

show_help() {
  cat <<EOF
${SCRIPT_NAME} ${SCRIPT_VERSION}

Standalone-Installer fuer Lager-MC.
Laedt die Programmdaten von GitHub, installiert Lager-MC und optional
shopify-sync, richtet PostgreSQL ein und kann den Shopify-Connect starten.

Verwendung:
  ${SCRIPT_NAME} [optionen]

Optionen:
  --mode <complete|app-only|workstation|update>
  --target <pfad>
  --archive-url <url>
  --privacy-url <url>
  --terms-url <url>
  --doc-url <url>
  --download-url <url>
  -h, --help

Standardmodus:
  complete = Komplettinstallation
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      MODE="${2:-}"
      MODE_SET_BY_ARG=1
      shift 2
      ;;
    --target)
      TARGET_DIR="${2:-}"
      shift 2
      ;;
    --archive-url)
      ARCHIVE_URL="${2:-}"
      shift 2
      ;;
    --privacy-url)
      PRIVACY_URL="${2:-}"
      shift 2
      ;;
    --terms-url)
      TERMS_URL="${2:-}"
      shift 2
      ;;
    --doc-url)
      DOC_URL="${2:-}"
      shift 2
      ;;
    --download-url)
      DOWNLOAD_URL="${2:-}"
      shift 2
      ;;
    -h|--help)
      show_help
      exit 0
      ;;
    *)
      echo "Unbekannte Option: $1" >&2
      show_help >&2
      exit 1
      ;;
  esac
done

log_line() {
  append_log_line "$*"
  if [[ "${UI_BACKEND}" == "text" ]]; then
    printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2
  fi
}

die() {
  log_line "FEHLER: $*"
  if [[ "${UI_BACKEND}" != "text" ]]; then
    ui_message "Fehler" "$*\n\nLogdatei: ${LOG_FILE}"
  fi
  exit 1
}

run_logged() {
  log_line "+ $*"
  "$@" >>"${LOG_FILE}" 2>&1
}

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

detect_ui_backend() {
  if [[ -t 1 ]] && command_exists dialog; then
    UI_BACKEND="dialog"
    return
  fi
  if [[ -t 1 ]] && command_exists whiptail; then
    UI_BACKEND="whiptail"
    return
  fi
  UI_BACKEND="text"
}

setup_dialog_theme() {
  if [[ "${UI_BACKEND}" != "dialog" ]]; then
    return
  fi

  DIALOGRC_FILE="$(mktemp "${TMPDIR:-/tmp}/lager-mc-dialogrc-XXXXXX")"
  cat > "${DIALOGRC_FILE}" <<'EOF'
use_shadow = OFF
screen_color = (WHITE,BLUE,ON)
shadow_color = (BLACK,BLACK,ON)
dialog_color = (BLACK,WHITE,OFF)
title_color = (YELLOW,WHITE,ON)
border_color = (BLUE,WHITE,ON)
button_active_color = (WHITE,BLUE,ON)
button_inactive_color = (BLACK,WHITE,OFF)
button_key_active_color = (YELLOW,BLUE,ON)
button_key_inactive_color = (BLUE,WHITE,ON)
button_label_active_color = (WHITE,BLUE,ON)
button_label_inactive_color = (BLACK,WHITE,OFF)
inputbox_color = (BLACK,WHITE,OFF)
inputbox_border_color = (BLUE,WHITE,ON)
searchbox_color = (BLACK,WHITE,OFF)
searchbox_title_color = (YELLOW,WHITE,ON)
searchbox_border_color = (BLUE,WHITE,ON)
position_indicator_color = (YELLOW,WHITE,ON)
menubox_color = (BLACK,WHITE,OFF)
menubox_border_color = (BLUE,WHITE,ON)
item_color = (BLACK,WHITE,OFF)
item_selected_color = (WHITE,BLUE,ON)
tag_color = (BLUE,WHITE,ON)
tag_selected_color = (YELLOW,BLUE,ON)
tag_key_color = (BLUE,WHITE,ON)
tag_key_selected_color = (YELLOW,BLUE,ON)
check_color = (BLACK,WHITE,OFF)
check_selected_color = (WHITE,BLUE,ON)
uarrow_color = (BLUE,WHITE,ON)
darrow_color = (BLUE,WHITE,ON)
itemhelp_color = (BLACK,WHITE,OFF)
form_active_text_color = (WHITE,BLUE,ON)
form_text_color = (BLACK,WHITE,OFF)
form_item_readonly_color = (BLUE,WHITE,ON)
gauge_color = (WHITE,BLUE,ON)
EOF
  export DIALOGRC="${DIALOGRC_FILE}"
}

ensure_guided_ui_backend() {
  if [[ ! -t 1 ]]; then
    return
  fi

  if command_exists dialog || command_exists whiptail; then
    return
  fi

  detect_package_manager
  if [[ -z "${PACKAGE_MANAGER}" ]]; then
    return
  fi

  printf '\n[Lager-MC Installer]\n'
  printf 'Fuer die gefuehrte Terminaloberflaeche wird jetzt zuerst das Paket "dialog" installiert, wenn der Paketmanager es bereitstellt.\n'
  printf 'Fortfahren? [J/n]: '

  local answer
  read -r answer
  answer="${answer:-J}"
  case "${answer}" in
    n|N|no|NO)
      return
      ;;
  esac

  GUIDED_UI_INSTALL_ATTEMPTED=1
  case "${PACKAGE_MANAGER}" in
    apt)
      run_logged sudo apt-get update
      run_logged sudo apt-get install -y dialog
      ;;
    dnf)
      run_logged sudo dnf install -y dialog
      ;;
    pacman)
      run_logged sudo pacman -Syu --noconfirm dialog
      ;;
    zypper)
      run_logged sudo zypper --non-interactive install dialog
      ;;
  esac
}

ui_message() {
  local title="$1"
  local text="$2"
  case "${UI_BACKEND}" in
    dialog)
      dialog --backtitle "Lager-MC Installer" --title "${title}" --msgbox "${text}" 18 78
      ;;
    whiptail)
      whiptail --backtitle "Lager-MC Installer" --title "${title}" --msgbox "${text}" 18 78
      ;;
    *)
      printf '\n[%s]\n%s\n\n' "${title}" "${text}"
      ;;
  esac
}

render_progress_bar() {
  local percent="$1"
  local width="${2:-28}"
  local filled empty
  if (( percent < 0 )); then
    percent=0
  elif (( percent > 100 )); then
    percent=100
  fi
  filled=$(( percent * width / 100 ))
  empty=$(( width - filled ))
  printf '['
  printf '%*s' "${filled}" '' | tr ' ' '#'
  printf '%*s' "${empty}" '' | tr ' ' '-'
  printf ']'
}

ui_progress() {
  local percent="$1"
  local text="$2"
  local bar
  LAST_PROGRESS_TEXT="${text}"
  bar="$(render_progress_bar "${percent}")"
  append_log_line "FORTSCHRITT ${percent}% ${text}"
  case "${UI_BACKEND}" in
    dialog)
      dialog --backtitle "Lager-MC Installer" --title "Fortschritt ${percent}%" --infobox "${bar}\n\n${text}" 8 76
      ;;
    whiptail)
      whiptail --backtitle "Lager-MC Installer" --title "Fortschritt ${percent}%" --infobox "${bar}\n\n${text}" 8 76
      ;;
    *)
      printf '\r%s %3s%% %s' "${bar}" "${percent}" "${text}" >&2
      if (( percent >= 100 )); then
        printf '\n' >&2
      fi
      ;;
  esac
}

ui_confirm() {
  local title="$1"
  local text="$2"
  case "${UI_BACKEND}" in
    dialog)
      dialog --backtitle "Lager-MC Installer" --title "${title}" --yesno "${text}" 18 78
      ;;
    whiptail)
      whiptail --backtitle "Lager-MC Installer" --title "${title}" --yesno "${text}" 18 78
      ;;
    *)
      local answer
      printf '\n[%s]\n%s\n[j/N]: ' "${title}" "${text}"
      read -r answer
      [[ "${answer:-}" =~ ^([jJyY]|ja|JA|yes|YES)$ ]]
      ;;
  esac
}

ui_input() {
  local title="$1"
  local prompt="$2"
  local default_value="${3:-}"
  case "${UI_BACKEND}" in
    dialog)
      dialog --stdout --backtitle "Lager-MC Installer" --title "${title}" --inputbox "${prompt}" 18 78 "${default_value}"
      ;;
    whiptail)
      whiptail --stdout --backtitle "Lager-MC Installer" --title "${title}" --inputbox "${prompt}" 18 78 "${default_value}"
      ;;
    *)
      local answer
      printf '\n[%s]\n%s\n[%s]: ' "${title}" "${prompt}" "${default_value}"
      read -r answer
      if [[ -z "${answer}" ]]; then
        printf '%s\n' "${default_value}"
      else
        printf '%s\n' "${answer}"
      fi
      ;;
  esac
}

ui_password() {
  local title="$1"
  local prompt="$2"
  case "${UI_BACKEND}" in
    dialog)
      dialog --stdout --insecure --backtitle "Lager-MC Installer" --title "${title}" --passwordbox "${prompt}" 18 78
      ;;
    whiptail)
      whiptail --stdout --backtitle "Lager-MC Installer" --title "${title}" --passwordbox "${prompt}" 18 78
      ;;
    *)
      local answer
      printf '\n[%s]\n%s\n: ' "${title}" "${prompt}"
      read -r -s answer
      printf '\n'
      printf '%s\n' "${answer}"
      ;;
  esac
}

ui_menu() {
  local title="$1"
  local prompt="$2"
  shift 2
  local rc=0
  case "${UI_BACKEND}" in
    dialog)
      local result=""
      result="$(dialog --stdout --backtitle "Lager-MC Installer" --title "${title}" --menu "${prompt}" 20 90 10 "$@")" || rc=$?
      if (( rc != 0 )); then
        printf '\n'
        return 1
      fi
      printf '%s\n' "${result}"
      ;;
    whiptail)
      local result=""
      result="$(whiptail --stdout --backtitle "Lager-MC Installer" --title "${title}" --menu "${prompt}" 20 90 10 "$@")" || rc=$?
      if (( rc != 0 )); then
        printf '\n'
        return 1
      fi
      printf '%s\n' "${result}"
      ;;
    *)
      local options=("$@")
      local count=$(( ${#options[@]} / 2 ))
      local index=1
      printf '\n[%s]\n%s\n' "${title}" "${prompt}"
      while [[ ${index} -le ${count} ]]; do
        local offset=$(( (index - 1) * 2 ))
        printf '  %d) %s - %s\n' "${index}" "${options[${offset}]}" "${options[$((offset + 1))]}"
        index=$((index + 1))
      done
      local selection
      while true; do
        printf 'Auswahl: '
        read -r selection || return 1
        if [[ -z "${selection}" ]]; then
          return 1
        fi
        if [[ "${selection}" =~ ^[0-9]+$ ]] && [[ ${selection} -ge 1 ]] && [[ ${selection} -le ${count} ]]; then
          local chosen_offset=$(( (selection - 1) * 2 ))
          printf '%s\n' "${options[${chosen_offset}]}"
          return 0
        fi
      done
      ;;
  esac
}

detect_package_manager() {
  if command_exists apt-get; then
    PACKAGE_MANAGER="apt"
    return
  fi
  if command_exists dnf; then
    PACKAGE_MANAGER="dnf"
    return
  fi
  if command_exists pacman; then
    PACKAGE_MANAGER="pacman"
    return
  fi
  if command_exists zypper; then
    PACKAGE_MANAGER="zypper"
    return
  fi
  PACKAGE_MANAGER=""
}

ensure_linux() {
  [[ "$(uname -s)" == "Linux" ]] || die "Dieser Installer unterstuetzt aktuell nur Linux."
}

fetch_url() {
  local url="$1"
  local destination="$2"
  if command_exists curl; then
    run_logged curl --fail --location --silent --show-error -o "${destination}" "${url}"
    return
  fi
  if command_exists wget; then
    run_logged wget -q -O "${destination}" "${url}"
    return
  fi
  die "Weder curl noch wget verfuegbar."
}

fetch_text_document() {
  local url="$1"
  local text_path
  text_path="$(mktemp "${TMPDIR:-/tmp}/lager-mc-doc-XXXXXX.txt")"
  if ! fetch_url "${url}" "${text_path}"; then
    rm -f "${text_path}"
    return 1
  fi
  printf '%s\n' "${text_path}"
}

dialog_dimensions() {
  local lines cols height width
  lines="$(tput lines 2>/dev/null || printf '24')"
  cols="$(tput cols 2>/dev/null || printf '80')"

  if [[ ! "${lines}" =~ ^[0-9]+$ ]]; then
    lines=24
  fi
  if [[ ! "${cols}" =~ ^[0-9]+$ ]]; then
    cols=80
  fi

  height=$(( lines - 4 ))
  width=$(( cols - 6 ))

  if (( height < 18 )); then
    height=18
  elif (( height > 42 )); then
    height=42
  fi

  if (( width < 76 )); then
    width=76
  elif (( width > 150 )); then
    width=150
  fi

  printf '%s %s\n' "${height}" "${width}"
}

wrap_text_document_for_display() {
  local source_path="$1"
  local wrap_width="$2"
  local wrapped_path
  wrapped_path="$(mktemp "${TMPDIR:-/tmp}/lager-mc-doc-wrap-XXXXXX.txt")"

  cat > "${wrapped_path}.py" <<'PY'
from pathlib import Path
import re
import sys
import textwrap

source = Path(sys.argv[1])
target = Path(sys.argv[2])
width = max(40, int(sys.argv[3]))
lines = source.read_text(encoding="utf-8").splitlines()
result = []
numbered_re = re.compile(r"^(\d+\.\s+)(.*)$")

for raw in lines:
    stripped = raw.rstrip()
    if not stripped:
        result.append("")
        continue

    numbered_match = numbered_re.match(stripped)
    if numbered_match:
        prefix = numbered_match.group(1)
        body = numbered_match.group(2).strip()
        wrapped = textwrap.fill(
            body,
            width=max(20, width - len(prefix)),
            initial_indent=prefix,
            subsequent_indent=" " * len(prefix),
            break_long_words=False,
            break_on_hyphens=False,
        )
        result.extend(wrapped.splitlines())
        continue

    if stripped.endswith(":"):
        result.append(stripped)
        continue

    wrapped = textwrap.fill(
        stripped,
        width=width,
        break_long_words=False,
        break_on_hyphens=False,
    )
    result.extend(wrapped.splitlines())

target.write_text("\n".join(result) + "\n", encoding="utf-8")
PY

  run_logged python3 "${wrapped_path}.py" "${source_path}" "${wrapped_path}" "${wrap_width}"
  rm -f "${wrapped_path}.py"
  printf '%s\n' "${wrapped_path}"
}

show_live_document() {
  local title="$1"
  local url="$2"
  local text_path wrapped_path dimensions height width
  if ! text_path="$(fetch_text_document "${url}")"; then
    ui_message "${title}" "Das Dokument konnte nicht automatisch geladen werden.\n\nBitte manuell pruefen:\n${url}"
    return 1
  fi

  case "${UI_BACKEND}" in
    dialog)
      dimensions="$(dialog_dimensions)"
      height="${dimensions%% *}"
      width="${dimensions##* }"
      wrapped_path="$(wrap_text_document_for_display "${text_path}" "$(( width - 6 ))")"
      dialog --backtitle "Lager-MC Installer" --title "${title}" --textbox "${wrapped_path}" "${height}" "${width}"
      rm -f "${wrapped_path}"
      ;;
    whiptail)
      dimensions="$(dialog_dimensions)"
      height="${dimensions%% *}"
      width="${dimensions##* }"
      wrapped_path="$(wrap_text_document_for_display "${text_path}" "$(( width - 6 ))")"
      whiptail --backtitle "Lager-MC Installer" --title "${title}" --textbox "${wrapped_path}" "${height}" "${width}"
      rm -f "${wrapped_path}"
      ;;
    *)
      if command_exists less; then
        less "${text_path}"
      else
        printf '\n[%s]\n' "${title}"
        cat "${text_path}"
        printf '\nWeiter mit Enter ...'
        read -r _
      fi
      ;;
  esac
  rm -f "${text_path}"
}

install_system_dependencies() {
  detect_package_manager
  if [[ -z "${PACKAGE_MANAGER}" ]]; then
    ui_message "Paketmanager" "Kein unterstuetzter Paketmanager erkannt. Abhaengigkeiten muessen manuell installiert werden."
    return 1
  fi

  if ! ui_confirm "Systempakete" "Fehlende Systempakete koennen jetzt soweit moeglich automatisch installiert werden. Fortfahren?"; then
    return 1
  fi

  case "${PACKAGE_MANAGER}" in
    apt)
      run_logged sudo apt-get update
      run_logged sudo apt-get install -y \
        python3 python3-dev python3-pip python3-venv \
        build-essential libpq-dev libusb-1.0-0 cups-client \
        libcairo2 libpango-1.0-0 libgdk-pixbuf-2.0-0 libffi-dev \
        shared-mime-info fonts-dejavu-core postgresql-client dialog
      ;;
    dnf)
      run_logged sudo dnf install -y \
        python3 python3-devel python3-pip python3-virtualenv gcc \
        postgresql-devel libusb1 cups cairo pango gdk-pixbuf2 libffi-devel dialog
      ;;
    pacman)
      run_logged sudo pacman -Syu --noconfirm \
        python python-pip python-virtualenv base-devel postgresql-libs \
        postgresql libusb cups cairo pango gdk-pixbuf2 libffi dialog
      ;;
    zypper)
      run_logged sudo zypper --non-interactive install \
        python3 python3-devel python3-pip python3-virtualenv gcc make \
        postgresql-devel libusb-1_0-0 cups-client cairo pango gdk-pixbuf \
        libffi-devel dialog
      ;;
  esac
}

check_required_tools() {
  local missing=()
  local tool
  for tool in python3 tar; do
    if ! command_exists "${tool}"; then
      missing+=("${tool}")
    fi
  done
  if ! command_exists curl && ! command_exists wget; then
    missing+=("curl/wget")
  fi
  if [[ ${#missing[@]} -gt 0 ]]; then
    log_line "Fehlende Werkzeuge: ${missing[*]}"
    install_system_dependencies || die "Bitte fehlende Abhaengigkeiten manuell installieren und den Installer erneut starten."
  fi
}

select_mode_interactively() {
  MODE="$(ui_menu "Installationsmodus" "Bitte den gewuenschten Installationsmodus waehlen." \
    "complete" "Komplettinstallation (empfohlen)" \
    "app-only" "Nur Lager-MC installieren" \
    "workstation" "Arbeitsplatz mit vorhandenem shopify-sync" \
    "update" "Update oder Reparatur")"
  [[ -n "${MODE}" ]] || die "Kein Installationsmodus gewaehlt."
}

consent_phase() {
  if ! show_live_document "Datenschutz" "${PRIVACY_URL}"; then
    ui_confirm "Datenschutz" "Datenschutztext konnte nicht geladen werden. Die URL wurde angezeigt. Trotzdem fortfahren?" || die "Installation abgebrochen: Datenschutz nicht bestaetigt."
  fi
  ui_confirm "Datenschutz" "Ich habe die Datenschutzhinweise gelesen und stimme ihnen fuer diese Installation zu." || die "Installation abgebrochen: Datenschutz nicht akzeptiert."
  if ! show_live_document "AGB" "${TERMS_URL}"; then
    ui_confirm "AGB" "AGB konnten nicht geladen werden. Die URL wurde angezeigt. Trotzdem fortfahren?" || die "Installation abgebrochen: AGB nicht bestaetigt."
  fi
  ui_confirm "AGB" "Ich habe die AGB gelesen und stimme ihnen fuer diese Installation zu." || die "Installation abgebrochen: AGB nicht akzeptiert."
  ui_confirm "Shopify-Berechtigungen" "Bei lokaler Shopify-Anbindung werden Produkt-, Bestands-, Bestell-, Kunden- und Fulfillment-Berechtigungen verwendet. Fortfahren?" || die "Installation abgebrochen: Shopify-Berechtigungen nicht akzeptiert."
}

choose_target_dir() {
  TARGET_DIR="$(ui_input "Zielverzeichnis" "Bitte Zielverzeichnis fuer Lager-MC angeben." "${TARGET_DIR}")"
  [[ -n "${TARGET_DIR}" ]] || die "Kein Zielverzeichnis angegeben."
  INSTALL_ROOT="$(python3 -c 'import os,sys; print(os.path.abspath(os.path.expanduser(sys.argv[1])))' "${TARGET_DIR}")"
}

download_release() {
  local archive_path
  local python_script
  archive_path="$(mktemp "${TMPDIR:-/tmp}/lager-mc-release-XXXXXX.tar.gz")"
  fetch_url "${ARCHIVE_URL}" "${archive_path}"

  if [[ -d "${INSTALL_ROOT}" && "$(find "${INSTALL_ROOT}" -mindepth 1 -maxdepth 1 2>/dev/null | head -n 1)" != "" ]]; then
    if [[ "${MODE}" != "update" ]]; then
      die "Zielverzeichnis ist nicht leer: ${INSTALL_ROOT}"
    fi
  else
    mkdir -p "${INSTALL_ROOT}"
  fi

  python_script="$(mktemp "${TMPDIR:-/tmp}/lager-mc-archive-layout-XXXXXX.py")"
  cat > "${python_script}" <<'PY'
import sys
import tarfile

archive_path = sys.argv[1]

with tarfile.open(archive_path, "r:*") as tar:
    names = [member.name for member in tar.getmembers() if member.name and member.name not in (".", "./")]

top_levels = set()
has_root_file = False
for name in names:
    normalized = name.lstrip("./")
    parts = [part for part in normalized.split("/") if part]
    if not parts:
        continue
    top_levels.add(parts[0])
    if len(parts) == 1:
        has_root_file = True

if len(top_levels) == 1 and not has_root_file:
    print("1")
else:
    print("0")
PY
  local strip_components
  strip_components="$(python3 "${python_script}" "${archive_path}")"
  rm -f "${python_script}"

  if [[ "${strip_components}" == "1" ]]; then
    run_logged tar -xzf "${archive_path}" --strip-components=1 -C "${INSTALL_ROOT}"
  else
    run_logged tar -xzf "${archive_path}" -C "${INSTALL_ROOT}"
  fi
  rm -f "${archive_path}"
}

write_json_settings() {
  local db_host="$1"
  local db_port="$2"
  local db_name="$3"
  local db_user="$4"
  local db_pass="$5"
  local pdf_dir="$6"
  local label_dir="$7"
  local python_script
  python_script="$(mktemp "${TMPDIR:-/tmp}/lager-mc-settings-XXXXXX.py")"
  cat > "${python_script}" <<'PY'
import json
import os
import sys
from pathlib import Path

target = Path(sys.argv[1])
settings_path = target / "settings.local.json"
settings = {}
if settings_path.exists():
    settings = json.loads(settings_path.read_text(encoding="utf-8"))

updates = {
    "db_host": sys.argv[2],
    "db_port": int(sys.argv[3]),
    "db_name": sys.argv[4],
    "db_user": sys.argv[5],
    "db_pass": sys.argv[6],
}
if sys.argv[7]:
    updates["pdf_output_dir"] = sys.argv[7]
if sys.argv[8]:
    updates["shipping_label_output_dir"] = sys.argv[8]

settings.update(updates)
settings_path.write_text(json.dumps(settings, indent=2, sort_keys=True) + "\n", encoding="utf-8")
os.chmod(settings_path, 0o600)
PY
  run_logged python3 "${python_script}" "${INSTALL_ROOT}" "${db_host}" "${db_port}" "${db_name}" "${db_user}" "${db_pass}" "${pdf_dir}" "${label_dir}"
  rm -f "${python_script}"
}

write_shopify_location_settings() {
  local location_mode="$1"
  local active_location_id="$2"
  local python_script
  python_script="$(mktemp "${TMPDIR:-/tmp}/lager-mc-shopify-settings-XXXXXX.py")"
  cat > "${python_script}" <<'PY'
import json
import os
import sys
from pathlib import Path

target = Path(sys.argv[1])
settings_path = target / "settings.local.json"
settings = {}
if settings_path.exists():
    settings = json.loads(settings_path.read_text(encoding="utf-8"))

mode = (sys.argv[2] or "single").strip().lower()
if mode not in {"single", "multi"}:
    mode = "single"

settings["shopify_location_mode"] = mode
settings["shopify_active_location_id"] = (sys.argv[3] or "").strip()
settings_path.write_text(json.dumps(settings, indent=2, sort_keys=True) + "\n", encoding="utf-8")
os.chmod(settings_path, 0o600)
PY
  run_logged python3 "${python_script}" "${INSTALL_ROOT}" "${location_mode}" "${active_location_id}"
  rm -f "${python_script}"
}

upsert_env_value() {
  local env_path="$1"
  local key="$2"
  local value="$3"
  mkdir -p "$(dirname "${env_path}")"
  touch "${env_path}"
  chmod 600 "${env_path}"
  if grep -q "^${key}=" "${env_path}" 2>/dev/null; then
    local tmp_path
    tmp_path="$(mktemp "${TMPDIR:-/tmp}/lager-mc-env-XXXXXX")"
    awk -v wanted="${key}" -v replacement="${value}" '
      BEGIN { replaced = 0 }
      {
        if ($0 ~ ("^" wanted "=")) {
          print wanted "=" replacement
          replaced = 1
        } else {
          print $0
        }
      }
      END {
        if (!replaced) {
          print wanted "=" replacement
        }
      }
    ' "${env_path}" > "${tmp_path}"
    mv "${tmp_path}" "${env_path}"
  else
    printf '%s=%s\n' "${key}" "${value}" >> "${env_path}"
  fi
}

prompt_db_values() {
  DB_HOST_VALUE="$(ui_input "PostgreSQL" "DB-Host" "${DB_HOST_DEFAULT}")"
  DB_PORT_VALUE="$(ui_input "PostgreSQL" "DB-Port" "${DB_PORT_DEFAULT}")"
  DB_NAME_VALUE="$(ui_input "PostgreSQL" "Datenbankname" "${DB_NAME_DEFAULT}")"
  DB_USER_VALUE="$(ui_input "PostgreSQL" "Datenbankbenutzer" "${DB_USER_DEFAULT}")"
  DB_PASS_VALUE="$(ui_password "PostgreSQL" "DB-Passwort")"

  [[ "${DB_PORT_VALUE}" =~ ^[0-9]+$ ]] || die "DB-Port muss numerisch sein."

  if [[ "${DB_HOST_VALUE}" == "localhost" || "${DB_HOST_VALUE}" == "127.0.0.1" ]]; then
    ui_message "PostgreSQL" "Lokale PostgreSQL-Installation gewaehlt. Der Installer prueft jetzt die Verbindung auf ${DB_HOST_VALUE}:${DB_PORT_VALUE}."
  fi
}

test_db_connection() {
  local venv_python="$1"
  local script_path
  script_path="$(mktemp "${TMPDIR:-/tmp}/lager-mc-dbtest-XXXXXX.py")"
  cat > "${script_path}" <<'PY'
import psycopg2
import sys

host, port, dbname, user, password = sys.argv[1:6]
maintenance_names = [dbname, "postgres", "template1"]
last_error = None
for candidate in maintenance_names:
    try:
        con = psycopg2.connect(host=host, port=int(port), database=candidate, user=user, password=password)
        con.close()
        raise SystemExit(0)
    except psycopg2.Error as exc:
        last_error = exc

raise SystemExit(str(last_error))
PY
  if ! run_logged "${venv_python}" "${script_path}" "${DB_HOST_VALUE}" "${DB_PORT_VALUE}" "${DB_NAME_VALUE}" "${DB_USER_VALUE}" "${DB_PASS_VALUE}"; then
    rm -f "${script_path}"
    return 1
  fi
  rm -f "${script_path}"
  return 0
}

prompt_optional_directories() {
  local default_pdf="${HOME}/Dokumente/Lager-MC/lieferscheine"
  local default_labels="${HOME}/Dokumente/Lager-MC/versandlabels"
  if ui_confirm "Grundordner" "Sollen wichtige Grundordner jetzt schon hinterlegt werden?"; then
    PDF_DIR_VALUE="$(ui_input "Grundordner" "Ordner fuer Lieferschein-PDFs" "${default_pdf}")"
    LABEL_DIR_VALUE="$(ui_input "Grundordner" "Ordner fuer Versandlabels" "${default_labels}")"
  else
    PDF_DIR_VALUE=""
    LABEL_DIR_VALUE=""
  fi
}

prepare_runtime_environment() {
  VENV_DIR="${INSTALL_ROOT}/.venv"
  LAUNCHER_DIR="${HOME}/.local/bin"
  LAUNCHER_PATH="${LAUNCHER_DIR}/lager-mc"
  DESKTOP_DIR="${HOME}/.local/share/applications"
  ICON_DIR="${HOME}/.local/share/icons/hicolor/scalable/apps"
  DESKTOP_FILE="${DESKTOP_DIR}/lager-mc.desktop"
  ICON_TARGET="${ICON_DIR}/lager-mc.svg"

  ui_progress 38 "Python-Umgebung wird angelegt..."
  run_logged python3 -m venv "${VENV_DIR}"
  ui_progress 46 "pip und wheel werden aktualisiert..."
  run_logged "${VENV_DIR}/bin/python" -m ensurepip --upgrade
  run_logged "${VENV_DIR}/bin/python" -m pip install --upgrade pip wheel
  ui_progress 56 "Python-Abhaengigkeiten werden installiert..."
  run_logged "${VENV_DIR}/bin/python" -m pip install -r "${INSTALL_ROOT}/requirements.txt"

  mkdir -p "${LAUNCHER_DIR}" "${DESKTOP_DIR}" "${ICON_DIR}"

  cat > "${LAUNCHER_PATH}" <<EOF
#!/usr/bin/env bash
exec "${VENV_DIR}/bin/python" "${INSTALL_ROOT}/lager_mc.py" "\$@"
EOF
  chmod +x "${LAUNCHER_PATH}"

  if [[ -f "${INSTALL_ROOT}/assets/lager-mc.svg" ]]; then
    install -m 0644 "${INSTALL_ROOT}/assets/lager-mc.svg" "${ICON_TARGET}"
  fi

  cat > "${DESKTOP_FILE}" <<EOF
[Desktop Entry]
Version=1.0
Type=Application
Name=Lager MC
Comment=Terminaloberflaeche fuer Lager-MC
Exec=${LAUNCHER_PATH}
Icon=${ICON_TARGET}
Terminal=true
Categories=Office;Utility;
Keywords=Lager;Inventar;Shopify;
StartupNotify=true
EOF
}

configure_shopify_sync_env() {
  local sync_env="${INSTALL_ROOT}/shopify-sync/.env"
  upsert_env_value "${sync_env}" "DB_HOST" "${DB_HOST_VALUE}"
  upsert_env_value "${sync_env}" "DB_PORT" "${DB_PORT_VALUE}"
  upsert_env_value "${sync_env}" "DB_NAME" "${DB_NAME_VALUE}"
  upsert_env_value "${sync_env}" "DB_USER" "${DB_USER_VALUE}"
  upsert_env_value "${sync_env}" "DB_PASS" "${DB_PASS_VALUE}"
  upsert_env_value "${sync_env}" "SHOPIFY_APP_SCOPES" "${SHOPIFY_APP_SCOPES}"
  upsert_env_value "${sync_env}" "SHOPIFY_APP_REDIRECT_URI" "${SHOPIFY_APP_REDIRECT_URI}"
}

load_shopify_locations_json() {
  local output_path
  output_path="$(mktemp "${TMPDIR:-/tmp}/lager-mc-locations-XXXXXX.json")"
  if ! run_logged "${VENV_DIR}/bin/python" "${INSTALL_ROOT}/shopify-sync/shopify_sync.py" list-locations --json > /dev/null; then
    rm -f "${output_path}"
    return 1
  fi
  if ! "${VENV_DIR}/bin/python" "${INSTALL_ROOT}/shopify-sync/shopify_sync.py" list-locations --json > "${output_path}" 2>>"${LOG_FILE}"; then
    rm -f "${output_path}"
    return 1
  fi
  printf '%s\n' "${output_path}"
}

select_shopify_location_mode() {
  if ! SHOPIFY_LOCATION_MODE_VALUE="$(
    ui_menu \
      "Shopify-Location" \
      "Bitte den Betriebsmodus fuer diese Lager-MC-Installation waehlen." \
      "single" "Nur eine Shopify-Location an diesem Arbeitsplatz verwenden" \
      "multi" "Zwischen mehreren Shopify-Locations in Lager-MC umschalten"
  )"; then
    die "Kein Shopify-Location-Modus gewaehlt."
  fi
  [[ -n "${SHOPIFY_LOCATION_MODE_VALUE}" ]] || die "Kein Shopify-Location-Modus gewaehlt."
}

select_shopify_location_from_json() {
  local json_path="$1"
  local default_location_id
  local selected_index=""
  local selected_location=""
  local location_count=0
  local menu_args=()
  local parser_script
  parser_script="$(mktemp "${TMPDIR:-/tmp}/lager-mc-location-menu-XXXXXX.py")"
  cat > "${parser_script}" <<'PY'
import json
import sys
from pathlib import Path

rows = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8")).get("locations") or []
index = 1
for row in rows:
    location_id = str(row.get("location_id") or "").strip()
    if not location_id:
        continue
    name = str(row.get("location_name") or location_id).strip() or location_id
    flags = ["aktiv" if row.get("is_active") else "inaktiv"]
    if row.get("fulfills_online_orders"):
        flags.append("online")
    print(f"{index}\t{location_id}\t{name} ({', '.join(flags)})")
    index += 1
PY
  while IFS=$'\t' read -r idx location_id label; do
    [[ -n "${idx}" && -n "${location_id}" ]] || continue
    if [[ -z "${default_location_id:-}" ]]; then
      default_location_id="${location_id}"
    fi
    menu_args+=("${idx}" "${label}")
    printf -v "SHOPIFY_LOCATION_ID_${idx}" '%s' "${location_id}"
    location_count=$((location_count + 1))
  done < <(python3 "${parser_script}" "${json_path}")
  rm -f "${parser_script}"
  if (( location_count == 0 )); then
    die "Es wurden keine Shopify-Locations gefunden."
  fi
  if ! selected_index="$(
    ui_menu \
      "Shopify-Location" \
      "Bitte die Shopify-Location fuer diese Lager-MC-Installation waehlen." \
      "${menu_args[@]}"
  )"; then
    selected_index="1"
  fi
  [[ -n "${selected_index}" ]] || selected_index="1"
  local id_var="SHOPIFY_LOCATION_ID_${selected_index}"
  selected_location="${!id_var:-}"
  if [[ -z "${selected_location}" ]]; then
    selected_location="${default_location_id}"
  fi
  [[ -n "${selected_location}" ]] || die "Keine Shopify-Location gewaehlt."
  SHOPIFY_ACTIVE_LOCATION_ID_VALUE="${selected_location}"
}

prompt_shopify_location_settings() {
  local locations_json_path
  select_shopify_location_mode
  locations_json_path="$(load_shopify_locations_json || true)"
  if [[ -z "${locations_json_path}" || ! -f "${locations_json_path}" ]]; then
    ui_message "Shopify-Location" "Die Shopify-Locations konnten nicht automatisch geladen werden. Der Modus wird gespeichert, die konkrete Arbeitslocation kann spaeter in Lager-MC unter Shift+F11 gesetzt werden."
    write_shopify_location_settings "${SHOPIFY_LOCATION_MODE_VALUE}" ""
    return
  fi

  if [[ "${SHOPIFY_LOCATION_MODE_VALUE}" == "single" ]]; then
    select_shopify_location_from_json "${locations_json_path}"
  else
    select_shopify_location_from_json "${locations_json_path}"
  fi
  rm -f "${locations_json_path}"
  write_shopify_location_settings "${SHOPIFY_LOCATION_MODE_VALUE}" "${SHOPIFY_ACTIVE_LOCATION_ID_VALUE}"
}

run_shopify_connect() {
  local shop_domain
  local client_id
  local client_secret
  local scopes
  local redirect_uri
  local output_path
  local connect_pid
  local attempts
  local connect_url=""
  local connect_mode_args=()
  shop_domain="$(ui_input "Shop-Verbindung" "Shop-Domain eingeben (z. B. beispiel.myshopify.com)." "")"
  [[ -n "${shop_domain}" ]] || die "Keine Shop-Domain angegeben."
  client_id="$(ui_input "Shop-Verbindung" "Client ID der Shopify-App." "")"
  [[ -n "${client_id}" ]] || die "Keine Client ID angegeben."
  client_secret="$(ui_password "Shop-Verbindung" "Client Secret der Shopify-App.")"
  [[ -n "${client_secret}" ]] || die "Kein Client Secret angegeben."
  scopes="$(ui_input "Shop-Verbindung" "Shopify-Scopes (kommagetrennt)." "${SHOPIFY_APP_SCOPES}")"
  [[ -n "${scopes}" ]] || die "Keine Shopify-Scopes angegeben."
  redirect_uri="$(ui_input "Shop-Verbindung" "Redirect-URI der Shopify-App (z. B. https://install.lagerverwaltung.org/manual-oauth-callback.html)." "${SHOPIFY_APP_REDIRECT_URI}")"
  [[ -n "${redirect_uri}" ]] || die "Keine Redirect-URI angegeben."
  SHOPIFY_APP_SCOPES="${scopes}"
  SHOPIFY_APP_REDIRECT_URI="${redirect_uri}"
  local sync_env="${INSTALL_ROOT}/shopify-sync/.env"
  upsert_env_value "${sync_env}" "SHOPIFY_APP_CLIENT_ID" "${client_id}"
  upsert_env_value "${sync_env}" "SHOPIFY_APP_CLIENT_SECRET" "${client_secret}"
  upsert_env_value "${sync_env}" "SHOPIFY_APP_SCOPES" "${scopes}"
  upsert_env_value "${sync_env}" "SHOPIFY_APP_REDIRECT_URI" "${redirect_uri}"
  connect_mode_args+=(--no-browser)
  ui_message "Shop-Verbindung" "Die lokale Shopify-Verbindung wird jetzt vorbereitet. Der Autorisierungslink wird angezeigt und lokal auf Port 3459 empfangen."

  output_path="$(mktemp "${TMPDIR:-/tmp}/lager-mc-connect-XXXXXX.log")"
  append_log_line "+ PYTHONUNBUFFERED=1 ${VENV_DIR}/bin/python -u ${INSTALL_ROOT}/shopify-sync/shopify_sync.py manual-connect --shop ${shop_domain} --client-id [redacted] --client-secret [redacted] --scopes ${scopes} --redirect-uri ${redirect_uri} ${connect_mode_args[*]:-}"
  PYTHONUNBUFFERED=1 \
    "${VENV_DIR}/bin/python" -u "${INSTALL_ROOT}/shopify-sync/shopify_sync.py" manual-connect \
    --shop "${shop_domain}" \
    --client-id "${client_id}" \
    --client-secret "${client_secret}" \
    --scopes "${scopes}" \
    --redirect-uri "${redirect_uri}" \
    "${connect_mode_args[@]}" \
    >"${output_path}" 2>&1 &
  connect_pid=$!

  attempts=80
  while (( attempts > 0 )); do
    if grep -q '^https\?://' "${output_path}" 2>/dev/null; then
      connect_url="$(grep '^https\?://' "${output_path}" | tail -n 1)"
      break
    fi
    if ! kill -0 "${connect_pid}" 2>/dev/null; then
      break
    fi
    sleep 0.25
    attempts=$((attempts - 1))
  done

  if [[ -z "${connect_url}" ]]; then
    if kill -0 "${connect_pid}" 2>/dev/null; then
      kill "${connect_pid}" 2>/dev/null || true
      wait "${connect_pid}" 2>/dev/null || true
    fi
    wait "${connect_pid}" || true
    cat "${output_path}" >> "${LOG_FILE}"
    rm -f "${output_path}"
    die "Shopify-Verbindung konnte nicht initialisiert werden. Der Install-Link wurde nicht erzeugt."
  fi

  ui_message "Shop-Verbindung" "Bitte diesen Autorisierungslink jetzt in deinem normalen Browser oeffnen und die App autorisieren:\n\n${connect_url}\n\nNach erfolgreicher Autorisierung schreibt der lokale shopify-sync die Verbindungsdaten in die lokale Konfiguration."

  if ! wait "${connect_pid}"; then
    cat "${output_path}" >> "${LOG_FILE}"
    rm -f "${output_path}"
    die "Shopify-Verbindung fehlgeschlagen. Details stehen im Log: ${LOG_FILE}"
  fi

  cat "${output_path}" >> "${LOG_FILE}"
  rm -f "${output_path}"
}

run_database_migrations() {
  if ! run_logged "${VENV_DIR}/bin/python" "${INSTALL_ROOT}/scripts/run_db_migrations.py"; then
    die "DB-Migration fehlgeschlagen. Details stehen im Log: ${LOG_FILE}"
  fi
}

summarize_completion() {
  local message
  message="Installation abgeschlossen.\n\nZielverzeichnis: ${INSTALL_ROOT}\nStarter: ${LAUNCHER_PATH}\nLogdatei: ${LOG_FILE}\nDoku: ${DOC_URL}\nDownloadseite: ${DOWNLOAD_URL}"
  if [[ "${MODE}" == "complete" ]]; then
    message="${message}\n\nLokale Shopify-Anbindung wurde eingerichtet.\nshopify-sync ist lokal installiert."
  elif [[ "${MODE}" == "workstation" ]]; then
    message="${message}\n\nDiese Installation nutzt einen bereits vorhandenen shopify-sync ueber dieselbe Datenbank. Eine lokale Shopify-Autorisierung wurde uebersprungen."
  else
    message="${message}\n\nShopify-Anbindung wurde in diesem Lauf nicht eingerichtet."
  fi
  ui_message "Fertig" "${message}"
  printf '\nStarten mit:\n  %s\n\n' "${LAUNCHER_PATH}"
  printf 'Wenn shopify-sync manuell gestartet werden soll:\n  %s %s/shopify-sync/shopify_sync.py\n\n' "${VENV_DIR}/bin/python" "${INSTALL_ROOT}"
}

main() {
  ensure_linux
  ensure_guided_ui_backend
  detect_ui_backend
  setup_dialog_theme
  ui_progress 2 "Installer wird vorbereitet..."
  check_required_tools

  if [[ "${GUIDED_UI_INSTALL_ATTEMPTED}" -eq 1 ]]; then
    ui_message "Oberflaeche" "Die gefuehrte Terminaloberflaeche wurde nachinstalliert und wird fuer den weiteren Ablauf verwendet."
  fi

  ui_message "Willkommen" "Der Lager-MC Installer laedt Programmdaten direkt von GitHub und richtet Lager-MC sowie optional shopify-sync auf diesem System ein.\n\nLogdatei: ${LOG_FILE}"
  ui_progress 8 "Rechtstexte und Zustimmung werden geprueft..."
  consent_phase

  if [[ "${MODE_SET_BY_ARG}" -eq 0 ]]; then
    select_mode_interactively
  elif [[ "${MODE}" != "complete" && "${MODE}" != "app-only" && "${MODE}" != "workstation" && "${MODE}" != "update" ]]; then
    die "Ungueltiger Installationsmodus: ${MODE}"
  fi

  choose_target_dir

  if [[ "${MODE}" == "update" && ! -d "${INSTALL_ROOT}" ]]; then
    die "Update/Reparatur wurde gewaehlt, aber das Zielverzeichnis existiert nicht: ${INSTALL_ROOT}"
  fi

  log_line "Installationsmodus: ${MODE}"
  log_line "Zielverzeichnis: ${INSTALL_ROOT}"
  log_line "Archiv-URL: ${ARCHIVE_URL}"

  ui_progress 18 "Release wird von GitHub geladen..."
  download_release
  ui_progress 32 "Installationsdateien werden vorbereitet..."
  prepare_runtime_environment

  ui_progress 62 "Datenbankdaten werden abgefragt..."
  prompt_db_values
  write_json_settings "${DB_HOST_VALUE}" "${DB_PORT_VALUE}" "${DB_NAME_VALUE}" "${DB_USER_VALUE}" "${DB_PASS_VALUE}" "" ""
  configure_shopify_sync_env

  ui_progress 72 "PostgreSQL-Verbindung wird geprueft..."
  if ! test_db_connection "${VENV_DIR}/bin/python"; then
    die "PostgreSQL-Verbindung fehlgeschlagen. Bitte Zugangsdaten pruefen und Installer erneut starten."
  fi

  ui_progress 78 "Grundordner werden abgefragt..."
  prompt_optional_directories
  write_json_settings "${DB_HOST_VALUE}" "${DB_PORT_VALUE}" "${DB_NAME_VALUE}" "${DB_USER_VALUE}" "${DB_PASS_VALUE}" "${PDF_DIR_VALUE}" "${LABEL_DIR_VALUE}"

  if [[ "${MODE}" == "complete" ]]; then
    ui_progress 84 "Shopify-Verbindung wird gestartet..."
    run_shopify_connect
    ui_progress 88 "Shopify-Location wird konfiguriert..."
    prompt_shopify_location_settings
  elif [[ "${MODE}" == "workstation" ]]; then
    ui_message "Arbeitsplatzmodus" "Die lokale Shopify-Autorisierung wird uebersprungen. Es wird davon ausgegangen, dass ein vorhandener shopify-sync bereits dieselbe PostgreSQL-Datenbank bedient."
  fi

  ui_progress 92 "Datenbankschema wird eingerichtet..."
  run_database_migrations
  ui_progress 100 "Installation abgeschlossen."
  summarize_completion
}

main "$@"
