#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAMP="$(date +%Y%m%d_%H%M%S)"
EXPORT_DIR="$ROOT_DIR/exports"
STAGE_DIR="$EXPORT_DIR/local_bundle_$STAMP"
ZIP_PATH="$EXPORT_DIR/lager_mc_local_bundle_$STAMP.zip"

mkdir -p "$EXPORT_DIR"
rm -rf "$STAGE_DIR"
mkdir -p "$STAGE_DIR/files/gls" "$STAGE_DIR/files/shopify-sync" "$STAGE_DIR/files/fonts" "$STAGE_DIR/files/assets"

copy_if_exists() {
  local src="$1"
  local dest="$2"
  if [[ -f "$src" ]]; then
    mkdir -p "$(dirname "$dest")"
    cp -p "$src" "$dest"
  fi
}

copy_gls_credentials() {
  shopt -s nullglob
  for pdf in "$ROOT_DIR"/gls/*.pdf; do
    case "$(basename "$pdf")" in
      testlabel*|gls_*|post_*|test_*)
        continue
        ;;
    esac
    cp -p "$pdf" "$STAGE_DIR/files/gls/"
  done
  shopt -u nullglob
}

copy_if_exists "$ROOT_DIR/shopify-sync/.env" "$STAGE_DIR/files/shopify-sync/.env"
copy_if_exists "$ROOT_DIR/fonts/bahnschrift.ttf" "$STAGE_DIR/files/fonts/bahnschrift.ttf"
copy_if_exists "$ROOT_DIR/fonts/bahnschrift-condensed.ttf" "$STAGE_DIR/files/fonts/bahnschrift-condensed.ttf"
copy_if_exists "$ROOT_DIR/assets/lager-mc.svg" "$STAGE_DIR/files/assets/lager-mc.svg"
copy_gls_credentials || true

python3 - "$ROOT_DIR" "$STAGE_DIR" <<'PY'
import json
import os
import shutil
import sys
from pathlib import Path

root = Path(sys.argv[1])
stage = Path(sys.argv[2])
settings_path = root / "settings.json"
local_settings_path = root / "settings.local.json"
settings = {}
if settings_path.exists():
    settings.update(json.loads(settings_path.read_text(encoding="utf-8")))
if local_settings_path.exists():
    settings.update(json.loads(local_settings_path.read_text(encoding="utf-8")))

portable_keys = {
    "db_host", "db_name", "db_user", "db_pass",
    "language", "color_theme",
    "gls_api_url", "gls_user", "gls_password", "gls_contact_id",
    "post_api_url", "post_api_key", "post_api_secret", "post_user", "post_password", "post_partner_id",
    "dhl_private_api_url", "dhl_private_api_test_url", "dhl_private_api_key", "dhl_private_api_secret", "dhl_private_use_test_api",
    "shipping_services", "shipping_packaging_weight_grams",
    "delivery_note_sender_name", "delivery_note_sender_street", "delivery_note_sender_city", "delivery_note_sender_email",
}

bundle_settings = {key: settings[key] for key in portable_keys if key in settings}

file_mappings = []

def copy_optional_setting_file(setting_key, relative_target):
    value = (settings.get(setting_key) or "").strip()
    if not value:
        return
    if value.startswith("http://") or value.startswith("https://"):
        bundle_settings[setting_key] = value
        return
    source = Path(os.path.expanduser(value))
    if not source.is_file():
        return
    target = stage / "files" / relative_target
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    bundle_settings[setting_key] = f"__BUNDLE__/{relative_target.replace(os.sep, '/')}"
    file_mappings.append({"setting": setting_key, "target": relative_target.replace(os.sep, "/")})

copy_optional_setting_file("delivery_note_template_path", "templates/delivery_note_template" + Path((settings.get("delivery_note_template_path") or "template")).suffix)
copy_optional_setting_file("delivery_note_logo_source", "templates/delivery_note_logo" + Path((settings.get("delivery_note_logo_source") or "logo")).suffix)
copy_optional_setting_file("label_font_regular", "fonts/label_font_regular" + Path((settings.get("label_font_regular") or "font")).suffix)
copy_optional_setting_file("label_font_condensed", "fonts/label_font_condensed" + Path((settings.get("label_font_condensed") or "font")).suffix)
copy_optional_setting_file("color_theme_file", "themes/custom_theme" + Path((settings.get("color_theme_file") or "theme")).suffix)

manifest = {
    "bundle_version": 2,
    "portable_settings": sorted(bundle_settings.keys()),
    "non_overwritten_local_keys": [
        "picklist_printer", "delivery_note_printer", "delivery_note_format",
        "shipping_label_printer", "shipping_label_printer_gls", "shipping_label_printer_dhl", "shipping_label_printer_dhl_private", "shipping_label_printer_post",
        "shipping_label_output_dir", "shipping_label_format", "shipping_label_format_gls", "shipping_label_format_dhl", "shipping_label_format_dhl_private", "shipping_label_format_post",
        "pdf_output_dir", "printer_uri", "printer_model", "label_size",
    ],
    "setting_file_mappings": file_mappings,
}

(stage / "settings.bundle.json").write_text(json.dumps(bundle_settings, indent=2, sort_keys=True) + "\n", encoding="utf-8")
(stage / "bundle_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

cat > "$STAGE_DIR/README.txt" <<'EOF2'
Dieses Archiv enthaelt lokale/private Lagerverwaltungs-Dateien fuer einen neuen Arbeitsplatz.

Installationsablauf:
1. Git-Repo normal klonen.
2. Dieses Archiv in das Projektverzeichnis kopieren.
3. ./scripts/apply_local_bundle.sh <archiv.zip> ausfuehren.

Nicht ueberschrieben werden lokale Arbeitsplatz-Einstellungen wie:
- Drucker
- Druckformate
- PDF-/Label-Zielordner
- lokale Labeldrucker URI/Modell-Angaben
EOF2

(
  cd "$STAGE_DIR"
  if command -v zip >/dev/null 2>&1; then
    zip -qr "$ZIP_PATH" .
  else
    python3 - "$STAGE_DIR" "$ZIP_PATH" <<'PY'
import os
import sys
import zipfile
stage_dir = os.path.abspath(sys.argv[1])
zip_path = os.path.abspath(sys.argv[2])
with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
    for root, _dirs, files in os.walk(stage_dir):
        for name in files:
            full_path = os.path.join(root, name)
            rel_path = os.path.relpath(full_path, stage_dir)
            zf.write(full_path, rel_path)
PY
  fi
)

rm -rf "$STAGE_DIR"
printf '%s\n' "$ZIP_PATH"
