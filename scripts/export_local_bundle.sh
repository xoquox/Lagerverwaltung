#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAMP="$(date +%Y%m%d_%H%M%S)"
EXPORT_DIR="$ROOT_DIR/exports"
STAGE_DIR="$EXPORT_DIR/local_bundle_$STAMP"
ZIP_PATH="$EXPORT_DIR/lager_mc_local_bundle_$STAMP.zip"

mkdir -p "$EXPORT_DIR"
rm -rf "$STAGE_DIR"
mkdir -p "$STAGE_DIR/gls" "$STAGE_DIR/shopify-sync" "$STAGE_DIR/fonts" "$STAGE_DIR/assets"

copy_if_exists() {
  local src="$1"
  local dest="$2"
  if [[ -f "$src" ]]; then
    cp -p "$src" "$dest"
  fi
}

copy_gls_credentials() {
  local found=0
  shopt -s nullglob
  for pdf in "$ROOT_DIR"/gls/*.pdf; do
    if [[ "$pdf" == *"/gls/labels/"* ]]; then
      continue
    fi
    case "$(basename "$pdf")" in
      testlabel*|gls_*)
        continue
        ;;
    esac
    cp -p "$pdf" "$STAGE_DIR/gls/"
    found=1
  done
  shopt -u nullglob
  return $found
}

copy_if_exists "$ROOT_DIR/settings.json" "$STAGE_DIR/settings.json"
copy_if_exists "$ROOT_DIR/settings.local.json" "$STAGE_DIR/settings.local.json"
copy_if_exists "$ROOT_DIR/shopify-sync/.env" "$STAGE_DIR/shopify-sync/.env"
copy_if_exists "$ROOT_DIR/fonts/bahnschrift.ttf" "$STAGE_DIR/fonts/bahnschrift.ttf"
copy_if_exists "$ROOT_DIR/fonts/bahnschrift-condensed.ttf" "$STAGE_DIR/fonts/bahnschrift-condensed.ttf"
copy_if_exists "$ROOT_DIR/assets/lager-mc.svg" "$STAGE_DIR/assets/lager-mc.svg"
copy_gls_credentials || true

cat > "$STAGE_DIR/README.txt" <<'EOF'
Dieses Archiv enthaelt nur lokale/private Lagerverwaltungs-Dateien.

Enthalten:
- settings.json
- settings.local.json
- shopify-sync/.env
- fonts/bahnschrift.ttf
- fonts/bahnschrift-condensed.ttf
- assets/lager-mc.svg
- gls/*.pdf (ohne gls/labels/)

Nicht enthalten:
- erzeugte Labels/PDFs
- Logs
- Datenbankinhalte
- virtuelle Umgebung

Wiederherstellung:
1. Archiv im Projektverzeichnis entpacken.
2. Dateien an dieselben relativen Pfade kopieren.
3. Rechte der Secret-Dateien bei Bedarf wieder auf den lokalen Benutzer pruefen.
4. shopify-sync und Lagerverwaltung danach neu starten.
EOF

if command -v zip >/dev/null 2>&1; then
  (
    cd "$STAGE_DIR"
    zip -qr "$ZIP_PATH" .
  )
else
  python3 - "$STAGE_DIR" "$ZIP_PATH" <<'PY'
import os
import sys
import zipfile

stage_dir = os.path.abspath(sys.argv[1])
zip_path = os.path.abspath(sys.argv[2])

with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for root, _dirs, files in os.walk(stage_dir):
        for name in files:
            full_path = os.path.join(root, name)
            rel_path = os.path.relpath(full_path, stage_dir)
            zf.write(full_path, rel_path)
PY
fi

rm -rf "$STAGE_DIR"
printf '%s\n' "$ZIP_PATH"
