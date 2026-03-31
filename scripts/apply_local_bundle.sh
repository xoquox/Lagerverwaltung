#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <bundle.zip>" >&2
  exit 1
fi

BUNDLE_PATH="$1"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d -t lager-bundle-XXXXXX)"
trap 'rm -rf "$TMP_DIR"' EXIT

python3 - "$BUNDLE_PATH" "$TMP_DIR" <<'PY'
import os
import sys
import zipfile
bundle = os.path.abspath(sys.argv[1])
out_dir = os.path.abspath(sys.argv[2])
with zipfile.ZipFile(bundle, 'r') as zf:
    zf.extractall(out_dir)
PY

python3 - "$ROOT_DIR" "$TMP_DIR" <<'PY'
import json
import shutil
import sys
from pathlib import Path

root = Path(sys.argv[1])
stage = Path(sys.argv[2])
manifest = json.loads((stage / "bundle_manifest.json").read_text(encoding="utf-8"))
bundle_settings = json.loads((stage / "settings.bundle.json").read_text(encoding="utf-8"))
protected_keys = set(manifest.get("non_overwritten_local_keys") or [])
local_settings_path = root / "settings.local.json"
local_settings = {}
if local_settings_path.exists():
    local_settings = json.loads(local_settings_path.read_text(encoding="utf-8"))

for src in (stage / "files").rglob('*'):
    if not src.is_file():
        continue
    rel = src.relative_to(stage / "files")
    destination = root / rel
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, destination)

for mapping in manifest.get("setting_file_mappings", []):
    key = mapping["setting"]
    target = mapping["target"]
    if bundle_settings.get(key, "").startswith("__BUNDLE__/"):
        bundle_settings[key] = str(root / target)

for key, value in bundle_settings.items():
    if key in protected_keys:
        continue
    local_settings[key] = value

local_settings_path.write_text(json.dumps(local_settings, indent=2, sort_keys=True) + "\n", encoding="utf-8")
print(str(local_settings_path))
PY
