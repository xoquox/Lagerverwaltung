"""Projektweite Versionsdefinition aus externer Versionsdatei."""

import json
from pathlib import Path


VERSION_FILE = Path(__file__).resolve().parent / "version.json"
_DEFAULT_VERSION = {
    "major": 1,
    "minor": 21,
    "patch": 0,
    "stage": "dev",
    "build": 2,
}


def load_version_data():
    try:
        with VERSION_FILE.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except Exception:
        raw = {}
    data = dict(_DEFAULT_VERSION)
    if isinstance(raw, dict):
        data.update(raw)
    return data


def build_version(data=None) -> str:
    version_data = data or load_version_data()
    major = int(version_data.get("major", 0) or 0)
    minor = int(version_data.get("minor", 0) or 0)
    patch = int(version_data.get("patch", 0) or 0)
    stage = str(version_data.get("stage", "") or "").strip()
    build = int(version_data.get("build", 0) or 0)

    base = f"{major}.{minor}.{patch}"
    if stage:
        if build >= 0:
            return f"{base}-{stage}.{build}"
        return f"{base}-{stage}"
    return base


VERSION_DATA = load_version_data()
APP_VERSION = build_version(VERSION_DATA)
