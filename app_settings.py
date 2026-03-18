#!/usr/bin/env python3
import json
from pathlib import Path


SETTINGS_PATH = Path(__file__).resolve().parent / "settings.json"
LOCAL_SETTINGS_PATH = Path(__file__).resolve().parent / "settings.local.json"

DEFAULT_SETTINGS = {
    "db_host": "localhost",
    "db_name": "lagerdb",
    "db_user": "lager",
    "db_pass": "",
    "language": "de",
    "color_theme": "blue",
    "color_theme_file": "",
    "printer_uri": "tcp://label-printer:9100",
    "printer_model": "QL-810W",
    "label_size": "62x29",
    "label_font_regular": "",
    "label_font_condensed": "",
    "location_regex_regal": "^[A-Z]$",
    "location_regex_fach": "^([1-9][0-9]?)$",
    "location_regex_platz": "^([1-9][0-9]?)$",
    "picklist_printer": "",
    "delivery_note_printer": "",
    "pdf_output_dir": "",
    "delivery_note_template_path": "",
    "delivery_note_logo_source": "",
    "delivery_note_sender_name": "Firmenname",
    "delivery_note_sender_street": "Strasse 1",
    "delivery_note_sender_city": "12345 Musterstadt",
    "delivery_note_sender_email": "info@example.com",
}


def _load_json(path):
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _normalized_settings(raw_settings):
    settings = DEFAULT_SETTINGS.copy()
    settings.update(raw_settings or {})
    return settings


def load_settings():
    if not SETTINGS_PATH.exists():
        with SETTINGS_PATH.open("w", encoding="utf-8") as handle:
            json.dump(DEFAULT_SETTINGS, handle, indent=2, sort_keys=True)
            handle.write("\n")

    project_settings = _normalized_settings(_load_json(SETTINGS_PATH))
    local_settings = _load_json(LOCAL_SETTINGS_PATH)
    settings = _normalized_settings(project_settings)
    settings.update(local_settings)

    if not LOCAL_SETTINGS_PATH.exists():
        migrated_local_settings = {
            key: value
            for key, value in project_settings.items()
            if DEFAULT_SETTINGS.get(key) != value
        }
        if migrated_local_settings:
            with LOCAL_SETTINGS_PATH.open("w", encoding="utf-8") as handle:
                json.dump(migrated_local_settings, handle, indent=2, sort_keys=True)
                handle.write("\n")
            settings = _normalized_settings(project_settings)

    return settings


def save_settings(settings):
    normalized = _normalized_settings(settings)
    project_settings = _normalized_settings(_load_json(SETTINGS_PATH))
    local_settings = {
        key: value
        for key, value in normalized.items()
        if project_settings.get(key) != value
    }

    if local_settings:
        with LOCAL_SETTINGS_PATH.open("w", encoding="utf-8") as handle:
            json.dump(local_settings, handle, indent=2, sort_keys=True)
            handle.write("\n")
    elif LOCAL_SETTINGS_PATH.exists():
        LOCAL_SETTINGS_PATH.unlink()

    return normalized
