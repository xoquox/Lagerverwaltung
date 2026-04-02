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
    "delivery_note_format": "A4",
    "pdf_output_dir": "",
    "delivery_note_template_path": "",
    "delivery_note_logo_source": "",
    "delivery_note_sender_name": "Firmenname",
    "delivery_note_sender_street": "Strasse 1",
    "delivery_note_sender_city": "12345 Musterstadt",
    "delivery_note_sender_email": "info@example.com",
    "shipping_label_output_dir": "",
    "shipping_active_carriers": ["gls", "post", "free"],
    "shipping_label_format": "A6",
    "shipping_label_printer": "",
    "shipping_label_format_gls": "A6",
    "shipping_label_format_free": "A6",
    "shipping_label_format_post": "100x62",
    "shipping_label_printer_gls": "",
    "shipping_label_printer_free": "",
    "shipping_label_printer_post": "",
    "shipping_services": ["service_flexdelivery"],
    "shipping_packaging_weight_grams": 400,
    "shopify_tracking_mode_gls": "company",
    "shopify_tracking_mode_post": "company_and_url",
    "shopify_tracking_url_gls": "",
    "shopify_tracking_url_post": "https://www.deutschepost.de/sendung/simpleQuery.html?form.sendungsnummer={tracking_number}",
    "gls_api_url": "",
    "gls_user": "",
    "gls_password": "",
    "gls_contact_id": "",
    "post_api_url": "",
    "post_api_key": "",
    "post_api_secret": "",
    "post_user": "",
    "post_password": "",
    "post_partner_id": "",
    "free_label_template_path": "",
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
