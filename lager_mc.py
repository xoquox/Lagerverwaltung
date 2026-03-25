#!/usr/bin/env python3
import curses
import csv
import datetime
import base64
import binascii
import html
import json
import os
import psycopg2
import psycopg2.extras
import locale
import re
import ssl
import subprocess
import string
import tempfile
import textwrap
import time
from pathlib import Path
from urllib.parse import urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from app_logging import MAIN_LOG_PATH, PRINT_LOG_PATH, get_logger
from app_settings import DEFAULT_SETTINGS, load_settings, save_settings
from app_version import APP_VERSION
from delivery_note import build_delivery_note_pdf, build_delivery_note_rows
from dhl.private_client import DHLPrivateClient
from post.internetmarke_client import InternetmarkeClient

locale.setlocale(locale.LC_ALL, "")

SETTINGS = load_settings()
LOGGER = get_logger("lager_mc")
PRINT_LOGGER = get_logger("print")
BASE_DIR = Path(__file__).resolve().parent
LABEL_PRINT_SCRIPT = str(BASE_DIR / "label_print.py")
GLS_DIR = BASE_DIR / "gls"
GLS_LABEL_DIR = GLS_DIR / "labels"
POST_DIR = BASE_DIR / "post"
POST_LABEL_DIR = POST_DIR / "labels"

SHIPPING_SERVICE_OPTIONS = [
    {"code": "service_flexdelivery", "label": "FlexDelivery", "locked": True},
    {"code": "service_addresseeonly", "label": "AddresseeOnly", "locked": False},
    {"code": "service_guaranteed24", "label": "Guaranteed24", "locked": False},
    {"code": "service_preadvice", "label": "PreAdvice", "locked": False},
    {"code": "service_smsservice", "label": "SMS Service", "locked": False},
]

IMPLEMENTED_SHIPPING_CARRIERS = {"gls", "post", "dhl_private", "test"}

MANUAL_LABEL_COUNTRY_OPTIONS = [
    {"value": "AD", "label": "Andorra"},
    {"value": "AT", "label": "Austria"},
    {"value": "BE", "label": "Belgium"},
    {"value": "BG", "label": "Bulgaria"},
    {"value": "CH", "label": "Switzerland"},
    {"value": "CY", "label": "Cyprus"},
    {"value": "CZ", "label": "Czechia"},
    {"value": "DE", "label": "Germany"},
    {"value": "DK", "label": "Denmark"},
    {"value": "EE", "label": "Estonia"},
    {"value": "ES", "label": "Spain"},
    {"value": "FI", "label": "Finland"},
    {"value": "FR", "label": "France"},
    {"value": "GB", "label": "United Kingdom"},
    {"value": "GR", "label": "Greece"},
    {"value": "HR", "label": "Croatia"},
    {"value": "HU", "label": "Hungary"},
    {"value": "IE", "label": "Ireland"},
    {"value": "IS", "label": "Iceland"},
    {"value": "IT", "label": "Italy"},
    {"value": "LI", "label": "Liechtenstein"},
    {"value": "LT", "label": "Lithuania"},
    {"value": "LU", "label": "Luxembourg"},
    {"value": "LV", "label": "Latvia"},
    {"value": "MC", "label": "Monaco"},
    {"value": "MT", "label": "Malta"},
    {"value": "NL", "label": "Netherlands"},
    {"value": "NO", "label": "Norway"},
    {"value": "PL", "label": "Poland"},
    {"value": "PT", "label": "Portugal"},
    {"value": "RO", "label": "Romania"},
    {"value": "SE", "label": "Sweden"},
    {"value": "SI", "label": "Slovenia"},
    {"value": "SK", "label": "Slovakia"},
    {"value": "SM", "label": "San Marino"},
    {"value": "VA", "label": "Vatican City"},
]

COUNTRY_NAME_DE = {
    "AD": "Andorra",
    "AT": "Oesterreich",
    "BE": "Belgien",
    "BG": "Bulgarien",
    "CH": "Schweiz",
    "CY": "Zypern",
    "CZ": "Tschechien",
    "DE": "Deutschland",
    "DK": "Daenemark",
    "EE": "Estland",
    "ES": "Spanien",
    "FI": "Finnland",
    "FR": "Frankreich",
    "GB": "Vereinigtes Koenigreich",
    "GR": "Griechenland",
    "HR": "Kroatien",
    "HU": "Ungarn",
    "IE": "Irland",
    "IS": "Island",
    "IT": "Italien",
    "LI": "Liechtenstein",
    "LT": "Litauen",
    "LU": "Luxemburg",
    "LV": "Lettland",
    "MC": "Monaco",
    "MT": "Malta",
    "NL": "Niederlande",
    "NO": "Norwegen",
    "PL": "Polen",
    "PT": "Portugal",
    "RO": "Rumänien",
    "SE": "Schweden",
    "SI": "Slowenien",
    "SK": "Slowakei",
    "SM": "San Marino",
    "VA": "Vatikanstadt",
}

FULFILLMENT_FILTER_SEQUENCE = ["all", "open", "unfulfilled", "partial", "fulfilled"]
PAYMENT_FILTER_SEQUENCE = ["all", "paid", "pending", "authorized", "partially_paid", "refunded", "voided"]
ORDERS_AUTO_REFRESH_SECONDS = 10.0


COLS = [
    ("SKU", 18),
    ("Name", 60),
    ("Regal", 7),
    ("Fach", 6),
    ("Platz", 7),
    ("Gesamt", 7),
    ("N. verf.", 8),
    ("Best.", 7),
    ("Verfüg.", 7),
    ("S", 2),
]

SUPPORTED_LANGUAGES = {"de", "en"}

BASE_THEMES = {
    "blue": {
        "pair_1_fg": "white",
        "pair_1_bg": "blue",
        "pair_2_fg": "black",
        "pair_2_bg": "cyan",
        "pair_3_fg": "black",
        "pair_3_bg": "white",
    },
    "green": {
        "pair_1_fg": "black",
        "pair_1_bg": "green",
        "pair_2_fg": "black",
        "pair_2_bg": "yellow",
        "pair_3_fg": "black",
        "pair_3_bg": "white",
    },
    "mono": {
        "pair_1_fg": "white",
        "pair_1_bg": "black",
        "pair_2_fg": "black",
        "pair_2_bg": "white",
        "pair_3_fg": "white",
        "pair_3_bg": "black",
    },
    "megatrends": {
        "pair_1_fg": "brightyellow",
        "pair_1_bg": "blue",
        "pair_2_fg": "blue",
        "pair_2_bg": "brightwhite",
        "pair_3_fg": "brightblack",
        "pair_3_bg": "black",
    },
    "smoth": {
        "pair_1_fg": "white",
        "pair_1_bg": "brightblue",
        "pair_2_fg": "brightblue",
        "pair_2_bg": "brightwhite",
        "pair_3_fg": "brightblue",
        "pair_3_bg": "blue",
    },
    "norton": {
        "pair_1_fg": "brightcyan",
        "pair_1_bg": "blue",
        "pair_2_fg": "brightcyan",
        "pair_2_bg": "black",
        "pair_3_fg": "brightwhite",
        "pair_3_bg": "black",
    },
    "gold-standard": {
        "pair_1_fg": "brightyellow",
        "pair_1_bg": "brown",
        "pair_2_fg": "brown",
        "pair_2_bg": "brightyellow",
        "pair_3_fg": "brown",
        "pair_3_bg": "black",
    },
    "subtile": {
        "pair_1_fg": "brightwhite",
        "pair_1_bg": "white",
        "pair_2_fg": "brightblack",
        "pair_2_bg": "white",
        "pair_3_fg": "white",
        "pair_3_bg": "brightblack",
    },
    "monokai": {
        "pair_1_fg": "brightwhite",
        "pair_1_bg": "brightblack",
        "pair_2_fg": "brightwhite",
        "pair_2_bg": "white",
        "pair_3_fg": "white",
        "pair_3_bg": "black",
    },
}

CUSTOM_COLOR_RGB = {
    "brown": (680, 340, 0),
    "darkgray": (350, 350, 350),
    "darkgrey": (350, 350, 350),
    "gray": (650, 650, 650),
    "grey": (650, 650, 650),
    "lightgray": (800, 800, 800),
    "lightgrey": (800, 800, 800),
    "brightblack": (500, 500, 500),
    "brightred": (1000, 200, 200),
    "brightgreen": (200, 1000, 200),
    "brightyellow": (1000, 1000, 300),
    "brightblue": (300, 300, 1000),
    "brightmagenta": (1000, 300, 1000),
    "brightcyan": (300, 1000, 1000),
    "brightwhite": (1000, 1000, 1000),
}
CUSTOM_COLOR_IDS = {}
THEME_KEY_SET = {
    "pair_1_fg",
    "pair_1_bg",
    "pair_2_fg",
    "pair_2_bg",
    "pair_3_fg",
    "pair_3_bg",
}

TRANSLATIONS = {
    "de": {
        "app_title": "Lagerverwaltung",
        "settings": "Einstellungen",
        "focus_items": " Fokus: Artikel ",
        "focus_locations": " Fokus: Regale ",
        "view_external": " | Ansicht: Extern ",
        "filter_prefix": " Filter: {value} ",
        "status_primary": " Tab Fokus  F1 Sortieren  F2 Lokal  F3 Ohne  F4 Info  F5 Neu  F6 Platz  F7 Menge  F8 Label  F9 Reset  F10 Ende  F11 Mehr  F12 Auftraege ",
        "status_secondary": " Shift+F1 Inventur  Shift+F5 Bearb.  Shift+F8 Multi-Label  Shift+F11 Einst.  F11 Standard  F12 Auftraege  F10 Ende ",
        "no_locations": "Keine Lagerplaetze",
        "locations_panel": "Regale",
        "items_panel": "Artikel",
        "press_key": "Taste druecken ...",
        "confirm_yes_no": "[J]a / [N]ein",
        "search": "Suche",
        "search_footer": "Enter suchen  F9 Abbrechen",
        "printer_dialog": "Drucker",
        "printer_error": "Drucker Fehler",
        "printer_none": "Keinen Drucker auswaehlen",
        "printer_empty": "(leer)",
        "printer_active": "aktiv",
        "printer_default": "default",
        "printer_reload_footer": "Enter waehlen  F5 Neu laden  F9 Zurueck",
        "settings_footer": "Enter weiter  ↑↓ wechseln  F2 Speichern  F3 Drucker  F9 Abbrechen",
        "settings_footer_select": "Enter weiter/Auswahl  ↑↓ wechseln  F2 Speichern  F3 Drucker  F9 Abbrechen",
        "pick_language": "Sprache waehlen",
        "pick_theme": "Farbthema waehlen",
        "pick_cancel": "F9 Zurueck",
        "field_db_host": "DB Host",
        "field_db_name": "DB Name",
        "field_db_user": "DB User",
        "field_db_pass": "DB Passwort",
        "field_language": "Sprache",
        "field_theme": "Farbthema",
        "field_theme_file": "Theme Datei",
        "field_printer_uri": "Drucker URI",
        "field_printer_model": "Drucker Modell",
        "field_label_size": "Labelformat",
        "field_label_font_regular": "Label Font (Reg)",
        "field_label_font_condensed": "Label Font (Cond)",
        "field_regex_regal": "Regex Regal",
        "field_regex_fach": "Regex Fach",
        "field_regex_platz": "Regex Platz",
        "field_picklist_printer": "Pickliste Drucker",
        "field_delivery_printer": "Lieferschein Drucker",
        "field_delivery_format": "Lieferschein Format",
        "field_shipping_printer": "Versandlabel Drucker",
        "field_shipping_printer_gls": "GLS Label Drucker",
        "field_shipping_printer_dhl": "DHL Label Drucker",
        "field_shipping_printer_dhl_private": "DHL Privat Label Drucker",
        "field_shipping_printer_post": "POST Label Drucker",
        "field_shipping_printer_fallback": "Label Drucker Fallback",
        "field_shipping_carrier": "Versand Dienstleister",
        "field_shipping_label_output_dir": "Versandlabel Ordner",
        "field_shipping_format": "Versand Labelformat",
        "field_shipping_format_gls": "GLS Labelformat",
        "field_shipping_format_dhl": "DHL Labelformat",
        "field_shipping_format_dhl_private": "DHL Privat Labelformat",
        "field_shipping_format_post": "POST Labelformat",
        "field_shipping_services": "Versand Services",
        "field_shipping_packaging_weight": "Verpackung Gewicht (g)",
        "field_gls_api_url": "GLS API URL",
        "field_gls_user": "GLS User",
        "field_gls_password": "GLS Passwort",
        "field_gls_contact_id": "GLS ContactID",
        "field_post_api_url": "POST API URL",
        "field_post_api_key": "POST API Key",
        "field_post_api_secret": "POST API Secret",
        "field_post_user": "POST User",
        "field_post_password": "POST Passwort",
        "field_post_partner_id": "POST Partner-ID",
        "field_dhl_private_api_url": "DHL Privat API URL",
        "field_dhl_private_api_test_url": "DHL Privat Test API URL",
        "field_dhl_private_api_key": "DHL Privat API Key",
        "field_dhl_private_api_secret": "DHL Privat API Secret",
        "field_dhl_private_use_test_api": "DHL Privat Testmodus",
        "field_pdf_dir": "PDF Ordner",
        "field_template": "LS Vorlage",
        "field_logo": "LS Logo URL/Pfad",
        "field_sender_name": "LS Name",
        "field_sender_street": "LS Strasse",
        "field_sender_city": "LS Ort",
        "field_sender_email": "LS E-Mail",
        "col_shelf": "Regal",
        "col_bin": "Fach",
        "col_slot": "Platz",
        "col_total": "Gesamt",
        "col_unavailable": "N. verf.",
        "col_committed": "Best.",
        "col_available": "Verf.",
        "error": "Fehler",
        "saved": "Gespeichert",
        "saved_settings": "Einstellungen wurden gespeichert.",
        "theme_file_missing": "Theme-Datei existiert nicht.",
        "theme_invalid": "Farbthema ungueltig. Erlaubt: {names}",
        "lang_de": "Deutsch",
        "lang_en": "Englisch",
        "theme_blue": "Blau",
        "theme_green": "Gruen",
        "theme_mono": "Monochrom",
        "theme_megatrends": "Megatrends (DOS)",
        "theme_smoth": "Smoth (DOS)",
        "theme_norton": "Norton (DOS)",
        "theme_gold_standard": "Gold Standard (DOS)",
        "theme_subtile": "Subtile (DOS)",
        "theme_monokai": "Monokai (DOS)",
    },
    "en": {
        "app_title": "Inventory Manager",
        "settings": "Settings",
        "focus_items": " Focus: Items ",
        "focus_locations": " Focus: Shelves ",
        "view_external": " | View: External ",
        "filter_prefix": " Filter: {value} ",
        "status_primary": " Tab Focus  F1 Sort  F2 Local  F3 Missing  F4 Info  F5 New  F6 Location  F7 Qty  F8 Label  F9 Reset  F10 Exit  F11 More  F12 Orders ",
        "status_secondary": " Shift+F1 Stocktake  Shift+F5 Edit  Shift+F8 Multi-Label  Shift+F11 Settings  F11 Standard  F12 Orders  F10 Exit ",
        "no_locations": "No storage locations",
        "locations_panel": "Locations",
        "items_panel": "Items",
        "press_key": "Press any key ...",
        "confirm_yes_no": "[Y]es / [N]o",
        "search": "Search",
        "search_footer": "Enter search  F9 Cancel",
        "printer_dialog": "Printers",
        "printer_error": "Printer Error",
        "printer_none": "Select no printer",
        "printer_empty": "(empty)",
        "printer_active": "active",
        "printer_default": "default",
        "printer_reload_footer": "Enter select  F5 Reload  F9 Back",
        "settings_footer": "Enter next  ↑↓ move  F2 Save  F3 Printer  F9 Cancel",
        "settings_footer_select": "Enter next/select  ↑↓ move  F2 Save  F3 Printer  F9 Cancel",
        "pick_language": "Select language",
        "pick_theme": "Select color theme",
        "pick_cancel": "F9 Back",
        "field_db_host": "DB Host",
        "field_db_name": "DB Name",
        "field_db_user": "DB User",
        "field_db_pass": "DB Password",
        "field_language": "Language",
        "field_theme": "Color Theme",
        "field_theme_file": "Theme File",
        "field_printer_uri": "Printer URI",
        "field_printer_model": "Printer Model",
        "field_label_size": "Label Format",
        "field_label_font_regular": "Label Font (Reg)",
        "field_label_font_condensed": "Label Font (Cond)",
        "field_regex_regal": "Regex Shelf",
        "field_regex_fach": "Regex Bin",
        "field_regex_platz": "Regex Slot",
        "field_picklist_printer": "Picklist Printer",
        "field_delivery_printer": "Delivery Printer",
        "field_delivery_format": "Delivery Format",
        "field_shipping_printer": "Shipping Label Printer",
        "field_shipping_printer_gls": "GLS Label Printer",
        "field_shipping_printer_dhl": "DHL Label Printer",
        "field_shipping_printer_dhl_private": "DHL Private Label Printer",
        "field_shipping_printer_post": "POST Label Printer",
        "field_shipping_printer_fallback": "Label Printer Fallback",
        "field_shipping_carrier": "Shipping Carrier",
        "field_shipping_label_output_dir": "Shipping Label Folder",
        "field_shipping_format": "Shipping Label Format",
        "field_shipping_format_gls": "GLS Label Format",
        "field_shipping_format_dhl": "DHL Label Format",
        "field_shipping_format_dhl_private": "DHL Private Label Format",
        "field_shipping_format_post": "POST Label Format",
        "field_shipping_services": "Shipping Services",
        "field_shipping_packaging_weight": "Packaging Weight (g)",
        "field_gls_api_url": "GLS API URL",
        "field_gls_user": "GLS User",
        "field_gls_password": "GLS Password",
        "field_gls_contact_id": "GLS ContactID",
        "field_post_api_url": "POST API URL",
        "field_post_api_key": "POST API Key",
        "field_post_api_secret": "POST API Secret",
        "field_post_user": "POST User",
        "field_post_password": "POST Password",
        "field_post_partner_id": "POST Partner ID",
        "field_dhl_private_api_url": "DHL Private API URL",
        "field_dhl_private_api_test_url": "DHL Private Test API URL",
        "field_dhl_private_api_key": "DHL Private API Key",
        "field_dhl_private_api_secret": "DHL Private API Secret",
        "field_dhl_private_use_test_api": "DHL Private Test Mode",
        "field_pdf_dir": "PDF Folder",
        "field_template": "Delivery Template",
        "field_logo": "Delivery Logo URL/Path",
        "field_sender_name": "Sender Name",
        "field_sender_street": "Sender Street",
        "field_sender_city": "Sender City",
        "field_sender_email": "Sender E-Mail",
        "col_shelf": "Shelf",
        "col_bin": "Bin",
        "col_slot": "Slot",
        "col_total": "Total",
        "col_unavailable": "Unav.",
        "col_committed": "Comm.",
        "col_available": "Avail.",
        "error": "Error",
        "saved": "Saved",
        "saved_settings": "Settings were saved.",
        "theme_file_missing": "Theme file does not exist.",
        "theme_invalid": "Invalid color theme. Allowed: {names}",
        "lang_de": "German",
        "lang_en": "English",
        "theme_blue": "Blue",
        "theme_green": "Green",
        "theme_mono": "Monochrome",
        "theme_megatrends": "Megatrends (DOS)",
        "theme_smoth": "Smoth (DOS)",
        "theme_norton": "Norton (DOS)",
        "theme_gold_standard": "Gold Standard (DOS)",
        "theme_subtile": "Subtile (DOS)",
        "theme_monokai": "Monokai (DOS)",
    },
}


def current_language():
    language = (SETTINGS.get("language") or DEFAULT_SETTINGS.get("language") or "de").lower().strip()
    if language not in SUPPORTED_LANGUAGES:
        return "de"
    return language


def t(key, **kwargs):
    language = current_language()
    value = TRANSLATIONS.get(language, {}).get(key)
    if value is None:
        value = TRANSLATIONS["de"].get(key, key)
    if kwargs:
        try:
            return value.format(**kwargs)
        except Exception:
            return value
    return value


def get_active_theme_name():
    themes = get_all_themes()
    theme_name = (SETTINGS.get("color_theme") or DEFAULT_SETTINGS.get("color_theme") or "blue").strip().lower()
    if theme_name not in themes:
        return "blue"
    return theme_name


def get_theme_file_candidates():
    configured = (SETTINGS.get("color_theme_file") or "").strip()
    candidates = []
    if configured:
        candidates.append(Path(os.path.expanduser(configured)))
    else:
        candidates.append(Path(__file__).resolve().parent / "themes.local.json")
        candidates.append(Path(__file__).resolve().parent / "local_only" / "themes.json")
    return candidates


def _is_valid_theme_map(value):
    if not isinstance(value, dict):
        return False
    return THEME_KEY_SET.issubset(set(value.keys()))


def load_custom_themes_from_file(path):
    candidate = Path(path)
    try:
        raw = candidate.read_text(encoding="utf-8")
        data = json.loads(raw)
    except Exception as exc:
        LOGGER.warning("Konnte Theme-Datei nicht lesen: %s (%s)", candidate, exc)
        return {}

    if isinstance(data, dict) and isinstance(data.get("themes"), dict):
        data = data["themes"]

    if not isinstance(data, dict):
        LOGGER.warning("Theme-Datei hat ungueltiges Format: %s", candidate)
        return {}

    custom = {}
    for name, theme in data.items():
        theme_name = str(name).strip().lower()
        if not theme_name:
            continue
        if _is_valid_theme_map(theme):
            custom[theme_name] = {key: str(theme[key]).strip().lower() for key in THEME_KEY_SET}
        else:
            LOGGER.warning("Theme '%s' in %s ist unvollstaendig und wird ignoriert.", name, candidate)
    return custom


def load_custom_themes():
    for candidate in get_theme_file_candidates():
        if not candidate.exists():
            continue
        custom = load_custom_themes_from_file(candidate)
        if custom:
            return custom
    return {}


def get_all_themes():
    themes = dict(BASE_THEMES)
    themes.update(load_custom_themes())
    return themes


def _color_from_name(name):
    color_name = (name or "white").lower()
    custom_id = _custom_color_id(color_name)
    if custom_id is not None:
        return custom_id

    return {
        "black": curses.COLOR_BLACK,
        "red": curses.COLOR_RED,
        "green": curses.COLOR_GREEN,
        "darkgray": curses.COLOR_BLACK,
        "darkgrey": curses.COLOR_BLACK,
        "gray": curses.COLOR_BLACK,
        "grey": curses.COLOR_BLACK,
        "lightgray": curses.COLOR_WHITE,
        "lightgrey": curses.COLOR_WHITE,
        "brown": curses.COLOR_RED,
        "yellow": curses.COLOR_YELLOW,
        "blue": curses.COLOR_BLUE,
        "magenta": curses.COLOR_MAGENTA,
        "cyan": curses.COLOR_CYAN,
        "brightblack": curses.COLOR_BLACK,
        "brightred": curses.COLOR_RED,
        "brightgreen": curses.COLOR_GREEN,
        "brightyellow": curses.COLOR_YELLOW,
        "brightblue": curses.COLOR_BLUE,
        "brightmagenta": curses.COLOR_MAGENTA,
        "brightcyan": curses.COLOR_CYAN,
        "brightwhite": curses.COLOR_WHITE,
        "white": curses.COLOR_WHITE,
    }.get(color_name, curses.COLOR_WHITE)


def _custom_color_id(color_name):
    if color_name not in CUSTOM_COLOR_RGB:
        return None
    if color_name in CUSTOM_COLOR_IDS:
        return CUSTOM_COLOR_IDS[color_name]

    can_customize = bool(getattr(curses, "can_change_color", lambda: False)())
    color_slots = int(getattr(curses, "COLORS", 0) or 0)
    if not can_customize or color_slots < 32:
        return None

    next_id = 16 + len(CUSTOM_COLOR_IDS)
    if next_id >= color_slots:
        return None

    try:
        curses.init_color(next_id, *CUSTOM_COLOR_RGB[color_name])
    except curses.error:
        return None
    CUSTOM_COLOR_IDS[color_name] = next_id
    return next_id


def apply_color_theme(stdscr):
    theme = get_all_themes()[get_active_theme_name()]
    pair_1 = _resolve_pair_colors(theme["pair_1_fg"], theme["pair_1_bg"], fallback_fg="white", fallback_bg="blue")
    pair_2 = _resolve_pair_colors(theme["pair_2_fg"], theme["pair_2_bg"], fallback_fg="black", fallback_bg="cyan")
    pair_3 = _resolve_pair_colors(theme["pair_3_fg"], theme["pair_3_bg"], fallback_fg="black", fallback_bg="white")
    curses.init_pair(1, pair_1[0], pair_1[1])
    curses.init_pair(2, pair_2[0], pair_2[1])
    curses.init_pair(3, pair_3[0], pair_3[1])
    stdscr.bkgd(" ", curses.color_pair(1))


def _resolve_pair_colors(fg_name, bg_name, fallback_fg, fallback_bg):
    fg = _color_from_name(fg_name)
    bg = _color_from_name(bg_name)
    if fg == bg:
        return _color_from_name(fallback_fg), _color_from_name(fallback_bg)
    return fg, bg


def init_db():
    con = db()
    cur = con.cursor()
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS available integer")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS reserved integer DEFAULT 0")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS committed integer DEFAULT 0")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS unavailable integer DEFAULT 0")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS external_fulfillment boolean NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS barcode text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_product_status text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_description text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_price text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_compare_at_price text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_unit_cost text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_unit_cost_currency text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_weight_grams integer")
    cur.execute("UPDATE items SET reserved = COALESCE(reserved, 0)")
    cur.execute("UPDATE items SET committed = COALESCE(committed, 0)")
    cur.execute(
        """
        UPDATE items
        SET unavailable = COALESCE(unavailable, COALESCE(reserved, 0))
        """
    )
    cur.execute(
        """
        UPDATE items
        SET available = GREATEST(
            menge - COALESCE(unavailable, 0) - COALESCE(committed, 0),
            0
        )
        WHERE available IS NULL
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shopify_orders (
            order_id text PRIMARY KEY,
            order_name text NOT NULL,
            created_at timestamptz,
            shipping_name text,
            shipping_address1 text,
            shipping_zip text,
            shipping_city text,
            shipping_country text,
            shipping_email text,
            shipping_phone text,
            fulfillment_status text,
            payment_status text,
            updated_at timestamptz NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute("ALTER TABLE shopify_orders ADD COLUMN IF NOT EXISTS shipping_country text")
    cur.execute("ALTER TABLE shopify_orders ADD COLUMN IF NOT EXISTS shipping_email text")
    cur.execute("ALTER TABLE shopify_orders ADD COLUMN IF NOT EXISTS shipping_phone text")
    cur.execute("ALTER TABLE shopify_orders ADD COLUMN IF NOT EXISTS payment_status text")
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_shopify_orders_name
        ON shopify_orders(order_name)
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shopify_order_items (
            order_id text NOT NULL,
            line_index integer NOT NULL,
            order_line_item_id text,
            sku text,
            title text NOT NULL,
            quantity integer NOT NULL,
            fulfilled_quantity integer NOT NULL DEFAULT 0,
            PRIMARY KEY (order_id, line_index)
        )
        """
    )
    cur.execute("ALTER TABLE shopify_order_items ADD COLUMN IF NOT EXISTS order_line_item_id text")
    cur.execute("ALTER TABLE shopify_order_items ADD COLUMN IF NOT EXISTS fulfilled_quantity integer NOT NULL DEFAULT 0")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gls_labels (
            id serial PRIMARY KEY,
            carrier text NOT NULL DEFAULT 'gls',
            order_id text NOT NULL,
            order_name text NOT NULL,
            shipment_reference text NOT NULL,
            track_id text NOT NULL UNIQUE,
            parcel_number text,
            weight_kg numeric(8,3) NOT NULL DEFAULT 1.0,
            status text NOT NULL DEFAULT 'CREATED',
            label_path text NOT NULL,
            last_error text,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            updated_at timestamptz NOT NULL DEFAULT NOW(),
            cancel_requested_at timestamptz,
            cancelled_at timestamptz
        )
        """
    )
    cur.execute("ALTER TABLE gls_labels ADD COLUMN IF NOT EXISTS carrier text NOT NULL DEFAULT 'gls'")
    cur.execute("ALTER TABLE gls_labels ADD COLUMN IF NOT EXISTS shipment_reference text")
    cur.execute("ALTER TABLE gls_labels ADD COLUMN IF NOT EXISTS parcel_number text")
    cur.execute("ALTER TABLE gls_labels ADD COLUMN IF NOT EXISTS weight_kg numeric(8,3) NOT NULL DEFAULT 1.0")
    cur.execute("ALTER TABLE gls_labels ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'CREATED'")
    cur.execute("ALTER TABLE gls_labels ADD COLUMN IF NOT EXISTS label_path text NOT NULL DEFAULT ''")
    cur.execute("ALTER TABLE gls_labels ADD COLUMN IF NOT EXISTS last_error text")
    cur.execute("ALTER TABLE gls_labels ADD COLUMN IF NOT EXISTS cancel_requested_at timestamptz")
    cur.execute("ALTER TABLE gls_labels ADD COLUMN IF NOT EXISTS cancelled_at timestamptz")
    cur.execute("ALTER TABLE gls_labels ADD COLUMN IF NOT EXISTS source text NOT NULL DEFAULT 'local'")
    cur.execute("ALTER TABLE gls_labels ADD COLUMN IF NOT EXISTS shopify_fulfillment_id text")
    cur.execute("ALTER TABLE gls_labels ADD COLUMN IF NOT EXISTS shopify_synced_at timestamptz")
    cur.execute("ALTER TABLE gls_labels ADD COLUMN IF NOT EXISTS tracking_url text")
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gls_labels_order_created
        ON gls_labels(order_id, created_at DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gls_labels_created
        ON gls_labels(created_at DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gls_labels_shopify_fulfillment
        ON gls_labels(shopify_fulfillment_id)
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shopify_fulfillment_jobs (
            id serial PRIMARY KEY,
            label_id integer,
            order_id text NOT NULL,
            tracking_number text NOT NULL,
            carrier text NOT NULL,
            line_items_json text,
            notify_customer boolean NOT NULL DEFAULT FALSE,
            status text NOT NULL DEFAULT 'pending',
            attempts integer NOT NULL DEFAULT 0,
            result_message text,
            shopify_fulfillment_id text,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            updated_at timestamptz NOT NULL DEFAULT NOW(),
            processed_at timestamptz
        )
        """
    )
    cur.execute("ALTER TABLE shopify_fulfillment_jobs ADD COLUMN IF NOT EXISTS label_id integer")
    cur.execute("ALTER TABLE shopify_fulfillment_jobs ADD COLUMN IF NOT EXISTS line_items_json text")
    cur.execute("ALTER TABLE shopify_fulfillment_jobs ADD COLUMN IF NOT EXISTS notify_customer boolean NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE shopify_fulfillment_jobs ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'pending'")
    cur.execute("ALTER TABLE shopify_fulfillment_jobs ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0")
    cur.execute("ALTER TABLE shopify_fulfillment_jobs ADD COLUMN IF NOT EXISTS result_message text")
    cur.execute("ALTER TABLE shopify_fulfillment_jobs ADD COLUMN IF NOT EXISTS shopify_fulfillment_id text")
    cur.execute("ALTER TABLE shopify_fulfillment_jobs ADD COLUMN IF NOT EXISTS processed_at timestamptz")
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_shopify_fulfillment_jobs_status_created
        ON shopify_fulfillment_jobs(status, created_at)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_shopify_fulfillment_jobs_label_created
        ON shopify_fulfillment_jobs(label_id, created_at DESC)
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory_sessions (
            session_id serial PRIMARY KEY,
            session_name text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            status text NOT NULL DEFAULT 'active'
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory_lines (
            session_id integer NOT NULL REFERENCES inventory_sessions(session_id) ON DELETE CASCADE,
            line_no integer NOT NULL,
            sku text NOT NULL,
            name text NOT NULL,
            regal text,
            fach text,
            platz text,
            soll_menge integer NOT NULL,
            ist_menge integer,
            PRIMARY KEY (session_id, line_no)
        )
        """
    )
    con.commit()
    cur.close()
    con.close()



def db():
    return psycopg2.connect(
        host=SETTINGS["db_host"],
        dbname=SETTINGS["db_name"],
        user=SETTINGS["db_user"],
        password=SETTINGS["db_pass"],
        cursor_factory=psycopg2.extras.RealDictCursor
    )


def test_db_connection(settings):
    con = psycopg2.connect(
        host=settings["db_host"],
        dbname=settings["db_name"],
        user=settings["db_user"],
        password=settings["db_pass"],
    )
    con.close()


def _is_default_db_settings(settings):
    for key in ("db_host", "db_name", "db_user", "db_pass"):
        if settings.get(key) != DEFAULT_SETTINGS.get(key):
            return False
    return True


def ensure_database_ready(stdscr):
    while True:
        if _is_default_db_settings(SETTINGS):
            message_box(stdscr, "Setup", "Bitte zuerst DB Einstellungen in Shift+F11 speichern.")
            settings_dialog(stdscr)
            if _is_default_db_settings(SETTINGS):
                if confirm_box(stdscr, "Setup", "Keine DB Daten gesetzt. Programm beenden?"):
                    return False
                continue

        try:
            init_db()
            return True
        except Exception as exc:
            message_box(stdscr, "DB Fehler", str(exc)[:56])
            settings_dialog(stdscr)


def get_items(filter_text=None, filter_no_location=False, filter_local=False, sort_mode="location", external_mode="hide"):
    con = db()
    cur = con.cursor()

    conditions = []
    params = []

    if filter_text:
        conditions.append("(name ILIKE %s OR sku ILIKE %s OR COALESCE(barcode, '') ILIKE %s)")
        params.extend([f"%{filter_text}%", f"%{filter_text}%", f"%{filter_text}%"])

    if filter_no_location:
        conditions.append("(regal IS NULL OR regal='' OR fach IS NULL OR platz IS NULL)")

    if filter_local:
        conditions.append("sync_status='local'")

    if external_mode == "only":
        conditions.append("COALESCE(external_fulfillment, FALSE) = TRUE")
    elif external_mode == "hide":
        conditions.append("COALESCE(external_fulfillment, FALSE) = FALSE")

    where = ""
    if conditions:
        where = "WHERE " + " AND ".join(conditions)

    if sort_mode == "location":
        order = "ORDER BY regal NULLS LAST, fach NULLS LAST, platz NULLS LAST"
    elif sort_mode == "name":
        order = "ORDER BY name"
    elif sort_mode == "sku":
        order = "ORDER BY sku"
    else:
        order = "ORDER BY regal NULLS LAST, fach NULLS LAST, platz NULLS LAST"

    query = f"""
    SELECT
        sku,
        name,
        regal,
        fach,
        platz,
        menge,
        COALESCE(reserved, 0) AS reserved,
        COALESCE(committed, 0) AS committed,
        COALESCE(unavailable, COALESCE(reserved, 0)) AS unavailable,
        COALESCE(
            available,
            GREATEST(
                menge
                - COALESCE(unavailable, COALESCE(reserved, 0))
                - COALESCE(committed, 0),
                0
            )
        ) AS available,
        dirty,
        shopify_variant_id,
        barcode,
        shopify_product_status,
        shopify_description,
        shopify_price,
        shopify_compare_at_price,
        shopify_unit_cost,
        shopify_unit_cost_currency,
        shopify_weight_grams,
        sync_status,
        COALESCE(external_fulfillment, FALSE) AS external_fulfillment
    FROM items
    {where}
    {order}
    """

    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def get_orders(order_filter=None, only_pending=False, fulfillment_filter="all", payment_filter="all"):
    con = db()
    cur = con.cursor()

    conditions = []
    params = []

    if order_filter:
        conditions.append(
            """
            (
                REPLACE(so.order_name, '#', '') ILIKE %s
                OR COALESCE(so.shipping_name, '') ILIKE %s
                OR COALESCE(so.shipping_city, '') ILIKE %s
            )
            """
        )
        match = f"%{order_filter.replace('#', '')}%"
        params = [match, f"%{order_filter}%", f"%{order_filter}%"]

    status_expr = "LOWER(COALESCE(so.fulfillment_status, ''))"
    payment_expr = "LOWER(COALESCE(so.payment_status, ''))"

    if fulfillment_filter == "open":
        conditions.append(f"{status_expr} NOT IN ('fulfilled', 'cancelled')")
    elif fulfillment_filter == "unfulfilled":
        conditions.append(f"({status_expr} = '' OR POSITION('unfulfilled' IN {status_expr}) > 0)")
    elif fulfillment_filter == "partial":
        conditions.append(f"(POSITION('partial' IN {status_expr}) > 0 OR POSITION('in_progress' IN {status_expr}) > 0)")
    elif fulfillment_filter == "fulfilled":
        conditions.append(f"{status_expr} = 'fulfilled'")

    if only_pending:
        conditions.append(f"{status_expr} NOT IN ('fulfilled', 'cancelled')")

    if payment_filter != "all":
        conditions.append(f"{payment_expr} = %s")
        params.append(payment_filter.lower())

    where = ""
    if conditions:
        where = "WHERE " + " AND ".join(conditions)

    cur.execute(
        f"""
        SELECT
            so.order_id,
            so.order_name,
            so.created_at,
            so.shipping_name,
            so.shipping_address1,
            so.shipping_zip,
            so.shipping_city,
            so.shipping_country,
            so.shipping_email,
            so.shipping_phone,
            so.fulfillment_status,
            so.payment_status,
            COALESCE(order_stats.local_internal_qty, 0) AS local_internal_qty
        FROM shopify_orders so
        LEFT JOIN (
            SELECT
                oi.order_id,
                SUM(
                    CASE WHEN COALESCE(i.external_fulfillment, FALSE) = FALSE
                    THEN oi.quantity
                    ELSE 0
                    END
                ) AS local_internal_qty
            FROM shopify_order_items oi
            LEFT JOIN items i ON i.sku = oi.sku
            GROUP BY oi.order_id
        ) AS order_stats ON order_stats.order_id = so.order_id
        {where}
        ORDER BY so.created_at DESC NULLS LAST, so.order_name DESC
        """,
        params,
    )
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def should_refresh_orders(last_refresh_at, now=None, interval_seconds=ORDERS_AUTO_REFRESH_SECONDS):
    if last_refresh_at is None:
        return True
    current = time.monotonic() if now is None else now
    return (current - last_refresh_at) >= max(0.0, float(interval_seconds))


def get_order_items(order_id):
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT
            oi.line_index,
            oi.order_line_item_id,
            oi.sku,
            oi.title,
            oi.quantity,
            COALESCE(oi.fulfilled_quantity, 0) AS fulfilled_quantity,
            i.regal,
            i.fach,
            i.platz,
            i.shopify_weight_grams,
            COALESCE(i.external_fulfillment, FALSE) AS external_fulfillment
        FROM shopify_order_items oi
        LEFT JOIN items i ON i.sku = oi.sku
        WHERE oi.order_id = %s
        ORDER BY oi.line_index
        """,
        (order_id,),
    )
    rows = cur.fetchall()
    cur.close()
    con.close()
    local_fulfilled = get_local_fulfilled_quantities_for_order(order_id)
    for row in rows:
        line_item_id = (row.get("order_line_item_id") or "").strip()
        quantity = int(row.get("quantity") or 0)
        remote_fulfilled = int(row.get("fulfilled_quantity") or 0)
        local_seen = int(local_fulfilled.get(line_item_id, 0)) if line_item_id else 0
        row["fulfilled_quantity"] = max(0, min(quantity, max(remote_fulfilled, local_seen)))
    return rows


def get_local_fulfilled_quantities_for_order(order_id):
    if not order_id:
        return {}
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT line_items_json
        FROM shopify_fulfillment_jobs
        WHERE order_id = %s
          AND status IN ('pending', 'processing', 'done')
          AND line_items_json IS NOT NULL
          AND line_items_json <> ''
        ORDER BY created_at
        """,
        (order_id,),
    )
    rows = cur.fetchall()
    cur.close()
    con.close()

    totals = {}
    for row in rows:
        raw = row.get("line_items_json")
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, list):
            continue
        for item in payload:
            if not isinstance(item, dict):
                continue
            line_item_id = (item.get("order_line_item_id") or "").strip()
            if not line_item_id:
                continue
            try:
                quantity = int(item.get("quantity") or 0)
            except (TypeError, ValueError):
                continue
            if quantity <= 0:
                continue
            totals[line_item_id] = totals.get(line_item_id, 0) + quantity
    return totals


def list_gls_labels(order_id=None):
    con = db()
    cur = con.cursor()
    if order_id:
        cur.execute(
            """
            SELECT
                id,
                carrier,
                order_id,
                order_name,
                shipment_reference,
                track_id,
                parcel_number,
                status,
                weight_kg,
                label_path,
                last_error,
                source,
                shopify_fulfillment_id,
                shopify_synced_at,
                tracking_url,
                created_at,
                updated_at,
                cancel_requested_at,
                cancelled_at
            FROM gls_labels
            WHERE order_id = %s
            ORDER BY created_at DESC, id DESC
            """,
            (order_id,),
        )
    else:
        cur.execute(
            """
            SELECT
                id,
                carrier,
                order_id,
                order_name,
                shipment_reference,
                track_id,
                parcel_number,
                status,
                weight_kg,
                label_path,
                last_error,
                source,
                shopify_fulfillment_id,
                shopify_synced_at,
                tracking_url,
                created_at,
                updated_at,
                cancel_requested_at,
                cancelled_at
            FROM gls_labels
            ORDER BY created_at DESC, id DESC
            LIMIT 400
            """
        )
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def insert_gls_label_history(
    order,
    shipment_reference,
    track_id,
    parcel_number,
    label_path,
    status,
    weight_kg=1.0,
    carrier="gls",
    source="local",
    shopify_fulfillment_id=None,
    tracking_url=None,
):
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO gls_labels (
            carrier,
            order_id,
            order_name,
            shipment_reference,
            track_id,
            parcel_number,
            weight_kg,
            status,
            label_path,
            source,
            shopify_fulfillment_id,
            shopify_synced_at,
            tracking_url
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (track_id)
        DO UPDATE
           SET carrier = EXCLUDED.carrier,
               order_id = EXCLUDED.order_id,
               order_name = EXCLUDED.order_name,
               shipment_reference = EXCLUDED.shipment_reference,
               parcel_number = EXCLUDED.parcel_number,
               weight_kg = EXCLUDED.weight_kg,
               status = EXCLUDED.status,
               label_path = COALESCE(NULLIF(EXCLUDED.label_path, ''), gls_labels.label_path),
               source = CASE
                   WHEN gls_labels.source = 'local' AND EXCLUDED.source = 'shopify' THEN gls_labels.source
                   ELSE EXCLUDED.source
               END,
               shopify_fulfillment_id = COALESCE(EXCLUDED.shopify_fulfillment_id, gls_labels.shopify_fulfillment_id),
               shopify_synced_at = COALESCE(EXCLUDED.shopify_synced_at, gls_labels.shopify_synced_at),
               tracking_url = COALESCE(EXCLUDED.tracking_url, gls_labels.tracking_url),
               updated_at = NOW(),
               last_error = NULL
        RETURNING id
        """,
        (
            carrier,
            order["order_id"],
            order["order_name"],
            shipment_reference,
            track_id,
            parcel_number,
            weight_kg,
            status,
            label_path,
            (source or "local").strip().lower() or "local",
            shopify_fulfillment_id,
            datetime.datetime.now() if shopify_fulfillment_id else None,
            (tracking_url or "").strip() or None,
        ),
    )
    row = cur.fetchone()
    con.commit()
    cur.close()
    con.close()
    return row["id"] if row else None


def update_gls_label_status(label_id, status, last_error=None):
    con = db()
    cur = con.cursor()
    if status == "CANCELLED":
        cur.execute(
            """
            UPDATE gls_labels
            SET status = %s,
                last_error = %s,
                cancelled_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
            """,
            (status, last_error, label_id),
        )
    elif status == "CANCELLATION_PENDING":
        cur.execute(
            """
            UPDATE gls_labels
            SET status = %s,
                last_error = %s,
                cancel_requested_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
            """,
            (status, last_error, label_id),
        )
    else:
        cur.execute(
            """
            UPDATE gls_labels
            SET status = %s,
                last_error = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (status, last_error, label_id),
        )
    con.commit()
    cur.close()
    con.close()


def update_gls_label_reprint(label_id, label_path):
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        UPDATE gls_labels
        SET label_path = %s,
            status = 'REPRINTED',
            last_error = NULL,
            updated_at = NOW()
        WHERE id = %s
        """,
        (label_path, label_id),
    )
    con.commit()
    cur.close()
    con.close()


def get_latest_shopify_job_for_label(label_id):
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT
            id,
            order_id,
            tracking_number,
            carrier,
            line_items_json,
            notify_customer,
            status,
            attempts,
            result_message,
            shopify_fulfillment_id,
            created_at,
            updated_at,
            processed_at
        FROM shopify_fulfillment_jobs
        WHERE label_id = %s
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (label_id,),
    )
    row = cur.fetchone()
    cur.close()
    con.close()
    return row


def _shipment_number(row):
    return ((row.get("parcel_number") or "").strip() or (row.get("track_id") or "").strip() or "-")


def _shipment_source_label(value):
    normalized = (value or "").strip().lower()
    if current_language() == "de":
        labels = {
            "local": "Lokal",
            "shopify": "Shopify",
        }
    else:
        labels = {
            "local": "Local",
            "shopify": "Shopify",
        }
    return labels.get(normalized, value or "-")


def _shipment_summary_lines(rows, width):
    if not rows:
        return ["Sendungen: -"]
    entries = []
    for row in rows:
        carrier = (row.get("carrier") or "-").upper()
        number = _shipment_number(row)
        source = _shipment_source_label(row.get("source"))
        status = row.get("status") or "-"
        entries.append(f"{carrier} {number} [{source}/{status}]")
    wrapped = textwrap.wrap(" | ".join(entries), width=max(12, width), break_long_words=False, break_on_hyphens=False)
    if not wrapped:
        return ["Sendungen: -"]
    lines = [f"Sendungen: {wrapped[0]}"]
    lines.extend(wrapped[1:])
    return lines


def enqueue_shopify_fulfillment_job(label_row, notify_customer=False):
    label_id = label_row.get("id")
    order_id = (label_row.get("order_id") or "").strip()
    tracking_number = (label_row.get("parcel_number") or label_row.get("track_id") or "").strip()
    carrier = (label_row.get("carrier") or "gls").strip().upper()
    if carrier == "TEST":
        raise RuntimeError("Test-Labels duerfen nicht an Shopify uebertragen werden.")
    if not order_id:
        raise RuntimeError("order_id fehlt.")
    if not tracking_number:
        raise RuntimeError("track_id fehlt.")

    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, status
        FROM shopify_fulfillment_jobs
        WHERE label_id = %s
          AND status IN ('pending', 'processing')
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (label_id,),
    )
    existing = cur.fetchone()
    if existing:
        cur.close()
        con.close()
        return {"job_id": existing["id"], "status": existing["status"], "created": False}

    cur.execute(
        """
        INSERT INTO shopify_fulfillment_jobs (
            label_id,
            order_id,
            tracking_number,
            carrier,
            line_items_json,
            notify_customer,
            status,
            attempts,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, 'pending', 0, NOW(), NOW())
        RETURNING id, status
        """,
        (label_id, order_id, tracking_number, carrier, None, bool(notify_customer)),
    )
    row = cur.fetchone()
    con.commit()
    cur.close()
    con.close()
    return {"job_id": row["id"], "status": row["status"], "created": True}


def enqueue_shopify_fulfillment_job_for_items(label_row, selected_items, notify_customer=False):
    label_id = label_row.get("id")
    order_id = (label_row.get("order_id") or "").strip()
    tracking_number = (label_row.get("parcel_number") or label_row.get("track_id") or "").strip()
    carrier = (label_row.get("carrier") or "gls").strip().upper()
    if carrier == "TEST":
        raise RuntimeError("Test-Labels duerfen nicht an Shopify uebertragen werden.")
    if not label_id:
        raise RuntimeError("label_id fehlt.")
    if not order_id:
        raise RuntimeError("order_id fehlt.")
    if not tracking_number:
        raise RuntimeError("tracking_number fehlt.")

    line_items = []
    for item in selected_items:
        line_item_id = (item.get("order_line_item_id") or "").strip()
        selected_qty = item.get("selected_quantity") or 0
        if not line_item_id:
            continue
        try:
            qty_int = int(selected_qty)
        except (TypeError, ValueError):
            continue
        if qty_int <= 0:
            continue
        line_items.append(
            {
                "order_line_item_id": line_item_id,
                "quantity": qty_int,
                "sku": item.get("sku") or "",
            }
        )

    if not line_items:
        raise RuntimeError("Keine gueltigen Positionen fuer Shopify-Fulfillment.")

    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, status
        FROM shopify_fulfillment_jobs
        WHERE label_id = %s
          AND status IN ('pending', 'processing')
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (label_id,),
    )
    existing = cur.fetchone()
    if existing:
        cur.close()
        con.close()
        return {"job_id": existing["id"], "status": existing["status"], "created": False}

    cur.execute(
        """
        INSERT INTO shopify_fulfillment_jobs (
            label_id,
            order_id,
            tracking_number,
            carrier,
            line_items_json,
            notify_customer,
            status,
            attempts,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, 'pending', 0, NOW(), NOW())
        RETURNING id, status
        """,
        (
            label_id,
            order_id,
            tracking_number,
            carrier,
            json.dumps(line_items, ensure_ascii=True),
            bool(notify_customer),
        ),
    )
    row = cur.fetchone()
    con.commit()
    cur.close()
    con.close()
    return {"job_id": row["id"], "status": row["status"], "created": True}


def _gls_extract_from_pdf(pdf_path):
    temp_txt = Path(tempfile.gettempdir()) / f"gls_login_{os.getpid()}.txt"
    try:
        subprocess.run(["pdftotext", "-layout", str(pdf_path), str(temp_txt)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    except FileNotFoundError as exc:
        raise RuntimeError("pdftotext fehlt. Bitte Zugangsdaten in settings.local.json setzen.") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"PDF konnte nicht gelesen werden: {(exc.stderr or '').strip()[:80]}") from exc

    try:
        text = temp_txt.read_text(encoding="utf-8", errors="ignore")
    finally:
        temp_txt.unlink(missing_ok=True)

    def pick(pattern, field_name):
        match = re.search(pattern, text)
        if not match:
            raise RuntimeError(f"GLS Feld fehlt in PDF: {field_name}")
        return match.group(1).strip()

    return {
        "api_url": pick(r"(https://[^\s]+/backend/rs/shipments)", "REST endpoint"),
        "user": pick(r"Login/User:\s*([^\n\r]+)", "Login/User"),
        "password": pick(r"Passwort:\s*([^\n\r]+)", "Passwort"),
        "contact_id": pick(r"Kontakt ID:\s*([^\n\r]+)", "Kontakt ID"),
    }


def load_gls_credentials():
    creds = {
        "api_url": SETTINGS.get("gls_api_url", "").strip(),
        "user": SETTINGS.get("gls_user", "").strip(),
        "password": SETTINGS.get("gls_password", "").strip(),
        "contact_id": SETTINGS.get("gls_contact_id", "").strip(),
    }
    if all(creds.values()):
        return creds

    pdf_candidates = sorted(GLS_DIR.glob("*.pdf"))
    if not pdf_candidates:
        raise RuntimeError("GLS Zugangsdaten fehlen (settings.local.json oder gls/*.pdf).")
    return _gls_extract_from_pdf(pdf_candidates[0])


def load_post_credentials():
    creds = {
        "api_url": (SETTINGS.get("post_api_url") or "").strip(),
        "api_key": (SETTINGS.get("post_api_key") or "").strip(),
        "api_secret": (SETTINGS.get("post_api_secret") or "").strip(),
        "user": (SETTINGS.get("post_user") or "").strip(),
        "password": (SETTINGS.get("post_password") or "").strip(),
        "partner_id": (SETTINGS.get("post_partner_id") or "").strip(),
    }
    missing = []
    if not creds["api_url"]:
        missing.append("api_url")
    if not creds["partner_id"]:
        missing.append("partner_id")
    has_oauth = bool(creds["api_key"] and creds["api_secret"])
    has_legacy = bool(creds["user"] and creds["password"])
    if not has_oauth and not has_legacy:
        missing.append("api_key/api_secret oder user/password")
    if missing:
        raise RuntimeError(f"POST INTERNETMARKE Daten fehlen: {', '.join(missing)}")
    return creds


def load_dhl_private_credentials():
    creds = {
        "api_url": (SETTINGS.get("dhl_private_api_url") or "").strip(),
        "test_api_url": (SETTINGS.get("dhl_private_api_test_url") or "").strip(),
        "api_key": (SETTINGS.get("dhl_private_api_key") or "").strip(),
        "api_secret": (SETTINGS.get("dhl_private_api_secret") or "").strip(),
        "use_test_api": bool(SETTINGS.get("dhl_private_use_test_api", True)),
    }
    missing = []
    if creds["use_test_api"]:
        if not creds["test_api_url"]:
            missing.append("test_api_url")
    else:
        if not creds["api_url"]:
            missing.append("api_url")
    if not creds["api_key"]:
        missing.append("api_key")
    if not creds["api_secret"]:
        missing.append("api_secret")
    if missing:
        raise RuntimeError(f"DHL Privat Daten fehlen: {', '.join(missing)}")
    return creds


def _gls_country_code(country_value):
    country_raw = (country_value or "").strip()
    if len(country_raw) == 2 and country_raw.isalpha():
        return country_raw.upper()
    country = country_raw.lower()
    mapping = {
        "deutschland": "DE",
        "germany": "DE",
        "de": "DE",
        "austria": "AT",
        "oesterreich": "AT",
        "österreich": "AT",
        "at": "AT",
        "switzerland": "CH",
        "schweiz": "CH",
        "ch": "CH",
        "vereinigtes koenigreich": "GB",
        "united kingdom": "GB",
        "uk": "GB",
        "great britain": "GB",
        "england": "GB",
        "france": "FR",
        "italy": "IT",
        "spain": "ES",
        "netherlands": "NL",
        "belgium": "BE",
        "luxembourg": "LU",
    }
    return mapping.get(country, "")


def _sanitize_order_reference(order_name):
    raw = (order_name or "").replace("#", "").strip()
    cleaned = "".join(ch if ch in string.ascii_letters + string.digits + "-_/" else "-" for ch in raw).strip("-")
    return cleaned or f"order-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"


def _shipping_packaging_weight_grams():
    raw_value = SETTINGS.get("shipping_packaging_weight_grams", DEFAULT_SETTINGS.get("shipping_packaging_weight_grams", 400))
    try:
        grams = int(str(raw_value).strip())
    except (TypeError, ValueError):
        grams = 400
    return max(0, grams)


def calculate_order_shipping_weight(order, order_items=None):
    total_grams = _shipping_packaging_weight_grams()
    items = order_items if order_items is not None else get_order_items(order["order_id"])
    for row in items:
        if row.get("external_fulfillment"):
            continue
        quantity = order_item_remaining_qty(row)
        item_weight = row.get("shopify_weight_grams")
        if item_weight is None:
            continue
        try:
            quantity_int = max(0, int(quantity))
            weight_int = max(0, int(item_weight))
        except (TypeError, ValueError):
            continue
        total_grams += quantity_int * weight_int
    weight_kg = max(0.001, round(total_grams / 1000.0, 3))
    return weight_kg, total_grams


def calculate_selected_shipping_weight(selected_items):
    total_grams = _shipping_packaging_weight_grams()
    for row in selected_items:
        if row.get("external_fulfillment"):
            continue
        try:
            quantity = int(row.get("selected_quantity") or 0)
        except (TypeError, ValueError):
            quantity = 0
        if quantity <= 0:
            continue
        item_weight = row.get("shopify_weight_grams")
        if item_weight is None:
            continue
        try:
            weight_int = max(0, int(item_weight))
        except (TypeError, ValueError):
            continue
        total_grams += quantity * weight_int
    return max(0.001, round(total_grams / 1000.0, 3)), total_grams


def _manual_label_country_display(country_code):
    normalized = _gls_country_code(country_code)
    return _localized_country_display(normalized)


def _normalized_country_code_for_display(country_value):
    raw = (country_value or "").strip()
    if not raw:
        return ""
    if len(raw) == 2 and raw.isalpha():
        return raw.upper()

    normalized = raw.lower()
    mapping = {
        "deutschland": "DE",
        "germany": "DE",
        "austria": "AT",
        "oesterreich": "AT",
        "österreich": "AT",
        "switzerland": "CH",
        "schweiz": "CH",
        "vereinigtes koenigreich": "GB",
        "united kingdom": "GB",
        "uk": "GB",
        "great britain": "GB",
        "england": "GB",
        "france": "FR",
        "italy": "IT",
        "spain": "ES",
        "netherlands": "NL",
        "belgium": "BE",
        "luxembourg": "LU",
    }
    return mapping.get(normalized, "")


def _localized_country_name_by_code(country_code):
    code = (country_code or "").strip().upper()
    if not code:
        return ""
    english = ""
    for option in MANUAL_LABEL_COUNTRY_OPTIONS:
        if option["value"] == code:
            english = option["label"]
            break
    if current_language() == "de":
        return COUNTRY_NAME_DE.get(code, english or code)
    return english or code


def _localized_country_display(country_value):
    code = _normalized_country_code_for_display(country_value)
    if code:
        return f"{_localized_country_name_by_code(code)} ({code})"
    raw = (country_value or "").strip()
    return raw or "-"


def _localized_fulfillment_status(status_value):
    raw = (status_value or "").strip()
    if not raw:
        return "Unausgeführt" if current_language() == "de" else "Unfulfilled"

    normalized = raw.lower()
    if current_language() == "de":
        if normalized in {"fulfilled"}:
            return "Ausgeführt"
        if "partial" in normalized:
            return "Teilweise ausgeführt"
        if "in_progress" in normalized or "in progress" in normalized:
            return "In Arbeit"
        if normalized in {"unfulfilled", "open"}:
            return "Unausgeführt"
        if normalized in {"cancelled", "canceled"}:
            return "Storniert"
        if normalized in {"on_hold", "on hold"}:
            return "Pausiert"
        return raw

    if normalized in {"unfulfilled"}:
        return "Unfulfilled"
    if normalized in {"fulfilled"}:
        return "Fulfilled"
    if "partial" in normalized:
        return "Partially Fulfilled"
    if "in_progress" in normalized or "in progress" in normalized:
        return "In Progress"
    if normalized in {"cancelled", "canceled"}:
        return "Cancelled"
    if normalized in {"on_hold", "on hold"}:
        return "On Hold"
    return raw


def _localized_payment_status(status_value):
    raw = (status_value or "").strip()
    if not raw:
        return "-"
    normalized = raw.lower()
    if current_language() == "de":
        mapping = {
            "paid": "Bezahlt",
            "pending": "Ausstehend",
            "authorized": "Autorisiert",
            "partially_paid": "Teilbezahlt",
            "refunded": "Erstattet",
            "voided": "Storniert",
        }
        return mapping.get(normalized, raw)
    return raw


def manual_country_dialog(stdscr, current_country):
    normalized = _gls_country_code(current_country)
    options = [
        {"value": option["value"], "label": f"{_localized_country_name_by_code(option['value'])} ({option['value']})"}
        for option in MANUAL_LABEL_COUNTRY_OPTIONS
    ]
    return choice_dialog(stdscr, "Zielland", options, normalized)


def manual_label_print_mode_dialog(stdscr, current_mode):
    return choice_dialog(
        stdscr,
        "Label Ausgabe",
        [
            {"value": "print", "label": "PDF + Drucken"},
            {"value": "pdf", "label": "Nur PDF speichern"},
        ],
        current_mode,
        cancel_returns_none=True,
    )


def _gls_api_json_request(url, credentials, payload=None):
    auth_raw = f"{credentials['user']}:{credentials['password']}"
    auth = base64.b64encode(auth_raw.encode("utf-8")).decode("ascii")
    body = b"" if payload is None else json.dumps(payload).encode("utf-8")
    headers = {
        "Accept": "application/glsVersion1+json, application/json",
        "Content-Type": "application/glsVersion1+json",
        "Authorization": f"Basic {auth}",
    }
    req = Request(url, data=body, headers=headers, method="POST")
    ctx = ssl.create_default_context()

    try:
        with urlopen(req, timeout=45, context=ctx) as response:
            status_code = response.status
            raw = response.read()
    except HTTPError as exc:
        status_code = exc.code
        raw = exc.read() if hasattr(exc, "read") else b""
    except URLError as exc:
        raise RuntimeError(f"GLS Netzwerkfehler: {exc.reason}") from exc

    parsed = None
    if raw:
        try:
            parsed = json.loads(raw.decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            parsed = None
    return status_code, parsed, raw


def _extract_first_pdf_blob(data):
    candidates = []

    def walk(value):
        if isinstance(value, dict):
            for nested in value.values():
                walk(nested)
        elif isinstance(value, list):
            for nested in value:
                walk(nested)
        elif isinstance(value, str):
            s = value.strip()
            if len(s) > 200 and s.startswith("JVBERi0"):
                candidates.append(s)

    walk(data)
    if not candidates:
        return None
    try:
        return base64.b64decode(candidates[0], validate=True)
    except binascii.Error:
        return base64.b64decode(candidates[0])


def _gls_error_summary(data, raw):
    messages = []

    def walk(value):
        if isinstance(value, dict):
            for key, nested in value.items():
                key_lower = str(key).lower()
                if key_lower in {"message", "messages", "description", "error", "errors", "detail", "details", "faultstring"}:
                    if isinstance(nested, str):
                        text = nested.strip()
                        if text:
                            messages.append(text)
                    elif isinstance(nested, list):
                        for entry in nested:
                            if isinstance(entry, str) and entry.strip():
                                messages.append(entry.strip())
                            else:
                                walk(entry)
                    else:
                        walk(nested)
                else:
                    walk(nested)
        elif isinstance(value, list):
            for nested in value:
                walk(nested)
        elif isinstance(value, str):
            text = value.strip()
            if text and len(text) < 240:
                messages.append(text)

    if isinstance(data, dict):
        walk(data)
    elif isinstance(data, list):
        walk(data)

    seen = []
    for entry in messages:
        if entry not in seen:
            seen.append(entry)
    if seen:
        return " | ".join(seen)[:500]

    if raw:
        try:
            text = raw.decode("utf-8", errors="replace").strip()
        except Exception:
            text = ""
        if text:
            return text[:500]
    return ""


def _build_test_label_pdf(order_name, shipment_reference, track_id):
    text = f"TEST LABEL {order_name} {shipment_reference} {track_id}"
    safe_text = "".join(ch if 32 <= ord(ch) <= 126 else " " for ch in text)[:120]
    stream = f"BT /F1 18 Tf 36 140 Td ({safe_text}) Tj ET"
    objects = [
        "1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj",
        "2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj",
        "3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 283 170] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >> endobj",
        f"4 0 obj << /Length {len(stream)} >> stream\n{stream}\nendstream endobj",
        "5 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj",
    ]
    pdf = "%PDF-1.4\n"
    offsets = [0]
    for obj in objects:
        offsets.append(len(pdf.encode("latin-1")))
        pdf += obj + "\n"
    xref_start = len(pdf.encode("latin-1"))
    pdf += f"xref\n0 {len(objects) + 1}\n"
    pdf += "0000000000 65535 f \n"
    for offset in offsets[1:]:
        pdf += f"{offset:010d} 00000 n \n"
    pdf += f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_start}\n%%EOF\n"
    return pdf.encode("latin-1")


def _save_shipping_label_pdf(carrier, order_name, track_id, pdf_bytes, suffix=""):
    configured_dir = os.path.expanduser((SETTINGS.get("shipping_label_output_dir") or "").strip())
    output_dir = Path(configured_dir) if configured_dir else GLS_LABEL_DIR
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_order = _sanitize_order_reference(order_name)
    safe_track = "".join(ch for ch in (track_id or "unknown") if ch.isalnum() or ch in "-_") or "unknown"
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix_part = f"_{suffix}" if suffix else ""
    safe_carrier = "".join(ch for ch in (carrier or "shipping") if ch.isalnum() or ch in "-_") or "shipping"
    filename = f"{safe_carrier}_{safe_order}_{safe_track}_{timestamp}{suffix_part}.pdf"
    output_path = output_dir / filename
    output_path.write_bytes(pdf_bytes)
    os.chmod(output_path, 0o600)
    return str(output_path)


def _normalize_shipping_label_format(value):
    raw = (value or "").strip()
    if not raw:
        return "A6"
    compact = raw.upper().replace(" ", "")
    if compact in {"A4", "A5", "A6"}:
        return compact
    if compact in {"100X62", "62X100"}:
        return "100x62"
    return raw


def _shipping_printer_for_carrier(carrier):
    c = (carrier or "").strip().lower()
    specific = (SETTINGS.get(f"shipping_label_printer_{c}") or "").strip() if c else ""
    if not specific and c == "dhl_private":
        specific = (SETTINGS.get("shipping_label_printer_dhl") or "").strip()
    fallback = (SETTINGS.get("shipping_label_printer") or "").strip()
    return specific or fallback


def _shipping_format_for_carrier(carrier):
    c = (carrier or "").strip().lower()
    defaults = {"gls": "A6", "dhl": "A5", "dhl_private": "A5", "post": "100x62"}
    specific = SETTINGS.get(f"shipping_label_format_{c}") if c else None
    if not specific and c == "dhl_private":
        specific = SETTINGS.get("shipping_label_format_dhl")
    fallback = SETTINGS.get("shipping_label_format", defaults.get(c, "A6"))
    return _normalize_shipping_label_format(specific or fallback or defaults.get(c, "A6"))


def _delivery_note_format():
    return _normalize_shipping_label_format(SETTINGS.get("delivery_note_format", DEFAULT_SETTINGS.get("delivery_note_format", "A4")))


def _cups_media_value_for_format(label_format):
    normalized = _normalize_shipping_label_format(label_format)
    if normalized == "A6":
        return "A6"
    if normalized == "A5":
        return "A5"
    if normalized == "A4":
        return "A4"
    if normalized == "100x62":
        return "Custom.100x62mm"
    return (label_format or "").strip() or None


def _cups_label_print_options(label_format):
    media = _cups_media_value_for_format(label_format)
    options = []
    if media:
        options.extend(["-o", f"media={media}"])
        options.extend(["-o", f"PageSize={media}"])
    options.extend(
        [
            "-o",
            "print-scaling=none",
            "-o",
            "fit-to-page=false",
            "-o",
            "scaling=100",
            "-o",
            "page-border=none",
            "-o",
            "number-up=1",
        ]
    )
    return options


def _print_pdf_via_lp(stdscr, pdf_path, title, carrier=None):
    carrier_key = (carrier or active_shipping_carrier() or "gls").strip().lower()
    printer = _shipping_printer_for_carrier(carrier_key)
    if not printer:
        message_box(stdscr, "Fehler", f"Bitte Shift+F11: {carrier_key.upper()} Label Drucker setzen.")
        return False
    label_format = _shipping_format_for_carrier(carrier_key)
    cmd = ["lp", "-d", printer, "-t", title]
    cmd.extend(_cups_label_print_options(label_format))
    cmd.append(pdf_path)
    try:
        subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
    except FileNotFoundError:
        PRINT_LOGGER.exception("lp nicht verfuegbar fuer Versandlabel path=%s", pdf_path)
        message_box(stdscr, "Druckfehler", "lp/Drucksystem ist auf diesem System nicht verfuegbar.")
        return False
    except subprocess.CalledProcessError as exc:
        PRINT_LOGGER.exception("Versandlabel Druck fehlgeschlagen carrier=%s path=%s", carrier_key, pdf_path)
        error_text = (exc.stderr or str(exc)).strip()
        message_box(stdscr, "Druckfehler", f"{(error_text[:20] or 'Druckfehler')} {PRINT_LOG_PATH.name}"[:56])
        return False
    return True


def _validate_order_for_gls(order):
    checks = [
        ("shipping_name", "Empfaenger Name fehlt"),
        ("shipping_address1", "Empfaenger Strasse fehlt"),
        ("shipping_zip", "Empfaenger PLZ fehlt"),
        ("shipping_city", "Empfaenger Ort fehlt"),
    ]
    for key, message in checks:
        if not (order.get(key) or "").strip():
            raise ValueError(message)
    if not _gls_country_code(order.get("shipping_country")):
        raise ValueError("Empfaenger Land ungueltig oder fehlt (ISO2 erwartet, z.B. DE).")


def gls_create_label(order, weight_kg=1.0, shipment_reference=None, service_codes=None):
    _validate_order_for_gls(order)
    creds = load_gls_credentials()
    try:
        weight_value = float(weight_kg)
    except (TypeError, ValueError):
        raise ValueError("Gewicht ist ungueltig.")
    if weight_value <= 0:
        raise ValueError("Gewicht muss groesser als 0 sein.")
    weight_value = round(weight_value, 3)

    shipment_reference = _sanitize_order_reference(shipment_reference or order["order_name"])
    normalized_services = _normalize_shipping_services(
        service_codes if service_codes is not None else SETTINGS.get("shipping_services", [])
    )
    if "service_flexdelivery" in normalized_services and not (order.get("shipping_email") or "").strip():
        raise ValueError("Empfaenger E-Mail fehlt fuer FlexDelivery.")
    # GLS expects the generic shipment-level service wrapper:
    # "Service": [{"Service": {"ServiceName": "service_flexdelivery"}}]
    service_entries = [{"Service": {"ServiceName": code}} for code in normalized_services]
    payload = {
        "Shipment": {
            "ShipmentReference": [shipment_reference],
            "ShippingDate": datetime.date.today().isoformat(),
            "Identifier": "lager-mc",
            "Middleware": "Lagerverwaltung",
            "Product": "PARCEL",
            "Shipper": {"ContactID": creds["contact_id"]},
            "Consignee": {
                "Category": "PRIVATE",
                "Address": {
                    "Name1": (order.get("shipping_name") or "").strip(),
                    "CountryCode": _gls_country_code(order.get("shipping_country")),
                    "ZIPCode": (order.get("shipping_zip") or "").strip(),
                    "City": (order.get("shipping_city") or "").strip(),
                    "Street": (order.get("shipping_address1") or "").strip(),
                    "eMail": (order.get("shipping_email") or "").strip(),
                    "FixedLinePhonenumber": (order.get("shipping_phone") or "").strip(),
                },
            },
            "ShipmentUnit": [{"Weight": weight_value}],
            "Service": service_entries,
        },
        "PrintingOptions": {"ReturnLabels": {"TemplateSet": "NONE", "LabelFormat": "PDF"}},
    }

    status_code, data, raw = _gls_api_json_request(creds["api_url"], creds, payload)
    if status_code >= 400 or not isinstance(data, dict):
        error_detail = _gls_error_summary(data, raw)
        LOGGER.error(
            "GLS Label-API Fehler status=%s order=%s ref=%s country=%s zip=%s city=%s weight=%.3f detail=%s",
            status_code,
            order.get("order_name"),
            shipment_reference,
            _gls_country_code(order.get("shipping_country")),
            (order.get("shipping_zip") or "").strip(),
            (order.get("shipping_city") or "").strip(),
            weight_value,
            error_detail or "-",
        )
        if error_detail:
            raise RuntimeError(f"GLS Label-API Fehler HTTP {status_code}: {error_detail[:180]}")
        raise RuntimeError(f"GLS Label-API Fehler HTTP {status_code}")

    created = data.get("CreatedShipment") or {}
    parcel_data = created.get("ParcelData") or []
    parcel_number = ""
    track_id = ""
    if isinstance(parcel_data, list) and parcel_data and isinstance(parcel_data[0], dict):
        parcel_number = (parcel_data[0].get("ParcelNumber") or "").strip()
        track_id = (parcel_data[0].get("TrackID") or "").strip()
    if not track_id:
        track_id = (created.get("TrackID") or "").strip()

    pdf_blob = _extract_first_pdf_blob(data)
    if not pdf_blob:
        if raw.startswith(b"%PDF-"):
            pdf_blob = raw
        else:
            raise RuntimeError("GLS Labelantwort ohne PDF-Daten.")

    label_path = _save_shipping_label_pdf("gls", order["order_name"], track_id, pdf_blob)
    label_id = insert_gls_label_history(
        order=order,
        shipment_reference=shipment_reference,
        track_id=track_id,
        parcel_number=parcel_number,
        label_path=label_path,
        status="CREATED",
        weight_kg=weight_value,
        carrier="gls",
    )
    return {
        "label_id": label_id,
        "track_id": track_id,
        "parcel_number": parcel_number,
        "label_path": label_path,
        "shipment_reference": shipment_reference,
    }


def post_create_label(order, weight_kg=1.0, shipment_reference=None, service_codes=None):
    _validate_order_for_gls(order)
    _creds = load_post_credentials()
    client = InternetmarkeClient(
        api_url=_creds["api_url"],
        partner_id=_creds["partner_id"],
        api_key=_creds["api_key"],
        api_secret=_creds["api_secret"],
        user=_creds["user"],
        password=_creds["password"],
    )
    client.validate()
    _weight_value = float(weight_kg)
    _reference = _sanitize_order_reference(shipment_reference or order["order_name"])
    raise RuntimeError("POST INTERNETMARKE ist vorbereitet, API-Call folgt im naechsten Schritt.")


def dhl_private_create_label(order, weight_kg=1.0, shipment_reference=None, service_codes=None):
    _validate_order_for_gls(order)
    _creds = load_dhl_private_credentials()
    client = DHLPrivateClient(
        api_url=_creds["api_url"],
        test_api_url=_creds["test_api_url"],
        api_key=_creds["api_key"],
        api_secret=_creds["api_secret"],
        use_test_api=_creds["use_test_api"],
    )
    client.validate()
    _weight_value = float(weight_kg)
    _reference = _sanitize_order_reference(shipment_reference or order["order_name"])
    mode = "Test" if _creds["use_test_api"] else "Produktion"
    raise RuntimeError(f"DHL Private Shipping ({mode}) ist vorbereitet, API-Call folgt im naechsten Schritt.")


def test_create_label(order, weight_kg=1.0, shipment_reference=None, service_codes=None):
    _validate_order_for_gls(order)
    shipment_reference = _sanitize_order_reference(shipment_reference or order["order_name"])
    track_id = f"TEST{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
    parcel_number = f"999{datetime.datetime.now().strftime('%H%M%S')}"
    pdf_blob = _build_test_label_pdf(order.get("order_name") or "TEST", shipment_reference, track_id)
    label_path = _save_shipping_label_pdf("test", order["order_name"], track_id, pdf_blob)
    label_id = insert_gls_label_history(
        order=order,
        shipment_reference=shipment_reference,
        track_id=track_id,
        parcel_number=parcel_number,
        label_path=label_path,
        status="CREATED",
        weight_kg=round(float(weight_kg), 3),
        carrier="test",
    )
    return {
        "label_id": label_id,
        "track_id": track_id,
        "parcel_number": parcel_number,
        "label_path": label_path,
        "shipment_reference": shipment_reference,
    }


def gls_reprint_label(label_row):
    creds = load_gls_credentials()
    track_id = (label_row.get("track_id") or "").strip()
    if not track_id:
        raise ValueError("TrackID fehlt")
    url = f"{creds['api_url'].rstrip('/')}/reprint/{track_id}"
    status_code, data, raw = _gls_api_json_request(url, creds)
    if status_code >= 400:
        error_detail = _gls_error_summary(data, raw)
        LOGGER.error("GLS Reprint Fehler status=%s track_id=%s detail=%s", status_code, track_id, error_detail or "-")
        if error_detail:
            raise RuntimeError(f"GLS Reprint Fehler HTTP {status_code}: {error_detail[:180]}")
        raise RuntimeError(f"GLS Reprint Fehler HTTP {status_code}")

    pdf_blob = _extract_first_pdf_blob(data)
    if not pdf_blob and raw.startswith(b"%PDF-"):
        pdf_blob = raw
    if not pdf_blob:
        raise RuntimeError("GLS Reprint ohne PDF-Daten.")

    label_path = _save_shipping_label_pdf("gls", label_row["order_name"], track_id, pdf_blob, suffix="reprint")
    update_gls_label_reprint(label_row["id"], label_path)
    return label_path


def gls_cancel_label(label_row):
    creds = load_gls_credentials()
    track_id = (label_row.get("track_id") or "").strip()
    if not track_id:
        raise ValueError("TrackID fehlt")
    url = f"{creds['api_url'].rstrip('/')}/cancel/{track_id}"
    status_code, data, raw = _gls_api_json_request(url, creds)
    if status_code >= 400:
        error_detail = _gls_error_summary(data, raw)
        update_gls_label_status(label_row["id"], "CANCEL_FAILED", f"HTTP {status_code} {error_detail[:120]}".strip())
        LOGGER.error("GLS Storno Fehler status=%s track_id=%s detail=%s", status_code, track_id, error_detail or "-")
        if error_detail:
            raise RuntimeError(f"GLS Storno Fehler HTTP {status_code}: {error_detail[:180]}")
        raise RuntimeError(f"GLS Storno Fehler HTTP {status_code}")

    result = ""
    if isinstance(data, dict):
        result = (data.get("result") or "").strip().upper()
    if result == "CANCELLED":
        update_gls_label_status(label_row["id"], "CANCELLED")
    elif result == "CANCELLATION_PENDING":
        update_gls_label_status(label_row["id"], "CANCELLATION_PENDING")
    else:
        update_gls_label_status(label_row["id"], "CANCEL_REQUESTED")
    return result or "CANCEL_REQUESTED"


def _normalize_shipping_services(raw_value):
    if isinstance(raw_value, list):
        selected = [str(item).strip() for item in raw_value if str(item).strip()]
    elif isinstance(raw_value, str):
        selected = [part.strip() for part in raw_value.split(",") if part.strip()]
    else:
        selected = []

    allowed = {entry["code"] for entry in SHIPPING_SERVICE_OPTIONS}
    normalized = []
    for code in selected:
        if code in allowed and code not in normalized:
            normalized.append(code)

    if "service_flexdelivery" not in normalized:
        normalized.insert(0, "service_flexdelivery")
    return normalized


def _shipping_services_summary(service_codes):
    code_to_label = {entry["code"]: entry["label"] for entry in SHIPPING_SERVICE_OPTIONS}
    labels = [code_to_label.get(code, code) for code in _normalize_shipping_services(service_codes)]
    return ", ".join(labels)


def shipping_services_dialog(stdscr, current_services):
    selected_codes = set(_normalize_shipping_services(current_services))
    selected = 0
    top_index = 0

    while True:
        h, w = stdscr.getmaxyx()
        width = min(78, w - 4)
        height = min(max(12, len(SHIPPING_SERVICE_OPTIONS) + 6), h - 2)
        y = max(1, (h - height) // 2)
        x = max(2, (w - width) // 2)

        win = curses.newwin(height, width, y, x)
        win.keypad(True)
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        win.addstr(0, 2, " Versand Services ")

        visible_rows = max(1, height - 4)
        if selected < top_index:
            top_index = selected
        if selected >= top_index + visible_rows:
            top_index = selected - visible_rows + 1

        for row_idx, option in enumerate(SHIPPING_SERVICE_OPTIONS[top_index:top_index + visible_rows]):
            real_idx = top_index + row_idx
            y_pos = 2 + row_idx
            checked = "[x]" if option["code"] in selected_codes else "[ ]"
            suffix = " (immer aktiv)" if option.get("locked") else ""
            line = _fit(f"{checked} {option['label']}{suffix}", width - 3)
            if real_idx == selected:
                win.attrset(curses.color_pair(2))
                win.addstr(y_pos, 1, line.ljust(width - 2))
                win.attrset(curses.color_pair(1))
            else:
                win.addstr(y_pos, 1, line.ljust(width - 2))

        footer = "Space/Enter umschalten  F2 Speichern  F9 Zurueck"
        win.attrset(curses.color_pair(3))
        win.addstr(height - 1, 1, _fit(footer, width - 2))
        win.attrset(curses.color_pair(1))
        win.refresh()

        key = win.get_wch()
        if key in (27, curses.KEY_F9):
            return _normalize_shipping_services(current_services)
        if key == curses.KEY_F2:
            return _normalize_shipping_services(list(selected_codes))
        if key == curses.KEY_DOWN:
            selected = move_selection(SHIPPING_SERVICE_OPTIONS, selected, 1)
            continue
        if key == curses.KEY_UP:
            selected = move_selection(SHIPPING_SERVICE_OPTIONS, selected, -1)
            continue
        if key in (" ", 10, 13, "\n", "\r", curses.KEY_ENTER):
            option = SHIPPING_SERVICE_OPTIONS[selected]
            if option.get("locked"):
                continue
            code = option["code"]
            if code in selected_codes:
                selected_codes.remove(code)
            else:
                selected_codes.add(code)


def active_shipping_carrier():
    carrier = (SETTINGS.get("shipping_carrier") or "gls").strip().lower()
    if carrier not in {"gls", "dhl_private", "post", "test"}:
        return "gls"
    return carrier


def effective_shipping_carrier(requested_carrier=None):
    carrier = (requested_carrier or active_shipping_carrier() or "gls").strip().lower()
    if carrier in IMPLEMENTED_SHIPPING_CARRIERS:
        return carrier
    return "gls"


def create_shipping_label(order, weight_kg=None, shipment_reference=None, service_codes=None, carrier=None):
    selected_carrier = (carrier or active_shipping_carrier()).strip().lower()
    if weight_kg is None:
        weight_kg, _total_grams = calculate_order_shipping_weight(order)
    if selected_carrier == "gls":
        return gls_create_label(
            order,
            weight_kg=weight_kg,
            shipment_reference=shipment_reference,
            service_codes=service_codes,
        )
    if selected_carrier == "post":
        return post_create_label(
            order,
            weight_kg=weight_kg,
            shipment_reference=shipment_reference,
            service_codes=service_codes,
        )
    if selected_carrier == "dhl_private":
        return dhl_private_create_label(
            order,
            weight_kg=weight_kg,
            shipment_reference=shipment_reference,
            service_codes=service_codes,
        )
    if selected_carrier == "test":
        return test_create_label(
            order,
            weight_kg=weight_kg,
            shipment_reference=shipment_reference,
            service_codes=service_codes,
        )
    raise RuntimeError(f"Dienstleister {selected_carrier} ist noch nicht implementiert.")


def reprint_shipping_label(label_row):
    carrier = (label_row.get("carrier") or "gls").strip().lower()
    if carrier == "gls":
        return gls_reprint_label(label_row)
    raise RuntimeError(f"Reprint fuer {carrier} ist noch nicht implementiert.")


def cancel_shipping_label(label_row):
    carrier = (label_row.get("carrier") or "gls").strip().lower()
    if carrier == "gls":
        return gls_cancel_label(label_row)
    raise RuntimeError(f"Storno fuer {carrier} ist noch nicht implementiert.")


def get_active_inventory_session():
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT session_id, session_name, created_at, status
        FROM inventory_sessions
        WHERE status = 'active'
        ORDER BY created_at DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    cur.close()
    con.close()
    return row


def create_inventory_session():
    con = db()
    cur = con.cursor()
    cur.execute("UPDATE inventory_sessions SET status = 'archived' WHERE status = 'active'")
    session_name = f"Inventur {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}"
    cur.execute(
        """
        INSERT INTO inventory_sessions (session_name)
        VALUES (%s)
        RETURNING session_id, session_name, created_at, status
        """,
        (session_name,),
    )
    session = cur.fetchone()
    cur.execute(
        """
        SELECT
            sku,
            name,
            regal,
            fach,
            platz,
            menge
        FROM items
        WHERE COALESCE(external_fulfillment, FALSE) = FALSE
        ORDER BY regal NULLS LAST, fach NULLS LAST, platz NULLS LAST, sku
        """
    )
    items = cur.fetchall()

    for index, item in enumerate(items, start=1):
        cur.execute(
            """
            INSERT INTO inventory_lines (
                session_id,
                line_no,
                sku,
                name,
                regal,
                fach,
                platz,
                soll_menge,
                ist_menge
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NULL)
            """,
            (
                session["session_id"],
                index,
                item["sku"],
                item["name"],
                item["regal"],
                item["fach"],
                item["platz"],
                item["menge"],
            ),
        )

    con.commit()
    cur.close()
    con.close()
    return session


def get_inventory_lines(session_id, differences_only=False):
    con = db()
    cur = con.cursor()
    where = ""
    if differences_only:
        where = "AND COALESCE(ist_menge, -1) <> soll_menge"

    cur.execute(
        f"""
        SELECT
            line_no,
            sku,
            name,
            regal,
            fach,
            platz,
            soll_menge,
            ist_menge
        FROM inventory_lines
        WHERE session_id = %s
        {where}
        ORDER BY line_no
        """,
        (session_id,),
    )
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def set_inventory_count(session_id, line_no, qty):
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        UPDATE inventory_lines
        SET ist_menge = %s
        WHERE session_id = %s AND line_no = %s
        """,
        (qty, session_id, line_no),
    )
    con.commit()
    cur.close()
    con.close()


def apply_inventory_session(session_id):
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        UPDATE items i
        SET menge = l.ist_menge,
            available = GREATEST(
                l.ist_menge - COALESCE(i.unavailable, COALESCE(i.reserved, 0)) - COALESCE(i.committed, 0),
                0
            ),
            dirty = TRUE,
            updated_at = NOW()
        FROM inventory_lines l
        WHERE l.session_id = %s
          AND l.ist_menge IS NOT NULL
          AND i.sku = l.sku
        """,
        (session_id,),
    )
    cur.execute(
        """
        UPDATE inventory_sessions
        SET status = 'applied'
        WHERE session_id = %s
        """,
        (session_id,),
    )
    con.commit()
    cur.close()
    con.close()


def _fit(text, width):
    text = str(text)
    if len(text) <= width:
        return text.ljust(width)
    return text[:width-1] + "…"


def _sort_location_value(value):
    if value is None:
        return (2, 1, "")

    text = str(value).strip()

    if text == "":
        return (2, 1, "")

    if text.isdigit():
        return (0, int(text), "")

    return (1, 0, text)


def normalize_regal(value):
    return normalize_location_value("regal", value)


def normalize_fach(value):
    return normalize_location_value("fach", value)


def normalize_platz(value):
    return normalize_location_value("platz", value)


def get_location_regex(field_name):
    setting_key = f"location_regex_{field_name}"
    configured = (SETTINGS.get(setting_key) or "").strip()
    if configured:
        return configured
    return DEFAULT_SETTINGS[setting_key]


def normalize_location_value(field_name, value):
    value = (value or "").strip()

    if value == "":
        return ""

    pattern = get_location_regex(field_name)
    try:
        if re.fullmatch(pattern, value):
            return value
    except re.error:
        return None

    return None


def validate_location_or_error(stdscr, field_name, raw_value):
    value = normalize_location_value(field_name, raw_value)
    if value is None:
        label = {"regal": "Regal", "fach": "Fach", "platz": "Platz"}[field_name]
        pattern = get_location_regex(field_name)
        message_box(stdscr, "Fehler", f"{label} passt nicht zu Regex: {pattern}"[:56])
        return None
    return value


def is_location_input_allowed(field_name, raw_value):
    value = (raw_value or "").strip()
    if value == "":
        return True
    return normalize_location_value(field_name, value) is not None


def validate_regal_or_error(stdscr, raw_value):
    return validate_location_or_error(stdscr, "regal", raw_value)


def format_row(row):
    status = row["sync_status"]

    if status == "local":
        status = "L"
    elif row["dirty"]:
        status = "D"
    else:
        status = "S"


    vals = [
        row["sku"],
        row["name"],
        row["regal"],
        row["fach"],
        row["platz"],
        str(row["menge"]),
        str(row["unavailable"]),
        str(row["committed"]),
        str(row["available"]),
        status
    ]
    cells = [_fit(vals[i], COLS[i][1]) for i in range(len(vals))]
    return " ".join(cells)


def format_header():
    header_cols = [
        ("SKU", 18),
        ("Name", 60),
        (t("col_shelf"), 7),
        (t("col_bin"), 6),
        (t("col_slot"), 7),
        (t("col_total"), 7),
        (t("col_unavailable"), 8),
        (t("col_committed"), 7),
        (t("col_available"), 7),
        ("S", 2),
    ]
    cells = [_fit(name, width) for name, width in header_cols]
    return " ".join(cells)


def _format_eur(value):
    if value is None:
        return "-"
    text = str(value).strip()
    if not text:
        return "-"
    return f"{text} EUR"


def clean_shopify_description(value):
    text = value or ""
    text = re.sub(r"(?i)<\s*br\s*/?\s*>", "\n", text)
    text = re.sub(r"(?i)</\s*p\s*>", "\n", text)
    text = re.sub(r"(?i)<\s*p[^>]*>", "", text)
    text = re.sub(r"(?i)<\s*/?\s*li\s*>", "\n", text)
    text = re.sub(r"(?i)<\s*/?\s*ul\s*>", "\n", text)
    text = re.sub(r"(?i)<\s*/?\s*ol\s*>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    lines = [line.strip() for line in text.splitlines()]
    cleaned = "\n".join(line for line in lines if line)
    return cleaned or "-"


def build_item_info_lines(item):
    lines = []
    lines.append(f"SKU: {item.get('sku') or '-'}")
    lines.append(f"Name: {item.get('name') or '-'}")
    lines.append(f"Barcode/GTIN: {item.get('barcode') or '-'}")
    lines.append(f"Shopify Status: {item.get('shopify_product_status') or '-'}")
    lines.append(f"VK Preis: {_format_eur(item.get('shopify_price'))}")
    lines.append(f"VK Vergleich: {_format_eur(item.get('shopify_compare_at_price'))}")
    lines.append(f"EK Kosten: {_format_eur(item.get('shopify_unit_cost'))}")

    weight_grams = item.get("shopify_weight_grams")
    lines.append(f"Gewicht: {weight_grams} g" if weight_grams is not None else "Gewicht: -")
    lines.append(f"Sync: {item.get('sync_status') or '-'}")
    lines.append(f"Lagerplatz: {(item.get('regal') or '-')}/{(item.get('fach') or '-')}/{(item.get('platz') or '-')}")
    return lines


def item_info_dialog(stdscr, item):
    h, w = stdscr.getmaxyx()
    width = min(max(84, int(w * 0.8)), w - 4)
    height = min(max(20, int(h * 0.82)), h - 2)
    y = max(1, (h - height) // 2)
    x = max(2, (w - width) // 2)

    info_lines = build_item_info_lines(item)
    description_lines = clean_shopify_description(item.get("shopify_description")).splitlines()
    description_top = 0

    while True:
        draw_shadow(stdscr, y, x, height, width)
        win = curses.newwin(height, width, y, x)
        win.keypad(True)
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        win.addstr(0, 2, " Produktdaten ")

        for index, line in enumerate(info_lines):
            y_line = 1 + index
            if y_line >= height - 4:
                break
            win.addstr(y_line, 2, line[: width - 4])

        desc_y = min(height - 5, 2 + len(info_lines))
        desc_height = max(4, height - desc_y - 2)
        desc_width = width - 4
        desc_win = win.derwin(desc_height, desc_width, desc_y, 2)
        desc_win.box()
        desc_win.addstr(0, 2, " Beschreibung ")

        wrapped_description = []
        for line in description_lines:
            wrapped_description.extend(textwrap.wrap(line, width=max(10, desc_width - 4)) or [""])

        visible_desc_rows = max(1, desc_height - 2)
        visible_desc = wrapped_description[description_top : description_top + visible_desc_rows]
        for index, line in enumerate(visible_desc):
            desc_win.addstr(1 + index, 2, line[: desc_width - 4])

        win.attrset(curses.color_pair(3))
        footer = " PgUp/PgDn oder Pfeile scrollen  F9/Esc schliessen "
        win.addstr(height - 1, 1, " " * (width - 2))
        win.addstr(height - 1, 1, footer[: width - 2])
        win.refresh()

        key = win.get_wch()
        if key in (27, curses.KEY_F9, curses.KEY_ENTER, "\n", "\r"):
            return
        if key == curses.KEY_NPAGE:
            description_top = min(max(0, len(wrapped_description) - visible_desc_rows), description_top + visible_desc_rows)
        elif key == curses.KEY_PPAGE:
            description_top = max(0, description_top - visible_desc_rows)
        elif key == curses.KEY_DOWN:
            description_top = min(max(0, len(wrapped_description) - visible_desc_rows), description_top + 1)
        elif key == curses.KEY_UP:
            description_top = max(0, description_top - 1)


def build_location_rows(items):
    grouped = {}

    for item in items:
        regal = (item["regal"] or "").strip()
        fach = "" if item["fach"] is None else str(item["fach"]).strip()
        grouped.setdefault(regal, {}).setdefault(fach, []).append(item)

    rows = []

    for regal in sorted(grouped, key=_sort_location_value):
        regal_label = f"Regal {regal}" if regal else "Ohne Regal"
        rows.append({
            "kind": "regal",
            "label": regal_label,
            "item": None,
        })

        faecher = grouped[regal]

        for fach in sorted(faecher, key=_sort_location_value):
            fach_label = f"  Fach {fach}" if fach else "  Ohne Fach"
            rows.append({
                "kind": "fach",
                "label": fach_label,
                "item": None,
            })

            fach_items = sorted(
                faecher[fach],
                key=lambda row: (
                    _sort_location_value(row["platz"]),
                    str(row["sku"]),
                ),
            )

            for item in fach_items:
                platz = "" if item["platz"] is None else str(item["platz"]).strip()
                platz_label = platz if platz else "-"
                rows.append({
                    "kind": "item",
                    "label": f"    {platz_label:>4}  {_fit(item['sku'], 18)} {_fit(item['name'], 22)}",
                    "item": item,
                })

    return rows


def move_selection(rows, selected, step):
    if not rows:
        return 0

    return max(0, min(len(rows) - 1, selected + step))


def get_selected_item(items, selected):
    if not items:
        return None

    if selected < 0 or selected >= len(items):
        return None

    return items[selected]


def get_selected_location_item(rows, selected):
    if not rows:
        return None

    if selected < 0 or selected >= len(rows):
        return None

    return rows[selected]["item"]


def draw_shadow(stdscr, y, x, h, w):
    max_y, max_x = stdscr.getmaxyx()
    if y + h + 1 >= max_y or x + w + 2 >= max_x:
        return
    shadow = curses.newwin(h, w, y + 1, x + 2)
    shadow.bkgd(" ", curses.color_pair(3))
    shadow.erase()
    shadow.refresh()


def draw_panel(win, title, lines, selected, top_index, active):
    h, w = win.getmaxyx()
    max_rows = max(0, h - 4)

    win.erase()
    win.box()
    panel_title = f" {title} "

    if active:
        win.attrset(curses.color_pair(2))
        win.addstr(0, 2, panel_title[:w-4])
        win.attrset(curses.color_pair(1))
    else:
        win.addstr(0, 2, panel_title[:w-4], curses.A_BOLD)

    visible = lines[top_index:top_index + max_rows]

    for i, line in enumerate(visible):
        y = 2 + i
        idx = top_index + i

        if idx == selected:
            win.attrset(curses.color_pair(2))
            win.addstr(y, 1, line[:w-2].ljust(w-2))
            win.attrset(curses.color_pair(1))
        else:
            win.addstr(y, 1, line[:w-2].ljust(w-2))

    win.refresh()


def draw_items_panel(win, items, selected, top_index, active):
    h, w = win.getmaxyx()
    max_rows = max(0, h - 5)

    win.erase()
    win.box()
    panel_title = f" {t('items_panel')} "

    if active:
        win.attrset(curses.color_pair(2))
        win.addstr(0, 2, panel_title[:w-4])
        win.attrset(curses.color_pair(1))
    else:
        win.addstr(0, 2, panel_title[:w-4], curses.A_BOLD)

    win.addstr(2, 1, format_header()[:w-2].ljust(w-2), curses.A_BOLD)

    visible = items[top_index:top_index + max_rows]

    for i, row in enumerate(visible):
        y = 3 + i
        line = format_row(row)
        idx = top_index + i

        if idx == selected:
            win.attrset(curses.color_pair(2))
            win.addstr(y, 1, line[:w-2].ljust(w-2))
            win.attrset(curses.color_pair(1))
        else:
            win.addstr(y, 1, line[:w-2].ljust(w-2))

    win.refresh()

def draw(stdscr, items, left_selected, left_top_index, location_rows, right_selected, right_top_index, active_pane, filter_text, show_secondary_help, external_mode):
    h, w = stdscr.getmaxyx()

    stdscr.attrset(curses.color_pair(1))
    stdscr.erase()
    stdscr.box()
    stdscr.addstr(0, 2, f" {t('app_title')} ")
    version_label = f" v{APP_VERSION} "
    version_x = max(2, w - len(version_label) - 2)
    stdscr.addstr(0, version_x, version_label[: max(0, w - version_x - 1)])
    inner_width = w - 4
    left_width = max(40, int(inner_width * 0.62))
    left_width = min(left_width, inner_width - 24)
    right_width = inner_width - left_width - 1

    if right_width < 20:
        left_width = max(30, inner_width - 21)
        right_width = inner_width - left_width - 1

    panel_height = max(6, h - 4)
    left_win = stdscr.derwin(panel_height, left_width, 1, 2)
    right_win = stdscr.derwin(panel_height, right_width, 1, 3 + left_width)

    draw_items_panel(left_win, items, left_selected, left_top_index, active_pane == "left")

    right_lines = [row["label"] for row in location_rows] if location_rows else [t("no_locations")]
    draw_panel(
        right_win,
        t("locations_panel"),
        right_lines,
        right_selected if location_rows else 0,
        right_top_index,
        active_pane == "right",
    )

    stdscr.attrset(curses.color_pair(3))

    if show_secondary_help:
        status = t("status_secondary")
    else:
        status = t("status_primary")
    focus = t("focus_items") if active_pane == "left" else t("focus_locations")
    if external_mode == "only":
        focus = focus[:-1] + t("view_external")

    stdscr.addstr(h-2, 0, " "*(w-1))

    if filter_text:
        stdscr.addstr(h-2, 0, t("filter_prefix", value=filter_text)[:w-1])
    else:
        stdscr.addstr(h-2, 0, focus[:w-1])

    stdscr.addstr(h-1, 0, " "*(w-1))
    stdscr.addstr(h-1, 0, status[:w-1])

    stdscr.refresh()


def message_box(stdscr, title, message):

    h, w = stdscr.getmaxyx()

    width = min(60, w-4)
    height = 6

    y = h//2 - height//2
    x = w//2 - width//2

    draw_shadow(stdscr, y, x, height, width)

    win = curses.newwin(height, width, y, x)
    win.bkgd(" ", curses.color_pair(1))
    win.box()

    win.addstr(0, 2, f" {title} ")
    win.addstr(2, 2, message[:width-4])
    win.addstr(4, 2, t("press_key"))

    win.refresh()
    key = stdscr.get_wch()


def confirm_box(stdscr, title, message, default_yes=True):

    h, w = stdscr.getmaxyx()

    width = min(60, w-4)
    height = 6

    y = h//2 - height//2
    x = w//2 - width//2

    draw_shadow(stdscr, y, x, height, width)
    curses.flushinp()

    win = curses.newwin(height, width, y, x)
    win.keypad(True)
    win.bkgd(" ", curses.color_pair(1))
    win.box()

    win.addstr(0, 2, f" {title} ")
    win.addstr(2, 2, message[:width-4])
    prompt = t("confirm_yes_no")
    if default_yes:
        prompt = f"{prompt}  Enter=Ja"
    else:
        prompt = f"{prompt}  Enter=Nein"
    win.addstr(4, 2, prompt[:width - 4])

    win.refresh()

    while True:

        key = win.get_wch()

        if key in ("j", "J", "y", "Y"):
            return True

        if key in ('\n', '\r', 10, 13, curses.KEY_ENTER):
            return bool(default_yes)

        if key in ("n", "N", 27):
            return False

def form_dialog(stdscr, title, fields, initial_active=0, footer_text=None, extra_actions=None, field_validators=None):

    h, w = stdscr.getmaxyx()

    longest_label = max((len(field["label"]) for field in fields), default=10)
    preferred_width = longest_label + 68
    width = min(max(70, preferred_width), w - 4)
    height = len(fields) + 6

    y = max(1, (h - height) // 2)
    x = max(2, (w - width) // 2)

    draw_shadow(stdscr, y, x, height, width)

    win = curses.newwin(height, width, y, x)
    win.keypad(True)
    win.bkgd(" ", curses.color_pair(1))

    values = [f["value"] for f in fields]
    active = max(0, min(initial_active, len(fields) - 1))
    cursor_positions = [len(value) for value in values]
    scroll_offsets = [0 for _ in values]
    footer = footer_text or "Enter weiter  ↑↓ wechseln  F2 Speichern  F9 Abbrechen"
    extra_actions = extra_actions or []
    field_validators = field_validators or {}

    def normalize_view(index, field_width):
        field_width = max(1, field_width)
        value_len = len(values[index])
        max_scroll = max(0, value_len - field_width)

        cursor = max(0, min(cursor_positions[index], value_len))
        cursor_positions[index] = cursor

        scroll = max(0, min(scroll_offsets[index], max_scroll))
        if cursor < scroll:
            scroll = cursor
        elif cursor > scroll + field_width - 1:
            scroll = cursor - field_width + 1
        scroll_offsets[index] = max(0, min(scroll, max_scroll))

    while True:

        win.erase()
        win.box()
        win.addstr(0, 2, f" {title} ")

        for i, field in enumerate(fields):

            row = 2 + i
            label = field["label"]
            val = values[i]

            if i == active:
                win.attron(curses.color_pair(2))

            win.addstr(row, 2, f"{label}: ")
            
            xpos = len(label) + 4

            if i == active:
                win.attron(curses.color_pair(2))
                field_width = max(1, width - xpos - 2)
                normalize_view(i, field_width)
                visible = val[scroll_offsets[i]: scroll_offsets[i] + field_width]
            else:
                visible = val[-(width - xpos - 2):]

            win.addstr(row, xpos, visible.ljust(width - xpos - 2))




        win.addstr(height - 2, 2, footer[:width - 4])
        
        cursor_y = 2 + active

        label = fields[active]["label"]
        val = values[active]

        xpos = len(label) + 4
        field_width = max(1, width - xpos - 2)
        normalize_view(active, field_width)
        cursor_x = xpos + min(max(0, cursor_positions[active] - scroll_offsets[active]), field_width - 1)

        win.move(cursor_y, cursor_x)

        win.refresh()


        key = win.get_wch()

        if key in (27, curses.KEY_F9):
            return None

        for action in extra_actions:
            if key in action["keys"]:
                return {
                    "__action__": action["name"],
                    "__values__": {fields[i]["name"]: values[i] for i in range(len(fields))},
                    "__active__": active,
                }

        if key == curses.KEY_F2:
            return {fields[i]["name"]: values[i] for i in range(len(fields))}

        if key in (curses.KEY_DOWN, '\n'):
            active = (active + 1) % len(fields)
            continue

        if key == curses.KEY_UP:
            active = (active - 1) % len(fields)
            continue

        if key in (curses.KEY_BACKSPACE, 127, 8, '\x7f', '\b'):
            pos = cursor_positions[active]
            if pos > 0:
                values[active] = values[active][:pos - 1] + values[active][pos:]
                cursor_positions[active] = pos - 1
            continue

        if key == curses.KEY_DC:
            pos = cursor_positions[active]
            if pos < len(values[active]):
                values[active] = values[active][:pos] + values[active][pos + 1:]
            continue

        if key == curses.KEY_LEFT:
            if cursor_positions[active] > 0:
                cursor_positions[active] -= 1
            continue

        if key == curses.KEY_RIGHT:
            if cursor_positions[active] < len(values[active]):
                cursor_positions[active] += 1
            continue

        if key == curses.KEY_HOME:
            cursor_positions[active] = 0
            continue

        if key == curses.KEY_END:
            cursor_positions[active] = len(values[active])
            continue

        elif isinstance(key, str):
            if key.isprintable():
                pos = cursor_positions[active]
                candidate = values[active][:pos] + key + values[active][pos:]
                field_name = fields[active]["name"]
                validator = field_validators.get(field_name)
                if validator and not validator(candidate):
                    curses.beep()
                    continue
                values[active] = candidate
                cursor_positions[active] = pos + 1

def search_dialog(stdscr, initial):

    curses.curs_set(1)

    h, w = stdscr.getmaxyx()

    width = 60
    height = 5

    y = h//2 - height//2
    x = w//2 - width//2

    draw_shadow(stdscr, y, x, height, width)

    win = curses.newwin(height, width, y, x)
    win.keypad(True)
    win.bkgd(" ", curses.color_pair(1))

    value = initial or ""

    while True:

        win.erase()
        win.box()

        win.addstr(0, 2, f" {t('search')} ")

        win.addstr(2, 2, f"{t('search')}:")

        win.attron(curses.color_pair(2))

        field_width = width - 12

        field = value[-field_width:]
        win.addstr(2, 10, field[:field_width].ljust(field_width))
        
        win.attroff(curses.color_pair(2))

        win.addstr(height-1, 2, t("search_footer"))

        cursor_pos = min(len(value), field_width - 1)
        win.move(2, 10 + cursor_pos)

        win.refresh()

        key = win.get_wch()

        if key in (10, 13, '\n', '\r', curses.KEY_ENTER):
            return value.strip()

        if key in (27, curses.KEY_F9):
            return initial

        if key in (curses.KEY_BACKSPACE, 127, 8, '\x7f', '\b'):
            value = value[:-1]
            continue

        elif isinstance(key, str):
            if key not in ('\n', '\r', '\t'):
                value += key


def order_jump_dialog(stdscr, initial):
    value = search_dialog(stdscr, initial)
    if value is None:
        return initial
    return value.strip()


def _parse_lpstat_printers(output):
    printers = []

    for line in output.splitlines():
        line = line.strip()
        if not line.startswith("printer "):
            continue

        parts = line.split()
        if len(parts) < 2:
            continue

        name = parts[1]
        detail = line[len(f"printer {name}"):].strip()
        printers.append({"name": name, "detail": detail})

    return printers


def _lpstat_env():
    env = os.environ.copy()
    env["LC_ALL"] = "C"
    env["LANG"] = "C"
    return env


def _parse_cups_media_options(output):
    values = []
    seen = set()
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if ":" not in line:
            continue
        key, remainder = line.split(":", 1)
        key = key.strip().split("/", 1)[0]
        if key not in {"PageSize", "PageRegion", "media"}:
            continue
        for token in remainder.strip().split():
            raw_value = token.lstrip("*").strip()
            if not raw_value:
                continue
            if "/" in raw_value:
                value, label = raw_value.split("/", 1)
            else:
                value, label = raw_value, raw_value
            value = value.strip()
            label = label.strip() or value
            if not value:
                continue
            if value not in seen:
                seen.add(value)
                values.append({"value": value, "label": label})
    return values


def get_cups_printers():
    try:
        PRINT_LOGGER.debug("Lade Drucker mit lpstat -p")
        result = subprocess.run(
            ["lpstat", "-p"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
            env=_lpstat_env(),
        )
    except FileNotFoundError:
        return [], None, "lpstat/Drucksystem ist auf diesem System nicht verfuegbar."
    except subprocess.CalledProcessError as exc:
        error_text = (exc.stderr or str(exc)).strip()
        return [], None, error_text or "Drucker konnten nicht geladen werden."

    printers = _parse_lpstat_printers(result.stdout)
    default_printer = None

    try:
        PRINT_LOGGER.debug("Lade Standarddrucker mit lpstat -d")
        default_result = subprocess.run(
            ["lpstat", "-d"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
            env=_lpstat_env(),
        )
        prefix = "system default destination: "
        for line in default_result.stdout.splitlines():
            if line.startswith(prefix):
                default_printer = line[len(prefix):].strip() or None
                break
    except (FileNotFoundError, subprocess.CalledProcessError):
        default_printer = None

    return printers, default_printer, None


def get_cups_printer_media_options(printer_name):
    printer = (printer_name or "").strip()
    if not printer:
        return [], "Bitte zuerst einen Drucker waehlen."
    try:
        result = subprocess.run(
            ["lpoptions", "-p", printer, "-l"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
            env=_lpstat_env(),
        )
    except FileNotFoundError:
        return [], "lpoptions/CUPS ist auf diesem System nicht verfuegbar."
    except subprocess.CalledProcessError as exc:
        error_text = (exc.stderr or str(exc)).strip()
        return [], error_text or "Druckerformate konnten nicht geladen werden."
    options = _parse_cups_media_options(result.stdout)
    return options, None


def cups_media_dialog(stdscr, printer_name, current_value, title):
    options, error = get_cups_printer_media_options(printer_name)
    if error:
        message_box(stdscr, "Formate", error[:56])
        return current_value
    if not options:
        message_box(stdscr, "Formate", "Keine CUPS-Formate fuer Drucker gefunden.")
        return current_value
    return choice_dialog(stdscr, title, options, current_value)


def cups_printer_dialog(stdscr, current_printer):
    selected_name = current_printer.strip()

    while True:
        printers, default_printer, error = get_cups_printers()
        if error:
            message_box(stdscr, t("printer_error"), error[:56])
            return current_printer

        options = [{"name": "", "detail": t("printer_none")}]
        options.extend(printers)

        selected = 0
        if selected_name:
            for index, printer in enumerate(options):
                if printer["name"] == selected_name:
                    selected = index
                    break

        top_index = 0

        while True:
            h, w = stdscr.getmaxyx()
            width = min(90, w - 4)
            height = min(max(10, len(options) + 5), h - 2)
            y = max(1, (h - height) // 2)
            x = max(2, (w - width) // 2)

            draw_shadow(stdscr, y, x, height, width)

            win = curses.newwin(height, width, y, x)
            win.keypad(True)
            win.bkgd(" ", curses.color_pair(1))
            win.erase()
            win.box()
            win.addstr(0, 2, f" {t('printer_dialog')} ")

            visible_rows = max(1, height - 4)
            if selected < top_index:
                top_index = selected
            if selected >= top_index + visible_rows:
                top_index = selected - visible_rows + 1

            lines = []
            for printer in options:
                name = printer["name"] or t("printer_empty")
                markers = []
                if printer["name"] == current_printer:
                    markers.append(t("printer_active"))
                if printer["name"] and printer["name"] == default_printer:
                    markers.append(t("printer_default"))
                suffix = f" [{' / '.join(markers)}]" if markers else ""
                lines.append(_fit(f"{name}{suffix}", width - 4))

            for row_index, line in enumerate(lines[top_index:top_index + visible_rows]):
                real_index = top_index + row_index
                y_pos = 2 + row_index

                if real_index == selected:
                    win.attrset(curses.color_pair(2))
                    win.addstr(y_pos, 1, line[:width - 2].ljust(width - 2))
                    win.attrset(curses.color_pair(1))
                else:
                    win.addstr(y_pos, 1, line[:width - 2].ljust(width - 2))

            detail = options[selected]["detail"] if options else ""
            footer = t("printer_reload_footer")
            if detail:
                footer = _fit(detail, width - 4)
            win.addstr(height - 2, 2, footer[:width - 4])
            win.refresh()

            key = win.get_wch()

            if key in (27, curses.KEY_F9):
                return current_printer
            if key == curses.KEY_F5:
                break
            if key == curses.KEY_DOWN:
                selected = move_selection(options, selected, 1)
            elif key == curses.KEY_UP:
                selected = move_selection(options, selected, -1)
            elif key == curses.KEY_NPAGE:
                selected = move_selection(options, selected, visible_rows)
            elif key == curses.KEY_PPAGE:
                selected = move_selection(options, selected, -visible_rows)
            elif key in (10, 13, "\n", "\r", curses.KEY_ENTER):
                return options[selected]["name"]


def get_language_options():
    return [
        {"value": "de", "label": t("lang_de")},
        {"value": "en", "label": t("lang_en")},
    ]


def get_theme_options():
    label_map = {
        "blue": t("theme_blue"),
        "green": t("theme_green"),
        "mono": t("theme_mono"),
        "megatrends": t("theme_megatrends"),
        "smoth": t("theme_smoth"),
        "norton": t("theme_norton"),
        "gold-standard": t("theme_gold_standard"),
        "subtile": t("theme_subtile"),
        "monokai": t("theme_monokai"),
    }
    options = []
    for name in sorted(get_all_themes()):
        options.append({"value": name, "label": label_map.get(name, name)})
    return options


def choice_dialog(stdscr, title, options, current_value, cancel_returns_none=False):
    selected = 0
    for index, option in enumerate(options):
        if option["value"] == current_value:
            selected = index
            break

    top_index = 0
    while True:
        h, w = stdscr.getmaxyx()
        width = min(72, w - 4)
        height = min(max(10, len(options) + 5), h - 2)
        y = max(1, (h - height) // 2)
        x = max(2, (w - width) // 2)

        draw_shadow(stdscr, y, x, height, width)
        win = curses.newwin(height, width, y, x)
        win.keypad(True)
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        win.addstr(0, 2, f" {title} ")

        visible_rows = max(1, height - 4)
        if selected < top_index:
            top_index = selected
        if selected >= top_index + visible_rows:
            top_index = selected - visible_rows + 1

        for row_index, option in enumerate(options[top_index:top_index + visible_rows]):
            real_index = top_index + row_index
            line = f"{option['label']} ({option['value']})"
            y_pos = 2 + row_index
            if real_index == selected:
                win.attrset(curses.color_pair(2))
                win.addstr(y_pos, 1, _fit(line, width - 2))
                win.attrset(curses.color_pair(1))
            else:
                win.addstr(y_pos, 1, _fit(line, width - 2))

        win.addstr(height - 2, 2, t("pick_cancel")[: width - 4])
        win.refresh()

        key = win.get_wch()
        if key in (27, curses.KEY_F9):
            if cancel_returns_none:
                return None
            return current_value
        if key == curses.KEY_DOWN:
            selected = move_selection(options, selected, 1)
        elif key == curses.KEY_UP:
            selected = move_selection(options, selected, -1)
        elif key == curses.KEY_NPAGE:
            selected = move_selection(options, selected, visible_rows)
        elif key == curses.KEY_PPAGE:
            selected = move_selection(options, selected, -visible_rows)
        elif key in (10, 13, "\n", "\r", curses.KEY_ENTER):
            return options[selected]["value"]


def settings_dialog(stdscr):
    global SETTINGS

    values = {
        "db_host": SETTINGS["db_host"],
        "db_name": SETTINGS["db_name"],
        "db_user": SETTINGS["db_user"],
        "db_pass": SETTINGS["db_pass"],
        "language": (SETTINGS.get("language") or DEFAULT_SETTINGS["language"]).strip().lower(),
        "color_theme": (SETTINGS.get("color_theme") or DEFAULT_SETTINGS["color_theme"]).strip().lower(),
        "color_theme_file": SETTINGS.get("color_theme_file", ""),
        "printer_uri": SETTINGS["printer_uri"],
        "printer_model": SETTINGS["printer_model"],
        "label_size": SETTINGS["label_size"],
        "label_font_regular": SETTINGS.get("label_font_regular", ""),
        "label_font_condensed": SETTINGS.get("label_font_condensed", ""),
        "location_regex_regal": SETTINGS.get("location_regex_regal", DEFAULT_SETTINGS["location_regex_regal"]),
        "location_regex_fach": SETTINGS.get("location_regex_fach", DEFAULT_SETTINGS["location_regex_fach"]),
        "location_regex_platz": SETTINGS.get("location_regex_platz", DEFAULT_SETTINGS["location_regex_platz"]),
        "picklist_printer": SETTINGS["picklist_printer"],
        "delivery_note_printer": SETTINGS["delivery_note_printer"],
        "delivery_note_format": _normalize_shipping_label_format(
            SETTINGS.get("delivery_note_format", DEFAULT_SETTINGS.get("delivery_note_format", "A4"))
        ),
        "shipping_label_printer": SETTINGS.get("shipping_label_printer", ""),
        "shipping_label_printer_gls": SETTINGS.get("shipping_label_printer_gls", ""),
        "shipping_label_printer_dhl": SETTINGS.get("shipping_label_printer_dhl", ""),
        "shipping_label_printer_dhl_private": SETTINGS.get("shipping_label_printer_dhl_private", SETTINGS.get("shipping_label_printer_dhl", "")),
        "shipping_label_printer_post": SETTINGS.get("shipping_label_printer_post", ""),
        "shipping_carrier": (SETTINGS.get("shipping_carrier") or "gls").strip().lower(),
        "shipping_label_output_dir": SETTINGS.get("shipping_label_output_dir", ""),
        "shipping_label_format": (SETTINGS.get("shipping_label_format") or "A6").strip().upper(),
        "shipping_label_format_gls": _normalize_shipping_label_format(
            SETTINGS.get("shipping_label_format_gls", SETTINGS.get("shipping_label_format", "A6"))
        ),
        "shipping_label_format_dhl": _normalize_shipping_label_format(
            SETTINGS.get("shipping_label_format_dhl", "A5")
        ),
        "shipping_label_format_dhl_private": _normalize_shipping_label_format(
            SETTINGS.get("shipping_label_format_dhl_private", SETTINGS.get("shipping_label_format_dhl", "A5"))
        ),
        "shipping_label_format_post": _normalize_shipping_label_format(
            SETTINGS.get("shipping_label_format_post", "100x62")
        ),
        "shipping_services": _normalize_shipping_services(SETTINGS.get("shipping_services", [])),
        "shipping_services_display": _shipping_services_summary(SETTINGS.get("shipping_services", [])),
        "shipping_packaging_weight_grams": str(
            SETTINGS.get("shipping_packaging_weight_grams", DEFAULT_SETTINGS.get("shipping_packaging_weight_grams", 400))
        ),
        "gls_api_url": SETTINGS.get("gls_api_url", ""),
        "gls_user": SETTINGS.get("gls_user", ""),
        "gls_password": SETTINGS.get("gls_password", ""),
        "gls_contact_id": SETTINGS.get("gls_contact_id", ""),
        "post_api_url": SETTINGS.get("post_api_url", ""),
        "post_api_key": SETTINGS.get("post_api_key", ""),
        "post_api_secret": SETTINGS.get("post_api_secret", ""),
        "post_user": SETTINGS.get("post_user", ""),
        "post_password": SETTINGS.get("post_password", ""),
        "post_partner_id": SETTINGS.get("post_partner_id", ""),
        "dhl_private_api_url": SETTINGS.get("dhl_private_api_url", ""),
        "dhl_private_api_test_url": SETTINGS.get("dhl_private_api_test_url", ""),
        "dhl_private_api_key": SETTINGS.get("dhl_private_api_key", ""),
        "dhl_private_api_secret": SETTINGS.get("dhl_private_api_secret", ""),
        "dhl_private_use_test_api": "ja" if SETTINGS.get("dhl_private_use_test_api", True) else "nein",
        "pdf_output_dir": SETTINGS["pdf_output_dir"],
        "delivery_note_template_path": SETTINGS.get("delivery_note_template_path", ""),
        "delivery_note_logo_source": SETTINGS.get("delivery_note_logo_source", ""),
        "delivery_note_sender_name": SETTINGS["delivery_note_sender_name"],
        "delivery_note_sender_street": SETTINGS["delivery_note_sender_street"],
        "delivery_note_sender_city": SETTINGS["delivery_note_sender_city"],
        "delivery_note_sender_email": SETTINGS["delivery_note_sender_email"],
    }
    tabs = [
        {
            "title": "Allgemein",
            "fields": [
                ("db_host", "field_db_host"),
                ("db_name", "field_db_name"),
                ("db_user", "field_db_user"),
                ("db_pass", "field_db_pass"),
                ("language", "field_language"),
                ("color_theme", "field_theme"),
                ("color_theme_file", "field_theme_file"),
            ],
        },
        {
            "title": "Lagerlabel",
            "fields": [
                ("printer_uri", "field_printer_uri"),
                ("printer_model", "field_printer_model"),
                ("label_size", "field_label_size"),
                ("label_font_regular", "field_label_font_regular"),
                ("label_font_condensed", "field_label_font_condensed"),
                ("location_regex_regal", "field_regex_regal"),
                ("location_regex_fach", "field_regex_fach"),
                ("location_regex_platz", "field_regex_platz"),
            ],
        },
        {
            "title": "Drucker",
            "fields": [
                ("picklist_printer", "field_picklist_printer"),
                ("delivery_note_printer", "field_delivery_printer"),
                ("delivery_note_format", "field_delivery_format"),
                ("shipping_label_printer_gls", "field_shipping_printer_gls"),
                ("shipping_label_printer_dhl_private", "field_shipping_printer_dhl_private"),
                ("shipping_label_printer_post", "field_shipping_printer_post"),
                ("shipping_label_printer", "field_shipping_printer_fallback"),
            ],
        },
        {
            "title": "Versand",
            "fields": [
                ("shipping_carrier", "field_shipping_carrier"),
                ("shipping_label_output_dir", "field_shipping_label_output_dir"),
                ("shipping_label_format_gls", "field_shipping_format_gls"),
                ("shipping_label_format_dhl_private", "field_shipping_format_dhl_private"),
                ("shipping_label_format_post", "field_shipping_format_post"),
                ("shipping_services_display", "field_shipping_services"),
                ("shipping_packaging_weight_grams", "field_shipping_packaging_weight"),
                ("gls_api_url", "field_gls_api_url"),
                ("gls_user", "field_gls_user"),
                ("gls_password", "field_gls_password"),
                ("gls_contact_id", "field_gls_contact_id"),
                ("post_api_url", "field_post_api_url"),
                ("post_api_key", "field_post_api_key"),
                ("post_api_secret", "field_post_api_secret"),
                ("post_user", "field_post_user"),
                ("post_password", "field_post_password"),
                ("post_partner_id", "field_post_partner_id"),
                ("dhl_private_api_url", "field_dhl_private_api_url"),
                ("dhl_private_api_test_url", "field_dhl_private_api_test_url"),
                ("dhl_private_api_key", "field_dhl_private_api_key"),
                ("dhl_private_api_secret", "field_dhl_private_api_secret"),
                ("dhl_private_use_test_api", "field_dhl_private_use_test_api"),
            ],
        },
        {
            "title": "Lieferschein",
            "fields": [
                ("pdf_output_dir", "field_pdf_dir"),
                ("delivery_note_template_path", "field_template"),
                ("delivery_note_logo_source", "field_logo"),
                ("delivery_note_sender_name", "field_sender_name"),
                ("delivery_note_sender_street", "field_sender_street"),
                ("delivery_note_sender_city", "field_sender_city"),
                ("delivery_note_sender_email", "field_sender_email"),
            ],
        },
    ]
    active_tab = 0
    active_field_by_tab = [0 for _ in tabs]
    cursor_positions = {name: len(str(values.get(name, ""))) for tab in tabs for name, _ in tab["fields"]}
    scroll_offsets = {name: 0 for tab in tabs for name, _ in tab["fields"]}

    def normalize_view(field_name, field_width):
        field_width = max(1, field_width)
        value = str(values.get(field_name, ""))
        value_len = len(value)
        max_scroll = max(0, value_len - field_width)
        cursor = max(0, min(cursor_positions.get(field_name, 0), value_len))
        cursor_positions[field_name] = cursor
        scroll = max(0, min(scroll_offsets.get(field_name, 0), max_scroll))
        if cursor < scroll:
            scroll = cursor
        elif cursor > scroll + field_width - 1:
            scroll = cursor - field_width + 1
        scroll_offsets[field_name] = max(0, min(scroll, max_scroll))

    while True:
        tab = tabs[active_tab]
        tab_fields = tab["fields"]
        if active_field_by_tab[active_tab] >= len(tab_fields):
            active_field_by_tab[active_tab] = max(0, len(tab_fields) - 1)
        if active_field_by_tab[active_tab] < 0:
            active_field_by_tab[active_tab] = 0

        active_index = active_field_by_tab[active_tab] if tab_fields else 0
        active_name = tab_fields[active_index][0] if tab_fields else ""

        h, w = stdscr.getmaxyx()
        max_label = max((len(t(label_key)) for entry in tabs for _, label_key in entry["fields"]), default=12)
        max_fields = max((len(entry["fields"]) for entry in tabs), default=1)
        width = min(max(90, max_label + 70), w - 4)
        height = min(max(16, max_fields + 9), h - 2)
        y = max(1, (h - height) // 2)
        x = max(2, (w - width) // 2)

        win = curses.newwin(height, width, y, x)
        win.keypad(True)
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        win.addstr(0, 2, f" {t('settings')} ")

        tab_x = 2
        for i, entry in enumerate(tabs):
            label = f" {entry['title']} "
            if tab_x + len(label) >= width - 2:
                break
            if i == active_tab:
                win.attrset(curses.color_pair(2))
                win.addstr(1, tab_x, label)
                win.attrset(curses.color_pair(1))
            else:
                win.addstr(1, tab_x, label)
            tab_x += len(label) + 1

        label_width = max((len(t(label_key)) for _, label_key in tab_fields), default=10)
        field_x = min(width - 20, label_width + 4)
        field_width = max(1, width - field_x - 3)

        for idx, (name, label_key) in enumerate(tab_fields):
            row = 3 + idx
            label = t(label_key)
            value = str(values.get(name, ""))
            win.addstr(row, 2, f"{label}:")
            normalize_view(name, field_width)
            start = scroll_offsets[name]
            visible = value[start:start + field_width]
            if idx == active_index:
                win.attrset(curses.color_pair(2))
                win.addstr(row, field_x, visible.ljust(field_width))
                win.attrset(curses.color_pair(1))
            else:
                win.addstr(row, field_x, visible.ljust(field_width))

        for filler in range(3 + len(tab_fields), height - 2):
            win.addstr(filler, 1, " " * (width - 2))

        footer = "F5 <- Tab  F6 Tab ->  F3 Drucker  F4 Format  Enter Auswahl  F2 Speichern  F9 Zurueck"
        win.attrset(curses.color_pair(3))
        win.addstr(height - 2, 1, " " * (width - 2))
        win.addstr(height - 2, 1, _fit(footer, width - 2))
        win.attrset(curses.color_pair(1))

        if active_name:
            normalize_view(active_name, field_width)
            cursor_x = field_x + min(max(0, cursor_positions[active_name] - scroll_offsets[active_name]), field_width - 1)
            win.move(3 + active_index, cursor_x)
        win.refresh()

        key = win.get_wch()
        if key in (27, curses.KEY_F9):
            return
        if key == curses.KEY_F2:
            break
        if key == curses.KEY_F5:
            active_tab = (active_tab - 1) % len(tabs)
            continue
        if key == curses.KEY_F6:
            active_tab = (active_tab + 1) % len(tabs)
            continue
        if key == curses.KEY_DOWN:
            active_field_by_tab[active_tab] = (active_index + 1) % max(1, len(tab_fields))
            continue
        if key == curses.KEY_UP:
            active_field_by_tab[active_tab] = (active_index - 1) % max(1, len(tab_fields))
            continue

        if not active_name:
            continue

        if active_name == "shipping_services_display":
            if key in (curses.KEY_LEFT, curses.KEY_RIGHT, curses.KEY_HOME, curses.KEY_END, curses.KEY_BACKSPACE, 127, 8, '\x7f', '\b', curses.KEY_DC):
                continue

        if key in (curses.KEY_BACKSPACE, 127, 8, '\x7f', '\b'):
            pos = cursor_positions[active_name]
            if pos > 0:
                value = str(values.get(active_name, ""))
                values[active_name] = value[:pos - 1] + value[pos:]
                cursor_positions[active_name] = pos - 1
            continue
        if key == curses.KEY_DC:
            pos = cursor_positions[active_name]
            value = str(values.get(active_name, ""))
            if pos < len(value):
                values[active_name] = value[:pos] + value[pos + 1:]
            continue
        if key == curses.KEY_LEFT:
            cursor_positions[active_name] = max(0, cursor_positions[active_name] - 1)
            continue
        if key == curses.KEY_RIGHT:
            cursor_positions[active_name] = min(len(str(values.get(active_name, ""))), cursor_positions[active_name] + 1)
            continue
        if key == curses.KEY_HOME:
            cursor_positions[active_name] = 0
            continue
        if key == curses.KEY_END:
            cursor_positions[active_name] = len(str(values.get(active_name, "")))
            continue

        if key == curses.KEY_F3 and active_name in {
            "picklist_printer",
            "delivery_note_printer",
            "shipping_label_printer",
            "shipping_label_printer_gls",
            "shipping_label_printer_dhl",
            "shipping_label_printer_dhl_private",
            "shipping_label_printer_post",
        }:
            values[active_name] = cups_printer_dialog(stdscr, values[active_name])
            cursor_positions[active_name] = len(str(values[active_name]))
            continue

        if key == curses.KEY_F4 and active_name in {
            "delivery_note_format",
            "shipping_label_format_gls",
            "shipping_label_format_dhl",
            "shipping_label_format_dhl_private",
            "shipping_label_format_post",
        }:
            printer_name = ""
            title = "Druckformat"
            if active_name == "delivery_note_format":
                printer_name = values.get("delivery_note_printer", "")
                title = "Lieferschein Format"
            elif active_name == "shipping_label_format_gls":
                printer_name = values.get("shipping_label_printer_gls") or values.get("shipping_label_printer")
                title = "GLS Labelformat"
            elif active_name == "shipping_label_format_dhl":
                printer_name = values.get("shipping_label_printer_dhl") or values.get("shipping_label_printer")
                title = "DHL Labelformat"
            elif active_name == "shipping_label_format_dhl_private":
                printer_name = values.get("shipping_label_printer_dhl_private") or values.get("shipping_label_printer")
                title = "DHL Privat Labelformat"
            elif active_name == "shipping_label_format_post":
                printer_name = values.get("shipping_label_printer_post") or values.get("shipping_label_printer")
                title = "POST Labelformat"
            values[active_name] = cups_media_dialog(stdscr, printer_name, values[active_name], title)
            cursor_positions[active_name] = len(str(values[active_name]))
            continue

        if key in (10, 13, "\n", "\r", curses.KEY_ENTER):
            if active_name == "language":
                values["language"] = choice_dialog(stdscr, t("pick_language"), get_language_options(), values["language"])
            elif active_name == "color_theme":
                values["color_theme"] = choice_dialog(stdscr, t("pick_theme"), get_theme_options(), values["color_theme"])
            elif active_name in {
                "picklist_printer",
                "delivery_note_printer",
                "shipping_label_printer",
                "shipping_label_printer_gls",
                "shipping_label_printer_dhl",
                "shipping_label_printer_dhl_private",
                "shipping_label_printer_post",
            }:
                values[active_name] = cups_printer_dialog(stdscr, values[active_name])
            elif active_name == "shipping_carrier":
                values["shipping_carrier"] = choice_dialog(
                    stdscr,
                    "Versand Dienstleister",
                    [
                        {"value": "gls", "label": "GLS"},
                        {"value": "dhl_private", "label": "DHL Privat"},
                        {"value": "post", "label": "POST (INTERNETMARKE)"},
                        {"value": "test", "label": "TEST"},
                    ],
                    values["shipping_carrier"],
                )
            elif active_name in {"shipping_label_format_gls", "shipping_label_format_dhl", "shipping_label_format_dhl_private", "shipping_label_format_post"}:
                printer_name = ""
                title = "Labelformat"
                if active_name == "shipping_label_format_gls":
                    printer_name = values.get("shipping_label_printer_gls") or values.get("shipping_label_printer")
                    title = "GLS Labelformat"
                elif active_name == "shipping_label_format_dhl":
                    printer_name = values.get("shipping_label_printer_dhl") or values.get("shipping_label_printer")
                    title = "DHL Labelformat"
                elif active_name == "shipping_label_format_dhl_private":
                    printer_name = values.get("shipping_label_printer_dhl_private") or values.get("shipping_label_printer")
                    title = "DHL Privat Labelformat"
                elif active_name == "shipping_label_format_post":
                    printer_name = values.get("shipping_label_printer_post") or values.get("shipping_label_printer")
                    title = "POST Labelformat"
                values[active_name] = cups_media_dialog(stdscr, printer_name, values[active_name], title)
            elif active_name == "delivery_note_format":
                values[active_name] = cups_media_dialog(
                    stdscr,
                    values.get("delivery_note_printer", ""),
                    values[active_name],
                    "Lieferschein Format",
                )
            elif active_name == "dhl_private_use_test_api":
                values["dhl_private_use_test_api"] = choice_dialog(
                    stdscr,
                    "DHL Privat Testmodus",
                    [
                        {"value": "ja", "label": "Ja"},
                        {"value": "nein", "label": "Nein"},
                    ],
                    values["dhl_private_use_test_api"],
                )
            elif active_name == "shipping_services_display":
                values["shipping_services"] = shipping_services_dialog(stdscr, values.get("shipping_services", []))
                values["shipping_services_display"] = _shipping_services_summary(values["shipping_services"])
            else:
                active_field_by_tab[active_tab] = (active_index + 1) % max(1, len(tab_fields))
            cursor_positions[active_name] = len(str(values.get(active_name, "")))
            continue

        if isinstance(key, str) and key.isprintable():
            if active_name == "shipping_services_display":
                continue
            value = str(values.get(active_name, ""))
            pos = cursor_positions[active_name]
            values[active_name] = value[:pos] + key + value[pos:]
            cursor_positions[active_name] = pos + 1

    updated = {
        "db_host": values["db_host"].strip(),
        "db_name": values["db_name"].strip(),
        "db_user": values["db_user"].strip(),
        "db_pass": values["db_pass"],
        "language": values["language"].strip().lower(),
        "color_theme": values["color_theme"].strip().lower(),
        "color_theme_file": os.path.expanduser(values["color_theme_file"].strip()),
        "printer_uri": values["printer_uri"].strip(),
        "printer_model": values["printer_model"].strip(),
        "label_size": values["label_size"].strip(),
        "label_font_regular": os.path.expanduser(values["label_font_regular"].strip()),
        "label_font_condensed": os.path.expanduser(values["label_font_condensed"].strip()),
        "location_regex_regal": values["location_regex_regal"].strip(),
        "location_regex_fach": values["location_regex_fach"].strip(),
        "location_regex_platz": values["location_regex_platz"].strip(),
        "picklist_printer": values["picklist_printer"].strip(),
        "delivery_note_printer": values["delivery_note_printer"].strip(),
        "delivery_note_format": _normalize_shipping_label_format(values["delivery_note_format"].strip()),
        "shipping_label_printer": values["shipping_label_printer"].strip(),
        "shipping_label_printer_gls": values["shipping_label_printer_gls"].strip(),
        "shipping_label_printer_dhl": values["shipping_label_printer_dhl"].strip(),
        "shipping_label_printer_dhl_private": values["shipping_label_printer_dhl_private"].strip(),
        "shipping_label_printer_post": values["shipping_label_printer_post"].strip(),
        "shipping_carrier": values["shipping_carrier"].strip().lower(),
        "shipping_label_output_dir": os.path.expanduser(values["shipping_label_output_dir"].strip()),
        "shipping_label_format": _normalize_shipping_label_format(values["shipping_label_format"].strip()),
        "shipping_label_format_gls": _normalize_shipping_label_format(values["shipping_label_format_gls"].strip()),
        "shipping_label_format_dhl": _normalize_shipping_label_format(values["shipping_label_format_dhl"].strip()),
        "shipping_label_format_dhl_private": _normalize_shipping_label_format(values["shipping_label_format_dhl_private"].strip()),
        "shipping_label_format_post": _normalize_shipping_label_format(values["shipping_label_format_post"].strip()),
        "shipping_services": _normalize_shipping_services(values.get("shipping_services", [])),
        "shipping_packaging_weight_grams": values["shipping_packaging_weight_grams"].strip(),
        "gls_api_url": values["gls_api_url"].strip(),
        "gls_user": values["gls_user"].strip(),
        "gls_password": values["gls_password"],
        "gls_contact_id": values["gls_contact_id"].strip(),
        "post_api_url": values["post_api_url"].strip(),
        "post_api_key": values["post_api_key"].strip(),
        "post_api_secret": values["post_api_secret"].strip(),
        "post_user": values["post_user"].strip(),
        "post_password": values["post_password"],
        "post_partner_id": values["post_partner_id"].strip(),
        "dhl_private_api_url": values["dhl_private_api_url"].strip(),
        "dhl_private_api_test_url": values["dhl_private_api_test_url"].strip(),
        "dhl_private_api_key": values["dhl_private_api_key"].strip(),
        "dhl_private_api_secret": values["dhl_private_api_secret"].strip(),
        "dhl_private_use_test_api": values["dhl_private_use_test_api"] == "ja",
        "pdf_output_dir": os.path.expanduser(values["pdf_output_dir"].strip()),
        "delivery_note_template_path": os.path.expanduser(values["delivery_note_template_path"].strip()),
        "delivery_note_logo_source": values["delivery_note_logo_source"].strip(),
        "delivery_note_sender_name": values["delivery_note_sender_name"].strip(),
        "delivery_note_sender_street": values["delivery_note_sender_street"].strip(),
        "delivery_note_sender_city": values["delivery_note_sender_city"].strip(),
        "delivery_note_sender_email": values["delivery_note_sender_email"].strip(),
    }

    missing = [
        label for key, label in [
            ("db_host", "DB Host"),
            ("db_name", "DB Name"),
            ("db_user", "DB User"),
            ("printer_uri", "Drucker URI"),
            ("printer_model", "Drucker Modell"),
            ("label_size", "Labelformat"),
        ]
        if not updated[key]
    ]

    if missing:
        message_box(stdscr, t("error"), f"Felder fehlen: {', '.join(missing)}")
        return

    if updated["language"] not in SUPPORTED_LANGUAGES:
        message_box(stdscr, t("error"), "Sprache muss 'de' oder 'en' sein.")
        return
    if updated["color_theme_file"] and not os.path.isfile(updated["color_theme_file"]):
        message_box(stdscr, t("error"), t("theme_file_missing"))
        return
    available_theme_names = set(BASE_THEMES)
    if updated["color_theme_file"]:
        available_theme_names.update(load_custom_themes_from_file(updated["color_theme_file"]).keys())
    else:
        available_theme_names.update(load_custom_themes().keys())
    if updated["color_theme"] not in available_theme_names:
        message_box(stdscr, t("error"), t("theme_invalid", names=", ".join(sorted(available_theme_names)))[:56])
        return

    if updated["pdf_output_dir"] and not os.path.isdir(updated["pdf_output_dir"]):
        message_box(stdscr, t("error"), "PDF Ordner existiert nicht.")
        return
    for key in ("label_font_regular", "label_font_condensed"):
        if updated[key] and not os.path.isfile(updated[key]):
            message_box(stdscr, t("error"), f"{key} Datei existiert nicht."[:56])
            return
    if updated["delivery_note_template_path"] and not os.path.isfile(updated["delivery_note_template_path"]):
        message_box(stdscr, t("error"), "LS Vorlage existiert nicht.")
        return
    for key, label in [
        ("location_regex_regal", "Regex Regal"),
        ("location_regex_fach", "Regex Fach"),
        ("location_regex_platz", "Regex Platz"),
    ]:
        if not updated[key]:
            message_box(stdscr, t("error"), f"{label} darf nicht leer sein.")
            return
        try:
            re.compile(updated[key])
        except re.error as exc:
            message_box(stdscr, t("error"), f"{label} ungueltig: {exc}"[:56])
            return
    if updated["delivery_note_logo_source"]:
        logo_source = updated["delivery_note_logo_source"]
        if not is_http_url(logo_source):
            logo_path = os.path.expanduser(logo_source)
            if not os.path.isfile(logo_path):
                message_box(stdscr, t("error"), "LS Logo Datei existiert nicht.")
                return
            updated["delivery_note_logo_source"] = logo_path
    if updated["shipping_carrier"] not in {"gls", "dhl_private", "post", "test"}:
        message_box(stdscr, t("error"), "Versand Dienstleister nur 'gls', 'dhl_private', 'post' oder 'test'.")
        return
    if updated["shipping_label_output_dir"] and not os.path.isdir(updated["shipping_label_output_dir"]):
        message_box(stdscr, t("error"), "Versandlabel Ordner existiert nicht.")
        return
    allowed_formats = {"A6", "A5", "A4", "100x62"}
    if not updated["delivery_note_format"]:
        updated["delivery_note_format"] = "A4"
    if updated["shipping_label_format"] not in allowed_formats:
        message_box(stdscr, t("error"), "Labelformat nur A6, A5, A4 oder 100x62.")
        return
    for key in ("shipping_label_format_gls", "shipping_label_format_dhl", "shipping_label_format_dhl_private", "shipping_label_format_post"):
        if updated[key] not in allowed_formats:
            message_box(stdscr, t("error"), f"{key} ungueltig.")
            return
    if not updated.get("shipping_label_format_gls"):
        updated["shipping_label_format_gls"] = "A6"
    if not updated.get("shipping_label_format_dhl"):
        updated["shipping_label_format_dhl"] = "A5"
    if not updated.get("shipping_label_format_dhl_private"):
        updated["shipping_label_format_dhl_private"] = "A5"
    if not updated.get("shipping_label_format_post"):
        updated["shipping_label_format_post"] = "100x62"
    if not updated.get("shipping_label_format"):
        updated["shipping_label_format"] = "A6"
    try:
        packaging_weight = int(updated["shipping_packaging_weight_grams"])
    except ValueError:
        message_box(stdscr, t("error"), "Verpackung Gewicht muss eine Zahl in g sein.")
        return
    if packaging_weight < 0:
        message_box(stdscr, t("error"), "Verpackung Gewicht darf nicht negativ sein.")
        return
    updated["shipping_packaging_weight_grams"] = packaging_weight
    if updated["shipping_carrier"] == "gls":
        has_gls_settings = all(
            updated[key]
            for key in ("gls_api_url", "gls_user", "gls_password", "gls_contact_id")
        )
        has_gls_pdf = any(GLS_DIR.glob("*.pdf"))
        if not has_gls_settings and not has_gls_pdf:
            message_box(stdscr, t("error"), "GLS Daten fehlen (Settings oder PDF).")
            return
    if updated["shipping_carrier"] == "post":
        has_post_settings = bool(updated["post_api_url"] and updated["post_partner_id"])
        has_post_oauth = bool(updated["post_api_key"] and updated["post_api_secret"])
        has_post_legacy = bool(updated["post_user"] and updated["post_password"])
        if not has_post_settings or (not has_post_oauth and not has_post_legacy):
            message_box(stdscr, t("error"), "POST INTERNETMARKE Daten fehlen.")
            return
    if updated["shipping_carrier"] == "dhl_private":
        active_url = updated["dhl_private_api_test_url"] if updated["dhl_private_use_test_api"] else updated["dhl_private_api_url"]
        if not active_url or not updated["dhl_private_api_key"] or not updated["dhl_private_api_secret"]:
            message_box(stdscr, t("error"), "DHL Privat Daten fehlen.")
            return

    try:
        test_db_connection(updated)
    except Exception as exc:
        message_box(stdscr, "DB Fehler", str(exc)[:56])
        return

    SETTINGS = save_settings(updated)
    apply_color_theme(stdscr)
    message_box(stdscr, t("saved"), t("saved_settings"))

def parse_int_or_error(stdscr, raw_value, field_name):

    try:
        return int(raw_value)

    except ValueError:
        message_box(stdscr, "Fehler", f"{field_name} muss eine Zahl sein.")
        return None


def summarize_subprocess_error(exc):
    for chunk in [getattr(exc, "stderr", None), getattr(exc, "stdout", None)]:
        if not chunk:
            continue
        for line in str(chunk).splitlines():
            cleaned = line.strip()
            if cleaned:
                return cleaned[:120]

    return str(exc)[:120]


def build_label_print_command(item):
    return [
        "python3",
        LABEL_PRINT_SCRIPT,
        item["sku"],
        item["name"],
        str(item["menge"]),
        item["regal"] or "",
        item["fach"] or "",
        item["platz"] or "",
    ]

def add_item(stdscr):

    res = form_dialog(
        stdscr,
        "Artikel anlegen",
        [
            {"name": "sku", "label": "SKU", "value": ""},
            {"name": "name", "label": "Name", "value": ""},
            {"name": "regal", "label": "Regal", "value": ""},
            {"name": "fach", "label": "Fach", "value": ""},
            {"name": "platz", "label": "Platz", "value": ""},
            {"name": "menge", "label": "Menge", "value": ""},
        ],
        field_validators={
            "regal": lambda value: is_location_input_allowed("regal", value),
            "fach": lambda value: is_location_input_allowed("fach", value),
            "platz": lambda value: is_location_input_allowed("platz", value),
        },
    )

    if res is None:
        return

    regal = validate_regal_or_error(stdscr, res["regal"])
    if regal is None:
        return
    fach = validate_location_or_error(stdscr, "fach", res["fach"])
    if fach is None:
        return
    platz = validate_location_or_error(stdscr, "platz", res["platz"])
    if platz is None:
        return

    menge = parse_int_or_error(stdscr, res["menge"], "Menge")

    if menge is None:
        return

    con = db()
    cur = con.cursor()

    cur.execute(
        """
        INSERT INTO items (
            sku,name,regal,fach,platz,menge,available,unavailable,committed,reserved,sync_status
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'local')
        """,

        (
            res["sku"],
            res["name"],
            regal,
            fach,
            platz,
            menge,
            menge,
            0,
            0,
            0,
        ),
    )

    con.commit()
    cur.close()
    con.close()

def change_qty(stdscr, item):

    curses.curs_set(1)

    current_qty = int(item["menge"])
    qty = current_qty

    h, w = stdscr.getmaxyx()

    width = 40
    height = 7

    y = h // 2 - height // 2
    x = w // 2 - width // 2

    draw_shadow(stdscr, y, x, height, width)

    win = curses.newwin(height, width, y, x)
    win.keypad(True)
    win.bkgd(" ", curses.color_pair(1))

    typed = None

    while True:

        win.erase()
        win.box()

        win.addstr(0, 2, " Menge ändern ")

        win.addstr(2, 2, f"Aktuell : {current_qty}")

        if typed is None:
            qty_str = str(qty)
        else:
            qty_str = typed
        win.addstr(3, 2, "Neu     : ")

        field_x = 12
        field_width = width - field_x - 2

        visible = qty_str[-field_width:]
        win.addstr(3, field_x, visible.ljust(field_width))

        win.addstr(5, 2, "+ / - ändern   Zahl eingeben")
        win.addstr(6, 2, "F2 Speichern   F9 Abbrechen")

        cursor_pos = min(len(qty_str), field_width - 1)
        win.move(3, field_x + cursor_pos)
        
        win.refresh()

        key = win.get_wch()

        if key in (27, curses.KEY_F9):
            return

        if key == curses.KEY_F2:

            con = db()
            cur = con.cursor()

            cur.execute(
                """
                UPDATE items
                SET menge=%s,
                    available=GREATEST(
                        %s - COALESCE(unavailable, COALESCE(reserved, 0)) - COALESCE(committed, 0),
                        0
                    ),
                    dirty=true,
                    updated_at=NOW()
                WHERE sku=%s
                """,
                (qty, qty, item["sku"])
            )



            con.commit()
            cur.close()
            con.close() 
            return

        elif key == '+':
            qty += 1
            typed = None

        elif key == '-':
            if qty > 0:
                qty -= 1
            typed = None

        elif key in (curses.KEY_BACKSPACE, 127, 8, '\x7f', '\b'):

            if typed is not None:

                typed = typed[:-1]

                if typed == "":
                    typed = None
                    qty = current_qty
                else:
                    qty = int(typed)

        elif isinstance(key, str) and key.isdigit():

            if typed is None:
                typed = key
            else:
                typed += key

            qty = int(typed)
            
def change_location(stdscr, item):

    res = form_dialog(
        stdscr,
        "Lagerplatz ändern",
        [
            {"name": "regal", "label": "Regal", "value": item["regal"] or ""},
            {"name": "fach", "label": "Fach", "value": item["fach"] or ""},
            {"name": "platz", "label": "Platz", "value": item["platz"] or ""},
        ],
        field_validators={
            "regal": lambda value: is_location_input_allowed("regal", value),
            "fach": lambda value: is_location_input_allowed("fach", value),
            "platz": lambda value: is_location_input_allowed("platz", value),
        },
    )

    if res is None:
        return

    regal = validate_regal_or_error(stdscr, res["regal"])
    if regal is None:
        return
    fach = validate_location_or_error(stdscr, "fach", res["fach"])
    if fach is None:
        return
    platz = validate_location_or_error(stdscr, "platz", res["platz"])
    if platz is None:
        return

    con = db()
    cur = con.cursor()

    cur.execute("""
        UPDATE items
        SET regal = %s,
            fach = %s,
            platz = %s,
            updated_at = NOW()
        WHERE sku = %s
    """,
    (
        regal,
        fach,
        platz,
        item["sku"]
    ))

    con.commit()
    cur.close()
    con.close()
    
def edit_item(stdscr, item):

    if item["sync_status"] != "local":
        message_box(stdscr, "Fehler", "Nur lokale Artikel können bearbeitet werden.")
        return

    res = form_dialog(
        stdscr,
        "Artikel bearbeiten",
        [
            {"name": "sku", "label": "SKU", "value": item["sku"]},
            {"name": "name", "label": "Name", "value": item["name"]},
            {"name": "regal", "label": "Regal", "value": item["regal"] or ""},
            {"name": "fach", "label": "Fach", "value": item["fach"] or ""},
            {"name": "platz", "label": "Platz", "value": item["platz"] or ""},
            {"name": "menge", "label": "Menge", "value": str(item["menge"])},
        ],
        field_validators={
            "regal": lambda value: is_location_input_allowed("regal", value),
            "fach": lambda value: is_location_input_allowed("fach", value),
            "platz": lambda value: is_location_input_allowed("platz", value),
        },
    )

    if res is None:
        return

    regal = validate_regal_or_error(stdscr, res["regal"])
    if regal is None:
        return
    fach = validate_location_or_error(stdscr, "fach", res["fach"])
    if fach is None:
        return
    platz = validate_location_or_error(stdscr, "platz", res["platz"])
    if platz is None:
        return

    try:
        menge = int(res["menge"])
    except:
        message_box(stdscr, "Fehler", "Menge muss eine Zahl sein.")
        return

    con = db()
    cur = con.cursor()

    cur.execute("""
        UPDATE items
        SET sku=%s,
            name=%s,
            regal=%s,
            fach=%s,
            platz=%s,
            menge=%s,
            available=GREATEST(
                %s - COALESCE(unavailable, COALESCE(reserved, 0)) - COALESCE(committed, 0),
                0
            ),
            updated_at=NOW()
        WHERE sku=%s
    """,
    (
        res["sku"],
        res["name"],
        regal,
        fach,
        platz,
        menge,
        menge,
        item["sku"]
    ))

    con.commit()
    cur.close()
    con.close()

def print_label(stdscr, item):

    try:
        PRINT_LOGGER.debug(
            "Rufe label_print.py auf sku=%s printer_model=%s printer_uri=%s",
            item["sku"],
            SETTINGS.get("printer_model"),
            SETTINGS.get("printer_uri"),
        )
        subprocess.run(build_label_print_command(item), capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as exc:
        PRINT_LOGGER.exception("Labeldruck fehlgeschlagen fuer SKU=%s", item["sku"])
        if exc.stderr:
            PRINT_LOGGER.error("label_print.py stderr: %s", exc.stderr.strip()[:500])
        if exc.stdout:
            PRINT_LOGGER.error("label_print.py stdout: %s", exc.stdout.strip()[:500])
        short_error = summarize_subprocess_error(exc)
        message_box(stdscr, "Druckfehler", f"{short_error[:24]} Log: {PRINT_LOG_PATH.name}"[:56])
    except Exception as exc:
        PRINT_LOGGER.exception("Unerwarteter Fehler beim Labeldruck fuer SKU=%s", item["sku"])
        message_box(stdscr, "Druckfehler", f"{str(exc)[:24]} Log: {PRINT_LOG_PATH.name}"[:56])

def print_label_multiple(stdscr, item):

    res = form_dialog(
        stdscr,
        "Labels drucken",
        [
            {"name": "count", "label": "Anzahl", "value": "1"},
        ],
    )

    if res is None:
        return

    try:
        count = int(res["count"])
    except:
        return

    for _ in range(count):
        try:
            PRINT_LOGGER.debug("Mehrfachdruck Label sku=%s", item["sku"])
            subprocess.run(build_label_print_command(item), capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as exc:
            PRINT_LOGGER.exception("Mehrfachdruck fehlgeschlagen fuer SKU=%s", item["sku"])
            if exc.stderr:
                PRINT_LOGGER.error("label_print.py stderr: %s", exc.stderr.strip()[:500])
            if exc.stdout:
                PRINT_LOGGER.error("label_print.py stdout: %s", exc.stdout.strip()[:500])
            short_error = summarize_subprocess_error(exc)
            message_box(stdscr, "Druckfehler", f"{short_error[:24]} Log: {PRINT_LOG_PATH.name}"[:56])
            return
        except Exception as exc:
            PRINT_LOGGER.exception("Unerwarteter Fehler beim Mehrfachdruck fuer SKU=%s", item["sku"])
            message_box(stdscr, "Druckfehler", f"{str(exc)[:24]} Log: {PRINT_LOG_PATH.name}"[:56])
            return

def delete_item(stdscr, item):

    if item["sync_status"] != "local":
        message_box(stdscr, "Fehler", "Nur lokale Artikel können gelöscht werden.")
        return

    if not confirm_box(stdscr, "Löschen", f"Artikel {item['sku']} wirklich löschen?"):
        return

    con = db()
    cur = con.cursor()
    cur.execute("DELETE FROM items WHERE sku=%s", (item["sku"],))
    con.commit()
    cur.close()
    con.close()


def toggle_external_fulfillment(stdscr, item):
    new_value = not bool(item["external_fulfillment"])

    con = db()
    cur = con.cursor()
    cur.execute(
        """
        UPDATE items
        SET external_fulfillment = %s,
            updated_at = NOW()
        WHERE sku = %s
        """,
        (new_value, item["sku"]),
    )
    con.commit()
    cur.close()
    con.close()


def format_address(order):
    parts = [
        order["shipping_name"] or "",
        order["shipping_address1"] or "",
        " ".join(part for part in [order["shipping_zip"] or "", order["shipping_city"] or ""] if part),
    ]
    text = ", ".join(part for part in parts if part)
    return text or "Keine Lieferadresse"


def format_location_short(row):
    regal = row["regal"] or "-"
    fach = row["fach"] or "-"
    platz = row["platz"] or "-"
    return f"{regal}/{fach}/{platz}"


def sort_order_items_for_picklist(rows):
    return sorted(
        [row for row in rows if not row["external_fulfillment"]],
        key=lambda row: (
            _sort_location_value(row["regal"]),
            _sort_location_value(row["fach"]),
            _sort_location_value(row["platz"]),
            row["sku"] or "",
            row["title"],
        ),
    )


def build_delivery_note_filename(order):
    order_name = (order["order_name"] or "lieferschein").replace("#", "")
    safe_name = "".join(ch if ch in string.ascii_letters + string.digits + "-_" else "_" for ch in order_name).strip("_")
    if not safe_name:
        safe_name = "lieferschein"
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"lieferschein_{safe_name}_{timestamp}.pdf"


def get_pdf_output_dir():
    configured = SETTINGS["pdf_output_dir"].strip()
    if not configured:
        return os.getcwd()
    return configured


def get_delivery_note_sender():
    return {
        "name": SETTINGS["delivery_note_sender_name"].strip(),
        "street": SETTINGS["delivery_note_sender_street"].strip(),
        "city": SETTINGS["delivery_note_sender_city"].strip(),
        "email": SETTINGS["delivery_note_sender_email"].strip(),
    }


def get_delivery_note_template_path():
    configured = SETTINGS.get("delivery_note_template_path", "").strip()
    if not configured:
        return None
    return Path(os.path.expanduser(configured))


def is_http_url(value):
    parsed = urlparse((value or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def get_delivery_note_logo_source():
    configured = SETTINGS.get("delivery_note_logo_source", "").strip()
    if not configured:
        return ""
    if is_http_url(configured):
        return configured
    return os.path.expanduser(configured)


def create_delivery_note_pdf(order, order_items, output_dir=None):
    template_path = get_delivery_note_template_path()
    if template_path and not template_path.exists():
        raise FileNotFoundError(f"Vorlage fehlt: {template_path.name}")
    logo_source = get_delivery_note_logo_source()
    if logo_source and not is_http_url(logo_source) and not os.path.isfile(logo_source):
        raise FileNotFoundError(f"Logo fehlt: {os.path.basename(logo_source)}")

    output_dir = output_dir or get_pdf_output_dir()
    if not os.path.isdir(output_dir):
        raise FileNotFoundError(f"PDF Ordner fehlt: {output_dir}")
    output_path = os.path.join(output_dir, build_delivery_note_filename(order))
    rows = build_delivery_note_rows(order_items)
    build_delivery_note_pdf(template_path, output_path, order, rows, sender=get_delivery_note_sender(), logo_source=logo_source)
    return output_path, rows


def build_picklist_text(order, order_items):
    sorted_items = sort_order_items_for_picklist(order_items)
    address = format_address(order)
    lines = [
        f"Pickliste {order['order_name']}",
        address,
        "",
        f"{'Menge':<6} {'SKU':<18} {'Name':<32} {'Regal':<6} {'Fach':<6} {'Platz':<6}",
        "-" * 80,
    ]

    for row in sorted_items:
        lines.append(
            f"{str(row['quantity']):<6} "
            f"{_fit(row['sku'] or '-', 18):<18} "
            f"{_fit(row['title'], 32):<32} "
            f"{_fit(row['regal'] or '-', 6):<6} "
            f"{_fit(row['fach'] or '-', 6):<6} "
            f"{_fit(row['platz'] or '-', 6):<6}"
        )

    lines.append("")
    lines.append(f"Positionen: {len(sorted_items)}")
    return "\n".join(lines) + "\n"


def print_picklist(stdscr, order, order_items):
    printer = SETTINGS["picklist_printer"].strip()

    if not printer:
        message_box(stdscr, "Fehler", "Bitte zuerst Shift+F11: Pickliste Drucker setzen.")
        return

    document = build_picklist_text(order, order_items)

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".txt", delete=False) as handle:
        handle.write(document)
        temp_path = handle.name

    try:
        PRINT_LOGGER.debug(
            "Sende Pickliste an Drucker printer=%s order=%s items=%s",
            printer,
            order["order_name"],
            len(order_items),
        )
        subprocess.run(
            ["lp", "-d", printer, "-t", f"Pickliste {order['order_name']}", temp_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
    except FileNotFoundError:
        PRINT_LOGGER.exception("lp/Drucksystem nicht verfuegbar fuer Pickliste order=%s", order["order_name"])
        message_box(stdscr, "Druckfehler", "lp/Drucksystem ist auf diesem System nicht verfuegbar.")
    except subprocess.CalledProcessError as exc:
        PRINT_LOGGER.exception("Picklisten-Druck fehlgeschlagen order=%s printer=%s", order["order_name"], printer)
        error_text = (exc.stderr or str(exc)).strip()
        message_box(stdscr, "Druckfehler", f"{(error_text[:20] or 'Druckfehler')} {PRINT_LOG_PATH.name}"[:56])
    else:
        PRINT_LOGGER.info("Pickliste erfolgreich gedruckt order=%s printer=%s", order["order_name"], printer)
        message_box(stdscr, "Druck", "Pickliste wurde an Drucker gesendet.")
    finally:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass


def export_delivery_note_pdf(stdscr, order, order_items):
    try:
        output_path, rows = create_delivery_note_pdf(order, order_items)
    except FileNotFoundError as exc:
        PRINT_LOGGER.exception("Lieferschein-Vorlage fehlt order=%s", order["order_name"])
        message_box(stdscr, "Fehler", str(exc)[:56])
    except ValueError as exc:
        PRINT_LOGGER.warning("Lieferschein nicht erstellt order=%s reason=%s", order["order_name"], exc)
        message_box(stdscr, "Fehler", str(exc)[:56])
    except Exception:
        PRINT_LOGGER.exception("Lieferschein-PDF fehlgeschlagen order=%s", order["order_name"])
        message_box(stdscr, "Fehler", f"Lieferschein fehlgeschlagen {PRINT_LOG_PATH.name}"[:56])
    else:
        PRINT_LOGGER.info(
            "Lieferschein-PDF erstellt order=%s items=%s path=%s",
            order["order_name"],
            len(rows),
            output_path,
        )
        message_box(stdscr, "Lieferschein PDF", output_path[-56:])


def print_delivery_note(stdscr, order, order_items):
    printer = SETTINGS["delivery_note_printer"].strip()
    if not printer:
        message_box(stdscr, "Fehler", "Bitte zuerst Shift+F11: Lieferschein Drucker setzen.")
        return

    temp_path = None
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path, rows = create_delivery_note_pdf(order, order_items, output_dir=temp_dir)
            PRINT_LOGGER.debug(
                "Sende Lieferschein an Drucker printer=%s order=%s items=%s",
                printer,
                order["order_name"],
                len(rows),
            )
            subprocess.run(
                ["lp", "-d", printer, "-t", f"Lieferschein {order['order_name']}", temp_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
            )
    except FileNotFoundError as exc:
        PRINT_LOGGER.exception("Lieferschein-Druck nicht moeglich order=%s", order["order_name"])
        message_box(stdscr, "Druckfehler", str(exc)[:56])
    except ValueError as exc:
        PRINT_LOGGER.warning("Lieferschein-Druck abgebrochen order=%s reason=%s", order["order_name"], exc)
        message_box(stdscr, "Druckfehler", str(exc)[:56])
    except subprocess.CalledProcessError as exc:
        PRINT_LOGGER.exception("Lieferschein-Druck fehlgeschlagen order=%s printer=%s", order["order_name"], printer)
        error_text = (exc.stderr or str(exc)).strip()
        message_box(stdscr, "Druckfehler", f"{(error_text[:20] or 'Druckfehler')} {PRINT_LOG_PATH.name}"[:56])
    except Exception:
        PRINT_LOGGER.exception("Lieferschein-Verarbeitung fehlgeschlagen order=%s temp=%s", order["order_name"], temp_path)
        message_box(stdscr, "Druckfehler", f"Lieferschein fehlgeschlagen {PRINT_LOG_PATH.name}"[:56])
    else:
        PRINT_LOGGER.info("Lieferschein erfolgreich gedruckt order=%s printer=%s", order["order_name"], printer)
        message_box(stdscr, "Druck", "Lieferschein wurde an Drucker gesendet.")


def inventory_session_summary(lines):
    total = len(lines)
    counted = sum(1 for row in lines if row["ist_menge"] is not None)
    differences = sum(1 for row in lines if row["ist_menge"] is not None and row["ist_menge"] != row["soll_menge"])
    return total, counted, differences


def format_inventory_line(row, width):
    qty_width = 5
    sku_width = 18
    name_width = max(16, width - 43)
    regal_width = 5
    fach_width = 5
    platz_width = 5
    soll = _fit(str(row["soll_menge"]), qty_width)
    ist = _fit("" if row["ist_menge"] is None else str(row["ist_menge"]), qty_width)
    sku = _fit(row["sku"], sku_width)
    name = _fit(row["name"], name_width)
    regal = _fit(row["regal"] or "-", regal_width)
    fach = _fit(row["fach"] or "-", fach_width)
    platz = _fit(row["platz"] or "-", platz_width)
    return f"{soll} {ist} {sku} {name} {regal} {fach} {platz}"[:width]


def build_inventory_lines_display(lines, width):
    header = f"{_fit('Soll', 5)} {_fit('Ist', 5)} {_fit('SKU', 18)} {_fit('Name', max(16, width - 43))} {_fit('Reg', 5)} {_fit('Fac', 5)} {_fit('Pl', 5)}"
    display = [header, "-" * max(1, width)]
    display.extend(format_inventory_line(row, width) for row in lines)
    return display


def build_inventory_export_text(session, lines):
    output = []
    current_regal = None

    output.append(session["session_name"])
    output.append("")

    for row in lines:
        regal_label = row["regal"] or "Ohne Regal"
        if regal_label != current_regal:
            if current_regal is not None:
                output.append("")
            current_regal = regal_label
            output.append(f"Regal {regal_label}")
            output.append("-" * 80)
            output.append(f"{'Soll':<6} {'Ist':<6} {'SKU':<18} {'Name':<28} {'Fach':<6} {'Platz':<6}")

        output.append(
            f"{str(row['soll_menge']):<6} "
            f"{'' if row['ist_menge'] is None else str(row['ist_menge']):<6} "
            f"{_fit(row['sku'], 18):<18} "
            f"{_fit(row['name'], 28):<28} "
            f"{_fit(row['fach'] or '-', 6):<6} "
            f"{_fit(row['platz'] or '-', 6):<6}"
        )

    output.append("")
    total, counted, differences = inventory_session_summary(lines)
    output.append(f"Positionen: {total}  Gezaehlt: {counted}  Abweichungen: {differences}")
    return "\n".join(output) + "\n"


def export_inventory_csv(session, lines):
    filename = f"inventur_{session['session_id']}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    path = os.path.join(os.getcwd(), filename)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter=";")
        writer.writerow(["session_id", "session_name", "line_no", "sku", "name", "regal", "fach", "platz", "soll_menge", "ist_menge"])
        for row in lines:
            writer.writerow([
                session["session_id"],
                session["session_name"],
                row["line_no"],
                row["sku"],
                row["name"],
                row["regal"] or "",
                row["fach"] or "",
                row["platz"] or "",
                row["soll_menge"],
                "" if row["ist_menge"] is None else row["ist_menge"],
            ])
    return path


def print_inventory_list(stdscr, session, lines):
    printer = SETTINGS["picklist_printer"].strip()
    if not printer:
        message_box(stdscr, "Fehler", "Bitte zuerst Shift+F11: Pickliste Drucker setzen.")
        return

    document = build_inventory_export_text(session, lines)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".txt", delete=False) as handle:
        handle.write(document)
        temp_path = handle.name

    try:
        PRINT_LOGGER.debug(
            "Sende Inventurliste an Drucker printer=%s session=%s positions=%s",
            printer,
            session["session_name"],
            len(lines),
        )
        subprocess.run(
            ["lp", "-d", printer, "-t", session["session_name"], temp_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
    except FileNotFoundError:
        PRINT_LOGGER.exception("lp/Drucksystem nicht verfuegbar fuer Inventurliste session=%s", session["session_name"])
        message_box(stdscr, "Druckfehler", "lp/Drucksystem ist auf diesem System nicht verfuegbar.")
    except subprocess.CalledProcessError as exc:
        PRINT_LOGGER.exception("Inventurlisten-Druck fehlgeschlagen session=%s printer=%s", session["session_name"], printer)
        error_text = (exc.stderr or str(exc)).strip()
        message_box(stdscr, "Druckfehler", f"{(error_text[:20] or 'Druckfehler')} {PRINT_LOG_PATH.name}"[:56])
    else:
        PRINT_LOGGER.info("Inventurliste erfolgreich gedruckt session=%s printer=%s", session["session_name"], printer)
        message_box(stdscr, "Druck", "Inventurliste wurde an Drucker gesendet.")
    finally:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass


def format_order_item_header(width):
    qty_width = 7
    sku_width = 18
    regal_width = 5
    fach_width = 5
    platz_width = 5
    used = qty_width + sku_width + regal_width + fach_width + platz_width + 8
    title_width = max(10, width - used)

    return qty_width, sku_width, regal_width, fach_width, platz_width, title_width


def order_item_remaining_qty(row):
    try:
        quantity = int(row.get("quantity") or 0)
    except (TypeError, ValueError):
        quantity = 0
    try:
        fulfilled = int(row.get("fulfilled_quantity") or 0)
    except (TypeError, ValueError):
        fulfilled = 0
    return max(0, quantity - fulfilled)


def format_order_item_row(row, width):
    qty_width, sku_width, regal_width, fach_width, platz_width, title_width = format_order_item_header(width)
    remaining = order_item_remaining_qty(row)
    try:
        quantity = int(row.get("quantity") or 0)
    except (TypeError, ValueError):
        quantity = 0
    qty = _fit(f"{remaining}/{quantity}", qty_width)
    sku = _fit(row["sku"] or "-", sku_width)
    title_text = row["title"]
    if row["external_fulfillment"]:
        title_text = f"[Extern] {title_text}"
    title = _fit(title_text, title_width)
    regal = _fit(row["regal"] or "-", regal_width)
    fach = _fit(row["fach"] or "-", fach_width)
    platz = _fit(row["platz"] or "-", platz_width)
    line = f"{qty} {sku} {title} {regal} {fach} {platz}"
    return line[:width]


def jump_to_order(orders, needle):
    if not needle:
        return None

    normalized = needle.strip().replace("#", "").lower()

    for index, order in enumerate(orders):
        order_name = (order["order_name"] or "").replace("#", "").lower()
        if order_name == normalized:
            return index

    for index, order in enumerate(orders):
        order_name = (order["order_name"] or "").replace("#", "").lower()
        if normalized in order_name:
            return index

    return None


def _fulfillment_filter_label(filter_value):
    if current_language() == "de":
        labels = {
            "all": "Status: Alle",
            "open": "Status: Offen",
            "unfulfilled": "Status: Unausgeführt",
            "partial": "Status: Teilweise",
            "fulfilled": "Status: Ausgeführt",
        }
    else:
        labels = {
            "all": "Status: All",
            "open": "Status: Open",
            "unfulfilled": "Status: Unfulfilled",
            "partial": "Status: Partial",
            "fulfilled": "Status: Fulfilled",
        }
    return labels.get(filter_value, f"Status: {filter_value}")


def _payment_filter_label(filter_value):
    if current_language() == "de":
        labels = {
            "all": "Zahlung: Alle",
            "paid": "Zahlung: Bezahlt",
            "pending": "Zahlung: Ausstehend",
            "authorized": "Zahlung: Autorisiert",
            "partially_paid": "Zahlung: Teilbezahlt",
            "refunded": "Zahlung: Erstattet",
            "voided": "Zahlung: Storniert",
        }
    else:
        labels = {
            "all": "Payment: All",
            "paid": "Payment: Paid",
            "pending": "Payment: Pending",
            "authorized": "Payment: Authorized",
            "partially_paid": "Payment: Partially Paid",
            "refunded": "Payment: Refunded",
            "voided": "Payment: Voided",
        }
    if current_language() == "de":
        return labels.get(filter_value, f"Zahlung: {filter_value}")
    return labels.get(filter_value, f"Payment: {filter_value}")


def _bulk_print_mode_dialog(stdscr):
    return choice_dialog(
        stdscr,
        "Bulk Druckmodus",
        [
            {"value": "both", "label": "Label + Lieferschein"},
            {"value": "label", "label": "Nur Label"},
            {"value": "note", "label": "Nur Lieferschein"},
            {"value": "none", "label": "Nichts drucken"},
        ],
        "both",
        cancel_returns_none=True,
    )


def _bulk_shopify_queue_mode_dialog(stdscr):
    return choice_dialog(
        stdscr,
        "Bulk Shopify-Tracking",
        [
            {"value": "queue", "label": "Tracking direkt an Shopify senden"},
            {"value": "manual", "label": "Spaeter manuell ueber History (F10)"},
        ],
        "manual",
        cancel_returns_none=True,
    )


def bulk_carrier_per_order_dialog(stdscr, selected_orders, current_map):
    if not selected_orders:
        return current_map

    options = sorted(IMPLEMENTED_SHIPPING_CARRIERS)
    selected = 0
    top_index = 0
    assignments = dict(current_map)

    while True:
        h, w = stdscr.getmaxyx()
        width = min(max(86, int(w * 0.82)), w - 4)
        height = min(max(14, len(selected_orders) + 6), h - 2)
        y = max(1, (h - height) // 2)
        x = max(2, (w - width) // 2)

        win = curses.newwin(height, width, y, x)
        win.keypad(True)
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        win.addstr(0, 2, " Bulk Dienstleister je Auftrag ")

        visible_rows = max(1, height - 4)
        if selected < top_index:
            top_index = selected
        if selected >= top_index + visible_rows:
            top_index = selected - visible_rows + 1

        for row_idx, order in enumerate(selected_orders[top_index:top_index + visible_rows]):
            real_idx = top_index + row_idx
            order_id = order["order_id"]
            carrier = effective_shipping_carrier(assignments.get(order_id, active_shipping_carrier()))
            line = _fit(f"{_fit(order['order_name'], 12)}  {carrier.upper()}  {format_address(order)}", width - 3)
            y_pos = 2 + row_idx
            if real_idx == selected:
                win.attrset(curses.color_pair(2))
                win.addstr(y_pos, 1, line.ljust(width - 2))
                win.attrset(curses.color_pair(1))
            else:
                win.addstr(y_pos, 1, line.ljust(width - 2))

        footer = "↑↓ Auftrag  ←/→ oder Space Carrier wechseln  F2 Uebernehmen  F9 Zurueck"
        win.attrset(curses.color_pair(3))
        win.addstr(height - 1, 1, _fit(footer, width - 2))
        win.attrset(curses.color_pair(1))
        win.refresh()

        key = win.get_wch()
        if key in (27, curses.KEY_F9):
            return current_map
        if key == curses.KEY_F2:
            return assignments
        if key == curses.KEY_DOWN:
            selected = move_selection(selected_orders, selected, 1)
            continue
        if key == curses.KEY_UP:
            selected = move_selection(selected_orders, selected, -1)
            continue
        if key in (curses.KEY_LEFT, curses.KEY_RIGHT, " ", 10, 13, "\n", "\r", curses.KEY_ENTER):
            order_id = selected_orders[selected]["order_id"]
            current = effective_shipping_carrier(assignments.get(order_id, active_shipping_carrier()))
            current_index = options.index(current) if current in options else 0
            if key == curses.KEY_LEFT:
                new_index = (current_index - 1) % len(options)
            else:
                new_index = (current_index + 1) % len(options)
            assignments[order_id] = options[new_index]


def _execution_carrier_dialog(stdscr, current_carrier=None):
    fallback = effective_shipping_carrier(current_carrier or active_shipping_carrier() or "gls")
    return choice_dialog(
        stdscr,
        "Versand Dienstleister",
        [
            {"value": "gls", "label": "GLS"},
            {"value": "test", "label": "TEST"},
        ],
        fallback,
        cancel_returns_none=True,
    )


def select_partial_items_dialog(stdscr, order, order_items):
    editable = []
    for row in order_items:
        if row.get("external_fulfillment"):
            continue
        total_qty = int(row.get("quantity") or 0)
        fulfilled_qty = int(row.get("fulfilled_quantity") or 0)
        remaining = order_item_remaining_qty(row)
        entry = dict(row)
        entry["total_quantity"] = total_qty
        entry["fulfilled_quantity"] = fulfilled_qty
        entry["remaining_quantity"] = remaining
        entry["selected_quantity"] = remaining if remaining > 0 else 0
        editable.append(entry)

    if not editable:
        message_box(stdscr, "Teilausfuehrung", "Keine offenen Positionen fuer Teilausfuehrung.")
        return None

    selected = 0
    top_index = 0
    while True:
        h, w = stdscr.getmaxyx()
        width = min(max(78, int(w * 0.74)), w - 8)
        height = min(max(14, len(editable) + 7), h - 4)
        y = max(1, (h - height) // 2)
        x = max(2, (w - width) // 2)

        win = curses.newwin(height, width, y, x)
        win.keypad(True)
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        win.addstr(0, 2, f" Teilausfuehrung {order.get('order_name') or ''} ")

        list_height = height - 4
        if selected < top_index:
            top_index = selected
        if selected >= top_index + list_height:
            top_index = selected - list_height + 1

        for row_idx, row in enumerate(editable[top_index:top_index + list_height]):
            real_idx = top_index + row_idx
            y_pos = 2 + row_idx
            qty = int(row.get("selected_quantity") or 0)
            remaining = int(row.get("remaining_quantity") or 0)
            total = int(row.get("total_quantity") or 0)
            sku = row.get("sku") or "-"
            title = row.get("title") or "-"
            state = " [bereits ausgeführt]" if remaining <= 0 else ""
            line = _fit(f"[{qty:>3}/{remaining:<3}/{total:<3}] {_fit(sku, 16)} {title}{state}", width - 3)
            if real_idx == selected:
                win.attrset(curses.color_pair(2))
                win.addstr(y_pos, 1, line.ljust(width - 2))
                win.attrset(curses.color_pair(1))
            else:
                win.addstr(y_pos, 1, line.ljust(width - 2))

        footer = "↑↓ Position  ←/→ Menge  Space Voll/0  F2 Weiter  F9 Zurück"
        win.attrset(curses.color_pair(3))
        win.addstr(height - 1, 1, _fit(footer, width - 2))
        win.attrset(curses.color_pair(1))
        win.refresh()

        key = win.get_wch()
        if key in (27, curses.KEY_F9):
            stdscr.erase()
            stdscr.refresh()
            return None
        if key == curses.KEY_DOWN:
            selected = move_selection(editable, selected, 1)
            continue
        if key == curses.KEY_UP:
            selected = move_selection(editable, selected, -1)
            continue
        if key == curses.KEY_RIGHT:
            row = editable[selected]
            if row["remaining_quantity"] > 0:
                row["selected_quantity"] = min(row["remaining_quantity"], int(row["selected_quantity"]) + 1)
            continue
        if key == curses.KEY_LEFT:
            row = editable[selected]
            if row["remaining_quantity"] > 0:
                row["selected_quantity"] = max(0, int(row["selected_quantity"]) - 1)
            continue
        if key == " ":
            row = editable[selected]
            if row["remaining_quantity"] > 0:
                row["selected_quantity"] = 0 if int(row["selected_quantity"]) > 0 else row["remaining_quantity"]
            continue
        if key == curses.KEY_F2:
            picked = [row for row in editable if int(row.get("selected_quantity") or 0) > 0]
            if not picked:
                message_box(stdscr, "Teilausfuehrung", "Bitte mindestens eine Menge > 0 waehlen.")
                continue
            stdscr.erase()
            stdscr.refresh()
            return picked


def get_latest_label_for_order(order_id):
    if not order_id:
        return None
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT
            id,
            carrier,
            track_id,
            parcel_number,
            status,
            created_at
        FROM gls_labels
        WHERE order_id = %s
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (order_id,),
    )
    row = cur.fetchone()
    cur.close()
    con.close()
    return row


def run_partial_execution_for_order(stdscr, order, order_items):
    selected_items = select_partial_items_dialog(stdscr, order, order_items)
    if not selected_items:
        return

    carrier = _execution_carrier_dialog(stdscr, effective_shipping_carrier(active_shipping_carrier()))
    if not carrier:
        return
    print_mode = _bulk_print_mode_dialog(stdscr)
    if print_mode is None:
        return

    selected_weight_kg, selected_weight_grams = calculate_selected_shipping_weight(selected_items)
    selected_for_note = []
    for item in selected_items:
        row = dict(item)
        row["quantity"] = int(item.get("selected_quantity") or 0)
        selected_for_note.append(row)

    try:
        created = create_shipping_label(
            order,
            weight_kg=selected_weight_kg,
            shipment_reference=f"{order.get('order_name') or ''}-PART",
            service_codes=None,
            carrier=carrier,
        )
        if print_mode in {"both", "label"} and created.get("label_path"):
            printed = _print_pdf_via_lp(stdscr, created["label_path"], f"{carrier.upper()} {created['shipment_reference']}", carrier=carrier)
            if printed and created.get("label_id") is not None:
                update_gls_label_status(created["label_id"], "PRINTED")

        note_path, _rows = create_delivery_note_pdf(order, selected_for_note)
        if print_mode in {"both", "note"}:
            _print_delivery_note_pdf_path(order, note_path)

        if created.get("label_id") is not None:
            fresh_rows = list_gls_labels(order["order_id"])
            current_label = next((row for row in fresh_rows if row["id"] == created["label_id"]), None)
            if current_label:
                queue_result = enqueue_shopify_fulfillment_job_for_items(current_label, selected_items, notify_customer=False)
                if queue_result.get("created"):
                    update_gls_label_status(created["label_id"], "SHOPIFY_QUEUED")
        message_box(
            stdscr,
            "Teilausfuehrung",
            f"OK: Label {created['track_id']} {selected_weight_grams}g erstellt",
        )
    except Exception as exc:
        PRINT_LOGGER.exception("Teilausfuehrung fehlgeschlagen order=%s", order.get("order_name"))
        message_box(stdscr, "Teilausfuehrung", f"{str(exc)[:28]} {PRINT_LOG_PATH.name}"[:56])


def _print_delivery_note_pdf_path(order, pdf_path):
    printer = SETTINGS["delivery_note_printer"].strip()
    if not printer:
        raise RuntimeError("Lieferschein Drucker nicht gesetzt.")
    cmd = ["lp", "-d", printer, "-t", f"Lieferschein {order['order_name']}"]
    cmd.extend(_cups_label_print_options(_delivery_note_format()))
    cmd.append(pdf_path)
    subprocess.run(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        check=True,
    )


def run_bulk_execution(stdscr, orders, order_items_cache, selected_order_ids):
    if not orders:
        message_box(stdscr, "Bulk", "Keine Bestellungen vorhanden.")
        return

    if selected_order_ids:
        selected_orders = [row for row in orders if row["order_id"] in selected_order_ids]
    else:
        selected_orders = []

    if not selected_orders:
        message_box(stdscr, "Bulk", "Bitte zuerst Auftraege markieren (Space).")
        return

    print_mode = _bulk_print_mode_dialog(stdscr)
    if print_mode is None:
        return
    shopify_mode = _bulk_shopify_queue_mode_dialog(stdscr)
    if shopify_mode is None:
        return

    carrier_map = {row["order_id"]: effective_shipping_carrier(active_shipping_carrier()) for row in selected_orders}
    if confirm_box(stdscr, "Bulk", "Dienstleister je Auftrag festlegen?", default_yes=False):
        carrier_map = bulk_carrier_per_order_dialog(stdscr, selected_orders, carrier_map)

    success_count = 0
    failure_count = 0
    queued_count = 0
    queue_failed_count = 0
    last_failure_summary = ""

    for order in selected_orders:
        carrier = effective_shipping_carrier(carrier_map.get(order["order_id"], active_shipping_carrier()))
        try:
            order_items = order_items_cache.get(order["order_id"])
            if order_items is None:
                order_items = get_order_items(order["order_id"])
                order_items_cache[order["order_id"]] = order_items

            order_weight_kg, _order_weight_grams = calculate_order_shipping_weight(order, order_items)
            created = create_shipping_label(order, weight_kg=order_weight_kg, carrier=carrier)
            if print_mode in {"both", "label"} and created.get("label_path"):
                printed = _print_pdf_via_lp(stdscr, created["label_path"], f"{carrier.upper()} {created['shipment_reference']}", carrier=carrier)
                if printed and created.get("label_id") is not None:
                    update_gls_label_status(created["label_id"], "PRINTED")

            note_path, _rows = create_delivery_note_pdf(order, order_items)
            if print_mode in {"both", "note"}:
                _print_delivery_note_pdf_path(order, note_path)

            if shopify_mode == "queue" and created.get("label_id") is not None:
                labels_for_order = list_gls_labels(order["order_id"])
                created_row = next((row for row in labels_for_order if row["id"] == created["label_id"]), None)
                if created_row:
                    try:
                        queue_result = enqueue_shopify_fulfillment_job(created_row, notify_customer=False)
                    except Exception:
                        PRINT_LOGGER.exception("Bulk Shopify Queue fehlgeschlagen order=%s", order.get("order_name"))
                        queue_failed_count += 1
                    else:
                        if queue_result.get("created"):
                            queued_count += 1
                            update_gls_label_status(created["label_id"], "SHOPIFY_QUEUED")

            success_count += 1
        except Exception as exc:
            failure_count += 1
            short_error = str(exc).strip() or exc.__class__.__name__
            last_failure_summary = f"{order.get('order_name')}: {short_error}"[:220]
            LOGGER.exception(
                "Bulk-Ausfuehrung fehlgeschlagen order=%s carrier=%s print_mode=%s shopify_mode=%s",
                order.get("order_name"),
                carrier,
                print_mode,
                shopify_mode,
            )
            LOGGER.error(
                "Bulk Fehler order=%s detail=%s",
                order.get("order_name"),
                short_error[:500],
            )
            PRINT_LOGGER.exception("Bulk-Ausfuehrung fehlgeschlagen order=%s", order.get("order_name"))
            PRINT_LOGGER.error("Bulk Fehler order=%s detail=%s", order.get("order_name"), short_error[:500])

    if failure_count and last_failure_summary:
        selected_order_ids.clear()
        message_box(
            stdscr,
            "Bulk",
            f"OK:{success_count} Err:{failure_count} {last_failure_summary[:28]} {MAIN_LOG_PATH.name}"[:56],
        )
        return

    selected_order_ids.clear()
    message_box(stdscr, "Bulk", f"Fertig OK:{success_count} Err:{failure_count} Q:{queued_count}/{queue_failed_count}"[:56])


def create_shipping_label_for_order(stdscr, order):
    resolved_carrier = effective_shipping_carrier(active_shipping_carrier())
    carrier = resolved_carrier.upper()
    if not confirm_box(stdscr, "Versandlabel", f"Label fuer {order['order_name']} erstellen?", default_yes=False):
        return
    order_weight_kg, total_grams = calculate_order_shipping_weight(order)
    try:
        created = create_shipping_label(order, weight_kg=order_weight_kg, carrier=resolved_carrier)
    except ValueError as exc:
        message_box(stdscr, "Versandlabel", str(exc)[:56])
        return
    except Exception as exc:
        PRINT_LOGGER.exception("Versandlabel Erstellung fehlgeschlagen carrier=%s order=%s", carrier, order["order_name"])
        message_box(stdscr, "Versandlabel", f"{str(exc)[:28]} {PRINT_LOG_PATH.name}"[:56])
        return

    printed = _print_pdf_via_lp(stdscr, created["label_path"], f"{carrier} {created['shipment_reference']}", carrier=carrier.lower())
    if printed:
        if created["label_id"] is not None:
            update_gls_label_status(created["label_id"], "PRINTED")
        message_box(stdscr, "Versandlabel", f"{carrier}: {created['track_id']} {total_grams}g gedruckt"[:56])
    else:
        if created["label_id"] is not None:
            update_gls_label_status(created["label_id"], "CREATED")
        message_box(stdscr, "Versandlabel", f"{carrier}: {created['track_id']} {total_grams}g nur PDF"[:56])


def create_manual_shipping_label(stdscr):
    state = {
        "name": "",
        "street": "",
        "zip": "",
        "city": "",
        "reference": "",
        "weight_grams": str(_shipping_packaging_weight_grams()),
    }
    active = 0
    country_code = "DE"
    selected_services = _normalize_shipping_services(SETTINGS.get("shipping_services", []))
    print_mode = "print"

    while True:
        fields = [
            {"name": "name", "label": "Empfaenger Name", "value": state["name"]},
            {"name": "street", "label": "Strasse", "value": state["street"]},
            {"name": "zip", "label": "PLZ", "value": state["zip"]},
            {"name": "city", "label": "Ort", "value": state["city"]},
            {"name": "reference", "label": "Referenz", "value": state["reference"]},
            {"name": "weight_grams", "label": "Gewicht (g)", "value": state["weight_grams"]},
            {"name": "country_display", "label": "Land (F3)", "value": _manual_label_country_display(country_code)},
            {"name": "services_display", "label": "Services (F4)", "value": _shipping_services_summary(selected_services)},
            {"name": "print_mode", "label": "Ausgabe (F5)", "value": "PDF + Drucken" if print_mode == "print" else "Nur PDF"},
        ]
        result = form_dialog(
            stdscr,
            "Versandlabel ohne Bestellung",
            fields,
            initial_active=active,
            footer_text="F3 Land  F4 Services  F5 Ausgabe  F2 Erstellen  F9 Zurueck",
            extra_actions=[
                {"name": "country", "keys": {curses.KEY_F3}},
                {"name": "services", "keys": {curses.KEY_F4}},
                {"name": "print_mode", "keys": {curses.KEY_F5}},
            ],
        )
        if result is None:
            return

        if "__action__" in result:
            state.update(result.get("__values__", {}))
            active = result.get("__active__", active)
            if result["__action__"] == "country":
                country_code = manual_country_dialog(stdscr, country_code)
            elif result["__action__"] == "services":
                selected_services = shipping_services_dialog(stdscr, selected_services)
            elif result["__action__"] == "print_mode":
                next_mode = manual_label_print_mode_dialog(stdscr, print_mode)
                if next_mode is None:
                    return
                print_mode = next_mode
            continue
        state.update(result)
        break

    required_fields = [
        ("name", "Empfaenger Name fehlt"),
        ("street", "Empfaenger Strasse fehlt"),
        ("zip", "Empfaenger PLZ fehlt"),
        ("city", "Empfaenger Ort fehlt"),
    ]
    for field_name, error_text in required_fields:
        if not state.get(field_name, "").strip():
            message_box(stdscr, "Versandlabel", error_text)
            return

    try:
        weight_grams = int((state.get("weight_grams") or "").strip())
    except ValueError:
        message_box(stdscr, "Versandlabel", "Gewicht muss eine ganze Zahl in g sein.")
        return
    if weight_grams <= 0:
        message_box(stdscr, "Versandlabel", "Gewicht muss groesser als 0 g sein.")
        return

    reference = state.get("reference", "").strip()
    if not reference:
        reference = f"MANUAL-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
    now_id = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    order_stub = {
        "order_id": f"manual-{now_id}",
        "order_name": reference,
        "shipping_name": state["name"].strip(),
        "shipping_address1": state["street"].strip(),
        "shipping_zip": state["zip"].strip(),
        "shipping_city": state["city"].strip(),
        "shipping_country": country_code,
    }
    carrier = active_shipping_carrier().upper()

    try:
        created = create_shipping_label(
            order_stub,
            weight_kg=round(weight_grams / 1000.0, 3),
            shipment_reference=reference,
            service_codes=selected_services,
        )
    except Exception as exc:
        PRINT_LOGGER.exception("Manuelles Versandlabel fehlgeschlagen reference=%s", reference)
        message_box(stdscr, "Versandlabel", f"{str(exc)[:28]} {PRINT_LOG_PATH.name}"[:56])
        return

    if print_mode == "print":
        printed = _print_pdf_via_lp(stdscr, created["label_path"], f"{carrier} {created['shipment_reference']}", carrier=carrier.lower())
        if printed and created.get("label_id") is not None:
            update_gls_label_status(created["label_id"], "PRINTED")
        if printed:
            message_box(stdscr, "Versandlabel", f"{carrier}: {created['track_id']} gedruckt"[:56])
        else:
            message_box(stdscr, "Versandlabel", f"{carrier}: {created['track_id']} nur PDF"[:56])
    else:
        message_box(stdscr, "Versandlabel", f"{carrier}: {created['track_id']} nur PDF"[:56])


def _format_gls_history_line(row, width):
    created_at = row.get("created_at")
    if isinstance(created_at, datetime.datetime):
        ts = created_at.strftime("%d.%m %H:%M")
    else:
        ts = "-"
    order_name = (row.get("order_name") or "-").replace("#", "")
    carrier = (row.get("carrier") or "gls").upper()
    track_id = _shipment_number(row)
    status = row.get("status") or "-"
    text = f"{ts} {_fit(carrier, 4)} {_fit(order_name, 11)} {_fit(track_id, 10)} {status}"
    return _fit(text, width)


def shipping_history_dialog(stdscr, selected_order=None):
    selected = 0
    top_index = 0
    show_all = False
    reload_rows = True
    rows = []

    while True:
        if reload_rows:
            filter_order_id = None if show_all else (selected_order["order_id"] if selected_order else None)
            rows = list_gls_labels(filter_order_id)
            reload_rows = False
            if selected >= len(rows):
                selected = max(0, len(rows) - 1)

        h, w = stdscr.getmaxyx()
        width = min(max(96, int(w * 0.9)), w - 4)
        height = min(max(18, int(h * 0.82)), h - 4)
        y = max(1, (h - height) // 2)
        x = max(2, (w - width) // 2)

        win = curses.newwin(height, width, y, x)
        win.keypad(True)
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        title = " Versandlabel-History (alle) " if show_all else " Versandlabel-History (Auftrag) "
        win.addstr(0, 2, title)

        left_width = max(46, int((width - 3) * 0.57))
        right_width = width - left_width - 3
        list_height = height - 4

        list_win = win.derwin(list_height, left_width, 1, 1)
        details_win = win.derwin(list_height, right_width, 1, 2 + left_width)

        display_lines = [_format_gls_history_line(row, left_width - 2) for row in rows] or ["Keine Versandlabels"]

        if selected < top_index:
            top_index = selected
        if selected >= top_index + max(1, list_height - 2):
            top_index = selected - max(1, list_height - 2) + 1

        draw_panel(list_win, "Labels", display_lines, selected if rows else 0, top_index, True)

        detail_lines = []
        chosen = rows[selected] if rows else None
        if chosen:
            job = get_latest_shopify_job_for_label(chosen["id"])
            detail_lines.append(_fit(f"Bestellung: {chosen.get('order_name') or '-'}", right_width - 2))
            detail_lines.append(_fit(f"Dienst: {(chosen.get('carrier') or 'gls').upper()}", right_width - 2))
            detail_lines.append(_fit(f"TrackID: {chosen.get('track_id') or '-'}", right_width - 2))
            detail_lines.append(_fit(f"Sendungsnr.: {_shipment_number(chosen)}", right_width - 2))
            detail_lines.append(_fit(f"Status: {chosen.get('status') or '-'}", right_width - 2))
            detail_lines.append(_fit(f"Quelle: {_shipment_source_label(chosen.get('source'))}", right_width - 2))
            detail_lines.append(_fit(f"Ref: {chosen.get('shipment_reference') or '-'}", right_width - 2))
            if chosen.get("shopify_fulfillment_id"):
                detail_lines.append(_fit(f"Shopify Fulfillment: {chosen.get('shopify_fulfillment_id')}", right_width - 2))
            if job:
                detail_lines.append(_fit(f"Shopify: {job.get('status') or '-'} (Versuch {job.get('attempts') or 0})", right_width - 2))
                if job.get("result_message"):
                    detail_lines.append(_fit(f"Shopify Msg: {job['result_message']}", right_width - 2))
            else:
                detail_lines.append(_fit("Shopify: -", right_width - 2))
            detail_lines.append("")
            detail_lines.append(_fit(f"PDF: {chosen.get('label_path') or '-'}", right_width - 2))
            if chosen.get("tracking_url"):
                detail_lines.append(_fit(f"Tracking URL: {chosen.get('tracking_url')}", right_width - 2))
            if chosen.get("last_error"):
                detail_lines.append(_fit(f"Fehler: {chosen['last_error']}", right_width - 2))
        else:
            detail_lines.append("Keine Labels gefunden")

        draw_panel(details_win, "Details", detail_lines, 0, 0, False)

        footer = " F2 Alle/Auftrag  F5 Drucken  F6 Storno  F7 Reprint  F9 Zurueck  F10 Shopify "
        win.attrset(curses.color_pair(3))
        win.addstr(height - 1, 1, " " * (width - 2))
        win.addstr(height - 1, 1, footer[: width - 2])
        win.refresh()

        key = win.get_wch()
        if key in (27, curses.KEY_F9):
            return
        if key == curses.KEY_DOWN:
            selected = move_selection(rows, selected, 1)
        elif key == curses.KEY_UP:
            selected = move_selection(rows, selected, -1)
        elif key == curses.KEY_NPAGE:
            selected = move_selection(rows, selected, max(1, list_height - 2))
        elif key == curses.KEY_PPAGE:
            selected = move_selection(rows, selected, -max(1, list_height - 2))
        elif key == curses.KEY_F2:
            show_all = not show_all
            selected = 0
            top_index = 0
            reload_rows = True
        elif key == curses.KEY_F5 and chosen:
            if not os.path.isfile(chosen["label_path"]):
                message_box(stdscr, "History", "PDF fehlt. Bitte Reprint (F7) nutzen.")
                continue
            if _print_pdf_via_lp(
                stdscr,
                chosen["label_path"],
                f"{(chosen.get('carrier') or 'gls').upper()} {chosen['shipment_reference']}",
                carrier=(chosen.get("carrier") or "gls").lower(),
            ):
                update_gls_label_status(chosen["id"], "REPRINTED")
                message_box(stdscr, "History", "Label erneut gedruckt.")
                reload_rows = True
        elif key == curses.KEY_F7 and chosen:
            try:
                new_path = reprint_shipping_label(chosen)
            except Exception as exc:
                PRINT_LOGGER.exception("Versand Reprint fehlgeschlagen carrier=%s track=%s", chosen.get("carrier"), chosen.get("track_id"))
                update_gls_label_status(chosen["id"], "REPRINT_FAILED", str(exc)[:120])
                message_box(stdscr, "Reprint", f"{str(exc)[:28]} {PRINT_LOG_PATH.name}"[:56])
            else:
                message_box(stdscr, "Reprint", new_path[-56:])
                reload_rows = True
        elif key == curses.KEY_F6 and chosen:
            if not confirm_box(stdscr, "Storno", f"Sendung {chosen['track_id']} stornieren?"):
                continue
            try:
                result = cancel_shipping_label(chosen)
            except Exception as exc:
                PRINT_LOGGER.exception("Versand Storno fehlgeschlagen carrier=%s track=%s", chosen.get("carrier"), chosen.get("track_id"))
                message_box(stdscr, "Storno", f"{str(exc)[:28]} {PRINT_LOG_PATH.name}"[:56])
            else:
                message_box(stdscr, "Storno", f"Status: {result}"[:56])
                reload_rows = True
        elif key == curses.KEY_F10 and chosen:
            if str(chosen.get("order_id") or "").startswith("manual-"):
                message_box(stdscr, "Shopify Queue", "Manuelle Labels haben keine Shopify-Bestellung.")
                continue
            if (chosen.get("carrier") or "").strip().lower() == "test":
                message_box(stdscr, "Shopify Queue", "Test-Labels duerfen nicht an Shopify gesendet werden.")
                continue
            if (chosen.get("source") or "").strip().lower() == "shopify":
                message_box(stdscr, "Shopify Queue", "Diese Sendung ist bereits aus Shopify eingelesen.")
                continue
            if not confirm_box(stdscr, "Shopify", f"Fulfillment senden fuer {chosen['track_id']}?"):
                continue
            try:
                queue_result = enqueue_shopify_fulfillment_job(chosen, notify_customer=False)
            except Exception as exc:
                PRINT_LOGGER.exception("Shopify Queue fehlgeschlagen track=%s", chosen.get("track_id"))
                update_gls_label_status(chosen["id"], "SHOPIFY_QUEUE_FAILED", str(exc)[:160])
                message_box(stdscr, "Shopify Queue", f"{str(exc)[:28]} {PRINT_LOG_PATH.name}"[:56])
            else:
                if queue_result.get("created"):
                    update_gls_label_status(chosen["id"], "SHOPIFY_QUEUED")
                    message_box(stdscr, "Shopify Queue", f"Job {queue_result['job_id']} eingereiht.")
                else:
                    message_box(stdscr, "Shopify Queue", f"Job {queue_result['job_id']} laeuft bereits.")
                reload_rows = True


def orders_dialog(stdscr):
    order_filter = None
    only_pending = False
    fulfillment_filter = "all"
    payment_filter = "all"
    selected = 0
    top_index = 0
    orders = []
    order_items_cache = {}
    selected_order_ids = set()
    reload_orders = True
    current_order_id = None
    last_orders_refresh_at = None

    while True:
        if reload_orders or should_refresh_orders(last_orders_refresh_at):
            orders = get_orders(
                order_filter,
                only_pending=only_pending,
                fulfillment_filter=fulfillment_filter,
                payment_filter=payment_filter,
            )
            order_items_cache = {}
            selected_order_ids = {order_id for order_id in selected_order_ids if any(row["order_id"] == order_id for row in orders)}
            reload_orders = False
            last_orders_refresh_at = time.monotonic()

        if selected >= len(orders):
            selected = len(orders) - 1
        if selected < 0:
            selected = 0

        selected_order = orders[selected] if orders else None
        selected_order_id = selected_order["order_id"] if selected_order else None

        if selected_order_id != current_order_id:
            current_order_id = selected_order_id

        if selected_order_id and selected_order_id not in order_items_cache:
            order_items_cache[selected_order_id] = get_order_items(selected_order_id)

        order_items = order_items_cache.get(selected_order_id, [])

        h, w = stdscr.getmaxyx()
        width = min(max(88, int(w * 0.84)), w - 6)
        height = min(max(18, int(h * 0.82)), h - 4)
        y = max(1, (h - height) // 2)
        x = max(2, (w - width) // 2)

        win = curses.newwin(height, width, y, x)
        win.keypad(True)
        win.timeout(1000)
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        win.addstr(0, 2, " Bestellungen ")

        left_width = max(34, int((width - 3) * 0.42))
        right_width = width - left_width - 3
        list_height = height - 4

        orders_win = win.derwin(list_height, left_width, 1, 1)
        details_win = win.derwin(list_height, right_width, 1, 2 + left_width)

        order_lines = []
        for order in orders:
            status_value = (order.get("fulfillment_status") or "").strip().lower()
            open_hint = "[!]" if status_value not in {"fulfilled", "cancelled"} else "   "
            mark = "[x]" if order["order_id"] in selected_order_ids else "[ ]"
            order_lines.append(
                f"{mark}{open_hint} {_fit(order['order_name'], 10)} {_fit(format_address(order), left_width - 19)}"
            )
        if not order_lines:
            order_lines = ["Keine Bestellungen"]

        if selected < top_index:
            top_index = selected
        if selected >= top_index + max(1, list_height - 2):
            top_index = selected - max(1, list_height - 2) + 1

        draw_panel(orders_win, "Auftraege", order_lines, selected if orders else 0, top_index, True)

        detail_lines = []
        if selected_order:
            selected_weight_kg, selected_weight_grams = calculate_order_shipping_weight(selected_order, order_items)
            country = _localized_country_display(selected_order.get("shipping_country"))
            order_shipments = list_gls_labels(selected_order["order_id"])
            created_at = selected_order.get("created_at")
            if isinstance(created_at, datetime.datetime):
                ordered_at_text = created_at.strftime("%d.%m.%Y %H:%M")
            else:
                ordered_at_text = "-"
            detail_lines.append(_fit(f"Bestellung: {selected_order['order_name']}", right_width - 2))
            detail_lines.append(_fit(format_address(selected_order), right_width - 2))
            detail_lines.append(_fit(f"Land: {country}", right_width - 2))
            detail_lines.append(_fit(f"E-Mail: {selected_order.get('shipping_email') or '-'}", right_width - 2))
            detail_lines.append(_fit(f"Telefon: {selected_order.get('shipping_phone') or '-'}", right_width - 2))
            detail_lines.append(_fit(f"Bestellt: {ordered_at_text}", right_width - 2))
            status = _localized_fulfillment_status(selected_order["fulfillment_status"])
            payment_status = _localized_payment_status(selected_order["payment_status"])
            internal_qty = selected_order.get("local_internal_qty") or 0
            detail_lines.append(_fit(f"Status: {status}", right_width - 2))
            detail_lines.append(_fit(f"Zahlung: {payment_status}", right_width - 2))
            detail_lines.append(_fit(f"Interne Pos.-Menge: {internal_qty}", right_width - 2))
            detail_lines.append(_fit(f"Versandgewicht: {selected_weight_grams} g ({selected_weight_kg:.3f} kg)", right_width - 2))
            detail_lines.extend(_shipment_summary_lines(order_shipments, right_width - 13))
            detail_lines.append("")
            qty_width, sku_width, regal_width, fach_width, platz_width, title_width = format_order_item_header(right_width - 2)
            detail_lines.append(
                f"{_fit('Off/Ges', qty_width)} {_fit('SKU', sku_width)} {_fit('Artikel', title_width)} {_fit('Regal', regal_width)} {_fit('Fach', fach_width)} {_fit('Platz', platz_width)}"
            )
            detail_lines.append("-" * max(1, right_width - 2))

            for row in order_items:
                detail_lines.append(format_order_item_row(row, right_width - 2))
        else:
            detail_lines.append("Keine Bestellung gefunden")

        draw_panel(details_win, "Positionen", detail_lines, 0, 0, False)

        footer = " Space Mark  F1 Offen  F2 Status  F3 Springen  F4 Versand-History  F5 Pickliste  F6 PDF  F7 Lieferschein  Shift+F7 Teilausf.  F8 Versandlabel  Shift+F8 Manuell  F9 Zurueck  F10 Zahlung  F11 Bulk  F12 Alle "
        filter_tags = []
        if order_filter:
            filter_tags.append(f"Text:{order_filter}")
        if only_pending:
            filter_tags.append("nur offen")
        if fulfillment_filter != "all":
            filter_tags.append(_fulfillment_filter_label(fulfillment_filter).replace("Status: ", ""))
        if payment_filter != "all":
            filter_tags.append(_payment_filter_label(payment_filter).replace("Zahlung: ", ""))
        if filter_tags:
            footer = f" Filter[{', '.join(filter_tags)}] " + footer
        win.attrset(curses.color_pair(3))
        win.addstr(height - 1, 1, " " * (width - 2))
        win.addstr(height - 1, 1, footer[:width - 2])
        win.refresh()

        try:
            key = win.get_wch()
        except curses.error:
            continue

        if key in (27, curses.KEY_F9, curses.KEY_F12):
            if key in (27, curses.KEY_F9):
                return
            if key == curses.KEY_F12:
                if not orders:
                    continue
                if len(selected_order_ids) == len(orders):
                    selected_order_ids.clear()
                else:
                    selected_order_ids = {row["order_id"] for row in orders}
                continue
        if key == curses.KEY_DOWN:
            selected = move_selection(orders, selected, 1)
        elif key == curses.KEY_UP:
            selected = move_selection(orders, selected, -1)
        elif key == curses.KEY_NPAGE:
            selected = move_selection(orders, selected, max(1, list_height - 2))
        elif key == curses.KEY_PPAGE:
            selected = move_selection(orders, selected, -max(1, list_height - 2))
        elif key == curses.KEY_F1:
            only_pending = not only_pending
            selected = 0
            top_index = 0
            reload_orders = True
        elif key == curses.KEY_F2:
            current_index = FULFILLMENT_FILTER_SEQUENCE.index(fulfillment_filter) if fulfillment_filter in FULFILLMENT_FILTER_SEQUENCE else 0
            fulfillment_filter = FULFILLMENT_FILTER_SEQUENCE[(current_index + 1) % len(FULFILLMENT_FILTER_SEQUENCE)]
            selected = 0
            top_index = 0
            reload_orders = True
        elif key == curses.KEY_F10:
            current_index = PAYMENT_FILTER_SEQUENCE.index(payment_filter) if payment_filter in PAYMENT_FILTER_SEQUENCE else 0
            payment_filter = PAYMENT_FILTER_SEQUENCE[(current_index + 1) % len(PAYMENT_FILTER_SEQUENCE)]
            selected = 0
            top_index = 0
            reload_orders = True
        elif key == curses.KEY_F11:
            run_bulk_execution(stdscr, orders, order_items_cache, selected_order_ids)
            reload_orders = True
        elif key == curses.KEY_F3:
            value = order_jump_dialog(stdscr, order_filter or "")
            if value is not None:
                order_filter = value or None
                selected = 0
                top_index = 0
                reload_orders = True
                if value:
                    matched_orders = get_orders(
                        order_filter,
                        only_pending=only_pending,
                        fulfillment_filter=fulfillment_filter,
                        payment_filter=payment_filter,
                    )
                    target_index = jump_to_order(matched_orders, value)
                    orders = matched_orders
                    reload_orders = False
                    if target_index is not None:
                        selected = target_index
        elif key == curses.KEY_F5 and selected_order:
            print_picklist(stdscr, selected_order, order_items)
        elif key == curses.KEY_F6 and selected_order:
            export_delivery_note_pdf(stdscr, selected_order, order_items)
        elif key == curses.KEY_F7 and selected_order:
            print_delivery_note(stdscr, selected_order, order_items)
        elif key in (curses.KEY_F19, "t", "T") and selected_order:
            run_partial_execution_for_order(stdscr, selected_order, order_items)
        elif key == curses.KEY_F8 and selected_order:
            create_shipping_label_for_order(stdscr, selected_order)
        elif key in (curses.KEY_F20, "m", "M"):
            create_manual_shipping_label(stdscr)
        elif key == curses.KEY_F4:
            shipping_history_dialog(stdscr, selected_order)
        elif key == " " and selected_order:
            order_id = selected_order["order_id"]
            if order_id in selected_order_ids:
                selected_order_ids.remove(order_id)
            else:
                selected_order_ids.add(order_id)


def inventory_count_dialog(stdscr, line):
    res = form_dialog(
        stdscr,
        "Inventur Menge",
        [
            {"name": "soll", "label": "Soll", "value": str(line["soll_menge"])},
            {"name": "ist", "label": "Ist", "value": "" if line["ist_menge"] is None else str(line["ist_menge"])},
        ],
        initial_active=1,
    )

    if res is None:
        return None

    ist_raw = res["ist"].strip()
    if ist_raw == "":
        return None

    return parse_int_or_error(stdscr, ist_raw, "Ist")


def inventory_dialog(stdscr):
    session = get_active_inventory_session()
    if session is None:
        if not confirm_box(stdscr, "Inventur", "Neue Inventur starten?"):
            return False
        session = create_inventory_session()

    selected = 0
    top_index = 0
    show_differences = False
    lines = []
    reload_lines = True

    while True:
        if reload_lines:
            lines = get_inventory_lines(session["session_id"], show_differences)
            reload_lines = False

        if selected >= len(lines):
            selected = len(lines) - 1
        if selected < 0:
            selected = 0

        h, w = stdscr.getmaxyx()
        width = min(max(96, int(w * 0.9)), w - 4)
        height = min(max(20, int(h * 0.88)), h - 2)
        y = max(1, (h - height) // 2)
        x = max(2, (w - width) // 2)

        win = curses.newwin(height, width, y, x)
        win.keypad(True)
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        win.addstr(0, 2, f" {session['session_name']} ")

        visible_width = width - 4
        display_lines = build_inventory_lines_display(lines, visible_width)
        list_height = height - 5

        if selected + 2 < top_index:
            top_index = selected + 2
        if selected + 2 >= top_index + max(1, list_height):
            top_index = selected + 2 - max(1, list_height) + 1

        draw_panel(
            win.derwin(list_height + 2, width - 2, 1, 1),
            "Inventur",
            display_lines,
            selected + 2 if lines else 0,
            top_index,
            True,
        )

        total, counted, differences = inventory_session_summary(get_inventory_lines(session["session_id"]))
        footer = f" F2 Neu  F3 Zaehlen  F4 CSV  F5 Drucken  F6 Diff  F7 Uebern.  F9 Zurueck | Pos {total} Gezaehlt {counted} Diff {differences} "
        win.attrset(curses.color_pair(3))
        win.addstr(height - 1, 1, " " * (width - 2))
        win.addstr(height - 1, 1, footer[:width - 2])
        win.refresh()

        key = win.get_wch()

        if key in (27, curses.KEY_F9):
            return False
        if key == curses.KEY_DOWN:
            selected = move_selection(lines, selected, 1)
        elif key == curses.KEY_UP:
            selected = move_selection(lines, selected, -1)
        elif key == curses.KEY_NPAGE:
            selected = move_selection(lines, selected, max(1, list_height - 2))
        elif key == curses.KEY_PPAGE:
            selected = move_selection(lines, selected, -max(1, list_height - 2))
        elif key == curses.KEY_F2:
            if confirm_box(stdscr, "Inventur", "Neue Inventur erzeugen? Aktive wird archiviert."):
                session = create_inventory_session()
                selected = 0
                top_index = 0
                reload_lines = True
        elif key == curses.KEY_F3 and lines:
            qty = inventory_count_dialog(stdscr, lines[selected])
            if qty is not None:
                set_inventory_count(session["session_id"], lines[selected]["line_no"], qty)
                reload_lines = True
        elif key == curses.KEY_F4:
            path = export_inventory_csv(session, get_inventory_lines(session["session_id"]))
            message_box(stdscr, "CSV Export", path[-56:])
        elif key == curses.KEY_F5:
            print_inventory_list(stdscr, session, get_inventory_lines(session["session_id"]))
        elif key == curses.KEY_F6:
            show_differences = not show_differences
            selected = 0
            top_index = 0
            reload_lines = True
        elif key == curses.KEY_F7:
            all_lines = get_inventory_lines(session["session_id"])
            counted_now = sum(1 for row in all_lines if row["ist_menge"] is not None)
            if counted_now == 0:
                message_box(stdscr, "Inventur", "Noch keine Ist-Mengen erfasst.")
            elif confirm_box(stdscr, "Inventur", "Erfasste Mengen in Bestand uebernehmen?"):
                apply_inventory_session(session["session_id"])
                message_box(stdscr, "Inventur", "Inventur wurde uebernommen.")
                return True


def main(stdscr):

    curses.curs_set(1)
    curses.start_color()
    curses.use_default_colors()
    stdscr.encoding = "utf-8"

    apply_color_theme(stdscr)
    stdscr.keypad(True)

    if not ensure_database_ready(stdscr):
        return

    left_selected = 0
    left_top_index = 0
    right_selected = 0
    right_top_index = 0
    active_pane = "left"
    show_secondary_help = False

    filter_text = None
    filter_no_location = False
    filter_local = False
    sort_mode = "location"
    external_mode = "hide"
    items = []
    location_rows = []
    reload_items = True

    while True:
        if reload_items:
            items = get_items(filter_text, filter_no_location, filter_local, sort_mode, external_mode)
            location_rows = build_location_rows(items)
            reload_items = False

        if left_selected >= len(items):
            left_selected = len(items) - 1

        if left_selected < 0:
            left_selected = 0

        if right_selected >= len(location_rows):
            right_selected = len(location_rows) - 1

        if right_selected < 0:
            right_selected = 0

        h, _ = stdscr.getmaxyx()
        page = max(1, h - 8)

        if left_selected + 1 < left_top_index:
            left_top_index = left_selected + 1

        if left_selected + 1 >= left_top_index + page:
            left_top_index = left_selected - page + 2

        if right_selected < right_top_index:
            right_top_index = right_selected

        if right_selected >= right_top_index + page:
            right_top_index = right_selected - page + 1

        draw(
            stdscr,
            items,
            left_selected,
            left_top_index,
            location_rows,
            right_selected,
            right_top_index,
            active_pane,
            filter_text,
            show_secondary_help,
            external_mode,
        )

        key = stdscr.get_wch()
        selected_item = (
            get_selected_item(items, left_selected)
            if active_pane == "left"
            else get_selected_location_item(location_rows, right_selected)
        )

        if key == '\t':
            active_pane = "right" if active_pane == "left" else "left"
            continue

        if key == curses.KEY_DOWN:
            if active_pane == "left":
                left_selected = move_selection(items, left_selected, 1)
            else:
                right_selected = move_selection(location_rows, right_selected, 1)

        elif key == curses.KEY_UP:
            if active_pane == "left":
                left_selected = move_selection(items, left_selected, -1)
            else:
                right_selected = move_selection(location_rows, right_selected, -1)

        elif key == curses.KEY_NPAGE:
            if active_pane == "left":
                left_selected = move_selection(items, left_selected, page)
            else:
                right_selected = move_selection(location_rows, right_selected, page)

        elif key == curses.KEY_PPAGE:
            if active_pane == "left":
                left_selected = move_selection(items, left_selected, -page)
            else:
                right_selected = move_selection(location_rows, right_selected, -page)

        elif key == curses.KEY_F1:
            if sort_mode == "location":
                sort_mode = "name"
            elif sort_mode == "name":
                sort_mode = "sku"
            else:
                sort_mode = "location"

            left_selected = 0
            left_top_index = 0
            right_selected = 0
            right_top_index = 0
            reload_items = True

        elif key == curses.KEY_F2:
            filter_local = not filter_local
            left_selected = 0
            left_top_index = 0
            right_selected = 0
            right_top_index = 0
            reload_items = True

        elif key == curses.KEY_F3:
            filter_no_location = not filter_no_location
            left_selected = 0
            left_top_index = 0
            right_selected = 0
            right_top_index = 0
            reload_items = True

        elif key == curses.KEY_F4 and selected_item:
            item_info_dialog(stdscr, selected_item)

        elif key == curses.KEY_F5:
            add_item(stdscr)
            reload_items = True

        elif key == curses.KEY_F6 and selected_item:
            change_location(stdscr, selected_item)
            reload_items = True

        elif key == curses.KEY_F7 and selected_item:
            change_qty(stdscr, selected_item)
            reload_items = True

        elif key == curses.KEY_F8 and selected_item:
            print_label(stdscr, selected_item)

        elif key == curses.KEY_F1 + 12:
            if inventory_dialog(stdscr):
                reload_items = True

        elif key == curses.KEY_F5 + 12 and selected_item:
            edit_item(stdscr, selected_item)
            reload_items = True

        elif key == curses.KEY_F8 + 12 and selected_item:
            print_label_multiple(stdscr, selected_item)

        elif key == curses.KEY_F11 + 12:
            settings_dialog(stdscr)

        elif key == curses.KEY_F9:
            filter_text = None
            filter_no_location = False
            filter_local = False
            external_mode = "hide"
            left_selected = 0
            left_top_index = 0
            right_selected = 0
            right_top_index = 0
            reload_items = True

        elif key == curses.KEY_F10:
            break

        elif key == curses.KEY_F11:
            show_secondary_help = not show_secondary_help

        elif key == curses.KEY_F12:
            orders_dialog(stdscr)

        elif key in (curses.KEY_BACKSPACE, 127, 8, '\x7f', '\b'):

            if filter_text:
                filter_text = filter_text[:-1]

            if filter_text == "":
                filter_text = None

            left_selected = 0
            left_top_index = 0
            right_selected = 0
            right_top_index = 0
            reload_items = True

        elif isinstance(key, str):
            if key in ('\n', '\r'):
                continue

            if filter_text is None:
                filter_text = ""

            filter_text += key

            left_selected = 0
            left_top_index = 0
            right_selected = 0
            right_top_index = 0
            reload_items = True


try:
    LOGGER.debug("Starte lager_mc")
    curses.wrapper(main)
except Exception:
    LOGGER.exception("lager_mc Start oder Laufzeitfehler")
    raise
