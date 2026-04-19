#!/usr/bin/env python3
import curses
import csv
import datetime
import base64
import binascii
import address_label
import html
import json
import os
import psycopg2
import psycopg2.extras
import locale
import queue
import re
import ssl
import subprocess
import sys
import string
import shutil
import tempfile
import textwrap
import time
import threading
from pathlib import Path
from urllib.parse import urlparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from address_label import build_address_label_pdf
from app_logging import MAIN_LOG_PATH, PRINT_LOG_PATH, get_logger
from app_settings import DEFAULT_SETTINGS, load_settings, save_settings
from app_version import APP_VERSION
from delivery_note import build_delivery_note_pdf, build_delivery_note_rows
from languages import COUNTRY_NAMES, COUNTRY_ORDER, SUPPORTED_LANGUAGES, TRANSLATIONS
from post.internetmarke_client import InternetmarkeClient
from post.product_catalog import find_post_product, list_post_base_products
from shipping.carriers import (
    DEFAULT_ACTIVE_SHIPPING_CARRIERS,
    SHIPPING_CARRIER_DEFINITIONS,
    SHIPPING_CARRIER_ORDER,
    ShippingCarrierRuntime,
    carrier_allows_shopify as _shipping_carrier_allows_shopify,
    carrier_definition as _shipping_carrier_definition,
    carrier_field_to_code as _shipping_carrier_field_to_code,
    carrier_label as _shipping_carrier_label,
    carrier_option_mode as _shipping_carrier_option_mode,
    carrier_setting_field as _shipping_carrier_setting_field,
    configurable_carrier_codes as _configurable_shipping_carrier_codes,
    default_tracking_mode_for_carrier as _default_tracking_mode_for_carrier,
    normalize_active_carriers as _normalize_active_shipping_carriers,
    shipping_active_carrier_values as _shipping_active_carrier_values,
    shipping_carrier_options as _shipping_carrier_options_impl,
    shopify_tracking_company as _shopify_tracking_company,
)
from shipping.history import (
    SHIPPING_LABEL_TABLE,
    find_or_create_shopify_fulfillment_job as _find_or_create_shopify_fulfillment_job,
    get_latest_shopify_job_for_label as _get_latest_shopify_job_for_label,
    get_latest_shipping_label_for_order as _get_latest_shipping_label_for_order,
    insert_shipping_label_history as _insert_shipping_label_history,
    list_shipping_labels as _list_shipping_labels,
    update_shipping_label_reprint as _update_shipping_label_reprint,
    update_shipping_label_status as _update_shipping_label_status,
)
from shipping.schema import apply_app_schema, collect_schema_issues

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
SHOPIFY_SYNC_SERVICE = "shopify-sync"
_SERVICE_RUNTIME_CACHE = {"loaded_at": 0.0, "rows": {}}
_POST_PAGE_FORMAT_CACHE = {"loaded_at": 0.0, "formats": []}
_POST_SELECTION_CACHE = {}
_SHIPPING_CARRIER_CACHE = "gls"
_SHOPIFY_CUSTOMER_CACHE = {"loaded_at": 0.0, "rows": []}
_BACKGROUND_UI_EVENTS = queue.Queue()
_PENDING_ITEM_WRITES = {}
_PENDING_ITEM_WRITES_LOCK = threading.Lock()
_UNSET = object()
_ACTIVE_SHOPIFY_LOCATION_ID = (SETTINGS.get("shopify_active_location_id") or "").strip() or None
_ACTIVE_SHOPIFY_LOCATION_NAME = ""

SHIPPING_SERVICE_OPTIONS = [
    {"code": "service_flexdelivery", "label_key": "shipping_service_flexdelivery", "locked": False},
    {"code": "service_addresseeonly", "label_key": "shipping_service_addresseeonly", "locked": False},
    {"code": "service_guaranteed24", "label_key": "shipping_service_guaranteed24", "locked": False},
    {"code": "service_preadvice", "label_key": "shipping_service_preadvice", "locked": False},
    {"code": "service_smsservice", "label_key": "shipping_service_smsservice", "locked": False},
]

MANUAL_LABEL_COUNTRY_OPTIONS = [
    {"value": code, "label": COUNTRY_NAMES["en"][code]}
    for code in COUNTRY_ORDER
]

COUNTRY_NAME_DE = COUNTRY_NAMES["de"]

COUNTRY_ALPHA3 = {
    "AD": "AND",
    "AT": "AUT",
    "BE": "BEL",
    "BG": "BGR",
    "CH": "CHE",
    "CY": "CYP",
    "CZ": "CZE",
    "DE": "DEU",
    "DK": "DNK",
    "EE": "EST",
    "ES": "ESP",
    "FI": "FIN",
    "FR": "FRA",
    "GB": "GBR",
    "GR": "GRC",
    "HR": "HRV",
    "HU": "HUN",
    "IE": "IRL",
    "IS": "ISL",
    "IT": "ITA",
    "LI": "LIE",
    "LT": "LTU",
    "LU": "LUX",
    "LV": "LVA",
    "MC": "MCO",
    "MT": "MLT",
    "NL": "NLD",
    "NO": "NOR",
    "PL": "POL",
    "PT": "PRT",
    "RO": "ROU",
    "SE": "SWE",
    "SI": "SVN",
    "SK": "SVK",
    "SM": "SMR",
    "VA": "VAT",
}

FULFILLMENT_FILTER_SEQUENCE = ["all", "open", "unfulfilled", "partial", "fulfilled"]
PAYMENT_FILTER_SEQUENCE = ["all", "paid", "pending", "authorized", "partially_paid", "refunded", "voided"]
ITEMS_AUTO_REFRESH_SECONDS = 10.0
ORDERS_AUTO_REFRESH_SECONDS = 10.0
ORDER_DETAILS_LOAD_DELAY_SECONDS = 0.25
SERVICE_RUNTIME_CACHE_SECONDS = 120.0
SHOPIFY_CUSTOMER_CACHE_SECONDS = 120.0


COLS = [
    ("SKU", 18),
    ("Name", 52),
    ("Regal", 7),
    ("Fach", 6),
    ("Platz", 7),
    ("Lokal", 7),
    ("Gesamt", 7),
    ("N. verf.", 8),
    ("Best.", 7),
    ("Verfüg.", 7),
    ("S", 2),
]


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
    apply_app_schema(cur)
    con.commit()
    cur.close()
    con.close()


def database_schema_issues():
    con = db()
    cur = con.cursor()
    try:
        return collect_schema_issues(cur)
    finally:
        cur.close()
        con.close()


class DatabaseUnavailableError(RuntimeError):
    pass


class DatabaseBusyError(RuntimeError):
    pass


class BackgroundValueLoader:
    def __init__(self):
        self._condition = threading.Condition()
        self._pending = None
        self._result = None
        self._result_id = 0
        self._consumed_id = 0
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def request(self, key, fn, *args, **kwargs):
        with self._condition:
            self._pending = (key, fn, args, kwargs)
            self._condition.notify()

    def poll(self):
        with self._condition:
            if self._result_id == self._consumed_id:
                return None
            self._consumed_id = self._result_id
            return self._result

    def _run(self):
        while True:
            with self._condition:
                while self._pending is None:
                    self._condition.wait()
                key, fn, args, kwargs = self._pending
                self._pending = None
            try:
                value = fn(*args, **kwargs)
                error = None
            except Exception as exc:
                value = None
                error = exc
            with self._condition:
                self._result = {
                    "key": key,
                    "value": value,
                    "error": error,
                    "loaded_at": time.monotonic(),
                }
                self._result_id += 1


class BackgroundMapLoader:
    def __init__(self):
        self._condition = threading.Condition()
        self._queue = []
        self._queued_keys = set()
        self._results = []
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def request(self, key, fn, *args, **kwargs):
        with self._condition:
            if key in self._queued_keys:
                return
            self._queue.append((key, fn, args, kwargs))
            self._queued_keys.add(key)
            self._condition.notify()

    def poll_all(self):
        with self._condition:
            if not self._results:
                return []
            results = list(self._results)
            self._results.clear()
            return results

    def clear(self):
        with self._condition:
            self._queue.clear()
            self._queued_keys.clear()
            self._results.clear()

    def _run(self):
        while True:
            with self._condition:
                while not self._queue:
                    self._condition.wait()
                key, fn, args, kwargs = self._queue.pop(0)
                self._queued_keys.discard(key)
            try:
                value = fn(*args, **kwargs)
                error = None
            except Exception as exc:
                value = None
                error = exc
            with self._condition:
                self._results.append(
                    {
                        "key": key,
                        "value": value,
                        "error": error,
                        "loaded_at": time.monotonic(),
                    }
                )


_SERVICE_RUNTIME_LOADER = BackgroundValueLoader()
_ITEMS_SNAPSHOT_LOADER = BackgroundValueLoader()
_ORDERS_SNAPSHOT_LOADER = BackgroundValueLoader()
_ORDER_ITEMS_LOADER = BackgroundMapLoader()
_ORDER_SHIPMENTS_LOADER = BackgroundMapLoader()


def _summarize_db_error(exc):
    text = str(exc or "").strip()
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return "Datenbank ist nicht erreichbar."
    return " | ".join(lines[:2])[:180]


def _execute_db_query(cur, query, params=None, deadlock_retries=1):
    params = params or []
    for attempt in range(deadlock_retries + 1):
        try:
            cur.execute(query, params)
            return
        except psycopg2.errors.DeadlockDetected as exc:
            if attempt < deadlock_retries:
                time.sleep(0.2 * (attempt + 1))
                continue
            raise DatabaseBusyError("Datenbank ist kurzzeitig blockiert. Bitte erneut versuchen.") from exc


def _probe_database_ready():
    ready, message, _steps = _probe_database_ready_verbose()
    return ready, message


def _probe_database_ready_verbose(progress_callback=None):
    progress_lines = []

    def report(message):
        text = (message or "").strip()
        if not text:
            return
        progress_lines.append(text)
        if progress_callback is not None:
            progress_callback(list(progress_lines))

    if _is_default_db_settings(SETTINGS):
        report(t("db_settings_missing"))
        return False, t("db_settings_save_first"), progress_lines
    try:
        report(t("db_wait_connecting", host=SETTINGS.get("db_host") or "-", db=SETTINGS.get("db_name") or "-"))
        con = db()
        cur = con.cursor()
        try:
            report(t("db_wait_connected"))
            report(t("db_wait_schema_check"))
            issues = collect_schema_issues(cur)
        finally:
            cur.close()
            con.close()
        if issues:
            report(t("db_wait_schema_incomplete"))
            return False, t("db_migration_required"), progress_lines
        report(t("db_wait_schema_complete"))
        return True, "", progress_lines
    except Exception as exc:
        report(t("db_wait_failed"))
        return False, _summarize_db_error(exc), progress_lines


def _request_database_probe(loader, force=False):
    def load():
        state = {"lines": []}

        def callback(lines):
            state["lines"] = list(lines)

        ready, message, lines = _probe_database_ready_verbose(callback)
        state["lines"] = list(lines)
        return {"ready": ready, "message": message, "lines": state["lines"]}

    if force:
        loader.request("database_probe", load)
        return
    loader.request("database_probe", load)


def _draw_database_wait_screen(stdscr, title, host_line, message, progress_lines, footer, spinner_index=0):
    stdscr.erase()
    stdscr.bkgd(" ", curses.color_pair(1))
    stdscr.box()
    h, w = stdscr.getmaxyx()
    stdscr.addstr(0, 2, f" {title} ")
    inner_width = max(20, w - 4)
    title_y = max(2, h // 2 - 5)
    lines = [
        t("db_wait_header"),
        "",
        host_line,
    ]
    message_lines = textwrap.wrap((message or "").strip() or t("db_wait_message_building"), width=max(20, inner_width - 4)) or ["-"]
    lines.extend(message_lines[:2])
    lines.append("")
    lines.append(t("db_wait_status_label"))
    visible_progress = progress_lines[-4:] if progress_lines else [t("db_wait_waiting_feedback")]
    spinner = ["|", "/", "-", "\\"]
    for index, line in enumerate(visible_progress[:4]):
        prefix = spinner[(spinner_index + index) % len(spinner)] if index == len(visible_progress) - 1 else "-"
        lines.append(f"{prefix} {line}")

    start_y = max(2, min(title_y, h - len(lines) - 3))
    for index, line in enumerate(lines):
        y = start_y + index
        if y >= h - 2:
            break
        if index == 0:
            stdscr.attrset(curses.color_pair(2))
            stdscr.addstr(y, 2, _fit(line, inner_width))
            stdscr.attrset(curses.color_pair(1))
        else:
            stdscr.addstr(y, 2, _fit(line, inner_width))

    stdscr.attrset(curses.color_pair(3))
    draw_footer_line(stdscr, h - 2, 1, w - 2, footer)
    stdscr.attrset(curses.color_pair(1))
    stdscr.refresh()


def database_connection_dialog(stdscr, error_text):
    message = (error_text or t("db_wait_default_error")).strip()
    loader = BackgroundValueLoader()
    _request_database_probe(loader, force=True)
    progress_lines = [t("db_wait_check_started")]
    spinner_index = 0
    while True:
        host_line = f"Host: {SETTINGS.get('db_host') or '-'}  DB: {SETTINGS.get('db_name') or '-'}"
        footer = t("db_wait_footer")
        probe_result = loader.poll()
        if probe_result and probe_result.get("key") == "database_probe":
            value = probe_result.get("value") or {}
            if value.get("lines"):
                progress_lines = list(value["lines"])
            if value.get("ready"):
                return True
            if value.get("message"):
                message = value["message"]

        _draw_database_wait_screen(
            stdscr,
            t("db_wait_window_title"),
            host_line,
            message,
            progress_lines,
            footer,
            spinner_index=spinner_index,
        )
        spinner_index += 1

        stdscr.timeout(150)
        try:
            key = stdscr.getch()
        finally:
            stdscr.timeout(-1)

        if key == -1:
            continue
        if key in (10, 13, curses.KEY_ENTER):
            progress_lines = [t("db_wait_check_started")]
            _request_database_probe(loader, force=True)
            continue
        if key == curses.KEY_F2:
            settings_dialog(stdscr)
            progress_lines = [t("db_wait_check_started")]
            message = t("db_wait_auto_continue")
            _request_database_probe(loader, force=True)
            continue
        if key in (27, curses.KEY_F9):
            return False


def db():
    try:
        return psycopg2.connect(
            host=SETTINGS["db_host"],
            port=int(SETTINGS.get("db_port", 5432)),
            dbname=SETTINGS["db_name"],
            user=SETTINGS["db_user"],
            password=SETTINGS["db_pass"],
            connect_timeout=5,
            cursor_factory=psycopg2.extras.RealDictCursor
        )
    except psycopg2.OperationalError as exc:
        raise DatabaseUnavailableError(_summarize_db_error(exc)) from exc


def _display_sku_value(row):
    value = (row.get("display_sku") or "").strip()
    return value or "-/-"


def _active_shopify_location_id():
    return _ACTIVE_SHOPIFY_LOCATION_ID


def _shopify_location_mode():
    mode = str(SETTINGS.get("shopify_location_mode") or DEFAULT_SETTINGS.get("shopify_location_mode") or "single").strip().lower()
    if mode not in {"single", "multi"}:
        return "single"
    return mode


def _shopify_location_switch_enabled():
    return _shopify_location_mode() == "multi"


def _set_active_shopify_location(location_id=None, location_name=""):
    global _ACTIVE_SHOPIFY_LOCATION_ID, _ACTIVE_SHOPIFY_LOCATION_NAME
    _ACTIVE_SHOPIFY_LOCATION_ID = (location_id or "").strip() or None
    _ACTIVE_SHOPIFY_LOCATION_NAME = (location_name or "").strip()


def get_shopify_locations_snapshot():
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT
            location_id,
            COALESCE(NULLIF(name, ''), location_id) AS name,
            COALESCE(fulfills_online_orders, FALSE) AS fulfills_online_orders,
            COALESCE(is_active, TRUE) AS is_active
        FROM shopify_locations
        ORDER BY LOWER(COALESCE(NULLIF(name, ''), location_id)), location_id
        """
    )
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def _resolve_active_shopify_location(locations, current_location_id=None):
    available = [row for row in (locations or []) if row.get("location_id")]
    if not available:
        _set_active_shopify_location(None, "")
        return None
    wanted = (current_location_id or _active_shopify_location_id() or "").strip()
    for row in available:
        if row["location_id"] == wanted:
            _set_active_shopify_location(row["location_id"], row.get("name") or "")
            return row
    fallback = available[0]
    _set_active_shopify_location(fallback["location_id"], fallback.get("name") or "")
    return fallback


def _cycle_shopify_location(locations, current_location_id, step):
    available = [row for row in (locations or []) if row.get("location_id")]
    if not available:
        return None
    if not current_location_id:
        target = available[0]
        _set_active_shopify_location(target["location_id"], target.get("name") or "")
        return target
    for index, row in enumerate(available):
        if row["location_id"] == current_location_id:
            target = available[(index + step) % len(available)]
            _set_active_shopify_location(target["location_id"], target.get("name") or "")
            return target
    target = available[0]
    _set_active_shopify_location(target["location_id"], target.get("name") or "")
    return target


def get_service_runtime_state(service=SHOPIFY_SYNC_SERVICE, max_age_seconds=SERVICE_RUNTIME_CACHE_SECONDS, force=False):
    now = time.monotonic()
    cached = _SERVICE_RUNTIME_CACHE["rows"].get(service)
    if not force and cached is not None and now - _SERVICE_RUNTIME_CACHE["loaded_at"] < max_age_seconds:
        return cached

    con = None
    cur = None
    try:
        con = db()
        cur = con.cursor()
        cur.execute(
            """
            SELECT
                service,
                version,
                status,
                last_seen_at,
                last_started_at,
                last_finished_at,
                last_pull_at,
                last_push_at,
                last_error,
                updated_at
            FROM service_runtime_state
            WHERE service = %s
            """,
            (service,),
        )
        row = cur.fetchone()
        _SERVICE_RUNTIME_CACHE["rows"][service] = row
        _SERVICE_RUNTIME_CACHE["loaded_at"] = now
        return row
    except Exception:
        return cached
    finally:
        if cur is not None:
            cur.close()
        if con is not None:
            con.close()


def _load_shopify_customers_snapshot():
    con = db()
    cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT
            customer_id,
            first_name,
            last_name,
            display_name,
            email,
            phone,
            default_name,
            default_address1,
            default_zip,
            default_city,
            default_country,
            default_phone
        FROM shopify_customers
        ORDER BY COALESCE(display_name, default_name, email, customer_id)
        """
    )
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def get_shopify_customers_snapshot(max_age_seconds=SHOPIFY_CUSTOMER_CACHE_SECONDS, force=False):
    now = time.monotonic()
    cached = _SHOPIFY_CUSTOMER_CACHE.get("rows") or []
    if not force and cached and now - _SHOPIFY_CUSTOMER_CACHE.get("loaded_at", 0.0) < max_age_seconds:
        return cached
    try:
        rows = _load_shopify_customers_snapshot()
    except Exception:
        return cached
    _SHOPIFY_CUSTOMER_CACHE["rows"] = rows
    _SHOPIFY_CUSTOMER_CACHE["loaded_at"] = now
    return rows


def _format_runtime_time_short(value):
    if not value:
        return "-"
    if not isinstance(value, datetime.datetime):
        return str(value)
    try:
        localized = value.astimezone() if value.tzinfo is not None else value
    except Exception:
        localized = value
    return localized.strftime("%H:%M")


def format_shopify_sync_status_label(row=None, now=None):
    data = row if row is not None else get_service_runtime_state()
    if not data:
        return "Sync: -"
    current = now or datetime.datetime.now(datetime.timezone.utc)
    last_seen = data.get("last_seen_at")
    if isinstance(last_seen, datetime.datetime) and last_seen.tzinfo is None:
        last_seen = last_seen.replace(tzinfo=datetime.timezone.utc)
    stale = bool(isinstance(last_seen, datetime.datetime) and (current - last_seen).total_seconds() > 180)
    prefix = "Sync!" if stale else "Sync:"
    parts = []
    if data.get("last_pull_at"):
        parts.append(f"In {_format_runtime_time_short(data['last_pull_at'])}")
    if data.get("last_push_at"):
        parts.append(f"Out {_format_runtime_time_short(data['last_push_at'])}")
    if not parts and last_seen:
        parts.append(_format_runtime_time_short(last_seen))
    label = f"{prefix} {' '.join(parts) if parts else '-'}"
    if (data.get("status") or "").strip().lower() == "error":
        label += " ERR"
    return label


def test_db_connection(settings):
    con = psycopg2.connect(
        host=settings["db_host"],
        port=int(settings.get("db_port", 5432)),
        dbname=settings["db_name"],
        user=settings["db_user"],
        password=settings["db_pass"],
        connect_timeout=5,
    )
    con.close()


def _is_default_db_settings(settings):
    for key in ("db_host", "db_port", "db_name", "db_user", "db_pass"):
        if settings.get(key) != DEFAULT_SETTINGS.get(key):
            return False
    return True


def ensure_database_ready(stdscr):
    if _is_default_db_settings(SETTINGS):
        return database_connection_dialog(stdscr, "Bitte zuerst DB Einstellungen in Shift+F11 speichern.")
    return database_connection_dialog(stdscr, "Verbindung wird aufgebaut.")


def get_items(filter_text=None, filter_no_location=False, filter_local=False, sort_mode="location", external_mode="hide", active_location_id=None):
    con = db()
    cur = con.cursor()

    conditions = []
    params = []

    if filter_text:
        conditions.append(
            "(name ILIKE %s OR COALESCE(display_sku, sku) ILIKE %s OR sku ILIKE %s OR COALESCE(barcode, '') ILIKE %s)"
        )
        params.extend([f"%{filter_text}%", f"%{filter_text}%", f"%{filter_text}%", f"%{filter_text}%"])

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
    WITH location_totals AS (
        SELECT
            sku,
            COALESCE(SUM(menge), 0) AS total_menge
        FROM item_location_inventory
        GROUP BY sku
    )
    SELECT
        items.sku,
        items.display_sku,
        items.name,
        COALESCE(ili.regal, items.regal) AS regal,
        COALESCE(ili.fach, items.fach) AS fach,
        COALESCE(ili.platz, items.platz) AS platz,
        COALESCE(ili.menge, items.menge, 0) AS menge,
        COALESCE(location_totals.total_menge, items.menge, 0) AS gesamt_menge,
        COALESCE(ili.reserved, items.reserved, 0) AS reserved,
        COALESCE(ili.committed, items.committed, 0) AS committed,
        COALESCE(ili.unavailable, items.unavailable, COALESCE(ili.reserved, items.reserved, 0)) AS unavailable,
        COALESCE(
            ili.available,
            items.available,
            GREATEST(
                COALESCE(ili.menge, items.menge, 0)
                - COALESCE(ili.unavailable, items.unavailable, COALESCE(ili.reserved, items.reserved, 0))
                - COALESCE(ili.committed, items.committed, 0),
                0
            )
        ) AS available,
        COALESCE(ili.dirty, items.dirty, FALSE) AS dirty,
        items.shopify_variant_id,
        items.barcode,
        items.shopify_product_status,
        items.shopify_description,
        items.shopify_price,
        items.shopify_compare_at_price,
        items.shopify_unit_cost,
        items.shopify_unit_cost_currency,
        items.shopify_weight_grams,
        items.sync_status,
        COALESCE(items.external_fulfillment, FALSE) AS external_fulfillment,
        ili.location_id AS shopify_location_id
    FROM items
    LEFT JOIN item_location_inventory ili
        ON ili.sku = items.sku AND ili.location_id = %s
    LEFT JOIN location_totals
        ON location_totals.sku = items.sku
    {where}
    {order}
    """

    cur.execute(query, [active_location_id] + params)
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def _load_items_snapshot(active_location_id=None):
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        WITH location_totals AS (
            SELECT
                sku,
                COALESCE(SUM(menge), 0) AS total_menge
            FROM item_location_inventory
            GROUP BY sku
        )
        SELECT
            items.sku,
            items.display_sku,
            items.name,
            COALESCE(ili.regal, items.regal) AS regal,
            COALESCE(ili.fach, items.fach) AS fach,
            COALESCE(ili.platz, items.platz) AS platz,
            COALESCE(ili.menge, items.menge, 0) AS menge,
            COALESCE(location_totals.total_menge, items.menge, 0) AS gesamt_menge,
            COALESCE(ili.reserved, items.reserved, 0) AS reserved,
            COALESCE(ili.committed, items.committed, 0) AS committed,
            COALESCE(ili.unavailable, items.unavailable, COALESCE(ili.reserved, items.reserved, 0)) AS unavailable,
            COALESCE(
                ili.available,
                items.available,
                GREATEST(
                    COALESCE(ili.menge, items.menge, 0)
                    - COALESCE(ili.unavailable, items.unavailable, COALESCE(ili.reserved, items.reserved, 0))
                    - COALESCE(ili.committed, items.committed, 0),
                    0
                )
            ) AS available,
            COALESCE(ili.dirty, items.dirty, FALSE) AS dirty,
            items.shopify_variant_id,
            items.barcode,
            items.shopify_product_status,
            items.shopify_description,
            items.shopify_price,
            items.shopify_compare_at_price,
            items.shopify_unit_cost,
            items.shopify_unit_cost_currency,
            items.shopify_weight_grams,
            items.sync_status,
            COALESCE(items.external_fulfillment, FALSE) AS external_fulfillment,
            ili.location_id AS shopify_location_id
        FROM items
        LEFT JOIN item_location_inventory ili
            ON ili.sku = items.sku AND ili.location_id = %s
        LEFT JOIN location_totals
            ON location_totals.sku = items.sku
        """,
        (active_location_id,),
    )
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def _match_item_filter(row, filter_text):
    needle = str(filter_text or "").strip().casefold()
    if not needle:
        return True
    fields = (
        row.get("name") or "",
        row.get("display_sku") or "",
        row.get("sku") or "",
        row.get("barcode") or "",
    )
    return any(needle in str(value).casefold() for value in fields)


def _sort_items_snapshot(rows, sort_mode):
    data = list(rows or [])
    if sort_mode == "name":
        data.sort(key=lambda row: (str(row.get("name") or "").casefold(), str(_display_sku_value(row)).casefold()))
        return data
    if sort_mode == "sku":
        data.sort(key=lambda row: str(_display_sku_value(row)).casefold())
        return data
    data.sort(
        key=lambda row: (
            _sort_location_value(row.get("regal")),
            _sort_location_value(row.get("fach")),
            _sort_location_value(row.get("platz")),
            str(_display_sku_value(row)).casefold(),
        )
    )
    return data


def _filter_items_snapshot(rows, filter_text=None, filter_no_location=False, filter_local=False, sort_mode="location", external_mode="hide"):
    filtered = []
    for row in rows or []:
        if not _match_item_filter(row, filter_text):
            continue
        if filter_no_location:
            regal = (row.get("regal") or "").strip()
            fach = row.get("fach")
            platz = row.get("platz")
            if regal and fach not in (None, "") and platz not in (None, ""):
                continue
        if filter_local and (row.get("sync_status") or "") != "local":
            continue
        is_external = bool(row.get("external_fulfillment"))
        if external_mode == "only" and not is_external:
            continue
        if external_mode == "hide" and is_external:
            continue
        filtered.append(row)
    return _sort_items_snapshot(filtered, sort_mode)


def get_orders(order_filter=None, only_pending=False, fulfillment_filter="all", payment_filter="all"):
    con = db()
    cur = con.cursor()
    active_location_id = _active_shopify_location_id()

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

    query_params = [active_location_id, active_location_id] + params

    _execute_db_query(
        cur,
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
            COALESCE(fo_stats.total_internal_qty, order_stats.local_internal_qty, 0) AS local_internal_qty,
            COALESCE(fo_stats.active_location_internal_qty, 0) AS active_location_internal_qty,
            COALESCE(fo_stats.active_location_remaining_qty, 0) AS active_location_remaining_qty,
            COALESCE(fo_stats.location_count, 0) AS shopify_location_count
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
        LEFT JOIN (
            SELECT
                foi.order_id,
                SUM(
                    CASE WHEN COALESCE(i.external_fulfillment, FALSE) = FALSE
                    THEN foi.quantity
                    ELSE 0
                    END
                ) AS total_internal_qty,
                SUM(
                    CASE
                        WHEN COALESCE(i.external_fulfillment, FALSE) = FALSE
                         AND foi.assigned_location_id = %s
                        THEN foi.quantity
                        ELSE 0
                    END
                ) AS active_location_internal_qty,
                SUM(
                    CASE
                        WHEN COALESCE(i.external_fulfillment, FALSE) = FALSE
                         AND foi.assigned_location_id = %s
                        THEN foi.remaining_quantity
                        ELSE 0
                    END
                ) AS active_location_remaining_qty,
                COUNT(DISTINCT NULLIF(foi.assigned_location_id, '')) AS location_count
            FROM shopify_fulfillment_order_items foi
            LEFT JOIN items i ON i.sku = foi.sku
            GROUP BY foi.order_id
        ) AS fo_stats ON fo_stats.order_id = so.order_id
        {where}
        ORDER BY so.created_at DESC NULLS LAST, so.order_name DESC
        """,
        query_params,
    )
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def _load_orders_snapshot():
    con = db()
    cur = con.cursor()
    active_location_id = _active_shopify_location_id()
    _execute_db_query(
        cur,
        """
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
            COALESCE(fo_stats.total_internal_qty, order_stats.local_internal_qty, 0) AS local_internal_qty,
            COALESCE(fo_stats.active_location_internal_qty, 0) AS active_location_internal_qty,
            COALESCE(fo_stats.active_location_remaining_qty, 0) AS active_location_remaining_qty,
            COALESCE(fo_stats.location_count, 0) AS shopify_location_count
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
        LEFT JOIN (
            SELECT
                foi.order_id,
                SUM(
                    CASE WHEN COALESCE(i.external_fulfillment, FALSE) = FALSE
                    THEN foi.quantity
                    ELSE 0
                    END
                ) AS total_internal_qty,
                SUM(
                    CASE
                        WHEN COALESCE(i.external_fulfillment, FALSE) = FALSE
                         AND foi.assigned_location_id = %s
                        THEN foi.quantity
                        ELSE 0
                    END
                ) AS active_location_internal_qty,
                SUM(
                    CASE
                        WHEN COALESCE(i.external_fulfillment, FALSE) = FALSE
                         AND foi.assigned_location_id = %s
                        THEN foi.remaining_quantity
                        ELSE 0
                    END
                ) AS active_location_remaining_qty,
                COUNT(DISTINCT NULLIF(foi.assigned_location_id, '')) AS location_count
            FROM shopify_fulfillment_order_items foi
            LEFT JOIN items i ON i.sku = foi.sku
            GROUP BY foi.order_id
        ) AS fo_stats ON fo_stats.order_id = so.order_id
        ORDER BY so.created_at DESC NULLS LAST, so.order_name DESC
        """,
        [active_location_id, active_location_id],
    )
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def _matches_fulfillment_filter(status_value, fulfillment_filter, only_pending=False):
    status_text = str(status_value or "").strip().lower()
    if only_pending and status_text in {"fulfilled", "cancelled"}:
        return False
    if fulfillment_filter == "all":
        return True
    if fulfillment_filter == "open":
        return status_text not in {"fulfilled", "cancelled"}
    if fulfillment_filter == "unfulfilled":
        return status_text == "" or "unfulfilled" in status_text
    if fulfillment_filter == "partial":
        return "partial" in status_text or "in_progress" in status_text
    if fulfillment_filter == "fulfilled":
        return status_text == "fulfilled"
    return True


def _filter_orders_snapshot(rows, order_filter=None, only_pending=False, fulfillment_filter="all", payment_filter="all"):
    needle_raw = (order_filter or "").strip()
    needle = needle_raw.replace("#", "").casefold()
    filtered = []
    for row in rows or []:
        if needle:
            order_name = str(row.get("order_name") or "").replace("#", "").casefold()
            shipping_name = str(row.get("shipping_name") or "").casefold()
            shipping_city = str(row.get("shipping_city") or "").casefold()
            if needle not in order_name and needle_raw.casefold() not in shipping_name and needle_raw.casefold() not in shipping_city:
                continue
        if not _matches_fulfillment_filter(row.get("fulfillment_status"), fulfillment_filter, only_pending=only_pending):
            continue
        if payment_filter != "all":
            payment_value = str(row.get("payment_status") or "").strip().lower()
            if payment_value != payment_filter.lower():
                continue
        filtered.append(row)
    filtered.sort(key=lambda row: str(row.get("order_name") or ""), reverse=True)
    filtered.sort(key=lambda row: row.get("created_at") or datetime.datetime.min, reverse=True)
    return filtered


def _prefetch_order_ids(rows, selected_index, ahead=5, behind=1):
    if not rows:
        return []
    start = max(0, int(selected_index) - max(0, int(behind)))
    end = min(len(rows), int(selected_index) + max(1, int(ahead)) + 1)
    return [rows[index]["order_id"] for index in range(start, end) if rows[index].get("order_id")]


def should_refresh_orders(last_refresh_at, now=None, interval_seconds=ORDERS_AUTO_REFRESH_SECONDS):
    if last_refresh_at is None:
        return True
    current = time.monotonic() if now is None else now
    return (current - last_refresh_at) >= max(0.0, float(interval_seconds))


def get_order_items(order_id):
    con = db()
    cur = con.cursor()
    active_location_id = _active_shopify_location_id()
    cur.execute(
        """
        SELECT COUNT(*) AS count
        FROM shopify_fulfillment_order_items
        WHERE order_id = %s
        """,
        (order_id,),
    )
    fulfillment_item_count = int((cur.fetchone() or {}).get("count") or 0)
    if fulfillment_item_count > 0:
        params = [active_location_id, order_id]
        location_filter = ""
        if active_location_id:
            location_filter = "AND foi.assigned_location_id = %s"
            params.append(active_location_id)
        cur.execute(
            f"""
            SELECT
                MIN(foi.fulfillment_order_line_item_id) AS line_index,
                COALESCE(foi.order_line_item_id, oi.order_line_item_id) AS order_line_item_id,
                COALESCE(foi.sku, oi.sku) AS sku,
                COALESCE(MAX(foi.title), MAX(oi.title), '-') AS title,
                SUM(COALESCE(foi.quantity, 0)) AS quantity,
                GREATEST(
                    SUM(COALESCE(foi.quantity, 0)) - SUM(COALESCE(foi.remaining_quantity, 0)),
                    0
                ) AS fulfilled_quantity,
                COALESCE(ili.regal, i.regal) AS regal,
                COALESCE(ili.fach, i.fach) AS fach,
                COALESCE(ili.platz, i.platz) AS platz,
                i.shopify_weight_grams,
                COALESCE(i.external_fulfillment, FALSE) AS external_fulfillment,
                MIN(COALESCE(foi.assigned_location_id, '')) AS assigned_location_id,
                MIN(COALESCE(foi.assigned_location_name, '')) AS assigned_location_name
            FROM shopify_fulfillment_order_items foi
            LEFT JOIN shopify_order_items oi
                ON oi.order_id = foi.order_id
               AND oi.order_line_item_id = foi.order_line_item_id
            LEFT JOIN items i
                ON i.sku = COALESCE(foi.sku, oi.sku)
            LEFT JOIN item_location_inventory ili
                ON ili.sku = i.sku AND ili.location_id = %s
            WHERE foi.order_id = %s
              {location_filter}
            GROUP BY
                COALESCE(foi.order_line_item_id, oi.order_line_item_id),
                COALESCE(foi.sku, oi.sku),
                COALESCE(ili.regal, i.regal),
                COALESCE(ili.fach, i.fach),
                COALESCE(ili.platz, i.platz),
                i.shopify_weight_grams,
                COALESCE(i.external_fulfillment, FALSE)
            ORDER BY MIN(foi.assigned_location_name), MIN(foi.fulfillment_order_id), MIN(foi.fulfillment_order_line_item_id)
            """,
            params,
        )
        rows = cur.fetchall()
    else:
        cur.execute(
            """
            SELECT
                oi.line_index,
                oi.order_line_item_id,
                oi.sku,
                oi.title,
                oi.quantity,
                COALESCE(oi.fulfilled_quantity, 0) AS fulfilled_quantity,
                COALESCE(ili.regal, i.regal) AS regal,
                COALESCE(ili.fach, i.fach) AS fach,
                COALESCE(ili.platz, i.platz) AS platz,
                i.shopify_weight_grams,
                COALESCE(i.external_fulfillment, FALSE) AS external_fulfillment,
                NULL::text AS assigned_location_id,
                NULL::text AS assigned_location_name
            FROM shopify_order_items oi
            LEFT JOIN items i ON i.sku = oi.sku
            LEFT JOIN item_location_inventory ili
                ON ili.sku = i.sku AND ili.location_id = %s
            WHERE oi.order_id = %s
            ORDER BY oi.line_index
            """,
            (active_location_id, order_id),
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


def ensure_order_items_loaded(order_id, order_items_cache=None):
    if not order_id:
        return []
    cache = order_items_cache if isinstance(order_items_cache, dict) else None
    if cache is not None and order_id in cache:
        return cache[order_id]
    rows = get_order_items(order_id)
    if cache is not None:
        cache[order_id] = rows
    return rows


def ensure_order_shipments_loaded(order_id, order_shipments_cache=None):
    if not order_id:
        return []
    cache = order_shipments_cache if isinstance(order_shipments_cache, dict) else None
    if cache is not None and order_id in cache:
        return cache[order_id]
    rows = list_shipping_labels(order_id)
    if cache is not None:
        cache[order_id] = rows
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


def _shipment_number(row):
    carrier = (row.get("carrier") or "").strip().lower()
    if carrier == "free":
        return "-"
    return ((row.get("parcel_number") or "").strip() or (row.get("track_id") or "").strip() or "-")


def list_shipping_labels(order_id=None):
    return _list_shipping_labels(db, order_id=order_id)


def insert_shipping_label_history(*args, **kwargs):
    return _insert_shipping_label_history(db, *args, **kwargs)


def update_shipping_label_status(label_id, status, last_error=None):
    return _update_shipping_label_status(db, label_id, status, last_error=last_error)


def update_shipping_label_reprint(label_id, label_path):
    return _update_shipping_label_reprint(db, label_id, label_path)


def get_latest_shopify_job_for_label(label_id):
    return _get_latest_shopify_job_for_label(db, label_id)


def _shipping_printer_field_map():
    return _shipping_carrier_field_to_code("printer")


def _shipping_format_field_map():
    return _shipping_carrier_field_to_code("format")


def _shipping_scale_field_map():
    return _shipping_carrier_field_to_code("scale")


def _shipping_template_field_map():
    return _shipping_carrier_field_to_code("template")


def _shipping_tracking_mode_field_map():
    return _shipping_carrier_field_to_code("tracking_mode")


def _shipping_tracking_url_field_map():
    return _shipping_carrier_field_to_code("tracking_url")


def _active_shipping_carriers():
    return _normalize_active_shipping_carriers(SETTINGS.get("shipping_active_carriers", DEFAULT_ACTIVE_SHIPPING_CARRIERS))

def _shipping_carrier_options(include_test=True):
    return _shipping_carrier_options_from_settings(include_test=include_test)


def _shipping_carrier_options_from_settings(include_test=True):
    return _shipping_carrier_options_impl(_active_shipping_carriers(), include_test=include_test)


def _shipping_active_carriers_summary(values, fallback_to_defaults=True):
    normalized = _normalize_active_shipping_carriers(values, fallback_to_defaults=fallback_to_defaults)
    if not normalized:
        return "Keine"
    return ", ".join(_shipping_carrier_label(code) for code in normalized)


def _tracking_url_for_carrier(carrier, tracking_number):
    number = (tracking_number or "").strip()
    if not number:
        return None
    normalized = (carrier or "").strip().lower()
    template_field = _shipping_carrier_setting_field(normalized, "tracking_url")
    template = (SETTINGS.get(template_field) or "").strip() if template_field else ""
    if not template:
        return None
    try:
        return template.format(tracking_number=number, number=number)
    except Exception:
        return template.replace("{tracking_number}", number).replace("{number}", number)


def _shopify_tracking_mode_for_carrier(carrier):
    normalized = (carrier or "").strip().lower()
    tracking_mode_field = _shipping_carrier_setting_field(normalized, "tracking_mode")
    default_mode = _default_tracking_mode_for_carrier(normalized)
    if tracking_mode_field:
        return (SETTINGS.get(tracking_mode_field) or default_mode).strip().lower()
    return "company"


def _effective_tracking_url_for_shopify(carrier, tracking_number, tracking_url=None):
    mode = _shopify_tracking_mode_for_carrier(carrier)
    if mode == "company_and_url":
        return (tracking_url or "").strip() or _tracking_url_for_carrier(carrier, tracking_number)
    return None


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


def search_shopify_customers(search_text="", limit=100):
    limit = max(1, int(limit))
    needle = (search_text or "").strip().casefold()
    rows = get_shopify_customers_snapshot()
    if not needle:
        return rows[:limit]

    filtered = []
    for row in rows:
        haystacks = [
            row.get("display_name"),
            row.get("email"),
            row.get("default_name"),
            row.get("default_address1"),
            row.get("default_city"),
            row.get("default_zip"),
        ]
        if any(needle in str(value or "").casefold() for value in haystacks):
            filtered.append(row)
            if len(filtered) >= limit:
                break
    return filtered


def get_latest_shopify_jobs_for_labels(label_ids):
    normalized_ids = [int(value) for value in label_ids or [] if str(value).strip()]
    if not normalized_ids:
        return {}
    con = db()
    cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT DISTINCT ON (label_id)
            label_id,
            id,
            status,
            attempts,
            result_message,
            shopify_fulfillment_id,
            created_at,
            updated_at,
            processed_at
        FROM shopify_fulfillment_jobs
        WHERE label_id = ANY(%s)
        ORDER BY label_id, created_at DESC, id DESC
        """,
        (normalized_ids,),
    )
    rows = cur.fetchall()
    cur.close()
    con.close()
    return {row["label_id"]: row for row in rows}


def _shopify_customer_dialog_label(row, width):
    name = (row.get("display_name") or row.get("default_name") or row.get("email") or "-").strip()
    address = (row.get("default_address1") or "").strip()
    postal = (row.get("default_zip") or "").strip()
    city = (row.get("default_city") or "").strip()
    email = (row.get("email") or "").strip()
    city_block = " ".join(part for part in [postal, city] if part).strip()
    line = " | ".join(part for part in [name, address, city_block, email] if part)
    return _fit(line, width)


def _apply_shopify_customer_to_manual_state(state, chosen_customer, country_code):
    updated = dict(state)
    if not chosen_customer:
        return updated, country_code
    updated["name"] = (
        (chosen_customer.get("default_name") or "").strip()
        or (chosen_customer.get("display_name") or "").strip()
    )
    updated["street"] = (chosen_customer.get("default_address1") or "").strip()
    updated["zip"] = (chosen_customer.get("default_zip") or "").strip()
    updated["city"] = (chosen_customer.get("default_city") or "").strip()
    customer_country = _normalized_country_code_for_display(chosen_customer.get("default_country"))
    return updated, (customer_country or country_code)


def shopify_customer_dialog(stdscr, search_text=""):
    query = search_text or ""
    selected = 0
    top_index = 0
    cursor_pos = len(query)
    rows = []
    last_query = None
    base_h, base_w = stdscr.getmaxyx()
    width = min(max(84, int(base_w * 0.86)), base_w - 4)
    height = min(max(18, int(base_h * 0.7)), base_h - 2)
    y = max(1, (base_h - height) // 2)
    x = max(2, (base_w - width) // 2)

    while True:
        if query != last_query:
            rows = search_shopify_customers(query, limit=150)
            last_query = query

        draw_shadow(stdscr, y, x, height, width)
        win = curses.newwin(height, width, y, x)
        win.keypad(True)
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        win.addstr(0, 2, t("customer_select_title"))

        prompt = t("search_prompt")
        input_width = max(1, width - len(prompt) - 4)
        input_scroll = max(0, cursor_pos - input_width + 1)
        visible_query = query[input_scroll: input_scroll + input_width]
        win.addstr(1, 2, prompt)
        win.attrset(curses.color_pair(2))
        win.addstr(1, 2 + len(prompt), visible_query.ljust(input_width))
        win.attrset(curses.color_pair(1))

        visible_rows = max(1, height - 5)
        if selected >= len(rows):
            selected = max(0, len(rows) - 1)
        if selected < top_index:
            top_index = selected
        if selected >= top_index + visible_rows:
            top_index = selected - visible_rows + 1

        if not rows:
            win.addstr(3, 2, _fit(t("no_customers_found"), width - 4))
        else:
            for row_index, row in enumerate(rows[top_index:top_index + visible_rows]):
                real_index = top_index + row_index
                line = _shopify_customer_dialog_label(row, width - 4)
                y_pos = 3 + row_index
                if real_index == selected:
                    win.attrset(curses.color_pair(2))
                    win.addstr(y_pos, 2, line.ljust(width - 4))
                    win.attrset(curses.color_pair(1))
                else:
                    win.addstr(y_pos, 2, line.ljust(width - 4))

        win.attrset(curses.color_pair(3))
        draw_footer_line(win, height - 1, 1, width - 2, t("customer_select_footer"))
        win.attrset(curses.color_pair(1))
        cursor_x = 2 + len(prompt) + min(max(0, cursor_pos - input_scroll), input_width - 1)
        win.move(1, min(width - 2, cursor_x))
        win.refresh()

        key = win.get_wch()
        if key in (27, curses.KEY_F9):
            return None
        if key == curses.KEY_DOWN:
            if rows:
                selected = move_selection(rows, selected, 1)
            continue
        if key == curses.KEY_UP:
            if rows:
                selected = move_selection(rows, selected, -1)
            continue
        if key in (10, 13, "\n", "\r", curses.KEY_ENTER):
            if rows:
                return rows[selected]
            continue
        if key in (curses.KEY_BACKSPACE, 127, 8, '\x7f', '\b'):
            if cursor_pos > 0:
                query = query[:cursor_pos - 1] + query[cursor_pos:]
                cursor_pos -= 1
                selected = 0
                top_index = 0
            continue
        if key == curses.KEY_LEFT:
            cursor_pos = max(0, cursor_pos - 1)
            continue
        if key == curses.KEY_RIGHT:
            cursor_pos = min(len(query), cursor_pos + 1)
            continue
        if isinstance(key, str) and key.isprintable():
            query = query[:cursor_pos] + key + query[cursor_pos:]
            cursor_pos += 1
            selected = 0
            top_index = 0


def _shipment_summary_lines(rows, width):
    if not rows:
        return [t("shipments_summary_empty")]
    entries = []
    for row in rows:
        carrier = _shipping_carrier_label(row.get("carrier") or "-", short=True)
        number = _shipment_number(row)
        source = _shipment_source_label(row.get("source"))
        status = row.get("status") or "-"
        entries.append(f"{carrier} {number} [{source}/{status}]")
    wrapped = textwrap.wrap(" | ".join(entries), width=max(12, width), break_long_words=False, break_on_hyphens=False)
    if not wrapped:
        return [t("shipments_summary_empty")]
    lines = [t("shipments_summary_prefix", value=wrapped[0])]
    lines.extend(wrapped[1:])
    return lines


def enqueue_shopify_fulfillment_job(label_row, notify_customer=False):
    label_id = label_row.get("id")
    order_id = (label_row.get("order_id") or "").strip()
    tracking_number = (label_row.get("parcel_number") or label_row.get("track_id") or "").strip()
    carrier_code = (label_row.get("carrier") or "gls").strip()
    carrier = _shopify_tracking_company(carrier_code)
    tracking_url = _effective_tracking_url_for_shopify(
        carrier_code,
        tracking_number,
        (label_row.get("tracking_url") or "").strip(),
    )
    if carrier_code.strip().lower() in {"test", "free"}:
        raise RuntimeError(t("test_and_free_no_shopify"))
    if not order_id:
        raise RuntimeError(t("order_id_missing"))
    if not tracking_number:
        raise RuntimeError(t("track_id_missing"))

    return _find_or_create_shopify_fulfillment_job(
        db,
        label_id=label_id,
        order_id=order_id,
        tracking_number=tracking_number,
        tracking_url=tracking_url,
        carrier=carrier,
        line_items_json=None,
        notify_customer=notify_customer,
    )


def enqueue_shopify_fulfillment_job_for_items(label_row, selected_items, notify_customer=False):
    label_id = label_row.get("id")
    order_id = (label_row.get("order_id") or "").strip()
    tracking_number = (label_row.get("parcel_number") or label_row.get("track_id") or "").strip()
    carrier_code = (label_row.get("carrier") or "gls").strip()
    carrier = _shopify_tracking_company(carrier_code)
    tracking_url = _effective_tracking_url_for_shopify(
        carrier_code,
        tracking_number,
        (label_row.get("tracking_url") or "").strip(),
    )
    if carrier_code.strip().lower() in {"test", "free"}:
        raise RuntimeError(t("test_and_free_no_shopify"))
    if not label_id:
        raise RuntimeError(t("label_id_missing"))
    if not order_id:
        raise RuntimeError(t("order_id_missing"))
    if not tracking_number:
        raise RuntimeError(t("tracking_number_missing"))

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
        raise RuntimeError(t("no_valid_shopify_fulfillment_positions"))

    return _find_or_create_shopify_fulfillment_job(
        db,
        label_id=label_id,
        order_id=order_id,
        tracking_number=tracking_number,
        tracking_url=tracking_url,
        carrier=carrier,
        line_items_json=json.dumps(line_items, ensure_ascii=True),
        notify_customer=notify_customer,
    )


def _gls_extract_from_pdf(pdf_path):
    temp_txt = Path(tempfile.gettempdir()) / f"gls_login_{os.getpid()}.txt"
    try:
        subprocess.run(["pdftotext", "-layout", str(pdf_path), str(temp_txt)], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    except FileNotFoundError as exc:
        raise RuntimeError(t("pdftotext_missing_settings")) from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(t("pdf_read_failed", detail=(exc.stderr or "").strip()[:80])) from exc

    try:
        text = temp_txt.read_text(encoding="utf-8", errors="ignore")
    finally:
        temp_txt.unlink(missing_ok=True)

    def pick(pattern, field_name):
        match = re.search(pattern, text)
        if not match:
            raise RuntimeError(t("gls_field_missing_in_pdf", field_name=field_name))
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
        raise RuntimeError(t("gls_credentials_missing"))
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
        raise RuntimeError(t("post_internetmarke_missing_data", fields=", ".join(missing)))
    return creds


def _country_to_alpha3(country_value):
    raw = (country_value or "").strip()
    if not raw:
        return ""
    if len(raw) == 3 and raw.isalpha():
        return raw.upper()
    if len(raw) == 2 and raw.isalpha():
        return COUNTRY_ALPHA3.get(raw.upper(), "")
    code2 = _gls_country_code(raw)
    if code2:
        return COUNTRY_ALPHA3.get(code2, "")
    return ""


def _post_sender_address(client):
    profile = client.get_profile()
    firstname = (profile.get("firstname") or "").strip()
    lastname = (profile.get("lastname") or "").strip()
    company = (profile.get("company") or "").strip()
    street = " ".join(part for part in [(profile.get("street") or "").strip(), (profile.get("houseNo") or "").strip()] if part).strip()
    sender_name = " ".join(part for part in [firstname, lastname] if part).strip() or company
    address = {
        "name": sender_name[:50],
        "addressLine1": street[:50],
        "postalCode": (profile.get("zip") or "").strip()[:5],
        "city": (profile.get("city") or "").strip()[:40],
        "country": (profile.get("country") or "DEU").strip().upper()[:3],
    }
    if company:
        address["additionalName"] = company[:40]
    return address


def _post_receiver_address(order):
    country = _country_to_alpha3(order.get("shipping_country"))
    if not country:
        raise ValueError(t("recipient_country_invalid_or_missing_iso2_iso3"))
    address = {
        "name": (order.get("shipping_name") or "").strip()[:50],
        "addressLine1": (order.get("shipping_address1") or "").strip()[:50],
        "postalCode": (order.get("shipping_zip") or "").strip()[:10],
        "city": (order.get("shipping_city") or "").strip()[:40],
        "country": country,
    }
    company = (order.get("shipping_company") or "").strip()
    if company:
        address["additionalName"] = company[:40]
    address_line2 = (order.get("shipping_address2") or "").strip()
    if address_line2:
        address["addressLine2"] = address_line2[:60]
    return address


def _normalize_post_option_codes(option_codes):
    result = []
    for code in option_codes or []:
        normalized = str(code or "").strip().lower()
        if normalized and normalized not in result:
            result.append(normalized)
    return sorted(result)


def _post_selection_summary(selection):
    if not selection:
        return "-"
    label = (selection.get("selection_label") or selection.get("name") or "").strip()
    price = str(selection.get("price_eur") or "").strip()
    if label and price:
        return f"{label} - {price} EUR"
    return label or "-"


def _post_selection_dialog(stdscr, scope="domestic"):
    current = dict(_POST_SELECTION_CACHE.get(scope) or {})
    selection = post_product_dialog(stdscr, current_selection=current, scope=scope)
    if selection:
        _POST_SELECTION_CACHE[scope] = dict(selection)
    return selection


def _get_post_page_formats(client, max_age_seconds=1800):
    now = time.time()
    if _POST_PAGE_FORMAT_CACHE["formats"] and now - _POST_PAGE_FORMAT_CACHE["loaded_at"] < max_age_seconds:
        return _POST_PAGE_FORMAT_CACHE["formats"]
    formats = client.get_page_formats()
    _POST_PAGE_FORMAT_CACHE["formats"] = formats
    _POST_PAGE_FORMAT_CACHE["loaded_at"] = now
    return formats


def _resolve_post_page_format_id(client, desired_format):
    formats = _get_post_page_formats(client)
    desired = _normalize_shipping_label_format(desired_format)
    compact = desired.lower().replace(" ", "")
    if compact.isdigit():
        return int(compact)
    for item in formats:
        if str(item.get("id")) == compact:
            return int(item["id"])

    candidates = []
    for item in formats:
        name = (item.get("name") or "").strip()
        layout = item.get("pageLayout") or {}
        size = layout.get("size") or {}
        width = size.get("x")
        height = size.get("y")
        dims = {int(round(width or 0)), int(round(height or 0))}
        item_type = (item.get("pageType") or "").strip().upper()
        address_possible = bool(item.get("isAddressPossible"))
        score = 0
        if compact in {"100x62", "62x100"}:
            if dims == {62, 100}:
                score += 100
            if "BROTHER" in name.upper():
                score += 50
            if item_type == "LABELPRINTER":
                score += 10
            if address_possible:
                score += 5
        elif compact == "A4":
            if "A4" in name.upper():
                score += 100
            if item_type == "REGULARPAGE":
                score += 10
            if address_possible:
                score += 5
        elif compact == "A5":
            if "A5" in name.upper():
                score += 100
            if item_type == "LABELPRINTER":
                score += 10
            if address_possible:
                score += 5
        else:
            if compact and compact in name.lower().replace(" ", ""):
                score += 100
        if score > 0:
            candidates.append((score, int(item["id"])))
    if candidates:
        candidates.sort(key=lambda entry: (-entry[0], entry[1]))
        return candidates[0][1]
    raise RuntimeError(t("post_page_format_not_found", value=desired_format))


def _resolve_post_product_selection(selection):
    if not isinstance(selection, dict):
        raise ValueError(t("post_product_missing"))
    product_code = str(selection.get("product_code") or "").strip()
    if product_code:
        product = find_post_product(product_code)
        if not product:
            raise ValueError(t("post_product_code_unknown", product_code=product_code))
        return product

    scope = str(selection.get("scope") or "domestic").strip()
    base_key = str(selection.get("base_key") or "").strip()
    option_codes = _normalize_post_option_codes(selection.get("option_codes") or [])
    if not base_key:
        raise ValueError(t("post_base_product_missing"))

    for group in list_post_base_products(scope=scope):
        if group.get("base_key") != base_key:
            continue
        for bucket in ("untracked_variants", "tracked_variants"):
            for variant in group.get(bucket, []):
                if _normalize_post_option_codes(variant.get("addons") or []) == option_codes:
                    product = find_post_product(variant["product_code"])
                    if product:
                        return product
        break
    raise ValueError(t("post_product_combination_unavailable"))


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
    return choice_dialog(stdscr, t("manual_country_title"), options, normalized)


def manual_label_print_mode_dialog(stdscr, current_mode):
    return choice_dialog(
        stdscr,
        t("manual_label_output_title"),
        [
            {"value": "print", "label": t("manual_label_output_print")},
            {"value": "pdf", "label": t("manual_label_output_pdf")},
        ],
        current_mode,
        cancel_returns_none=True,
    )


def gls_pickup_product_dialog(stdscr, current_value):
    return choice_dialog(
        stdscr,
        t("gls_pickup_product_title"),
        [
            {"value": "PARCEL", "label": t("gls_pickup_product_parcel")},
            {"value": "EXPRESS", "label": t("gls_pickup_product_express")},
        ],
        (current_value or "PARCEL").strip().upper(),
        cancel_returns_none=True,
    )


def gls_pickup_haz_goods_dialog(stdscr, current_value):
    return choice_dialog(
        stdscr,
        t("haz_goods_title"),
        [
            {"value": "nein", "label": t("haz_goods_no")},
            {"value": "ja", "label": t("haz_goods_yes")},
        ],
        "ja" if current_value else "nein",
        cancel_returns_none=True,
    )


# GLS-Abholung ist aktuell auskommentiert.
# Der API-Pfad ist vorbereitet, aber fachlich noch nicht ausreichend gegen das GLS-Portal verifiziert
# und deshalb vorerst experimentell/ungestestet.
def create_gls_sporadic_collection_dialog(stdscr):
    tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()
    state = {
        "pickup_date": tomorrow,
        "parcel_count": "1",
        "product": "PARCEL",
        "expected_total_weight": "",
        "contains_haz_goods": False,
        "additional_information": "",
    }
    active = 0

    while True:
        fields = [
            {"name": "pickup_date", "label": t("gls_pickup_field_pickup_date"), "value": state["pickup_date"]},
            {"name": "parcel_count", "label": t("gls_pickup_field_parcel_count"), "value": state["parcel_count"]},
            {"name": "product", "label": t("gls_pickup_field_product"), "value": state["product"]},
            {"name": "expected_total_weight", "label": t("gls_pickup_field_weight"), "value": state["expected_total_weight"]},
            {"name": "contains_haz_goods", "label": t("gls_pickup_field_haz_goods"), "value": t("haz_goods_yes") if state["contains_haz_goods"] else t("haz_goods_no")},
            {"name": "additional_information", "label": t("gls_pickup_field_additional_information"), "value": state["additional_information"]},
        ]
        result = form_dialog(
            stdscr,
            t("gls_pickup_title"),
            fields,
            initial_active=active,
            footer_text=t("gls_pickup_footer"),
            extra_actions=[
                {"name": "product", "keys": {curses.KEY_F3}},
                {"name": "haz", "keys": {curses.KEY_F4}},
            ],
        )
        if result is None:
            return None
        if "__action__" in result:
            state.update(result.get("__values__", {}))
            active = result.get("__active__", active)
            if result["__action__"] == "product":
                chosen = gls_pickup_product_dialog(stdscr, state["product"])
                if chosen:
                    state["product"] = chosen
            elif result["__action__"] == "haz":
                chosen = gls_pickup_haz_goods_dialog(stdscr, state["contains_haz_goods"])
                if chosen is not None:
                    state["contains_haz_goods"] = chosen == "ja"
            continue

        state.update(result)
        active = 0
        try:
            booking = gls_order_sporadic_collection(
                preferred_pickup_date=state["pickup_date"],
                number_of_parcels=state["parcel_count"],
                product=state["product"],
                expected_total_weight=state["expected_total_weight"],
                contains_haz_goods=state["contains_haz_goods"],
                additional_information=state["additional_information"],
            )
        except Exception as exc:
            message_box(stdscr, t("gls_pickup_error_title"), str(exc)[:220])
            continue

        estimated = booking.get("estimated_date") or state["pickup_date"]
        message_box(
            stdscr,
            t("gls_pickup_error_title"),
            t("gls_pickup_requested", requested=state["pickup_date"], estimated=estimated)[:56],
        )
        return booking


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
        raise RuntimeError(t("gls_network_error", reason=exc.reason)) from exc

    parsed = None
    if raw:
        try:
            parsed = json.loads(raw.decode("utf-8", errors="replace"))
        except json.JSONDecodeError:
            parsed = None
    return status_code, parsed, raw


def _gls_sporadic_collection_url(credentials):
    api_url = (credentials.get("api_url") or "").strip()
    if not api_url:
        raise RuntimeError(t("gls_api_url_missing"))
    base = api_url.rsplit("/", 1)[0] if "/" in api_url else api_url
    return base.rstrip("/") + "/sporadiccollection"


def gls_order_sporadic_collection(
    preferred_pickup_date,
    number_of_parcels,
    product="PARCEL",
    expected_total_weight=None,
    contains_haz_goods=False,
    additional_information="",
):
    creds = load_gls_credentials()
    pickup_date = (preferred_pickup_date or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", pickup_date):
        raise ValueError(t("gls_pickup_date_invalid"))
    try:
        parcel_count = int(number_of_parcels)
    except (TypeError, ValueError):
        raise ValueError(t("gls_pickup_parcel_count_invalid"))
    if parcel_count <= 0:
        raise ValueError(t("gls_pickup_parcel_count_positive"))

    product_value = (product or "PARCEL").strip().upper()
    if product_value not in {"PARCEL", "EXPRESS"}:
        raise ValueError(t("gls_pickup_product_invalid"))

    payload = {
        "ContactID": creds["contact_id"],
        "PreferredPickUpDate": pickup_date,
        "NumberOfParcels": parcel_count,
        "Product": product_value,
    }
    if expected_total_weight not in (None, ""):
        try:
            weight_value = float(expected_total_weight)
        except (TypeError, ValueError):
            raise ValueError(t("gls_pickup_weight_invalid"))
        if weight_value <= 0:
            raise ValueError(t("gls_pickup_weight_positive"))
        payload["ExpectedTotalWeight"] = round(weight_value, 3)
    if contains_haz_goods:
        payload["ContainsHazGoods"] = True
    info_text = (additional_information or "").strip()
    if info_text:
        payload["AdditionalInformation"] = info_text[:200]

    url = _gls_sporadic_collection_url(creds)
    status_code, data, raw = _gls_api_json_request(url, creds, payload)
    if status_code >= 400:
        error_detail = _gls_error_summary(data, raw)
        LOGGER.error(
            "GLS SporadicCollection Fehler status=%s date=%s parcels=%s product=%s detail=%s",
            status_code,
            pickup_date,
            parcel_count,
            product_value,
            error_detail or "-",
        )
        if error_detail:
            raise RuntimeError(t("gls_pickup_http_error", status_code=status_code, detail=error_detail[:180]))
        raise RuntimeError(t("gls_pickup_http_error_plain", status_code=status_code))

    estimated_date = ""
    if isinstance(data, dict):
        estimated_date = (data.get("EstimatedPickUpDate") or "").strip()
    return {
        "url": url,
        "requested_date": pickup_date,
        "estimated_date": estimated_date or pickup_date,
        "number_of_parcels": parcel_count,
        "product": product_value,
        "response": data,
    }


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
    output_dir = Path(get_shipping_label_output_dir())
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


def _merge_pdf_files(pdf_paths, output_path):
    valid_paths = [str(Path(path)) for path in pdf_paths if path and os.path.isfile(path)]
    if not valid_paths:
        raise RuntimeError(t("pdf_merge_no_files"))
    if len(valid_paths) == 1:
        shutil.copyfile(valid_paths[0], output_path)
        return output_path

    try:
        from pypdf import PdfWriter, PdfReader  # type: ignore

        writer = PdfWriter()
        for path in valid_paths:
            reader = PdfReader(path)
            for page in reader.pages:
                writer.add_page(page)
        with open(output_path, "wb") as handle:
            writer.write(handle)
        return output_path
    except Exception:
        pass

    try:
        from PyPDF2 import PdfWriter, PdfReader  # type: ignore

        writer = PdfWriter()
        for path in valid_paths:
            reader = PdfReader(path)
            for page in reader.pages:
                writer.add_page(page)
        with open(output_path, "wb") as handle:
            writer.write(handle)
        return output_path
    except Exception:
        pass

    if shutil.which("pdfunite"):
        subprocess.run(
            ["pdfunite", *valid_paths, output_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        return output_path

    raise RuntimeError(t("pdf_merge_unavailable"))


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


def _normalize_scale_mode(value):
    normalized = (value or "").strip().lower()
    if normalized in {"fit", "none"}:
        return normalized
    return "none"


def _normalize_duplex_mode(value):
    normalized = (value or "").strip().lower()
    if normalized in {"off", "long", "short"}:
        return normalized
    return "off"


def _normalize_color_mode(value):
    normalized = (value or "").strip().lower()
    if normalized in {"color", "grayscale"}:
        return normalized
    return "color"


def _shipping_printer_for_carrier(carrier):
    c = (carrier or "").strip().lower()
    printer_field = _shipping_carrier_setting_field(c, "printer")
    specific = (SETTINGS.get(printer_field) or "").strip() if printer_field else ""
    fallback = (SETTINGS.get("shipping_label_printer") or "").strip()
    return specific or fallback


def _shipping_format_for_carrier(carrier):
    c = (carrier or "").strip().lower()
    defaults = {code: data.get("default_format", "A6") for code, data in SHIPPING_CARRIER_DEFINITIONS.items()}
    format_field = _shipping_carrier_setting_field(c, "format")
    specific = SETTINGS.get(format_field) if format_field else None
    fallback = SETTINGS.get("shipping_label_format", defaults.get(c, "A6"))
    return _normalize_shipping_label_format(specific or fallback or defaults.get(c, "A6"))


def _shipping_scale_mode_for_carrier(carrier):
    c = (carrier or "").strip().lower()
    scale_field = _shipping_carrier_setting_field(c, "scale")
    specific = SETTINGS.get(scale_field) if scale_field else None
    fallback = SETTINGS.get("shipping_label_scale_mode", DEFAULT_SETTINGS.get("shipping_label_scale_mode", "none"))
    return _normalize_scale_mode(specific or fallback)


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


def _cups_print_options(label_format, *, scale_mode="none", duplex_mode="off", color_mode=None):
    media = _cups_media_value_for_format(label_format)
    options = []
    if media:
        options.extend(["-o", f"media={media}"])
        options.extend(["-o", f"PageSize={media}"])
    if _normalize_scale_mode(scale_mode) == "fit":
        options.extend(["-o", "print-scaling=fit", "-o", "fit-to-page=true"])
    else:
        options.extend(["-o", "print-scaling=none", "-o", "fit-to-page=false", "-o", "scaling=100"])
    normalized_duplex = _normalize_duplex_mode(duplex_mode)
    if normalized_duplex == "long":
        options.extend(["-o", "sides=two-sided-long-edge"])
    elif normalized_duplex == "short":
        options.extend(["-o", "sides=two-sided-short-edge"])
    else:
        options.extend(["-o", "sides=one-sided"])
    normalized_color = (color_mode or "").strip().lower()
    if normalized_color == "grayscale":
        options.extend(["-o", "print-color-mode=monochrome", "-o", "ColorModel=Gray"])
    elif normalized_color == "color":
        options.extend(["-o", "print-color-mode=color"])
    options.extend(["-o", "page-border=none", "-o", "number-up=1"])
    return options


def _cups_label_print_options(label_format, *, carrier=None):
    return _cups_print_options(
        label_format,
        scale_mode=_shipping_scale_mode_for_carrier(carrier),
        duplex_mode="off",
        color_mode=None,
    )


def _cups_delivery_note_print_options(label_format):
    return _cups_print_options(
        label_format,
        scale_mode=_normalize_scale_mode(
            SETTINGS.get("delivery_note_scale_mode", DEFAULT_SETTINGS.get("delivery_note_scale_mode", "none"))
        ),
        duplex_mode=_normalize_duplex_mode(
            SETTINGS.get("delivery_note_duplex", DEFAULT_SETTINGS.get("delivery_note_duplex", "off"))
        ),
        color_mode=_normalize_color_mode(
            SETTINGS.get("delivery_note_color_mode", DEFAULT_SETTINGS.get("delivery_note_color_mode", "color"))
        ),
    )


def _short_print_output(value, limit=220):
    text = (value or "").strip().replace("\n", " | ")
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _print_test_summary_line(context):
    parts = []
    printer = (context.get("printer") or "").strip() or "-"
    page_size = (context.get("page_size") or "").strip() or "-"
    parts.append(t("print_test_line_printer", value=printer))
    parts.append(t("print_test_line_format", value=page_size))
    for line in context.get("lines") or []:
        text = str(line or "").strip()
        if not text:
            continue
        if text in parts:
            continue
        parts.append(text)
    return " | ".join(parts)


def _build_simple_test_page_pdf(title, lines=None, page_size="A4"):
    width, height = address_label._page_dimensions_points(page_size)
    margin = 36
    text_lines = [str(title or "Testseite").strip() or "Testseite"]
    text_lines.extend(str(line).strip() for line in (lines or []) if str(line).strip())
    content_lines = [
        "BT",
        "/F1 18 Tf",
        f"1 0 0 1 {margin:.2f} {height - margin - 10:.2f} Tm ({address_label._pdf_escape(text_lines[0])}) Tj",
        "ET",
        "BT",
        "/F1 11 Tf",
    ]
    current_y = height - margin - 42
    for line in text_lines[1:]:
        content_lines.append(f"1 0 0 1 {margin:.2f} {current_y:.2f} Tm ({address_label._pdf_escape(line)}) Tj")
        current_y -= 15
    content_lines.append("ET")
    content = "\n".join(content_lines).encode("latin-1", errors="replace")

    objects = [
        "<< /Type /Catalog /Pages 2 0 R >>",
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {width:.2f} {height:.2f}] "
            "/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>"
        ),
        "<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        f"<< /Length {len(content)} >>\nstream\n{content.decode('latin-1')}\nendstream",
    ]
    pdf = "%PDF-1.4\n"
    offsets = []
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(pdf.encode("latin-1")))
        pdf += f"{index} 0 obj\n{obj}\nendobj\n"
    xref_start = len(pdf.encode("latin-1"))
    pdf += f"xref\n0 {len(objects) + 1}\n"
    pdf += "0000000000 65535 f \n"
    for offset in offsets:
        pdf += f"{offset:010d} 00000 n \n"
    pdf += f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_start}\n%%EOF\n"
    return pdf.encode("latin-1")


def _run_lp_command(cmd, *, print_kind, printer, title, source_path=None, extra=None):
    source_path = str(source_path or "")
    file_exists = bool(source_path and os.path.isfile(source_path))
    file_size = os.path.getsize(source_path) if file_exists else None
    PRINT_LOGGER.info(
        "Druckstart kind=%s printer=%s title=%s path=%s exists=%s size=%s extra=%s cmd=%s",
        print_kind,
        printer,
        title,
        source_path or "-",
        file_exists,
        file_size if file_size is not None else "-",
        extra or "-",
        json.dumps(cmd, ensure_ascii=True),
    )
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True,
    )
    PRINT_LOGGER.info(
        "Druckok kind=%s printer=%s title=%s stdout=%s stderr=%s",
        print_kind,
        printer,
        title,
        _short_print_output(result.stdout),
        _short_print_output(result.stderr),
    )
    return result


def _print_test_page_to_printer(stdscr, printer, title, page_size="A4", lines=None):
    if not (printer or "").strip():
        message_box(stdscr, t("error"), t("no_printer_for_test_page"))
        return False
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as handle:
        temp_path = handle.name
    try:
        Path(temp_path).write_bytes(_build_simple_test_page_pdf(title, lines=lines, page_size=page_size))
        cmd = ["lp", "-d", printer, "-t", title]
        cmd.extend(_cups_label_print_options(page_size))
        cmd.append(temp_path)
        _run_lp_command(
            cmd,
            print_kind="test_page",
            printer=printer,
            title=title,
            source_path=temp_path,
            extra=f"page_size={page_size}",
        )
    except FileNotFoundError:
        PRINT_LOGGER.exception("lp nicht verfuegbar fuer Testseite printer=%s title=%s", printer, title)
        message_box(stdscr, t("print_error_title"), t("lp_unavailable"))
        return False
    except subprocess.CalledProcessError as exc:
        PRINT_LOGGER.exception("Testseite fehlgeschlagen printer=%s title=%s", printer, title)
        error_text = _short_print_output(exc.stderr or str(exc), limit=160)
        message_box(stdscr, t("print_error_title"), f"{(error_text[:20] or t('print_error_title'))} {PRINT_LOG_PATH.name}"[:56])
        return False
    finally:
        try:
            os.unlink(temp_path)
        except FileNotFoundError:
            pass
    message_box(stdscr, t("print_title"), t("test_page_sent"))
    return True


def _print_pdf_via_lp(stdscr, pdf_path, title, carrier=None):
    carrier_key = effective_shipping_carrier(carrier)
    printer = _shipping_printer_for_carrier(carrier_key)
    if not printer:
        message_box(stdscr, t("error"), t("set_label_printer_for_carrier", carrier=carrier_key.upper()))
        return False
    label_format = _shipping_format_for_carrier(carrier_key)
    cmd = ["lp", "-d", printer, "-t", title]
    cmd.extend(_cups_label_print_options(label_format, carrier=carrier_key))
    cmd.append(pdf_path)
    try:
        _run_lp_command(
            cmd,
            print_kind="shipping_label",
            printer=printer,
            title=title,
            source_path=pdf_path,
            extra=f"carrier={carrier_key} format={label_format}",
        )
    except FileNotFoundError:
        PRINT_LOGGER.exception("lp nicht verfuegbar fuer Versandlabel path=%s", pdf_path)
        message_box(stdscr, t("print_error_title"), t("lp_unavailable"))
        return False
    except subprocess.CalledProcessError as exc:
        PRINT_LOGGER.exception("Versandlabel Druck fehlgeschlagen carrier=%s path=%s", carrier_key, pdf_path)
        error_text = (exc.stderr or str(exc)).strip()
        message_box(stdscr, t("print_error_title"), f"{(error_text[:20] or t('print_error_title'))} {PRINT_LOG_PATH.name}"[:56])
        return False
    return True


def _validate_shipping_address(order, require_country=False):
    checks = [
        ("shipping_name", t("recipient_name_missing")),
        ("shipping_address1", t("recipient_street_missing")),
        ("shipping_zip", t("recipient_zip_missing")),
        ("shipping_city", t("recipient_city_missing")),
    ]
    for key, message in checks:
        if not (order.get(key) or "").strip():
            raise ValueError(message)
    if require_country and not _gls_country_code(order.get("shipping_country")):
        raise ValueError(t("recipient_country_invalid_or_missing_iso2"))


def _validate_order_for_gls(order):
    _validate_shipping_address(order, require_country=True)


def gls_create_label(order, weight_kg=1.0, shipment_reference=None, service_codes=None):
    _validate_order_for_gls(order)
    creds = load_gls_credentials()
    try:
        weight_value = float(weight_kg)
    except (TypeError, ValueError):
        raise ValueError(t("weight_invalid"))
    if weight_value <= 0:
        raise ValueError(t("weight_positive"))
    weight_value = round(weight_value, 3)

    shipment_reference = _sanitize_order_reference(shipment_reference or order["order_name"])
    normalized_services = _normalize_shipping_services(
        service_codes if service_codes is not None else SETTINGS.get("shipping_services", [])
    )
    if "service_flexdelivery" in normalized_services and not (order.get("shipping_email") or "").strip():
        raise ValueError(t("flexdelivery_email_missing"))
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
            raise RuntimeError(t("gls_label_http_error", status_code=status_code, detail=error_detail[:180]))
        raise RuntimeError(t("gls_label_http_error_plain", status_code=status_code))

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
            raise RuntimeError(t("gls_label_response_missing_pdf"))

    label_path = _save_shipping_label_pdf("gls", order["order_name"], track_id, pdf_blob)
    label_id = insert_shipping_label_history(
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
    try:
        _weight_value = float(weight_kg)
    except (TypeError, ValueError):
        raise ValueError(t("weight_invalid"))
    if _weight_value <= 0:
        raise ValueError(t("weight_positive"))
    _weight_value = round(_weight_value, 3)
    _reference = _sanitize_order_reference(shipment_reference or order["order_name"])
    product = _resolve_post_product_selection(service_codes)
    page_format_id = _resolve_post_page_format_id(client, _shipping_format_for_carrier("post"))
    sender = _post_sender_address(client)
    receiver = _post_receiver_address(order)
    total_cents = int(product.get("price_cents") or 0)
    if total_cents <= 0:
        raise RuntimeError(t("post_product_price_invalid"))

    position = {
        "productCode": int(product["product_code"]),
        "voucherLayout": "ADDRESS_ZONE",
        "positionType": "AppShoppingCartPDFPosition",
        "position": {"page": 1, "labelX": 1, "labelY": 1},
        "address": {
            "sender": sender,
            "receiver": receiver,
        },
    }
    try:
        response, pdf_blob = client.checkout_pdf_binary(
            shop_order_id=_reference[:18],
            total_cents=total_cents,
            page_format_id=page_format_id,
            positions=[position],
            create_manifest=False,
            create_shipping_list="0",
            dpi="DPI300",
            direct_checkout=True,
        )
    except Exception:
        LOGGER.exception(
            "POST Label-Checkout fehlgeschlagen reference=%s product_code=%s product_name=%s page_format_id=%s country=%s",
            _reference,
            product.get("product_code"),
            product.get("name"),
            page_format_id,
            order.get("shipping_country"),
        )
        raise
    shopping_cart = response.get("shoppingCart") if isinstance(response.get("shoppingCart"), dict) else {}
    voucher_list = shopping_cart.get("voucherList") if isinstance(shopping_cart.get("voucherList"), list) else []
    first_voucher = voucher_list[0] if voucher_list else {}
    track_id = (first_voucher.get("trackId") or "").strip() or (first_voucher.get("voucherId") or "").strip()
    parcel_number = (first_voucher.get("trackId") or "").strip() or None
    tracking_url = _tracking_url_for_carrier("post", track_id or parcel_number or _reference)
    label_path = _save_shipping_label_pdf("post", order["order_name"], track_id or _reference, pdf_blob)
    label_id = insert_shipping_label_history(
        order=order,
        shipment_reference=_reference,
        track_id=track_id or _reference,
        parcel_number=parcel_number,
        label_path=label_path,
        status="CREATED",
        weight_kg=_weight_value,
        carrier="post",
        tracking_url=tracking_url,
    )
    return {
        "label_id": label_id,
        "track_id": track_id or _reference,
        "parcel_number": parcel_number,
        "label_path": label_path,
        "shipment_reference": _reference,
        "post_product_code": product["product_code"],
        "post_product_name": product["name"],
        "tracking_url": tracking_url,
    }


def free_create_label(order, weight_kg=1.0, shipment_reference=None, service_codes=None):
    _validate_shipping_address(order, require_country=False)
    try:
        weight_value = round(float(weight_kg), 3)
    except (TypeError, ValueError):
        weight_value = 0.0
    shipment_reference = _sanitize_order_reference(shipment_reference or order["order_name"])
    internal_id = f"FREE{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    template_path = get_free_label_template_path()
    if template_path and not template_path.exists():
        raise FileNotFoundError(f"Adresslabel Vorlage fehlt: {template_path.name}")

    with tempfile.NamedTemporaryFile(prefix="free-label-", suffix=".pdf", delete=False) as handle:
        temp_path = handle.name
    try:
        build_address_label_pdf(
            template_path,
            temp_path,
            sender=get_free_label_sender(),
            receiver=_free_label_receiver(order),
            page_size=_shipping_format_for_carrier("free"),
        )
        pdf_blob = Path(temp_path).read_bytes()
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            pass

    label_path = _save_shipping_label_pdf("free", order["order_name"], internal_id, pdf_blob)
    label_id = insert_shipping_label_history(
        order=order,
        shipment_reference=shipment_reference,
        track_id=internal_id,
        parcel_number=None,
        label_path=label_path,
        status="CREATED",
        weight_kg=weight_value,
        carrier="free",
    )
    return {
        "label_id": label_id,
        "track_id": internal_id,
        "parcel_number": None,
        "label_path": label_path,
        "shipment_reference": shipment_reference,
    }


def test_create_label(order, weight_kg=1.0, shipment_reference=None, service_codes=None):
    _validate_shipping_address(order, require_country=False)
    shipment_reference = _sanitize_order_reference(shipment_reference or order["order_name"])
    track_id = f"TEST{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
    parcel_number = f"999{datetime.datetime.now().strftime('%H%M%S')}"
    pdf_blob = _build_test_label_pdf(order.get("order_name") or "TEST", shipment_reference, track_id)
    label_path = _save_shipping_label_pdf("test", order["order_name"], track_id, pdf_blob)
    label_id = insert_shipping_label_history(
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


def _gls_label_identifiers(label_row):
    identifiers = []
    for value in (label_row.get("parcel_number"), label_row.get("track_id")):
        normalized = (value or "").strip()
        if normalized and normalized not in identifiers:
            identifiers.append(normalized)
    return identifiers


def gls_reprint_label(label_row):
    creds = load_gls_credentials()
    identifiers = _gls_label_identifiers(label_row)
    if not identifiers:
        raise ValueError(t("track_or_parcel_missing"))
    status_code = None
    data = None
    raw = b""
    chosen_identifier = identifiers[0]
    for identifier in identifiers:
        url = f"{creds['api_url'].rstrip('/')}/reprint/{identifier}"
        status_code, data, raw = _gls_api_json_request(url, creds)
        chosen_identifier = identifier
        if status_code < 400 or status_code != 404:
            break
    if status_code is None:
        raise RuntimeError(t("gls_reprint_failed"))
    if status_code >= 400:
        error_detail = _gls_error_summary(data, raw)
        LOGGER.error(
            "GLS Reprint Fehler status=%s identifiers=%s detail=%s",
            status_code,
            ",".join(identifiers),
            error_detail or "-",
        )
        if error_detail:
            raise RuntimeError(t("gls_reprint_http_error", status_code=status_code, detail=error_detail[:180]))
        raise RuntimeError(t("gls_reprint_http_error_plain", status_code=status_code))

    pdf_blob = _extract_first_pdf_blob(data)
    if not pdf_blob and raw.startswith(b"%PDF-"):
        pdf_blob = raw
    if not pdf_blob:
        raise RuntimeError(t("gls_reprint_missing_pdf"))

    label_path = _save_shipping_label_pdf("gls", label_row["order_name"], chosen_identifier, pdf_blob, suffix="reprint")
    update_shipping_label_reprint(label_row["id"], label_path)
    return label_path


def gls_cancel_label(label_row):
    creds = load_gls_credentials()
    identifiers = _gls_label_identifiers(label_row)
    if not identifiers:
        raise ValueError(t("track_or_parcel_missing"))
    status_code = None
    data = None
    raw = b""
    chosen_identifier = identifiers[0]
    for identifier in identifiers:
        url = f"{creds['api_url'].rstrip('/')}/cancel/{identifier}"
        status_code, data, raw = _gls_api_json_request(url, creds)
        chosen_identifier = identifier
        if status_code < 400 or status_code != 404:
            break
    if status_code is None:
        raise RuntimeError(t("gls_cancel_failed"))
    if status_code >= 400:
        error_detail = _gls_error_summary(data, raw)
        update_shipping_label_status(label_row["id"], "CANCEL_FAILED", f"HTTP {status_code} {error_detail[:120]}".strip())
        LOGGER.error(
            "GLS Storno Fehler status=%s identifiers=%s detail=%s",
            status_code,
            ",".join(identifiers),
            error_detail or "-",
        )
        if error_detail:
            raise RuntimeError(t("gls_cancel_http_error", status_code=status_code, detail=error_detail[:180]))
        raise RuntimeError(t("gls_cancel_http_error_plain", status_code=status_code))

    result = ""
    if isinstance(data, dict):
        result = (data.get("result") or "").strip().upper()
    if result == "CANCELLED":
        update_shipping_label_status(label_row["id"], "CANCELLED")
    elif result == "CANCELLATION_PENDING":
        update_shipping_label_status(label_row["id"], "CANCELLATION_PENDING")
    else:
        update_shipping_label_status(label_row["id"], "CANCEL_REQUESTED")
    return result or "CANCEL_REQUESTED"


SHIPPING_CARRIER_RUNTIME_SPECS = {
    "gls": {
        "create_label": "gls_create_label",
        "reprint_label": "gls_reprint_label",
        "cancel_label": "gls_cancel_label",
    },
    "post": {"create_label": "post_create_label"},
    "free": {"create_label": "free_create_label"},
    "test": {"create_label": "test_create_label"},
}


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

    return normalized


def _shipping_service_label(service_entry_or_code):
    if isinstance(service_entry_or_code, dict):
        label_key = service_entry_or_code.get("label_key")
        fallback = service_entry_or_code.get("code") or "-"
    else:
        label_key = None
        fallback = str(service_entry_or_code or "-")
        for entry in SHIPPING_SERVICE_OPTIONS:
            if entry["code"] == fallback:
                label_key = entry.get("label_key")
                break
    if label_key:
        return t(label_key)
    return fallback


def _shipping_services_summary(service_codes):
    code_to_label = {entry["code"]: _shipping_service_label(entry) for entry in SHIPPING_SERVICE_OPTIONS}
    labels = [code_to_label.get(code, code) for code in _normalize_shipping_services(service_codes)]
    return ", ".join(labels)


def shipping_services_dialog(stdscr, current_services, cancel_returns_none=False):
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
        win.addstr(0, 2, t("shipping_services_title"))

        visible_rows = max(1, height - 4)
        if selected < top_index:
            top_index = selected
        if selected >= top_index + visible_rows:
            top_index = selected - visible_rows + 1

        for row_idx, option in enumerate(SHIPPING_SERVICE_OPTIONS[top_index:top_index + visible_rows]):
            real_idx = top_index + row_idx
            y_pos = 2 + row_idx
            checked = "[x]" if option["code"] in selected_codes else "[ ]"
            suffix = t("shipping_service_locked_suffix") if option.get("locked") else ""
            line = _fit(f"{checked} {_shipping_service_label(option)}{suffix}", width - 3)
            if real_idx == selected:
                win.attrset(curses.color_pair(2))
                win.addstr(y_pos, 1, line.ljust(width - 2))
                win.attrset(curses.color_pair(1))
            else:
                win.addstr(y_pos, 1, line.ljust(width - 2))

        footer = t("shipping_services_footer")
        win.attrset(curses.color_pair(3))
        win.addstr(height - 1, 1, _fit(footer, width - 2))
        win.attrset(curses.color_pair(1))
        win.refresh()

        key = win.get_wch()
        if key in (27, curses.KEY_F9):
            if cancel_returns_none:
                return None
            return _normalize_shipping_services(current_services)
        if key in (10, 13, "\n", "\r", curses.KEY_ENTER):
            return _normalize_shipping_services(list(selected_codes))
        if key == curses.KEY_DOWN:
            selected = move_selection(SHIPPING_SERVICE_OPTIONS, selected, 1)
            continue
        if key == curses.KEY_UP:
            selected = move_selection(SHIPPING_SERVICE_OPTIONS, selected, -1)
            continue
        if key == " ":
            option = SHIPPING_SERVICE_OPTIONS[selected]
            if option.get("locked"):
                continue
            code = option["code"]
            if code in selected_codes:
                selected_codes.remove(code)
            else:
                selected_codes.add(code)


def _select_shipping_carrier_options(stdscr, carrier, scope="domestic"):
    mode = _shipping_carrier_option_mode(carrier)
    if mode == "post_products":
        return _post_selection_dialog(stdscr, scope=scope)
    if mode == "gls_services":
        return shipping_services_dialog(
            stdscr,
            SETTINGS.get("shipping_services", []),
            cancel_returns_none=True,
        )
    return []


def remember_shipping_carrier(carrier):
    global _SHIPPING_CARRIER_CACHE
    normalized = (carrier or "").strip().lower()
    if normalized in SHIPPING_CARRIER_DEFINITIONS:
        _SHIPPING_CARRIER_CACHE = normalized


def last_shipping_carrier():
    cached = (_SHIPPING_CARRIER_CACHE or "").strip().lower()
    active = _active_shipping_carriers()
    if cached in active:
        return cached
    return active[0] if active else "gls"


def effective_shipping_carrier(requested_carrier=None):
    active = _active_shipping_carriers()
    carrier = (requested_carrier or last_shipping_carrier() or (active[0] if active else "gls")).strip().lower()
    if carrier in SHIPPING_CARRIER_DEFINITIONS and (carrier in active or carrier == "test"):
        return carrier
    return active[0] if active else "gls"


def _shipping_carrier_runtime(carrier):
    spec = SHIPPING_CARRIER_RUNTIME_SPECS.get((carrier or "").strip().lower())
    if not spec:
        return None
    return ShippingCarrierRuntime(
        create_label=globals().get(spec.get("create_label")),
        reprint_label=globals().get(spec.get("reprint_label")),
        cancel_label=globals().get(spec.get("cancel_label")),
    )


def create_shipping_label(order, weight_kg=None, shipment_reference=None, service_codes=None, carrier=None):
    selected_carrier = effective_shipping_carrier(carrier)
    if weight_kg is None:
        weight_kg, _total_grams = calculate_order_shipping_weight(order)
    runtime = _shipping_carrier_runtime(selected_carrier)
    if runtime and runtime.create_label:
        return runtime.create_label(
            order,
            weight_kg=weight_kg,
            shipment_reference=shipment_reference,
            service_codes=service_codes,
        )
    raise RuntimeError(t("carrier_not_implemented", carrier=selected_carrier))


def _created_label_display_value(carrier, created):
    normalized = (carrier or "").strip().lower()
    if normalized == "free":
        return "Adresslabel"
    return (created.get("parcel_number") or created.get("track_id") or created.get("shipment_reference") or "-").strip() or "-"


def reprint_shipping_label(label_row):
    existing_path = (label_row.get("label_path") or "").strip()
    if existing_path and os.path.isfile(existing_path):
        return existing_path
    carrier = (label_row.get("carrier") or "gls").strip().lower()
    runtime = _shipping_carrier_runtime(carrier)
    if runtime and runtime.reprint_label:
        return runtime.reprint_label(label_row)
    raise RuntimeError(t("reprint_not_implemented", carrier=carrier))


def cancel_shipping_label(label_row):
    carrier = (label_row.get("carrier") or "gls").strip().lower()
    runtime = _shipping_carrier_runtime(carrier)
    if runtime and runtime.cancel_label:
        return runtime.cancel_label(label_row)
    raise RuntimeError(t("cancel_not_implemented", carrier=carrier))


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


def get_location_regex_ignore_case(field_name):
    setting_key = f"location_regex_ignore_case_{field_name}"
    return bool(SETTINGS.get(setting_key, DEFAULT_SETTINGS.get(setting_key, False)))


def get_location_regex_flags(field_name):
    flags = 0
    if get_location_regex_ignore_case(field_name):
        flags |= re.IGNORECASE
    return flags


def get_location_regex_normalize_case(field_name):
    setting_key = f"location_regex_normalize_case_{field_name}"
    mode = str(SETTINGS.get(setting_key, DEFAULT_SETTINGS.get(setting_key, "none")) or "none").strip().lower()
    if mode not in {"none", "upper", "lower"}:
        return "none"
    return mode


def apply_location_normalize_case(field_name, value):
    mode = get_location_regex_normalize_case(field_name)
    if mode == "upper":
        return value.upper()
    if mode == "lower":
        return value.lower()
    return value


def normalize_location_value(field_name, value):
    value = (value or "").strip()

    if value == "":
        return ""

    value = apply_location_normalize_case(field_name, value)

    pattern = get_location_regex(field_name)
    try:
        if re.fullmatch(pattern, value, get_location_regex_flags(field_name)):
            return value
    except re.error:
        return None

    return None


def validate_location_or_error(stdscr, field_name, raw_value):
    value = normalize_location_value(field_name, raw_value)
    if value is None:
        label = {
            "regal": t("field_regal_short"),
            "fach": t("field_fach_short"),
            "platz": t("field_platz_short"),
        }[field_name]
        pattern = get_location_regex(field_name)
        message_box(stdscr, t("error"), t("regex_mismatch", label=label, pattern=pattern)[:56])
        return None
    return value


def is_location_input_allowed(field_name, raw_value):
    value = (raw_value or "").strip()
    if value == "":
        return True
    return normalize_location_value(field_name, value) is not None


def _location_regex_field_label(field_name):
    return {
        "regal": t("field_regex_regal"),
        "fach": t("field_regex_fach"),
        "platz": t("field_regex_platz"),
    }[field_name]


def _location_field_normalizers():
    return {
        "regal": lambda value: apply_location_normalize_case("regal", value),
        "fach": lambda value: apply_location_normalize_case("fach", value),
        "platz": lambda value: apply_location_normalize_case("platz", value),
    }


def _location_regex_options_dialog(stdscr, field_name, values):
    regex_key = f"location_regex_{field_name}"
    ignore_case_key = f"location_regex_ignore_case_{field_name}"
    normalize_case_key = f"location_regex_normalize_case_{field_name}"
    pattern = str(values.get(regex_key, ""))
    ignore_case = bool(values.get(ignore_case_key, False))
    normalize_case = str(values.get(normalize_case_key, "none") or "none").strip().lower()
    if normalize_case not in {"none", "upper", "lower"}:
        normalize_case = "none"
    active = 0

    h, w = stdscr.getmaxyx()
    width = min(78, w - 4)
    height = 9
    y = max(1, (h - height) // 2)
    x = max(2, (w - width) // 2)

    while True:
        draw_shadow(stdscr, y, x, height, width)
        win = curses.newwin(height, width, y, x)
        win.keypad(True)
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        win.addstr(0, 2, f" {t('regex_options_title', label=_location_regex_field_label(field_name))} ")

        regex_label = t("field_regex_pattern")
        ignore_label = t("field_regex_ignore_case")
        normalize_label = t("field_regex_normalize_case")
        field_x = max(len(regex_label), len(ignore_label), len(normalize_label)) + 4
        field_width = max(1, width - field_x - 3)

        win.addstr(2, 2, f"{regex_label}:")
        regex_value = pattern[-field_width:]
        if active == 0:
            win.attrset(curses.color_pair(2))
            win.addstr(2, field_x, regex_value.ljust(field_width))
            win.attrset(curses.color_pair(1))
        else:
            win.addstr(2, field_x, regex_value.ljust(field_width))

        win.addstr(3, 2, f"{ignore_label}:")
        ignore_value = t("settings_value_yes") if ignore_case else t("settings_value_no")
        if active == 1:
            win.attrset(curses.color_pair(2))
            win.addstr(3, field_x, ignore_value.ljust(field_width))
            win.attrset(curses.color_pair(1))
        else:
            win.addstr(3, field_x, ignore_value.ljust(field_width))

        win.addstr(4, 2, f"{normalize_label}:")
        normalize_value = t(f"settings_value_{normalize_case}")
        if active == 2:
            win.attrset(curses.color_pair(2))
            win.addstr(4, field_x, normalize_value.ljust(field_width))
            win.attrset(curses.color_pair(1))
        else:
            win.addstr(4, field_x, normalize_value.ljust(field_width))

        draw_footer_line(win, height - 2, 2, width - 4, t("regex_options_footer"))

        if active == 0:
            cursor_pos = min(len(pattern), field_width - 1)
            win.move(2, field_x + cursor_pos)
        elif active == 1:
            win.move(3, field_x)
        else:
            win.move(4, field_x)
        win.refresh()

        key = win.get_wch()

        if key in (27, curses.KEY_F9):
            return values
        if key == curses.KEY_F2:
            values[regex_key] = pattern.strip()
            values[ignore_case_key] = ignore_case
            values[normalize_case_key] = normalize_case
            return values
        if key == curses.KEY_DOWN:
            active = (active + 1) % 3
            continue
        if key == curses.KEY_UP:
            active = (active - 1) % 3
            continue

        if active == 1:
            if key in (" ", curses.KEY_LEFT, curses.KEY_RIGHT, 10, 13, "\n", "\r", curses.KEY_ENTER):
                ignore_case = not ignore_case
            continue
        if active == 2:
            if key in (" ", curses.KEY_RIGHT, 10, 13, "\n", "\r", curses.KEY_ENTER):
                normalize_case = {
                    "none": "upper",
                    "upper": "lower",
                    "lower": "none",
                }[normalize_case]
            elif key == curses.KEY_LEFT:
                normalize_case = {
                    "none": "lower",
                    "upper": "none",
                    "lower": "upper",
                }[normalize_case]
            continue

        if key in (10, 13, "\n", "\r", curses.KEY_ENTER):
            active = 1
            continue
        if key in (curses.KEY_BACKSPACE, 127, 8, '\x7f', '\b'):
            pattern = pattern[:-1]
            continue
        if isinstance(key, str) and key.isprintable():
            pattern += key


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
        _display_sku_value(row),
        row["name"],
        row["regal"],
        row["fach"],
        row["platz"],
        str(row["menge"]),
        str(row.get("gesamt_menge", row["menge"])),
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
        ("Name", 52),
        (t("col_shelf"), 7),
        (t("col_bin"), 6),
        (t("col_slot"), 7),
        (t("col_local"), 7),
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
    lines.append(t("item_info_sku", value=_display_sku_value(item)))
    lines.append(t("item_info_name", value=item.get("name") or "-"))
    lines.append(t("item_info_barcode", value=item.get("barcode") or "-"))
    lines.append(t("item_info_shopify_status", value=item.get("shopify_product_status") or "-"))
    lines.append(t("item_info_sales_price", value=_format_eur(item.get("shopify_price"))))
    lines.append(t("item_info_compare_price", value=_format_eur(item.get("shopify_compare_at_price"))))
    lines.append(t("item_info_unit_cost", value=_format_eur(item.get("shopify_unit_cost"))))

    weight_grams = item.get("shopify_weight_grams")
    weight_value = f"{weight_grams} g" if weight_grams is not None else "-"
    lines.append(t("item_info_weight", value=weight_value))
    lines.append(t("item_info_sync", value=item.get("sync_status") or "-"))
    lines.append(t("item_info_local_qty", value=item.get("menge")))
    lines.append(t("item_info_total_qty", value=item.get("gesamt_menge", item.get("menge"))))
    lines.append(
        t(
            "item_info_location",
            value=f"{(item.get('regal') or '-')}/{(item.get('fach') or '-')}/{(item.get('platz') or '-')}",
        )
    )
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

    footer = t("item_info_footer")
    while True:
        draw_shadow(stdscr, y, x, height, width)
        win = curses.newwin(height, width, y, x)
        win.keypad(True)
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        win.addstr(0, 2, t("item_info_title"))

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
        desc_win.addstr(0, 2, t("item_info_description_title"))

        wrapped_description = []
        for line in description_lines:
            wrapped_description.extend(textwrap.wrap(line, width=max(10, desc_width - 4)) or [""])

        visible_desc_rows = max(1, desc_height - 2)
        visible_desc = wrapped_description[description_top : description_top + visible_desc_rows]
        for index, line in enumerate(visible_desc):
            desc_win.addstr(1 + index, 2, line[: desc_width - 4])

        win.attrset(curses.color_pair(3))
        try:
            win.addstr(height - 1, 1, " " * max(0, width - 2))
            win.addstr(height - 1, 1, footer[: max(0, width - 2)])
        except curses.error:
            pass
        win.refresh()

        try:
            key = win.get_wch()
        except curses.error:
            continue
        if key in (27, "\x1b", curses.KEY_F9, curses.KEY_ENTER, "\n", "\r"):
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
        regal_label = t("inventory_shelf_title", value=regal) if regal else t("inventory_no_shelf")
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
                    str(_display_sku_value(row)),
                ),
            )

            for item in fach_items:
                platz = "" if item["platz"] is None else str(item["platz"]).strip()
                platz_label = platz if platz else "-"
                rows.append({
                "kind": "item",
                    "label": f"    {platz_label:>4}  {_fit(_display_sku_value(item), 18)} {_fit(item['name'], 22)}",
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


def clamp_top_index(selected, top_index, visible_rows):
    visible_rows = max(1, int(visible_rows))
    selected = max(0, int(selected))
    top_index = max(0, int(top_index))
    if selected < top_index:
        return selected
    if selected >= top_index + visible_rows:
        return max(0, selected - visible_rows + 1)
    return top_index


def draw_shadow(stdscr, y, x, h, w):
    max_y, max_x = stdscr.getmaxyx()
    if y + h + 1 >= max_y or x + w + 2 >= max_x:
        return
    shadow = curses.newwin(h, w, y + 1, x + 2)
    shadow.bkgd(" ", curses.color_pair(3))
    shadow.erase()
    shadow.refresh()


def _scrolling_footer_slice(text, width):
    width = max(0, int(width))
    if width <= 0:
        return ""
    text = str(text or "")
    if len(text) <= width:
        return text.ljust(width)
    gap = "   "
    cycle = text + gap
    scroll_speed = 1.0
    start_pause = 6.0
    end_pause = 2.5
    scroll_frames = len(cycle)
    scroll_duration = scroll_frames / scroll_speed
    cycle_duration = start_pause + scroll_duration + end_pause
    phase = time.monotonic() % cycle_duration
    if phase < start_pause:
        offset = 0
    elif phase >= start_pause + scroll_duration:
        offset = scroll_frames - 1
    else:
        offset = min(scroll_frames - 1, int((phase - start_pause) * scroll_speed))
    window = cycle[offset:] + cycle[:offset] + cycle
    return window[:width]


def draw_footer_line(win, y, x, width, text):
    width = max(0, int(width))
    if width <= 0:
        return
    try:
        win.addstr(y, x, " " * width)
        win.addstr(y, x, _scrolling_footer_slice(text, width))
    except curses.error:
        return


def _safe_addstr(win, y, x, text, attr=None):
    try:
        max_y, max_x = win.getmaxyx()
    except curses.error:
        return
    if y < 0 or x < 0 or y >= max_y or x >= max_x:
        return
    width = max_x - x
    if width <= 0:
        return
    snippet = str(text)[:width]
    try:
        if attr is None:
            win.addstr(y, x, snippet)
        else:
            win.addstr(y, x, snippet, attr)
    except curses.error:
        return


def draw_panel(win, title, lines, selected, top_index, active):
    h, w = win.getmaxyx()
    max_rows = max(0, h - 4)

    win.erase()
    win.box()
    panel_title = f" {title} "

    if active:
        win.attrset(curses.color_pair(2))
        _safe_addstr(win, 0, 2, panel_title[: max(0, w - 4)])
        win.attrset(curses.color_pair(1))
    else:
        _safe_addstr(win, 0, 2, panel_title[: max(0, w - 4)], curses.A_BOLD)

    visible = lines[top_index:top_index + max_rows]

    for i, line in enumerate(visible):
        y = 2 + i
        idx = top_index + i

        if idx == selected:
            win.attrset(curses.color_pair(2))
            _safe_addstr(win, y, 1, line[: max(0, w - 2)].ljust(max(0, w - 2)))
            win.attrset(curses.color_pair(1))
        else:
            _safe_addstr(win, y, 1, line[: max(0, w - 2)].ljust(max(0, w - 2)))

    win.refresh()


def draw_items_panel(win, items, selected, top_index, active):
    h, w = win.getmaxyx()
    max_rows = max(0, h - 5)

    win.erase()
    win.box()
    panel_title = f" {t('items_panel')} "

    if active:
        win.attrset(curses.color_pair(2))
        _safe_addstr(win, 0, 2, panel_title[: max(0, w - 4)])
        win.attrset(curses.color_pair(1))
    else:
        _safe_addstr(win, 0, 2, panel_title[: max(0, w - 4)], curses.A_BOLD)

    _safe_addstr(win, 2, 1, format_header()[: max(0, w - 2)].ljust(max(0, w - 2)), curses.A_BOLD)

    visible = items[top_index:top_index + max_rows]

    for i, row in enumerate(visible):
        y = 3 + i
        line = format_row(row)
        idx = top_index + i

        if idx == selected:
            win.attrset(curses.color_pair(2))
            _safe_addstr(win, y, 1, line[: max(0, w - 2)].ljust(max(0, w - 2)))
            win.attrset(curses.color_pair(1))
        else:
            _safe_addstr(win, y, 1, line[: max(0, w - 2)].ljust(max(0, w - 2)))

    win.refresh()

def draw(
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
    current_shopify_location=None,
    sync_status_label=None,
    notice_text=None,
):
    h, w = stdscr.getmaxyx()

    stdscr.attrset(curses.color_pair(1))
    stdscr.erase()
    stdscr.box()
    _safe_addstr(stdscr, 0, 2, f" {t('app_title')} ")
    version_label = f" v{APP_VERSION} "
    version_x = max(2, w - len(version_label) - 2)
    _safe_addstr(stdscr, 0, version_x, version_label[: max(0, w - version_x - 1)])
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
    panel_title = t("locations_panel")
    if current_shopify_location and current_shopify_location.get("name"):
        panel_title = t("locations_panel_with_name", value=current_shopify_location["name"])

    draw_panel(
        right_win,
        panel_title,
        right_lines,
        right_selected if location_rows else 0,
        right_top_index,
        active_pane == "right",
    )

    stdscr.attrset(curses.color_pair(3))

    if _shopify_location_switch_enabled():
        status_key = "status_secondary_multi" if show_secondary_help else "status_primary_multi"
    else:
        status_key = "status_secondary_single" if show_secondary_help else "status_primary_single"
    status = t(status_key)
    focus = t("focus_items") if active_pane == "left" else t("focus_locations")
    if current_shopify_location and current_shopify_location.get("name"):
        focus += t("focus_shopify_location", value=current_shopify_location["name"])
    if external_mode == "only":
        focus = focus[:-1] + t("view_external")

    try:
        stdscr.addstr(h-2, 0, " " * max(0, w - 1))
        if notice_text:
            stdscr.addstr(h-2, 0, notice_text[: max(0, w - 1)])
        elif filter_text:
            stdscr.addstr(h-2, 0, t("filter_prefix", value=filter_text)[: max(0, w - 1)])
        else:
            stdscr.addstr(h-2, 0, focus[: max(0, w - 1)])
    except curses.error:
        pass
    sync_label = sync_status_label or "Sync: -"
    sync_x = max(0, w - len(sync_label) - 1)
    if sync_x > 4:
        try:
            stdscr.addstr(h-2, sync_x, sync_label[: max(0, w - sync_x - 1)])
        except curses.error:
            pass

    draw_footer_line(stdscr, h - 1, 0, w - 1, status)

    stdscr.refresh()


def _compact_item_write_updates(*, qty=_UNSET, regal=_UNSET, fach=_UNSET, platz=_UNSET):
    updates = {}
    if qty is not _UNSET:
        updates["qty"] = int(qty)
    if regal is not _UNSET:
        updates["regal"] = regal
    if fach is not _UNSET:
        updates["fach"] = fach
    if platz is not _UNSET:
        updates["platz"] = platz
    return updates


def _describe_item_write_updates(updates):
    has_qty = "qty" in (updates or {})
    has_location = any(key in (updates or {}) for key in ("regal", "fach", "platz"))
    if has_qty and has_location:
        return "item"
    if has_qty:
        return "qty"
    if has_location:
        return "location"
    return "item"


def _enqueue_item_write_state(state_map, sku, updates):
    state = state_map.setdefault(sku, {"pending": {}, "running": False})
    merged = bool(state["pending"]) or bool(state["running"])
    state["pending"].update(dict(updates or {}))
    start_worker = not state["running"]
    if start_worker:
        state["running"] = True
    return {
        "merged": merged,
        "start_worker": start_worker,
        "pending": dict(state["pending"]),
        "open_count": len(state_map),
    }


def _pending_item_write_count(state_map=None):
    target = _PENDING_ITEM_WRITES if state_map is None else state_map
    return len(target)


def _pending_item_write_skus(state_map=None):
    target = _PENDING_ITEM_WRITES if state_map is None else state_map
    values = []
    for key in target.keys():
        if isinstance(key, tuple):
            location_id, sku = key
            values.append(f"{sku}@{location_id.rsplit('/', 1)[-1]}" if location_id else sku)
        else:
            values.append(str(key))
    return sorted(values)


def _refresh_single_item_totals(cur, sku):
    cur.execute(
        """
        WITH totals AS (
            SELECT
                sku,
                COALESCE(SUM(menge), 0) AS menge,
                COALESCE(SUM(available), 0) AS available,
                COALESCE(SUM(reserved), 0) AS reserved,
                COALESCE(SUM(committed), 0) AS committed,
                COALESCE(SUM(unavailable), 0) AS unavailable,
                BOOL_OR(dirty) AS dirty
            FROM item_location_inventory
            WHERE sku = %s
            GROUP BY sku
        )
        UPDATE items
        SET menge = totals.menge,
            available = totals.available,
            reserved = totals.reserved,
            committed = totals.committed,
            unavailable = totals.unavailable,
            dirty = totals.dirty,
            updated_at = NOW()
        FROM totals
        WHERE items.sku = totals.sku
        """,
        (sku,),
    )


def _apply_item_write_db(sku, updates, location_id=None):
    if not updates:
        return
    con = db()
    cur = con.cursor()
    try:
        target_location_id = (location_id or "").strip()
        if target_location_id:
            cur.execute(
                """
                INSERT INTO shopify_locations(location_id, name, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (location_id)
                DO UPDATE SET updated_at = NOW()
                """,
                (target_location_id, target_location_id.rsplit("/", 1)[-1]),
            )
            cur.execute(
                """
                INSERT INTO item_location_inventory (
                    sku,
                    location_id,
                    regal,
                    fach,
                    platz,
                    menge,
                    available,
                    reserved,
                    committed,
                    unavailable,
                    dirty,
                    updated_at
                )
                SELECT
                    sku,
                    %s,
                    regal,
                    fach,
                    platz,
                    menge,
                    available,
                    COALESCE(reserved, 0),
                    COALESCE(committed, 0),
                    COALESCE(unavailable, COALESCE(reserved, 0)),
                    dirty,
                    NOW()
                FROM items
                WHERE sku = %s
                ON CONFLICT (sku, location_id) DO NOTHING
                """,
                (target_location_id, sku),
            )

            set_parts = []
            params = []
            if "qty" in updates:
                qty = int(updates["qty"])
                set_parts.append("menge=%s")
                params.append(qty)
                set_parts.append(
                    "available=GREATEST(%s - COALESCE(unavailable, COALESCE(reserved, 0)) - COALESCE(committed, 0), 0)"
                )
                params.append(qty)
                set_parts.append("dirty=TRUE")
            if "regal" in updates:
                set_parts.append("regal=%s")
                params.append(updates["regal"])
            if "fach" in updates:
                set_parts.append("fach=%s")
                params.append(updates["fach"])
            if "platz" in updates:
                set_parts.append("platz=%s")
                params.append(updates["platz"])
            set_parts.append("updated_at=NOW()")
            params.extend([sku, target_location_id])
            cur.execute(
                f"UPDATE item_location_inventory SET {', '.join(set_parts)} WHERE sku=%s AND location_id=%s",
                tuple(params),
            )
            _refresh_single_item_totals(cur, sku)
            con.commit()
            return
        set_parts = []
        params = []
        if "qty" in updates:
            qty = int(updates["qty"])
            set_parts.append("menge=%s")
            params.append(qty)
            set_parts.append(
                "available=GREATEST(%s - COALESCE(unavailable, COALESCE(reserved, 0)) - COALESCE(committed, 0), 0)"
            )
            params.append(qty)
            set_parts.append("dirty=TRUE")
        if "regal" in updates:
            set_parts.append("regal=%s")
            params.append(updates["regal"])
        if "fach" in updates:
            set_parts.append("fach=%s")
            params.append(updates["fach"])
        if "platz" in updates:
            set_parts.append("platz=%s")
            params.append(updates["platz"])
        set_parts.append("updated_at=NOW()")
        params.append(sku)
        cur.execute(
            f"UPDATE items SET {', '.join(set_parts)} WHERE sku=%s",
            tuple(params),
        )
        con.commit()
    finally:
        cur.close()
        con.close()


def _post_background_ui_event(event_type, message, error=None):
    _BACKGROUND_UI_EVENTS.put(
        {
            "type": event_type,
            "message": (message or "").strip(),
            "error": error,
        }
    )


def _run_item_write_worker(sku):
    while True:
        with _PENDING_ITEM_WRITES_LOCK:
            state = _PENDING_ITEM_WRITES.get(sku)
            if not state:
                return
            updates = dict(state.get("pending") or {})
            state["pending"].clear()

        if not updates:
            with _PENDING_ITEM_WRITES_LOCK:
                state = _PENDING_ITEM_WRITES.get(sku)
                if state and not state.get("pending"):
                    _PENDING_ITEM_WRITES.pop(sku, None)
                    return
            continue

        action_key = _describe_item_write_updates(updates)
        action_label = t(f"item_write_{action_key}")
        LOGGER.info("DB-Schreibaktion gestartet sku=%s typ=%s updates=%s", sku, action_key, updates)
        try:
            if isinstance(sku, tuple):
                target_location_id, target_sku = sku
            else:
                target_location_id, target_sku = None, sku
            _apply_item_write_db(target_sku, updates, target_location_id)
        except Exception as exc:
            LOGGER.exception("DB-Schreibaktion fehlgeschlagen sku=%s typ=%s updates=%s", sku, action_key, updates)
            _post_background_ui_event(
                "items_reload",
                t("item_write_failed", label=action_label, sku=(target_sku if 'target_sku' in locals() else sku)),
                error=exc,
            )
        else:
            LOGGER.info("DB-Schreibaktion abgeschlossen sku=%s typ=%s updates=%s", sku, action_key, updates)
            _post_background_ui_event(
                "items_reload",
                t("item_write_saved", label=action_label, sku=(target_sku if 'target_sku' in locals() else sku)),
                error=None,
            )

        with _PENDING_ITEM_WRITES_LOCK:
            state = _PENDING_ITEM_WRITES.get(sku)
            if not state:
                return
            if state.get("pending"):
                LOGGER.info("DB-Schreibaktion zusammengefuehrt sku=%s naechste_updates=%s", sku, state["pending"])
                continue
            _PENDING_ITEM_WRITES.pop(sku, None)
            return


def queue_item_write(sku, *, qty=_UNSET, regal=_UNSET, fach=_UNSET, platz=_UNSET, location_id=None):
    updates = _compact_item_write_updates(qty=qty, regal=regal, fach=fach, platz=platz)
    if not updates:
        return {"merged": False, "started": False, "pending": {}, "open_count": _pending_item_write_count()}
    write_key = ((location_id or "").strip(), sku)
    with _PENDING_ITEM_WRITES_LOCK:
        result = _enqueue_item_write_state(_PENDING_ITEM_WRITES, write_key, updates)
    LOGGER.info(
        "DB-Schreibaktion vorgemerkt sku=%s merged=%s started=%s offene=%s updates=%s",
        sku,
        result["merged"],
        result["start_worker"],
        result["open_count"],
        updates,
    )
    if result["start_worker"]:
        thread = threading.Thread(target=_run_item_write_worker, args=(write_key,), daemon=True)
        thread.start()
    return {
        "merged": result["merged"],
        "started": result["start_worker"],
        "pending": result["pending"],
        "open_count": result["open_count"],
    }


def poll_background_ui_events():
    events = []
    while True:
        try:
            events.append(_BACKGROUND_UI_EVENTS.get_nowait())
        except queue.Empty:
            break
    return events


def _apply_item_local_quantity(row, qty):
    if row is None:
        return None
    old_qty = int(row.get("menge") or 0)
    unavailable = int(row.get("unavailable") or 0)
    committed = int(row.get("committed") or 0)
    row["menge"] = qty
    if "gesamt_menge" in row and row.get("gesamt_menge") is not None:
        row["gesamt_menge"] = max(int(row.get("gesamt_menge") or 0) + (qty - old_qty), 0)
    row["available"] = max(qty - unavailable - committed, 0)
    row["dirty"] = True
    return row


def _update_item_snapshot_quantity(rows, sku, qty):
    for row in rows or []:
        if row.get("sku") != sku:
            continue
        return _apply_item_local_quantity(row, qty)
    return None


def pending_item_write_exit_dialog(stdscr):
    count = _pending_item_write_count()
    if count <= 0:
        return "exit"
    options = [
        {"value": "wait", "label": t("pending_writes_wait_label", count=count)},
        {"value": "force", "label": t("pending_writes_force_label")},
        {"value": "cancel", "label": t("back")},
    ]
    return choice_dialog(stdscr, t("pending_writes_title"), options, "wait", cancel_returns_none=True) or "cancel"


def wait_for_pending_item_writes_dialog(stdscr):
    spinner = ["|", "/", "-", "\\"]
    frame = 0
    while True:
        count = _pending_item_write_count()
        if count <= 0:
            return True
        skus = _pending_item_write_skus()[:5]
        lines = [
            t("pending_writes_count", count=count),
            t("pending_writes_auto_exit"),
        ]
        if skus:
            lines.append(t("pending_writes_skus", skus=", ".join(skus)))

        h, w = stdscr.getmaxyx()
        width = min(84, w - 4)
        height = min(8 + len(lines), h - 2)
        y = max(1, (h - height) // 2)
        x = max(2, (w - width) // 2)

        draw_shadow(stdscr, y, x, height, width)
        win = curses.newwin(height, width, y, x)
        win.bkgd(" ", curses.color_pair(1))
        win.box()
        win.addstr(0, 2, f" {t('pending_writes_title')} ")
        for index, line in enumerate(lines):
            _safe_addstr(win, 2 + index, 2, _fit(line, width - 4))
        _safe_addstr(win, height - 2, 2, spinner[frame % len(spinner)])
        win.refresh()
        frame += 1
        time.sleep(0.1)


def _apply_item_local_location(row, regal, fach, platz):
    if row is None:
        return None
    row["regal"] = regal
    row["fach"] = fach
    row["platz"] = platz
    row["dirty"] = True
    return row


def _update_item_snapshot_location(rows, sku, regal, fach, platz):
    for row in rows or []:
        if row.get("sku") != sku:
            continue
        return _apply_item_local_location(row, regal, fach, platz)
    return None


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


def run_background_action_dialog(stdscr, title, action, detail="Bitte warten..."):
    state = {"done": False, "result": None, "error": None}

    def runner():
        try:
            state["result"] = action()
        except Exception as exc:
            state["error"] = exc
        finally:
            state["done"] = True

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    spinner = ["|", "/", "-", "\\"]
    frame = 0

    while not state["done"]:
        h, w = stdscr.getmaxyx()
        width = min(64, w - 4)
        height = 6
        y = h // 2 - height // 2
        x = w // 2 - width // 2

        draw_shadow(stdscr, y, x, height, width)
        win = curses.newwin(height, width, y, x)
        win.bkgd(" ", curses.color_pair(1))
        win.box()
        win.addstr(0, 2, f" {title} ")
        win.addstr(2, 2, _fit(detail, width - 6))
        win.addstr(4, 2, f"{spinner[frame % len(spinner)]}".ljust(width - 4))
        win.refresh()
        frame += 1
        time.sleep(0.08)

    if state["error"] is not None:
        raise state["error"]
    return state["result"]


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


def _save_settings_checked(updated):
    try:
        con = psycopg2.connect(
            host=updated["db_host"],
            port=int(updated.get("db_port", 5432)),
            dbname=updated["db_name"],
            user=updated["db_user"],
            password=updated["db_pass"],
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
    except psycopg2.OperationalError as exc:
        raise DatabaseUnavailableError(_summarize_db_error(exc)) from exc
    con.close()
    return save_settings(updated)

def form_dialog(
    stdscr,
    title,
    fields,
    initial_active=0,
    footer_text=None,
    extra_actions=None,
    field_validators=None,
    field_normalizers=None,
):

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
    footer = footer_text or "Enter weiter/speichern  ↑↓ wechseln  F9 Abbrechen"
    extra_actions = extra_actions or []
    field_validators = field_validators or {}
    field_normalizers = field_normalizers or {}

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
            is_active = i == active
            if is_active:
                win.attrset(curses.color_pair(2))
            else:
                win.attrset(curses.color_pair(1))
            win.addstr(row, 2, f"{label}: ")

            xpos = len(label) + 4

            if is_active:
                field_width = max(1, width - xpos - 2)
                normalize_view(i, field_width)
                visible = val[scroll_offsets[i]: scroll_offsets[i] + field_width]
            else:
                visible = val[-(width - xpos - 2):]

            win.addstr(row, xpos, visible.ljust(width - xpos - 2))
            win.attrset(curses.color_pair(1))

        draw_footer_line(win, height - 2, 2, width - 4, footer)
        
        cursor_y = 2 + active

        label = fields[active]["label"]
        val = values[active]

        xpos = len(label) + 4
        field_width = max(1, width - xpos - 2)
        normalize_view(active, field_width)
        cursor_x = xpos + min(max(0, cursor_positions[active] - scroll_offsets[active]), field_width - 1)

        win.move(cursor_y, cursor_x)

        win.refresh()


        win.timeout(200)
        try:
            key = win.get_wch()
        except curses.error:
            continue
        finally:
            win.timeout(-1)

        if key in (27, curses.KEY_F9):
            return None

        for action in extra_actions:
            if key in action["keys"]:
                return {
                    "__action__": action["name"],
                    "__values__": {fields[i]["name"]: values[i] for i in range(len(fields))},
                    "__active__": active,
                }

        if key in (10, 13, "\n", "\r", curses.KEY_ENTER):
            if active >= len(fields) - 1:
                return {fields[i]["name"]: values[i] for i in range(len(fields))}
            active = (active + 1) % len(fields)
            continue

        if key == curses.KEY_DOWN:
            active = (active + 1) % len(fields)
            continue

        if key == curses.KEY_UP:
            active = (active - 1) % len(fields)
            continue

        if key in (curses.KEY_BACKSPACE, 127, 8, '\x7f', '\b'):
            pos = cursor_positions[active]
            if pos > 0:
                field_name = fields[active]["name"]
                candidate = values[active][:pos - 1] + values[active][pos:]
                normalizer = field_normalizers.get(field_name)
                if normalizer:
                    candidate = normalizer(candidate)
                values[active] = candidate
                cursor_positions[active] = pos - 1
            continue

        if key == curses.KEY_DC:
            pos = cursor_positions[active]
            if pos < len(values[active]):
                field_name = fields[active]["name"]
                candidate = values[active][:pos] + values[active][pos + 1:]
                normalizer = field_normalizers.get(field_name)
                if normalizer:
                    candidate = normalizer(candidate)
                values[active] = candidate
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
                normalizer = field_normalizers.get(field_name)
                if normalizer:
                    candidate = normalizer(candidate)
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
        message_box(stdscr, t("formats_title"), error[:56])
        return current_value
    if not options:
        message_box(stdscr, t("formats_title"), t("formats_not_found"))
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
            line = str(option.get("label") or option.get("value") or "")
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


def toggle_choice_dialog(stdscr, title, options, selected_values, footer_text=None):
    selected_set = {str(value) for value in (selected_values or [])}
    selected = 0
    top_index = 0
    while True:
        h, w = stdscr.getmaxyx()
        width = min(86, w - 4)
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
            marker = "[x]" if str(option["value"]) in selected_set else "[ ]"
            line = f"{marker} {option['label']}"
            y_pos = 2 + row_index
            if real_index == selected:
                win.attrset(curses.color_pair(2))
                win.addstr(y_pos, 1, _fit(line, width - 2).ljust(width - 2))
                win.attrset(curses.color_pair(1))
            else:
                win.addstr(y_pos, 1, _fit(line, width - 2).ljust(width - 2))

        footer = footer_text or t("shipping_services_footer")
        win.addstr(height - 2, 2, _fit(footer, width - 4))
        win.refresh()

        key = win.get_wch()
        if key in (27, curses.KEY_F9):
            return None
        if key in (10, 13, "\n", "\r", curses.KEY_ENTER):
            return [option["value"] for option in options if str(option["value"]) in selected_set]
        if key == curses.KEY_DOWN:
            selected = move_selection(options, selected, 1)
            continue
        if key == curses.KEY_UP:
            selected = move_selection(options, selected, -1)
            continue
        if key == " ":
            value = str(options[selected]["value"])
            if value in selected_set:
                selected_set.remove(value)
            else:
                selected_set.add(value)


def _post_base_product_options(scope="domestic"):
    options = []
    for group in list_post_base_products(scope=scope):
        label = group.get("base_label") or group.get("base_key") or "-"
        prices = [item.get("price_cents") for item in group.get("untracked_variants", []) + group.get("tracked_variants", []) if item.get("price_cents") is not None]
        if prices:
            min_price = min(prices) / 100.0
            label = f"{label} ab {min_price:.2f} EUR"
        options.append({"value": group["base_key"], "label": label})
    return options


def _post_group_for_base_key(base_key, scope="domestic"):
    for group in list_post_base_products(scope=scope):
        if group.get("base_key") == base_key:
            return group
    return None


def post_product_dialog(stdscr, current_selection=None, scope="domestic"):
    current_selection = current_selection or {}
    base_key = str(current_selection.get("base_key") or "").strip()
    options = _post_base_product_options(scope=scope)
    if not options:
        message_box(stdscr, t("post_title"), t("post_products_unavailable"))
        return None
    if not base_key:
        base_key = options[0]["value"]

    while True:
        chosen_base = choice_dialog(
            stdscr,
            t("post_base_product_title"),
            options,
            base_key,
            cancel_returns_none=True,
        )
        if chosen_base is None:
            return None
        group = _post_group_for_base_key(chosen_base, scope=scope)
        if not group:
            message_box(stdscr, t("post_title"), t("post_base_product_not_found"))
            return None

        selected_option_codes = _normalize_post_option_codes(current_selection.get("option_codes") or [])
        available_option_codes = group.get("option_codes") or []
        selected_option_codes = [code for code in selected_option_codes if code in available_option_codes]
        if available_option_codes:
            option_items = []
            option_label_map = {}
            for variant in group.get("untracked_variants", []) + group.get("tracked_variants", []):
                for code, label in zip(variant.get("addons") or [], variant.get("addon_labels") or []):
                    option_label_map.setdefault(code, label)
            for code in available_option_codes:
                option_label = option_label_map.get(code, code)
                matching_prices = [
                    (variant.get("price_cents") or 0) / 100.0
                    for variant in group.get("untracked_variants", []) + group.get("tracked_variants", [])
                    if code in (variant.get("addons") or [])
                ]
                if matching_prices:
                    option_label = f"{option_label} ab {min(matching_prices):.2f} EUR"
                option_items.append({"value": code, "label": option_label})
            toggled = toggle_choice_dialog(
                stdscr,
                t("post_options_title", label=group.get("base_label") or "-"),
                option_items,
                selected_option_codes,
                footer_text=t("toggle_footer_continue"),
            )
            if toggled is None:
                base_key = chosen_base
                continue
            selected_option_codes = _normalize_post_option_codes(toggled)
        else:
            selected_option_codes = []

        try:
            product = _resolve_post_product_selection(
                {
                    "scope": scope,
                    "base_key": chosen_base,
                    "option_codes": selected_option_codes,
                }
            )
        except Exception as exc:
            message_box(stdscr, t("post_title"), str(exc)[:56])
            base_key = chosen_base
            continue

        return {
            "scope": scope,
            "base_key": chosen_base,
            "option_codes": selected_option_codes,
            "product_code": product["product_code"],
            "selection_label": product["selection_label"],
            "name": product["name"],
            "price_eur": product["price_eur"],
        }


def _shipping_printer_tab_fields():
    fields = [
        ("picklist_printer", "field_picklist_printer"),
        ("delivery_note_printer", "field_delivery_printer"),
        ("delivery_note_format", "field_delivery_format"),
        ("delivery_note_scale_mode", "field_delivery_scale_mode"),
        ("delivery_note_duplex", "field_delivery_duplex"),
        ("delivery_note_color_mode", "field_delivery_color_mode"),
    ]
    for code in _configurable_shipping_carrier_codes():
        definition = _shipping_carrier_definition(code)
        printer_field = definition.get("printer_field")
        printer_label_key = definition.get("printer_field_label_key")
        if printer_field and printer_label_key:
            fields.append((printer_field, printer_label_key))
    fields.append(("shipping_label_printer", "field_shipping_printer_fallback"))
    return fields


def _shipping_settings_tab_fields():
    fields = [
        ("shipping_label_output_dir", "field_shipping_label_output_dir"),
        ("shipping_packaging_weight_grams", "field_shipping_packaging_weight"),
        ("shipping_active_carriers_display", "field_shipping_active_carriers"),
    ]
    for code in _configurable_shipping_carrier_codes():
        definition = _shipping_carrier_definition(code)
        fields.append((f"_heading_{code}", definition.get("label") or code.upper()))
        format_field = definition.get("format_field")
        format_label_key = definition.get("format_field_label_key")
        if format_field and format_label_key:
            fields.append((format_field, format_label_key))
        scale_field = definition.get("scale_field")
        scale_label_key = definition.get("scale_field_label_key")
        if scale_field and scale_label_key:
            fields.append((scale_field, scale_label_key))
        template_field = definition.get("template_field")
        template_label_key = definition.get("template_field_label_key")
        if template_field and template_label_key:
            fields.append((template_field, template_label_key))
        tracking_mode_field = definition.get("tracking_mode_field")
        tracking_url_field = definition.get("tracking_url_field")
        if tracking_mode_field:
            fields.append((tracking_mode_field, f"field_{tracking_mode_field}"))
        if tracking_url_field:
            fields.append((tracking_url_field, f"field_{tracking_url_field}"))
        fields.extend(definition.get("extra_settings_fields") or [])
    return fields


def _shipping_settings_initial_values():
    values = {
        "shipping_active_carriers": _normalize_active_shipping_carriers(
            SETTINGS.get("shipping_active_carriers", DEFAULT_ACTIVE_SHIPPING_CARRIERS)
        ),
        "shipping_active_carriers_display": _shipping_active_carriers_summary(
            SETTINGS.get("shipping_active_carriers", DEFAULT_ACTIVE_SHIPPING_CARRIERS)
        ),
        "shipping_label_printer": SETTINGS.get("shipping_label_printer", ""),
        "shipping_label_output_dir": SETTINGS.get("shipping_label_output_dir", ""),
        "shipping_label_format": (SETTINGS.get("shipping_label_format") or "A6").strip().upper(),
        "shipping_services": _normalize_shipping_services(SETTINGS.get("shipping_services", [])),
        "shipping_services_display": _shipping_services_summary(SETTINGS.get("shipping_services", [])),
        "shipping_packaging_weight_grams": str(
            SETTINGS.get("shipping_packaging_weight_grams", DEFAULT_SETTINGS.get("shipping_packaging_weight_grams", 400))
        ),
    }
    for code in _configurable_shipping_carrier_codes():
        definition = _shipping_carrier_definition(code)
        printer_field = definition.get("printer_field")
        if printer_field:
            values[printer_field] = SETTINGS.get(printer_field, "")
        format_field = definition.get("format_field")
        if format_field:
            values[format_field] = _normalize_shipping_label_format(
                SETTINGS.get(format_field, definition.get("default_format", "A6"))
            )
        scale_field = definition.get("scale_field")
        if scale_field:
            values[scale_field] = _normalize_scale_mode(
                SETTINGS.get(scale_field, SETTINGS.get("shipping_label_scale_mode", "none"))
            )
        tracking_mode_field = definition.get("tracking_mode_field")
        if tracking_mode_field:
            values[tracking_mode_field] = (
                SETTINGS.get(tracking_mode_field)
                or DEFAULT_SETTINGS.get(tracking_mode_field, "company")
            ).strip().lower()
        tracking_url_field = definition.get("tracking_url_field")
        if tracking_url_field:
            values[tracking_url_field] = SETTINGS.get(tracking_url_field, "")
        template_field = definition.get("template_field")
        if template_field:
            values[template_field] = SETTINGS.get(template_field, "")
        for field_name, _label_key in definition.get("extra_settings_fields") or []:
            if field_name == "shipping_services_display":
                continue
            values[field_name] = SETTINGS.get(field_name, "")
    return values


def _settings_print_test_context(active_name, values, shipping_printer_fields, shipping_format_fields, shipping_scale_fields):
    if active_name == "picklist_printer":
        printer = (values.get("picklist_printer") or "").strip()
        return {
            "printer": printer,
            "page_size": "A4",
            "title": t("print_test_picklist_title"),
            "lines": [
                t("print_test_line_printer", value=printer or "-"),
                t("print_test_line_type", value=t("print_test_type_picklist")),
                t("print_test_line_time", value=f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}"),
            ],
        }
    if active_name in {"delivery_note_printer", "delivery_note_format", "delivery_note_scale_mode", "delivery_note_duplex", "delivery_note_color_mode"}:
        printer = (values.get("delivery_note_printer") or "").strip()
        page_size = _normalize_shipping_label_format(values.get("delivery_note_format", "A4"))
        scale_mode = _normalize_scale_mode(values.get("delivery_note_scale_mode", "none"))
        duplex_mode = _normalize_duplex_mode(values.get("delivery_note_duplex", "off"))
        color_mode = _normalize_color_mode(values.get("delivery_note_color_mode", "color"))
        return {
            "printer": printer,
            "page_size": page_size,
            "title": t("print_test_delivery_note_title"),
            "lines": [
                t("print_test_line_printer", value=printer or "-"),
                t("print_test_line_format", value=page_size),
                t("print_test_line_scale_mode", value=t(f"print_scale_mode_{scale_mode}")),
                t("print_test_line_duplex", value=t(f"delivery_note_duplex_{duplex_mode}")),
                t("print_test_line_color_mode", value=t(f"delivery_note_color_mode_{color_mode}")),
                t("print_test_line_type", value=t("print_test_type_delivery_note")),
            ],
        }
    if active_name == "shipping_label_printer":
        printer = (values.get("shipping_label_printer") or "").strip()
        page_size = _normalize_shipping_label_format(values.get("shipping_label_format", "A6"))
        scale_mode = _normalize_scale_mode(values.get("shipping_label_scale_mode", "none"))
        return {
            "printer": printer,
            "page_size": page_size,
            "title": t("print_test_shipping_label_title"),
            "lines": [
                t("print_test_line_printer", value=printer or "-"),
                t("print_test_line_format", value=page_size),
                t("print_test_line_scale_mode", value=t(f"print_scale_mode_{scale_mode}")),
                t("print_test_line_type", value=t("print_test_type_shipping_label_fallback")),
            ],
        }
    if active_name in {"shipping_label_format", *shipping_printer_fields.keys(), *shipping_format_fields.keys(), *shipping_scale_fields.keys()}:
        carrier_code = (
            shipping_printer_fields.get(active_name)
            or shipping_format_fields.get(active_name)
            or shipping_scale_fields.get(active_name)
        )
        if not carrier_code:
            return None
        printer_field = _shipping_carrier_setting_field(carrier_code, "printer")
        format_field = _shipping_carrier_setting_field(carrier_code, "format")
        scale_field = _shipping_carrier_setting_field(carrier_code, "scale")
        printer = (values.get(printer_field) or values.get("shipping_label_printer") or "").strip()
        page_size = _normalize_shipping_label_format(
            values.get(format_field) or values.get("shipping_label_format") or "A6"
        )
        scale_mode = _normalize_scale_mode(values.get(scale_field) or values.get("shipping_label_scale_mode", "none"))
        return {
            "printer": printer,
            "page_size": page_size,
            "title": t("print_test_carrier_title", carrier=_shipping_carrier_label(carrier_code)),
            "lines": [
                t("print_test_line_printer", value=printer or "-"),
                t("print_test_line_format", value=page_size),
                t("print_test_line_scale_mode", value=t(f"print_scale_mode_{scale_mode}")),
                t("print_test_line_type", value=_shipping_carrier_label(carrier_code)),
            ],
        }
    return None


def _settings_context_select(
    stdscr,
    active_name,
    values,
    shipping_printer_fields,
    shipping_format_fields,
    shipping_scale_fields,
    shipping_template_fields,
    shipping_tracking_mode_fields,
):
    if active_name == "language":
        values["language"] = choice_dialog(stdscr, t("pick_language"), get_language_options(), values["language"])
    elif active_name == "shopify_location_mode":
        values["shopify_location_mode"] = choice_dialog(
            stdscr,
            t("pick_shopify_location_mode"),
            [
                {"value": "single", "label": t("shopify_location_mode_single")},
                {"value": "multi", "label": t("shopify_location_mode_multi")},
            ],
            values.get("shopify_location_mode", "single"),
        )
    elif active_name == "shopify_active_location_display":
        locations = get_shopify_locations_snapshot()
        if not locations:
            message_box(stdscr, t("settings_title"), t("shopify_locations_unavailable"))
            return values
        options = [
            {
                "value": row["location_id"],
                "label": f"{row.get('name') or row['location_id']} [{row['location_id']}]",
            }
            for row in locations
            if row.get("location_id")
        ]
        chosen = choice_dialog(
            stdscr,
            t("pick_shopify_location"),
            options,
            values.get("shopify_active_location_id", ""),
            cancel_returns_none=True,
        )
        if chosen is not None:
            values["shopify_active_location_id"] = chosen
            selected_row = next((row for row in locations if row.get("location_id") == chosen), None)
            values["shopify_active_location_display"] = (selected_row.get("name") if selected_row else chosen) or chosen
    elif active_name == "color_theme":
        values["color_theme"] = choice_dialog(stdscr, t("pick_theme"), get_theme_options(), values["color_theme"])
    elif active_name in {"location_regex_regal", "location_regex_fach", "location_regex_platz"}:
        values = _location_regex_options_dialog(stdscr, active_name.rsplit("_", 1)[-1], values)
    elif active_name in {
        "picklist_printer",
        "delivery_note_printer",
        "shipping_label_printer",
        *shipping_printer_fields.keys(),
    }:
        values[active_name] = cups_printer_dialog(stdscr, values[active_name])
    elif active_name in shipping_format_fields:
        carrier_code = shipping_format_fields[active_name]
        printer_field = _shipping_carrier_setting_field(carrier_code, "printer")
        printer_name = values.get(printer_field, "") or values.get("shipping_label_printer")
        values[active_name] = cups_media_dialog(
            stdscr,
            printer_name,
            values[active_name],
            f"{_shipping_carrier_label(carrier_code)} {t('formats_title')}",
        )
    elif active_name == "delivery_note_format":
        values[active_name] = cups_media_dialog(
            stdscr,
            values.get("delivery_note_printer", ""),
            values[active_name],
            f"{t('delivery_note_title')} {t('formats_title')}",
        )
    elif active_name == "shipping_label_scale_mode":
        values[active_name] = choice_dialog(
            stdscr,
            t("pick_print_scale_mode"),
            [
                {"value": "none", "label": t("print_scale_mode_none")},
                {"value": "fit", "label": t("print_scale_mode_fit")},
            ],
            _normalize_scale_mode(values.get(active_name, "none")),
        )
    elif active_name in shipping_scale_fields:
        values[active_name] = choice_dialog(
            stdscr,
            t("pick_print_scale_mode"),
            [
                {"value": "none", "label": t("print_scale_mode_none")},
                {"value": "fit", "label": t("print_scale_mode_fit")},
            ],
            _normalize_scale_mode(values.get(active_name, "none")),
        )
    elif active_name == "delivery_note_scale_mode":
        values[active_name] = choice_dialog(
            stdscr,
            t("pick_print_scale_mode"),
            [
                {"value": "none", "label": t("print_scale_mode_none")},
                {"value": "fit", "label": t("print_scale_mode_fit")},
            ],
            _normalize_scale_mode(values.get(active_name, "none")),
        )
    elif active_name == "delivery_note_duplex":
        values[active_name] = choice_dialog(
            stdscr,
            t("pick_delivery_note_duplex"),
            [
                {"value": "off", "label": t("delivery_note_duplex_off")},
                {"value": "long", "label": t("delivery_note_duplex_long")},
                {"value": "short", "label": t("delivery_note_duplex_short")},
            ],
            _normalize_duplex_mode(values.get(active_name, "off")),
        )
    elif active_name == "delivery_note_color_mode":
        values[active_name] = choice_dialog(
            stdscr,
            t("pick_delivery_note_color_mode"),
            [
                {"value": "color", "label": t("delivery_note_color_mode_color")},
                {"value": "grayscale", "label": t("delivery_note_color_mode_grayscale")},
            ],
            _normalize_color_mode(values.get(active_name, "color")),
        )
    elif active_name in {"pdf_output_dir", "shipping_label_output_dir"}:
        values[active_name] = directory_dialog(stdscr, values.get(active_name, ""), t("directory_choose_title"))
    elif active_name == "color_theme_file":
        values[active_name] = file_dialog(stdscr, values.get(active_name, ""), t("theme_file_choose_title"), extensions={".json"})
    elif active_name in {"label_font_regular", "label_font_condensed"}:
        values[active_name] = file_dialog(stdscr, values.get(active_name, ""), t("font_choose_title"), extensions={".ttf", ".otf"})
    elif active_name == "delivery_note_template_path":
        values[active_name] = file_dialog(stdscr, values.get(active_name, ""), t("template_choose_title"), extensions={".pdf", ".html", ".htm"})
    elif active_name in shipping_template_fields:
        carrier_code = shipping_template_fields[active_name]
        values[active_name] = file_dialog(
            stdscr,
            values.get(active_name, ""),
            f"{_shipping_carrier_label(carrier_code)} Vorlage waehlen",
            extensions={".html", ".htm"},
        )
    elif active_name == "delivery_note_logo_source":
        current_value = (values.get(active_name) or "").strip()
        if not is_http_url(current_value):
            values[active_name] = file_dialog(
                stdscr,
                current_value,
                t("logo_choose_title"),
                extensions={".png", ".jpg", ".jpeg", ".svg", ".pdf"},
            )
    elif active_name in shipping_tracking_mode_fields:
        values[active_name] = choice_dialog(
            stdscr,
            t("shipping_tracking_title"),
            [
                {"value": "company", "label": t("shipping_tracking_mode_company")},
                {"value": "company_and_url", "label": t("shipping_tracking_mode_company_and_url")},
            ],
            values[active_name],
        )
    elif active_name == "shipping_services_display":
        values["shipping_services"] = shipping_services_dialog(stdscr, values.get("shipping_services", []))
        values["shipping_services_display"] = _shipping_services_summary(values["shipping_services"])
    elif active_name == "shipping_active_carriers_display":
        chosen = toggle_choice_dialog(
            stdscr,
            t("shipping_active_carriers_title"),
            _shipping_carrier_options(include_test=True),
            values.get("shipping_active_carriers", []),
        )
        if chosen is not None:
            values["shipping_active_carriers"] = _normalize_active_shipping_carriers(
                chosen,
                fallback_to_defaults=False,
            )
            values["shipping_active_carriers_display"] = _shipping_active_carriers_summary(
                values["shipping_active_carriers"],
                fallback_to_defaults=False,
            )
    return values


def settings_dialog(stdscr):
    global SETTINGS

    shipping_printer_fields = _shipping_printer_field_map()
    shipping_format_fields = _shipping_format_field_map()
    shipping_scale_fields = _shipping_scale_field_map()
    shipping_template_fields = _shipping_template_field_map()
    shipping_tracking_mode_fields = _shipping_tracking_mode_field_map()

    values = {
        "db_host": SETTINGS["db_host"],
        "db_port": str(SETTINGS.get("db_port", DEFAULT_SETTINGS["db_port"])),
        "db_name": SETTINGS["db_name"],
        "db_user": SETTINGS["db_user"],
        "db_pass": SETTINGS["db_pass"],
        "language": (SETTINGS.get("language") or DEFAULT_SETTINGS["language"]).strip().lower(),
        "shopify_location_mode": (SETTINGS.get("shopify_location_mode") or DEFAULT_SETTINGS["shopify_location_mode"]).strip().lower(),
        "shopify_active_location_id": (SETTINGS.get("shopify_active_location_id") or "").strip(),
        "shopify_active_location_display": (SETTINGS.get("shopify_active_location_id") or "").strip() or "-",
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
        "location_regex_ignore_case_regal": bool(
            SETTINGS.get("location_regex_ignore_case_regal", DEFAULT_SETTINGS["location_regex_ignore_case_regal"])
        ),
        "location_regex_ignore_case_fach": bool(
            SETTINGS.get("location_regex_ignore_case_fach", DEFAULT_SETTINGS["location_regex_ignore_case_fach"])
        ),
        "location_regex_ignore_case_platz": bool(
            SETTINGS.get("location_regex_ignore_case_platz", DEFAULT_SETTINGS["location_regex_ignore_case_platz"])
        ),
        "location_regex_normalize_case_regal": str(
            SETTINGS.get("location_regex_normalize_case_regal", DEFAULT_SETTINGS["location_regex_normalize_case_regal"])
        ),
        "location_regex_normalize_case_fach": str(
            SETTINGS.get("location_regex_normalize_case_fach", DEFAULT_SETTINGS["location_regex_normalize_case_fach"])
        ),
        "location_regex_normalize_case_platz": str(
            SETTINGS.get("location_regex_normalize_case_platz", DEFAULT_SETTINGS["location_regex_normalize_case_platz"])
        ),
        "picklist_printer": SETTINGS["picklist_printer"],
        "delivery_note_printer": SETTINGS["delivery_note_printer"],
        "delivery_note_format": _normalize_shipping_label_format(
            SETTINGS.get("delivery_note_format", DEFAULT_SETTINGS.get("delivery_note_format", "A4"))
        ),
        "delivery_note_scale_mode": _normalize_scale_mode(
            SETTINGS.get("delivery_note_scale_mode", DEFAULT_SETTINGS.get("delivery_note_scale_mode", "none"))
        ),
        "delivery_note_duplex": _normalize_duplex_mode(
            SETTINGS.get("delivery_note_duplex", DEFAULT_SETTINGS.get("delivery_note_duplex", "off"))
        ),
        "delivery_note_color_mode": _normalize_color_mode(
            SETTINGS.get("delivery_note_color_mode", DEFAULT_SETTINGS.get("delivery_note_color_mode", "color"))
        ),
        "pdf_output_dir": SETTINGS["pdf_output_dir"],
        "delivery_note_template_path": SETTINGS.get("delivery_note_template_path", ""),
        "delivery_note_logo_source": SETTINGS.get("delivery_note_logo_source", ""),
        "delivery_note_sender_name": SETTINGS["delivery_note_sender_name"],
        "delivery_note_sender_street": SETTINGS["delivery_note_sender_street"],
        "delivery_note_sender_city": SETTINGS["delivery_note_sender_city"],
        "delivery_note_sender_email": SETTINGS["delivery_note_sender_email"],
    }
    values.update(_shipping_settings_initial_values())
    tabs = [
        {
            "title_key": "settings_tab_general",
            "fields": [
                ("db_host", "field_db_host"),
                ("db_port", "field_db_port"),
                ("db_name", "field_db_name"),
                ("db_user", "field_db_user"),
                ("db_pass", "field_db_pass"),
                ("language", "field_language"),
                ("shopify_location_mode", "field_shopify_location_mode"),
                ("shopify_active_location_display", "field_shopify_active_location"),
                ("color_theme", "field_theme"),
                ("color_theme_file", "field_theme_file"),
            ],
        },
        {
            "title_key": "settings_tab_lagerlabel",
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
            "title_key": "settings_tab_printers",
            "fields": _shipping_printer_tab_fields(),
        },
        {
            "title_key": "settings_tab_shipping",
            "fields": _shipping_settings_tab_fields(),
        },
        {
            "title_key": "settings_tab_delivery_note",
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
    initial_locations = get_shopify_locations_snapshot()
    if initial_locations:
        selected_row = next(
            (row for row in initial_locations if row.get("location_id") == values.get("shopify_active_location_id")),
            None,
        )
        if selected_row is None:
            selected_row = initial_locations[0]
            values["shopify_active_location_id"] = selected_row.get("location_id") or ""
        values["shopify_active_location_display"] = (selected_row.get("name") or selected_row.get("location_id") or "-")
    active_tab = 0
    active_field_by_tab = [0 for _ in tabs]
    sync_state = get_service_runtime_state(max_age_seconds=999999)
    sync_status_refresh_pending = False
    last_sync_status_request_at = 0.0
    editable_field_names = {
        name
        for tab in tabs
        for name, _ in tab["fields"]
        if not str(name).startswith("_heading_")
    }
    cursor_positions = {name: len(str(values.get(name, ""))) for name in editable_field_names}
    scroll_offsets = {name: 0 for name in editable_field_names}

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
        loader_result = _SERVICE_RUNTIME_LOADER.poll()
        if loader_result and loader_result.get("key") == "service_runtime_state":
            sync_status_refresh_pending = False
            if loader_result.get("error") is None:
                sync_state = loader_result.get("value")
                last_sync_status_request_at = loader_result.get("loaded_at") or time.monotonic()
        if not sync_status_refresh_pending and (
            sync_state is None or (time.monotonic() - last_sync_status_request_at) >= SERVICE_RUNTIME_CACHE_SECONDS
        ):
            _SERVICE_RUNTIME_LOADER.request(
                "service_runtime_state",
                get_service_runtime_state,
                SHOPIFY_SYNC_SERVICE,
                SERVICE_RUNTIME_CACHE_SECONDS,
                True,
            )
            sync_status_refresh_pending = True

        tab = tabs[active_tab]
        tab_fields = tab["fields"]
        editable_indices = [idx for idx, (name, _label_key) in enumerate(tab_fields) if not str(name).startswith("_heading_")]
        if not editable_indices:
            active_index = 0
            active_name = ""
        else:
            current_pos = active_field_by_tab[active_tab]
            if current_pos >= len(editable_indices):
                current_pos = len(editable_indices) - 1
            if current_pos < 0:
                current_pos = 0
            active_field_by_tab[active_tab] = current_pos
            active_index = editable_indices[current_pos]
            active_name = tab_fields[active_index][0]
        if not tab_fields:
            active_index = 0
            active_name = ""

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
        sync_version = ((sync_state or {}).get("version") or "-").strip() or "-"
        sync_label = f" Sync {sync_version} "
        sync_x = max(2, width - len(sync_label) - 2)
        win.addstr(0, sync_x, sync_label[: max(0, width - sync_x - 1)])

        tab_x = 2
        for i, entry in enumerate(tabs):
            label = f" {t(entry['title_key'])} "
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
            if str(name).startswith("_heading_"):
                label = str(label_key)
                win.attrset(curses.color_pair(2))
                win.addstr(row, 2, _fit(f"[{label}]", width - 4))
                win.attrset(curses.color_pair(1))
            else:
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

        footer = t("settings_footer_unified")
        win.attrset(curses.color_pair(3))
        draw_footer_line(win, height - 2, 1, width - 2, footer)
        win.attrset(curses.color_pair(1))

        if active_name:
            normalize_view(active_name, field_width)
            cursor_x = field_x + min(max(0, cursor_positions[active_name] - scroll_offsets[active_name]), field_width - 1)
            win.move(3 + active_index, cursor_x)
        win.refresh()

        win.timeout(200)
        try:
            key = win.get_wch()
        except curses.error:
            continue
        finally:
            win.timeout(-1)
        if key in (27, curses.KEY_F9):
            return
        if key == curses.KEY_F2:
            break
        if key == "\t":
            active_tab = (active_tab + 1) % len(tabs)
            continue
        if key == curses.KEY_BTAB:
            active_tab = (active_tab - 1) % len(tabs)
            continue
        if key == curses.KEY_DOWN:
            if editable_indices:
                active_field_by_tab[active_tab] = (active_field_by_tab[active_tab] + 1) % len(editable_indices)
            continue
        if key == curses.KEY_UP:
            if editable_indices:
                active_field_by_tab[active_tab] = (active_field_by_tab[active_tab] - 1) % len(editable_indices)
            continue

        if not active_name:
            continue

        if active_name in {
            "shipping_services_display",
            "shipping_active_carriers_display",
            "shopify_active_location_display",
            "shopify_location_mode",
            "shipping_label_scale_mode",
            "delivery_note_scale_mode",
            "delivery_note_duplex",
            "delivery_note_color_mode",
            *shipping_scale_fields.keys(),
        }:
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

        if key == curses.KEY_F6:
            _settings_context_select(
                stdscr,
                active_name,
                values,
                shipping_printer_fields,
                shipping_format_fields,
                shipping_scale_fields,
                shipping_template_fields,
                shipping_tracking_mode_fields,
            )
            cursor_positions[active_name] = len(str(values.get(active_name, "")))
            continue

        if key == curses.KEY_F7:
            context = _settings_print_test_context(
                active_name,
                values,
                shipping_printer_fields,
                shipping_format_fields,
                shipping_scale_fields,
            )
            if context is None:
                message_box(stdscr, t("printer_test_title"), t("test_page_unavailable"))
                continue
            summary = _fit(_print_test_summary_line(context), 36)
            if not confirm_box(
                stdscr,
                t("printer_test_title"),
                t("print_test_confirm", summary=summary),
                default_yes=True,
            ):
                continue
            _print_test_page_to_printer(
                stdscr,
                context["printer"],
                context["title"],
                page_size=context["page_size"],
                lines=context["lines"],
            )
            continue

        if key in (10, 13, "\n", "\r", curses.KEY_ENTER):
            previous_value = values.get(active_name, "")
            _settings_context_select(
                stdscr,
                active_name,
                values,
                shipping_printer_fields,
                shipping_format_fields,
                shipping_scale_fields,
                shipping_template_fields,
                shipping_tracking_mode_fields,
            )
            if values.get(active_name, "") == previous_value and active_name not in {
                "language",
                "shopify_location_mode",
                "color_theme",
                "delivery_note_format",
                "shipping_label_scale_mode",
                "delivery_note_scale_mode",
                "delivery_note_duplex",
                "delivery_note_color_mode",
                "shipping_services_display",
                "shipping_active_carriers_display",
                "shopify_active_location_display",
                *shipping_printer_fields.keys(),
                *shipping_format_fields.keys(),
                *shipping_scale_fields.keys(),
                *shipping_template_fields.keys(),
                *shipping_tracking_mode_fields.keys(),
                "picklist_printer",
                "delivery_note_printer",
                "shipping_label_printer",
                "pdf_output_dir",
                "shipping_label_output_dir",
                "color_theme_file",
                "label_font_regular",
                "label_font_condensed",
                "delivery_note_template_path",
                "delivery_note_logo_source",
            }:
                if editable_indices:
                    active_field_by_tab[active_tab] = (active_field_by_tab[active_tab] + 1) % len(editable_indices)
            cursor_positions[active_name] = len(str(values.get(active_name, "")))
            continue

        if isinstance(key, str) and key.isprintable():
            if active_name in {
                "shipping_services_display",
                "shipping_active_carriers_display",
                "shopify_active_location_display",
                "shopify_location_mode",
                "shipping_label_scale_mode",
                "delivery_note_scale_mode",
                "delivery_note_duplex",
                "delivery_note_color_mode",
                *shipping_scale_fields.keys(),
            }:
                continue
            value = str(values.get(active_name, ""))
            pos = cursor_positions[active_name]
            values[active_name] = value[:pos] + key + value[pos:]
            cursor_positions[active_name] = pos + 1

    updated = {
        "db_host": values["db_host"].strip(),
        "db_port": int((values.get("db_port") or str(DEFAULT_SETTINGS["db_port"])).strip()),
        "db_name": values["db_name"].strip(),
        "db_user": values["db_user"].strip(),
        "db_pass": values["db_pass"],
        "language": values["language"].strip().lower(),
        "shopify_location_mode": values["shopify_location_mode"].strip().lower(),
        "shopify_active_location_id": values["shopify_active_location_id"].strip(),
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
        "location_regex_ignore_case_regal": bool(values.get("location_regex_ignore_case_regal", False)),
        "location_regex_ignore_case_fach": bool(values.get("location_regex_ignore_case_fach", False)),
        "location_regex_ignore_case_platz": bool(values.get("location_regex_ignore_case_platz", False)),
        "location_regex_normalize_case_regal": str(values.get("location_regex_normalize_case_regal", "none")).strip().lower(),
        "location_regex_normalize_case_fach": str(values.get("location_regex_normalize_case_fach", "none")).strip().lower(),
        "location_regex_normalize_case_platz": str(values.get("location_regex_normalize_case_platz", "none")).strip().lower(),
        "picklist_printer": values["picklist_printer"].strip(),
        "delivery_note_printer": values["delivery_note_printer"].strip(),
        "delivery_note_format": _normalize_shipping_label_format(values["delivery_note_format"].strip()),
        "delivery_note_scale_mode": _normalize_scale_mode(values.get("delivery_note_scale_mode", "none")),
        "delivery_note_duplex": _normalize_duplex_mode(values.get("delivery_note_duplex", "off")),
        "delivery_note_color_mode": _normalize_color_mode(values.get("delivery_note_color_mode", "color")),
        "shipping_active_carriers": _normalize_active_shipping_carriers(
            values.get("shipping_active_carriers", []),
            fallback_to_defaults=False,
        ),
        "shipping_label_printer": values["shipping_label_printer"].strip(),
        "shipping_label_output_dir": os.path.expanduser(values["shipping_label_output_dir"].strip()),
        "shipping_label_format": _normalize_shipping_label_format(values["shipping_label_format"].strip()),
        "shipping_label_scale_mode": _normalize_scale_mode(values.get("shipping_label_scale_mode", "none")),
        "shipping_services": _normalize_shipping_services(values.get("shipping_services", [])),
        "shipping_packaging_weight_grams": values["shipping_packaging_weight_grams"].strip(),
        "pdf_output_dir": os.path.expanduser(values["pdf_output_dir"].strip()),
        "delivery_note_template_path": os.path.expanduser(values["delivery_note_template_path"].strip()),
        "delivery_note_logo_source": values["delivery_note_logo_source"].strip(),
        "delivery_note_sender_name": values["delivery_note_sender_name"].strip(),
        "delivery_note_sender_street": values["delivery_note_sender_street"].strip(),
        "delivery_note_sender_city": values["delivery_note_sender_city"].strip(),
        "delivery_note_sender_email": values["delivery_note_sender_email"].strip(),
    }
    for code in _configurable_shipping_carrier_codes():
        definition = _shipping_carrier_definition(code)
        printer_field = definition.get("printer_field")
        if printer_field:
            updated[printer_field] = values.get(printer_field, "").strip()
        format_field = definition.get("format_field")
        if format_field:
            updated[format_field] = _normalize_shipping_label_format(values.get(format_field, "").strip())
        scale_field = definition.get("scale_field")
        if scale_field:
            updated[scale_field] = _normalize_scale_mode(values.get(scale_field, "none"))
        tracking_mode_field = definition.get("tracking_mode_field")
        if tracking_mode_field:
            updated[tracking_mode_field] = values.get(tracking_mode_field, "").strip().lower()
        tracking_url_field = definition.get("tracking_url_field")
        if tracking_url_field:
            updated[tracking_url_field] = values.get(tracking_url_field, "").strip()
        template_field = definition.get("template_field")
        if template_field:
            updated[template_field] = os.path.expanduser(values.get(template_field, "").strip())
        for field_name, _label_key in definition.get("extra_settings_fields") or []:
            if field_name == "shipping_services_display":
                continue
            if field_name.endswith("_password"):
                updated[field_name] = values.get(field_name, "")
            else:
                updated[field_name] = values.get(field_name, "").strip()

    missing = [
        label for key, label in [
            ("db_host", "DB Host"),
            ("db_port", "DB Port"),
            ("db_name", "DB Name"),
            ("db_user", "DB User"),
            ("printer_uri", "Drucker URI"),
            ("printer_model", "Drucker Modell"),
            ("label_size", "Labelformat"),
        ]
        if not updated[key]
    ]

    if missing:
        message_box(stdscr, t("error"), t("missing_fields", fields=", ".join(missing)))
        return

    if updated["language"] not in SUPPORTED_LANGUAGES:
        message_box(stdscr, t("error"), t("language_must_be_supported"))
        return
    if updated["shopify_location_mode"] not in {"single", "multi"}:
        message_box(stdscr, t("error"), t("shopify_location_mode_invalid"))
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
        message_box(stdscr, t("error"), t("pdf_folder_missing"))
        return
    for key in ("label_font_regular", "label_font_condensed"):
        if updated[key] and not os.path.isfile(updated[key]):
            message_box(stdscr, t("error"), t("file_missing", name=key)[:56])
            return
    if updated["delivery_note_template_path"] and not os.path.isfile(updated["delivery_note_template_path"]):
        message_box(stdscr, t("error"), t("delivery_template_missing"))
        return
    for key, label in [
        ("location_regex_regal", "Regex Regal"),
        ("location_regex_fach", "Regex Fach"),
        ("location_regex_platz", "Regex Platz"),
        ]:
        if not updated[key]:
            message_box(stdscr, t("error"), t("field_must_not_be_empty", label=label))
            return
        try:
            re.compile(updated[key])
        except re.error as exc:
            message_box(stdscr, t("error"), t("regex_invalid", label=label, error=exc)[:56])
            return
    if updated["delivery_note_logo_source"]:
        logo_source = updated["delivery_note_logo_source"]
        if not is_http_url(logo_source):
            logo_path = os.path.expanduser(logo_source)
            if not os.path.isfile(logo_path):
                message_box(stdscr, t("error"), t("delivery_logo_missing"))
                return
            updated["delivery_note_logo_source"] = logo_path
    if updated["shipping_label_output_dir"] and not os.path.isdir(updated["shipping_label_output_dir"]):
        message_box(stdscr, t("error"), t("shipping_label_folder_missing"))
        return
    if not updated["delivery_note_format"]:
        updated["delivery_note_format"] = "A4"
    if not updated["shipping_label_format"]:
        message_box(stdscr, t("error"), t("label_format_required"))
        return
    if not updated["shipping_active_carriers"]:
        message_box(stdscr, t("error"), t("at_least_one_shipping_carrier"))
        return
    for code in _configurable_shipping_carrier_codes():
        definition = _shipping_carrier_definition(code)
        format_field = definition.get("format_field")
        if format_field and not updated.get(format_field):
            updated[format_field] = definition.get("default_format", "A6")
        scale_field = definition.get("scale_field")
        if scale_field and not updated.get(scale_field):
            updated[scale_field] = "none"
    if not updated.get("shipping_label_format"):
        updated["shipping_label_format"] = "A6"
    for code in _configurable_shipping_carrier_codes():
        template_field = _shipping_carrier_setting_field(code, "template")
        if template_field and updated.get(template_field) and not os.path.isfile(updated[template_field]):
            message_box(stdscr, t("error"), t("carrier_template_missing", carrier=_shipping_carrier_label(code))[:56])
            return
    try:
        packaging_weight = int(updated["shipping_packaging_weight_grams"])
    except ValueError:
        message_box(stdscr, t("error"), t("packaging_weight_must_be_number"))
        return
    if packaging_weight < 0:
        message_box(stdscr, t("error"), t("packaging_weight_negative"))
        return
    updated["shipping_packaging_weight_grams"] = packaging_weight

    try:
        SETTINGS = run_background_action_dialog(
            stdscr,
            t("settings_save_title"),
            lambda: (_save_settings_checked(updated)),
            detail=t("settings_save_detail"),
        )
    except Exception as exc:
        message_box(stdscr, t("db_error_title"), str(exc)[:56])
        return
    _set_active_shopify_location(
        SETTINGS.get("shopify_active_location_id"),
        values.get("shopify_active_location_display", ""),
    )
    apply_color_theme(stdscr)
    message_box(stdscr, t("saved"), t("saved_settings"))

def parse_int_or_error(stdscr, raw_value, field_name):

    try:
        return int(raw_value)

    except ValueError:
        message_box(stdscr, t("error"), t("field_must_be_number", field=field_name))
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
        sys.executable,
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
        t("add_item_title"),
        [
            {"name": "sku", "label": t("field_sku_short"), "value": ""},
            {"name": "name", "label": t("field_name_short"), "value": ""},
            {"name": "regal", "label": t("field_regal_short"), "value": ""},
            {"name": "fach", "label": t("field_fach_short"), "value": ""},
            {"name": "platz", "label": t("field_platz_short"), "value": ""},
            {"name": "menge", "label": t("field_menge_short"), "value": ""},
        ],
        field_validators={
            "regal": lambda value: is_location_input_allowed("regal", value),
            "fach": lambda value: is_location_input_allowed("fach", value),
            "platz": lambda value: is_location_input_allowed("platz", value),
        },
        field_normalizers=_location_field_normalizers(),
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

    menge = parse_int_or_error(stdscr, res["menge"], t("field_menge_short"))

    if menge is None:
        return

    def action():
        con = db()
        cur = con.cursor()
        try:
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
        finally:
            cur.close()
            con.close()

    run_background_action_dialog(stdscr, t("add_item_title"), action, detail=t("item_save_detail"))

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

        win.addstr(0, 2, t("qty_dialog_title"))

        win.addstr(2, 2, t("qty_current_label", value=current_qty))

        if typed is None:
            qty_str = str(qty)
        else:
            qty_str = typed
        win.addstr(3, 2, t("qty_new_label"))

        field_x = 12
        field_width = width - field_x - 2

        visible = qty_str[-field_width:]
        win.addstr(3, field_x, visible.ljust(field_width))

        win.addstr(5, 2, t("qty_input_hint"))
        win.addstr(6, 2, t("qty_change_footer"))

        cursor_pos = min(len(qty_str), field_width - 1)
        win.move(3, field_x + cursor_pos)
        
        win.refresh()

        key = win.get_wch()

        if key in (27, curses.KEY_F9):
            return

        if key in (curses.KEY_F2, 10, 13, "\n", "\r", curses.KEY_ENTER):
            queue_item_write(item["sku"], qty=qty, location_id=_active_shopify_location_id())
            return qty

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

    return None
            
def change_location(stdscr, item):

    res = form_dialog(
        stdscr,
        t("location_change_title"),
        [
            {"name": "regal", "label": t("field_regal_short"), "value": item["regal"] or ""},
            {"name": "fach", "label": t("field_fach_short"), "value": item["fach"] or ""},
            {"name": "platz", "label": t("field_platz_short"), "value": item["platz"] or ""},
        ],
        field_validators={
            "regal": lambda value: is_location_input_allowed("regal", value),
            "fach": lambda value: is_location_input_allowed("fach", value),
            "platz": lambda value: is_location_input_allowed("platz", value),
        },
        field_normalizers=_location_field_normalizers(),
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

    queue_item_write(
        item["sku"],
        regal=regal,
        fach=fach,
        platz=platz,
        location_id=_active_shopify_location_id(),
    )
    return {"regal": regal, "fach": fach, "platz": platz}
    
def edit_item(stdscr, item):

    if item["sync_status"] != "local":
        message_box(stdscr, t("error"), t("local_items_only_edit"))
        return

    res = form_dialog(
        stdscr,
        t("edit_item_title"),
        [
            {"name": "sku", "label": t("field_sku_short"), "value": item["sku"]},
            {"name": "name", "label": t("field_name_short"), "value": item["name"]},
            {"name": "regal", "label": t("field_regal_short"), "value": item["regal"] or ""},
            {"name": "fach", "label": t("field_fach_short"), "value": item["fach"] or ""},
            {"name": "platz", "label": t("field_platz_short"), "value": item["platz"] or ""},
            {"name": "menge", "label": t("field_menge_short"), "value": str(item["menge"])},
        ],
        field_validators={
            "regal": lambda value: is_location_input_allowed("regal", value),
            "fach": lambda value: is_location_input_allowed("fach", value),
            "platz": lambda value: is_location_input_allowed("platz", value),
        },
        field_normalizers=_location_field_normalizers(),
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
        message_box(stdscr, t("error"), t("quantity_must_be_number"))
        return

    def action():
        con = db()
        cur = con.cursor()
        try:
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
        finally:
            cur.close()
            con.close()

    run_background_action_dialog(stdscr, t("item_save_action_title"), action, detail=t("item_save_detail"))

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
        message_box(stdscr, t("print_error_title"), t("print_error_with_log", error=short_error[:24], log=PRINT_LOG_PATH.name)[:56])
    except Exception as exc:
        PRINT_LOGGER.exception("Unerwarteter Fehler beim Labeldruck fuer SKU=%s", item["sku"])
        message_box(stdscr, t("print_error_title"), t("print_error_with_log", error=str(exc)[:24], log=PRINT_LOG_PATH.name)[:56])

def print_label_multiple(stdscr, item):

    res = form_dialog(
        stdscr,
        t("labels_print_title"),
        [
            {"name": "count", "label": t("count_label"), "value": "1"},
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
            message_box(stdscr, t("print_error_title"), t("print_error_with_log", error=short_error[:24], log=PRINT_LOG_PATH.name)[:56])
            return
        except Exception as exc:
            PRINT_LOGGER.exception("Unerwarteter Fehler beim Mehrfachdruck fuer SKU=%s", item["sku"])
            message_box(stdscr, t("print_error_title"), t("print_error_with_log", error=str(exc)[:24], log=PRINT_LOG_PATH.name)[:56])
            return

def delete_item(stdscr, item):

    if item["sync_status"] != "local":
        message_box(stdscr, t("error"), t("delete_local_only"))
        return

    if not confirm_box(stdscr, t("delete_title"), t("delete_confirm_message", sku=item["sku"])):
        return

    def action():
        con = db()
        cur = con.cursor()
        try:
            cur.execute("DELETE FROM items WHERE sku=%s", (item["sku"],))
            con.commit()
        finally:
            cur.close()
            con.close()

    run_background_action_dialog(stdscr, t("delete_item_action_title"), action, detail=t("delete_item_action_detail"))


def toggle_external_fulfillment(stdscr, item):
    new_value = not bool(item["external_fulfillment"])

    def action():
        con = db()
        cur = con.cursor()
        try:
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
        finally:
            cur.close()
            con.close()

    run_background_action_dialog(stdscr, t("edit_item_action_title"), action, detail=t("edit_item_action_detail"))


def format_address(order):
    parts = [
        order["shipping_name"] or "",
        order["shipping_address1"] or "",
        " ".join(part for part in [order["shipping_zip"] or "", order["shipping_city"] or ""] if part),
    ]
    text = ", ".join(part for part in parts if part)
    return text or t("no_delivery_address")


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


def _documents_base_dir():
    home = Path.home()
    for name in ("Dokumente", "Documents"):
        candidate = home / name
        if candidate.exists():
            return candidate
    return home / "Dokumente"


def _lager_documents_dir():
    return _documents_base_dir() / "Lagerverwaltung"


def _default_shipping_label_output_dir():
    return str(_lager_documents_dir() / "Versandlabel")


def _default_delivery_note_output_dir():
    return str(_lager_documents_dir() / "Lieferscheine")


def get_shipping_label_output_dir():
    configured = (SETTINGS.get("shipping_label_output_dir") or "").strip()
    if configured:
        output_dir = Path(os.path.expanduser(configured))
    else:
        output_dir = Path(_default_shipping_label_output_dir())
    output_dir.mkdir(parents=True, exist_ok=True)
    return str(output_dir)


def get_pdf_output_dir():
    configured = SETTINGS["pdf_output_dir"].strip()
    if configured:
        output_dir = Path(os.path.expanduser(configured))
    else:
        output_dir = Path(_default_delivery_note_output_dir())
    output_dir.mkdir(parents=True, exist_ok=True)
    return str(output_dir)


def directory_dialog(stdscr, current_path="", title=None):
    title = title or t("directory_select_title")
    base = (current_path or "").strip()
    if base:
        current = Path(os.path.expanduser(base)).resolve()
    else:
        current = _lager_documents_dir()
    if not current.exists():
        current = current.parent if current.parent.exists() else Path.home()
    selected = 0

    while True:
        h, w = stdscr.getmaxyx()
        width = min(max(72, len(str(current)) + 6), w - 4)
        height = min(22, h - 2)
        y = max(1, (h - height) // 2)
        x = max(2, (w - width) // 2)
        win = curses.newwin(height, width, y, x)
        win.keypad(True)
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        win.addstr(0, 2, f" {title} ")
        win.addstr(1, 2, _fit(str(current), width - 4))

        entries = [
            {"kind": "select", "label": t("select_current_directory"), "path": current},
            {"kind": "mkdir", "label": t("create_directory"), "path": current},
        ]
        if current.parent != current:
            entries.append({"kind": "up", "label": "[..]", "path": current.parent})
        try:
            subdirs = sorted(
                [entry for entry in current.iterdir() if entry.is_dir()],
                key=lambda item: item.name.lower(),
            )
        except OSError:
            subdirs = []
        for entry in subdirs:
            entries.append({"kind": "dir", "label": entry.name + "/", "path": entry})

        if selected >= len(entries):
            selected = max(0, len(entries) - 1)

        visible_rows = height - 4
        scroll = 0
        if selected >= visible_rows:
            scroll = selected - visible_rows + 1

        for row in range(visible_rows):
            idx = scroll + row
            screen_y = 2 + row
            if idx >= len(entries):
                win.addstr(screen_y, 1, " " * (width - 2))
                continue
            label = _fit(entries[idx]["label"], width - 4)
            if idx == selected:
                win.attrset(curses.color_pair(2))
                win.addstr(screen_y, 2, label.ljust(width - 4))
                win.attrset(curses.color_pair(1))
            else:
                win.addstr(screen_y, 2, label.ljust(width - 4))

        footer = t("select_footer")
        win.attrset(curses.color_pair(3))
        win.addstr(height - 1, 1, " " * (width - 2))
        win.addstr(height - 1, 1, _fit(footer, width - 2))
        win.attrset(curses.color_pair(1))
        win.refresh()

        key = win.get_wch()
        if key in (27, curses.KEY_F9):
            return current_path
        if key == curses.KEY_UP:
            selected = (selected - 1) % max(1, len(entries))
            continue
        if key == curses.KEY_DOWN:
            selected = (selected + 1) % max(1, len(entries))
            continue
        if key in (10, 13, "\n", "\r", curses.KEY_ENTER):
            chosen = entries[selected]
            if chosen["kind"] == "select":
                return str(chosen["path"])
            if chosen["kind"] == "mkdir":
                form = form_dialog(
                    stdscr,
                    t("create_directory_title"),
                    [{"name": "dirname", "label": t("directory_name_label"), "value": ""}],
                    footer_text=t("confirm_footer"),
                )
                if form and form.get("dirname", "").strip():
                    name = form["dirname"].strip()
                    new_dir = chosen["path"] / name
                    try:
                        new_dir.mkdir(parents=True, exist_ok=False)
                    except FileExistsError:
                        message_box(stdscr, t("directory_select_title"), t("directory_exists"))
                    except OSError as exc:
                        message_box(stdscr, t("directory_select_title"), f"{str(exc)[:44]}")
                    else:
                        current = new_dir
                        selected = 0
                continue
            current = chosen["path"]
            selected = 0


def file_dialog(stdscr, current_path="", title=None, extensions=None):
    title = title or t("file_select_title")
    base = (current_path or "").strip()
    if base:
        current = Path(os.path.expanduser(base)).resolve()
        selected_name = current.name if current.exists() else ""
        current_dir = current.parent if current.parent.exists() else Path.home()
    else:
        current_dir = _lager_documents_dir()
        selected_name = ""
    if not current_dir.exists():
        current_dir = current_dir.parent if current_dir.parent.exists() else Path.home()
    allowed = {ext.lower() for ext in (extensions or [])}
    selected = 0

    while True:
        h, w = stdscr.getmaxyx()
        width = min(max(76, len(str(current_dir)) + 6), w - 4)
        height = min(24, h - 2)
        y = max(1, (h - height) // 2)
        x = max(2, (w - width) // 2)
        win = curses.newwin(height, width, y, x)
        win.keypad(True)
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        win.addstr(0, 2, f" {title} ")
        win.addstr(1, 2, _fit(str(current_dir), width - 4))

        entries = []
        if current_dir.parent != current_dir:
            entries.append({"kind": "up", "label": "[..]", "path": current_dir.parent})
        try:
            children = sorted(current_dir.iterdir(), key=lambda item: (not item.is_dir(), item.name.lower()))
        except OSError:
            children = []
        for entry in children:
            if entry.is_dir():
                entries.append({"kind": "dir", "label": entry.name + "/", "path": entry})
                continue
            if allowed and entry.suffix.lower() not in allowed:
                continue
            entries.append({"kind": "file", "label": entry.name, "path": entry})

        if selected_name:
            for idx, entry in enumerate(entries):
                if entry["kind"] == "file" and entry["path"].name == selected_name:
                    selected = idx
                    selected_name = ""
                    break
        if selected >= len(entries):
            selected = max(0, len(entries) - 1)

        visible_rows = height - 4
        scroll = 0
        if selected >= visible_rows:
            scroll = selected - visible_rows + 1

        for row in range(visible_rows):
            idx = scroll + row
            screen_y = 2 + row
            if idx >= len(entries):
                win.addstr(screen_y, 1, " " * (width - 2))
                continue
            label = _fit(entries[idx]["label"], width - 4)
            if idx == selected:
                win.attrset(curses.color_pair(2))
                win.addstr(screen_y, 2, label.ljust(width - 4))
                win.attrset(curses.color_pair(1))
            else:
                win.addstr(screen_y, 2, label.ljust(width - 4))

        footer = t("select_footer")
        win.attrset(curses.color_pair(3))
        win.addstr(height - 1, 1, " " * (width - 2))
        win.addstr(height - 1, 1, _fit(footer, width - 2))
        win.attrset(curses.color_pair(1))
        win.refresh()

        key = win.get_wch()
        if key in (27, curses.KEY_F9):
            return current_path
        if key == curses.KEY_UP:
            selected = (selected - 1) % max(1, len(entries))
            continue
        if key == curses.KEY_DOWN:
            selected = (selected + 1) % max(1, len(entries))
            continue
        if key in (10, 13, "\n", "\r", curses.KEY_ENTER):
            if not entries:
                continue
            chosen = entries[selected]
            if chosen["kind"] == "file":
                return str(chosen["path"])
            current_dir = chosen["path"]
            selected = 0


def get_delivery_note_sender():
    return {
        "name": SETTINGS["delivery_note_sender_name"].strip(),
        "street": SETTINGS["delivery_note_sender_street"].strip(),
        "city": SETTINGS["delivery_note_sender_city"].strip(),
        "email": SETTINGS["delivery_note_sender_email"].strip(),
    }


def get_free_label_sender():
    sender = get_delivery_note_sender()
    return {
        "name": sender["name"],
        "street": sender["street"],
        "zip_city": sender["city"],
    }


def get_free_label_template_path():
    configured = (SETTINGS.get("free_label_template_path") or "").strip()
    if not configured:
        return None
    return Path(os.path.expanduser(configured))


def _free_label_receiver(order):
    zip_city = " ".join(
        part for part in [(order.get("shipping_zip") or "").strip(), (order.get("shipping_city") or "").strip()] if part
    ).strip()
    country_code = (order.get("shipping_country") or "").strip().upper()
    country_label = COUNTRY_NAME_DE.get(country_code, country_code)
    return {
        "name": (order.get("shipping_name") or "").strip(),
        "additional_name": (order.get("shipping_company") or "").strip(),
        "street": (order.get("shipping_address1") or "").strip(),
        "address_line_2": (order.get("shipping_address2") or "").strip(),
        "zip_city": zip_city,
        "country": country_label if country_label and country_label != "DE" else "",
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
    lines.append(t("picklist_positions", count=len(sorted_items)))
    return "\n".join(lines) + "\n"


def print_picklist(stdscr, order, order_items):
    printer = SETTINGS["picklist_printer"].strip()

    if not printer:
        message_box(stdscr, t("error"), t("picklist_printer_missing"))
        return

    document = build_picklist_text(order, order_items)

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".txt", delete=False) as handle:
        handle.write(document)
        temp_path = handle.name

    try:
        _run_lp_command(
            ["lp", "-d", printer, "-t", f"Pickliste {order['order_name']}", temp_path],
            print_kind="picklist",
            printer=printer,
            title=f"Pickliste {order['order_name']}",
            source_path=temp_path,
            extra=f"items={len(order_items)}",
        )
    except FileNotFoundError:
        PRINT_LOGGER.exception("lp/Drucksystem nicht verfuegbar fuer Pickliste order=%s", order["order_name"])
        message_box(stdscr, t("print_error_title"), t("lp_unavailable"))
    except subprocess.CalledProcessError as exc:
        PRINT_LOGGER.exception("Picklisten-Druck fehlgeschlagen order=%s printer=%s", order["order_name"], printer)
        error_text = (exc.stderr or str(exc)).strip()
        message_box(stdscr, t("print_error_title"), f"{(error_text[:20] or t('print_error_title'))} {PRINT_LOG_PATH.name}"[:56])
    else:
        PRINT_LOGGER.info("Pickliste erfolgreich gedruckt order=%s printer=%s", order["order_name"], printer)
        message_box(stdscr, t("print_title"), t("inventory_list_sent"))
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
        message_box(stdscr, t("error"), str(exc)[:56])
    except ValueError as exc:
        PRINT_LOGGER.warning("Lieferschein nicht erstellt order=%s reason=%s", order["order_name"], exc)
        message_box(stdscr, t("error"), str(exc)[:56])
    except Exception:
        PRINT_LOGGER.exception("Lieferschein-PDF fehlgeschlagen order=%s", order["order_name"])
        message_box(stdscr, t("error"), t("delivery_note_failed_with_log", log=PRINT_LOG_PATH.name)[:56])
    else:
        PRINT_LOGGER.info(
            "Lieferschein-PDF erstellt order=%s items=%s path=%s",
            order["order_name"],
            len(rows),
            output_path,
        )
        message_box(stdscr, t("delivery_note_pdf_title"), output_path[-56:])


def delivery_note_output_mode_dialog(stdscr):
    return choice_dialog(
        stdscr,
        t("delivery_note_output_title"),
        [
            {"value": "print", "label": t("output_print")},
            {"value": "print_pdf", "label": t("output_print_pdf")},
            {"value": "pdf", "label": t("output_pdf_only")},
        ],
        "print",
        cancel_returns_none=True,
    )


def handle_delivery_note_output(stdscr, order, order_items=None, order_items_cache=None):
    if order_items is None:
        order_items = ensure_order_items_loaded(order.get("order_id"), order_items_cache)
    mode = delivery_note_output_mode_dialog(stdscr)
    if not mode:
        return
    if mode == "print":
        print_delivery_note(stdscr, order, order_items)
        return
    if mode == "pdf":
        export_delivery_note_pdf(stdscr, order, order_items)
        return

    try:
        output_path, rows = create_delivery_note_pdf(order, order_items)
        _print_delivery_note_pdf_path(order, output_path)
    except FileNotFoundError as exc:
        PRINT_LOGGER.exception("Lieferschein Druck+PDF nicht moeglich order=%s", order["order_name"])
        message_box(stdscr, t("print_error_title"), str(exc)[:56])
    except ValueError as exc:
        PRINT_LOGGER.warning("Lieferschein Druck+PDF abgebrochen order=%s reason=%s", order["order_name"], exc)
        message_box(stdscr, t("print_error_title"), str(exc)[:56])
    except subprocess.CalledProcessError as exc:
        PRINT_LOGGER.exception("Lieferschein Druck+PDF fehlgeschlagen order=%s", order["order_name"])
        error_text = (exc.stderr or str(exc)).strip()
        message_box(stdscr, t("print_error_title"), f"{(error_text[:20] or t('print_error_title'))} {PRINT_LOG_PATH.name}"[:56])
    except Exception:
        PRINT_LOGGER.exception("Lieferschein Druck+PDF fehlgeschlagen order=%s", order["order_name"])
        message_box(stdscr, t("print_error_title"), t("delivery_note_failed_with_log", log=PRINT_LOG_PATH.name)[:56])
    else:
        PRINT_LOGGER.info(
            "Lieferschein gedruckt+gespeichert order=%s items=%s path=%s",
            order["order_name"],
            len(rows),
            output_path,
        )
        message_box(stdscr, t("delivery_note_title"), output_path[-56:])


def print_delivery_note(stdscr, order, order_items):
    printer = SETTINGS["delivery_note_printer"].strip()
    if not printer:
        message_box(stdscr, t("error"), t("delivery_note_printer_missing"))
        return

    temp_path = None
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path, rows = create_delivery_note_pdf(order, order_items, output_dir=temp_dir)
            _run_lp_command(
                ["lp", "-d", printer, "-t", f"Lieferschein {order['order_name']}", temp_path],
                print_kind="delivery_note",
                printer=printer,
                title=f"Lieferschein {order['order_name']}",
                source_path=temp_path,
                extra=f"items={len(rows)} format={_delivery_note_format()}",
            )
    except FileNotFoundError as exc:
        PRINT_LOGGER.exception("Lieferschein-Druck nicht moeglich order=%s", order["order_name"])
        message_box(stdscr, t("print_error_title"), str(exc)[:56])
    except ValueError as exc:
        PRINT_LOGGER.warning("Lieferschein-Druck abgebrochen order=%s reason=%s", order["order_name"], exc)
        message_box(stdscr, t("print_error_title"), str(exc)[:56])
    except subprocess.CalledProcessError as exc:
        PRINT_LOGGER.exception("Lieferschein-Druck fehlgeschlagen order=%s printer=%s", order["order_name"], printer)
        error_text = (exc.stderr or str(exc)).strip()
        message_box(stdscr, t("print_error_title"), f"{(error_text[:20] or t('print_error_title'))} {PRINT_LOG_PATH.name}"[:56])
    except Exception:
        PRINT_LOGGER.exception("Lieferschein-Verarbeitung fehlgeschlagen order=%s temp=%s", order["order_name"], temp_path)
        message_box(stdscr, t("print_error_title"), t("delivery_note_failed_with_log", log=PRINT_LOG_PATH.name)[:56])
    else:
        PRINT_LOGGER.info("Lieferschein erfolgreich gedruckt order=%s printer=%s", order["order_name"], printer)
        message_box(stdscr, t("print_title"), t("delivery_note_sent"))


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
        regal_label = row["regal"] or t("inventory_no_shelf")
        if regal_label != current_regal:
            if current_regal is not None:
                output.append("")
            current_regal = regal_label
            output.append(t("inventory_shelf_title", value=regal_label))
            output.append("-" * 80)
            output.append(f"{t('field_soll_short'):<6} {t('field_ist_short'):<6} {'SKU':<18} {t('field_name_short'):<28} {t('field_fach_short'):<6} {t('field_platz_short'):<6}")

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
    output.append(t("inventory_export_summary", total=total, counted=counted, differences=differences))
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
        message_box(stdscr, t("error"), t("picklist_printer_missing"))
        return

    document = build_inventory_export_text(session, lines)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".txt", delete=False) as handle:
        handle.write(document)
        temp_path = handle.name

    try:
        _run_lp_command(
            ["lp", "-d", printer, "-t", session["session_name"], temp_path],
            print_kind="inventory_list",
            printer=printer,
            title=session["session_name"],
            source_path=temp_path,
            extra=f"positions={len(lines)}",
        )
    except FileNotFoundError:
        PRINT_LOGGER.exception("lp/Drucksystem nicht verfuegbar fuer Inventurliste session=%s", session["session_name"])
        message_box(stdscr, t("print_error_title"), t("lp_unavailable"))
    except subprocess.CalledProcessError as exc:
        PRINT_LOGGER.exception("Inventurlisten-Druck fehlgeschlagen session=%s printer=%s", session["session_name"], printer)
        error_text = (exc.stderr or str(exc)).strip()
        message_box(stdscr, t("print_error_title"), f"{(error_text[:20] or t('print_error_title'))} {PRINT_LOG_PATH.name}"[:56])
    else:
        PRINT_LOGGER.info("Inventurliste erfolgreich gedruckt session=%s printer=%s", session["session_name"], printer)
        message_box(stdscr, t("print_title"), t("picklist_sent"))
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
    sku = _fit(row["sku"] or "-/-", sku_width)
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
        t("bulk_print_mode_title"),
        [
            {"value": "both", "label": t("bulk_print_mode_both")},
            {"value": "label", "label": t("bulk_print_mode_label")},
            {"value": "note", "label": t("bulk_print_mode_note")},
            {"value": "none", "label": t("bulk_print_mode_none")},
        ],
        "both",
        cancel_returns_none=True,
    )


def _bulk_shopify_queue_mode_dialog(stdscr):
    return choice_dialog(
        stdscr,
        t("bulk_shopify_tracking_title"),
        [
            {"value": "queue", "label": t("bulk_shopify_tracking_queue")},
            {"value": "manual", "label": t("bulk_shopify_tracking_manual")},
        ],
        "manual",
        cancel_returns_none=True,
    )


def bulk_carrier_per_order_dialog(stdscr, selected_orders, current_map):
    if not selected_orders:
        return current_map

    options = _active_shipping_carriers()
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
        win.addstr(0, 2, t("bulk_per_order_title"))

        visible_rows = max(1, height - 4)
        if selected < top_index:
            top_index = selected
        if selected >= top_index + visible_rows:
            top_index = selected - visible_rows + 1

        for row_idx, order in enumerate(selected_orders[top_index:top_index + visible_rows]):
            real_idx = top_index + row_idx
            order_id = order["order_id"]
            carrier = effective_shipping_carrier(assignments.get(order_id))
            line = _fit(f"{_fit(order['order_name'], 12)}  {carrier.upper()}  {format_address(order)}", width - 3)
            y_pos = 2 + row_idx
            if real_idx == selected:
                win.attrset(curses.color_pair(2))
                win.addstr(y_pos, 1, line.ljust(width - 2))
                win.attrset(curses.color_pair(1))
            else:
                win.addstr(y_pos, 1, line.ljust(width - 2))

        footer = t("bulk_per_order_footer")
        win.attrset(curses.color_pair(3))
        win.addstr(height - 1, 1, _fit(footer, width - 2))
        win.attrset(curses.color_pair(1))
        win.refresh()

        key = win.get_wch()
        if key in (27, curses.KEY_F9):
            return current_map
        if key in (10, 13, "\n", "\r", curses.KEY_ENTER):
            return assignments
        if key == curses.KEY_DOWN:
            selected = move_selection(selected_orders, selected, 1)
            continue
        if key == curses.KEY_UP:
            selected = move_selection(selected_orders, selected, -1)
            continue
        if key in (curses.KEY_LEFT, curses.KEY_RIGHT, " ", 10, 13, "\n", "\r", curses.KEY_ENTER):
            order_id = selected_orders[selected]["order_id"]
            current = effective_shipping_carrier(assignments.get(order_id))
            current_index = options.index(current) if current in options else 0
            if key == curses.KEY_LEFT:
                new_index = (current_index - 1) % len(options)
            else:
                new_index = (current_index + 1) % len(options)
            assignments[order_id] = options[new_index]


def _execution_carrier_dialog(stdscr, current_carrier=None):
    fallback = effective_shipping_carrier(current_carrier or last_shipping_carrier() or "gls")
    chosen = choice_dialog(
        stdscr,
        t("shipping_provider_title"),
        _shipping_carrier_options(include_test=False),
        fallback,
        cancel_returns_none=True,
    )
    if chosen:
        remember_shipping_carrier(chosen)
    return chosen


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
        message_box(stdscr, t("partial_execution_title"), t("partial_execution_no_open_items"))
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
        win.addstr(0, 2, t("partial_execution_window_title", order=order.get("order_name") or ""))

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
            sku = row.get("sku") or "-/-"
            title = row.get("title") or "-"
            state = t("partial_execution_done_state") if remaining <= 0 else ""
            line = _fit(f"[{qty:>3}/{remaining:<3}/{total:<3}] {_fit(sku, 16)} {title}{state}", width - 3)
            if real_idx == selected:
                win.attrset(curses.color_pair(2))
                win.addstr(y_pos, 1, line.ljust(width - 2))
                win.attrset(curses.color_pair(1))
            else:
                win.addstr(y_pos, 1, line.ljust(width - 2))

        footer = t("partial_execution_footer")
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
        if key in (10, 13, "\n", "\r", curses.KEY_ENTER):
            picked = [row for row in editable if int(row.get("selected_quantity") or 0) > 0]
            if not picked:
                message_box(stdscr, t("partial_execution_title"), t("partial_execution_pick_one"))
                continue
            stdscr.erase()
            stdscr.refresh()
            return picked


def get_latest_label_for_order(order_id):
    return _get_latest_shipping_label_for_order(db, order_id)


def run_partial_execution_for_order(stdscr, order, order_items):
    selected_items = select_partial_items_dialog(stdscr, order, order_items)
    if not selected_items:
        return

    carrier = _execution_carrier_dialog(stdscr, last_shipping_carrier())
    if not carrier:
        return
    carrier_options = _select_shipping_carrier_options(stdscr, carrier, scope="domestic")
    if carrier_options is None:
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
            service_codes=carrier_options,
            carrier=carrier,
        )
        if print_mode in {"both", "label"} and created.get("label_path"):
            printed = _print_pdf_via_lp(stdscr, created["label_path"], f"{carrier.upper()} {created['shipment_reference']}", carrier=carrier)
            if printed and created.get("label_id") is not None:
                update_shipping_label_status(created["label_id"], "PRINTED")

        note_path, _rows = create_delivery_note_pdf(order, selected_for_note)
        if print_mode in {"both", "note"}:
            _print_delivery_note_pdf_path(order, note_path)

        if created.get("label_id") is not None and _shipping_carrier_allows_shopify(carrier):
            fresh_rows = list_shipping_labels(order["order_id"])
            current_label = next((row for row in fresh_rows if row["id"] == created["label_id"]), None)
            if current_label:
                queue_result = enqueue_shopify_fulfillment_job_for_items(current_label, selected_items, notify_customer=False)
                if queue_result.get("created"):
                    update_shipping_label_status(created["label_id"], "SHOPIFY_QUEUED")
        message_box(
            stdscr,
            t("partial_execution_title"),
            t("partial_execution_ok", label=_created_label_display_value(carrier, created), grams=selected_weight_grams),
        )
    except DatabaseUnavailableError:
        raise
    except Exception as exc:
        PRINT_LOGGER.exception("Teilausfuehrung fehlgeschlagen order=%s", order.get("order_name"))
        message_box(stdscr, t("partial_execution_title"), f"{str(exc)[:28]} {PRINT_LOG_PATH.name}"[:56])


def _print_delivery_note_pdf_path(order, pdf_path):
    printer = SETTINGS["delivery_note_printer"].strip()
    if not printer:
        raise RuntimeError(t("delivery_note_printer_missing"))
    queue_title = f"{t('delivery_note_title')} {order['order_name']}"
    cmd = ["lp", "-d", printer, "-t", queue_title]
    cmd.extend(_cups_delivery_note_print_options(_delivery_note_format()))
    cmd.append(pdf_path)
    _run_lp_command(
        cmd,
        print_kind="delivery_note",
        printer=printer,
        title=queue_title,
        source_path=pdf_path,
        extra=f"format={_delivery_note_format()}",
    )


def _print_merged_delivery_note_pdf(pdf_path, title=None):
    printer = SETTINGS["delivery_note_printer"].strip()
    if not printer:
        raise RuntimeError(t("delivery_note_printer_missing"))
    title = title or t("delivery_note_batch_title")
    cmd = ["lp", "-d", printer, "-t", title]
    cmd.extend(_cups_delivery_note_print_options(_delivery_note_format()))
    cmd.append(pdf_path)
    _run_lp_command(
        cmd,
        print_kind="delivery_note_bulk",
        printer=printer,
        title=title,
        source_path=pdf_path,
        extra=f"format={_delivery_note_format()}",
    )


def run_bulk_execution(stdscr, orders, order_items_cache, selected_order_ids):
    if not orders:
        message_box(stdscr, t("bulk_title"), t("bulk_no_orders"))
        return

    if selected_order_ids:
        selected_orders = [row for row in orders if row["order_id"] in selected_order_ids]
    else:
        selected_orders = []

    if not selected_orders:
        message_box(stdscr, t("bulk_title"), t("bulk_mark_orders_first"))
        return

    carrier = _execution_carrier_dialog(stdscr, last_shipping_carrier())
    if not carrier:
        return
    carrier_options = _select_shipping_carrier_options(stdscr, carrier, scope="domestic")
    if carrier_options is None:
        return
    print_mode = _bulk_print_mode_dialog(stdscr)
    if print_mode is None:
        return
    shopify_mode = "manual"
    if _shipping_carrier_allows_shopify(carrier):
        shopify_mode = _bulk_shopify_queue_mode_dialog(stdscr)
        if shopify_mode is None:
            return

    carrier_map = {row["order_id"]: carrier for row in selected_orders}

    success_count = 0
    failure_count = 0
    queued_count = 0
    queue_failed_count = 0
    last_failure_summary = ""
    label_paths_to_print = []
    note_paths_to_print = []
    printed_label_ids = []

    try:
        with tempfile.TemporaryDirectory(prefix="lager-bulk-print-") as temp_dir:
            for order in selected_orders:
                carrier = effective_shipping_carrier(carrier_map.get(order["order_id"]))
                try:
                    order_items = ensure_order_items_loaded(order["order_id"], order_items_cache)
                    order_weight_kg, _order_weight_grams = calculate_order_shipping_weight(order, order_items)
                    created = create_shipping_label(
                        order,
                        weight_kg=order_weight_kg,
                        carrier=carrier,
                        service_codes=carrier_options,
                    )
                    if print_mode in {"both", "label"} and created.get("label_path"):
                        label_paths_to_print.append(created["label_path"])
                        if created.get("label_id") is not None:
                            printed_label_ids.append(created["label_id"])

                    if print_mode in {"both", "note"}:
                        note_path, _rows = create_delivery_note_pdf(order, order_items, output_dir=temp_dir)
                        note_paths_to_print.append(note_path)

                    if shopify_mode == "queue" and created.get("label_id") is not None and _shipping_carrier_allows_shopify(carrier):
                        labels_for_order = ensure_order_shipments_loaded(order["order_id"])
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
                                    update_shipping_label_status(created["label_id"], "SHOPIFY_QUEUED")

                    success_count += 1
                except DatabaseUnavailableError:
                    raise
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

            if label_paths_to_print:
                merged_label_pdf = os.path.join(temp_dir, f"shipping_labels_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
                _merge_pdf_files(label_paths_to_print, merged_label_pdf)
                printed = _print_pdf_via_lp(stdscr, merged_label_pdf, f"{carrier.upper()} Sammeldruck", carrier=carrier)
                if printed:
                    for label_id in printed_label_ids:
                        update_shipping_label_status(label_id, "PRINTED")
            if note_paths_to_print:
                if len(note_paths_to_print) == 1:
                    _print_merged_delivery_note_pdf(note_paths_to_print[0], title=f"Lieferschein {selected_orders[0]['order_name']}")
                else:
                    merged_note_pdf = os.path.join(temp_dir, f"delivery_notes_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf")
                    _merge_pdf_files(note_paths_to_print, merged_note_pdf)
                    _print_merged_delivery_note_pdf(merged_note_pdf)
    except Exception as exc:
        LOGGER.exception("Bulk-Sammeldruck fehlgeschlagen carrier=%s print_mode=%s", carrier, print_mode)
        PRINT_LOGGER.exception("Bulk-Sammeldruck fehlgeschlagen carrier=%s", carrier)
        selected_order_ids.clear()
        message_box(stdscr, t("bulk_title"), t("bulk_print_error", error=str(exc)[:22], log=PRINT_LOG_PATH.name)[:56])
        return

    selected_order_ids.clear()
    message_box(
        stdscr,
        t("bulk_title"),
        t("bulk_finished", ok=success_count, err=failure_count, queued=queued_count, queue_err=queue_failed_count)[:56],
    )


def create_shipping_label_for_order(stdscr, order):
    resolved_carrier = _execution_carrier_dialog(stdscr, last_shipping_carrier())
    if not resolved_carrier:
        return
    carrier = _shipping_carrier_label(resolved_carrier)
    carrier_options = _select_shipping_carrier_options(stdscr, resolved_carrier, scope="domestic")
    if carrier_options is None:
        return
    order_weight_kg, total_grams = calculate_order_shipping_weight(order)
    try:
        created = create_shipping_label(order, weight_kg=order_weight_kg, service_codes=carrier_options, carrier=resolved_carrier)
    except DatabaseUnavailableError:
        raise
    except ValueError as exc:
        message_box(stdscr, t("shipping_label_title"), str(exc)[:56])
        return
    except Exception as exc:
        PRINT_LOGGER.exception("Versandlabel Erstellung fehlgeschlagen carrier=%s order=%s", carrier, order["order_name"])
        message_box(stdscr, t("shipping_label_title"), f"{str(exc)[:28]} {PRINT_LOG_PATH.name}"[:56])
        return

    printed = _print_pdf_via_lp(stdscr, created["label_path"], f"{carrier} {created['shipment_reference']}", carrier=resolved_carrier)
    if printed:
        if created["label_id"] is not None:
            update_shipping_label_status(created["label_id"], "PRINTED")
        message_box(
            stdscr,
            t("shipping_label_title"),
            t("shipping_label_printed", carrier=carrier, value=_created_label_display_value(resolved_carrier, created), grams=total_grams)[:56],
        )
    else:
        if created["label_id"] is not None:
            update_shipping_label_status(created["label_id"], "CREATED")
        message_box(
            stdscr,
            t("shipping_label_title"),
            t("shipping_label_pdf_only", carrier=carrier, value=_created_label_display_value(resolved_carrier, created), grams=total_grams)[:56],
        )


def create_manual_shipping_label(stdscr):
    carrier_key = _execution_carrier_dialog(stdscr, last_shipping_carrier())
    if not carrier_key:
        return
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
    post_selection = dict(_POST_SELECTION_CACHE.get("domestic") or {})
    print_mode = "print"

    while True:
        option_mode = _shipping_carrier_option_mode(carrier_key)
        fields = [
            {"name": "name", "label": t("manual_label_field_name"), "value": state["name"]},
            {"name": "street", "label": t("manual_label_field_street"), "value": state["street"]},
            {"name": "zip", "label": t("manual_label_field_zip"), "value": state["zip"]},
            {"name": "city", "label": t("manual_label_field_city"), "value": state["city"]},
            {"name": "reference", "label": t("manual_label_field_reference"), "value": state["reference"]},
            {"name": "weight_grams", "label": t("manual_label_field_weight"), "value": state["weight_grams"]},
            {"name": "country_display", "label": t("manual_label_field_country"), "value": _manual_label_country_display(country_code)},
        ]
        if option_mode == "post_products":
            fields.append({"name": "post_product", "label": t("manual_label_field_post_product"), "value": _post_selection_summary(post_selection)})
        elif option_mode == "gls_services":
            fields.append({"name": "services_display", "label": t("manual_label_field_services"), "value": _shipping_services_summary(selected_services)})
        fields.append({"name": "print_mode", "label": t("manual_label_field_output"), "value": t("manual_label_output_print") if print_mode == "print" else t("manual_label_output_pdf")})
        footer_text = t("manual_label_footer_base")
        if option_mode in {"gls_services", "post_products"}:
            footer_text = t("manual_label_footer_options")
        result = form_dialog(
            stdscr,
            t("manual_shipping_label_title"),
            fields,
            initial_active=active,
            footer_text=footer_text,
            extra_actions=[
                {"name": "country", "keys": {curses.KEY_F3}},
                {"name": "services", "keys": {curses.KEY_F4}},
                {"name": "print_mode", "keys": {curses.KEY_F5}},
                {"name": "customer", "keys": {curses.KEY_F6}},
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
                if option_mode == "post_products":
                    chosen_post = _post_selection_dialog(stdscr, scope="domestic")
                    if chosen_post:
                        post_selection = chosen_post
                elif option_mode == "gls_services":
                    selected_services = shipping_services_dialog(stdscr, selected_services)
            elif result["__action__"] == "print_mode":
                next_mode = manual_label_print_mode_dialog(stdscr, print_mode)
                if next_mode is None:
                    return
                print_mode = next_mode
            elif result["__action__"] == "customer":
                chosen_customer = shopify_customer_dialog(stdscr)
                if chosen_customer:
                    state, country_code = _apply_shopify_customer_to_manual_state(state, chosen_customer, country_code)
            continue
        state.update(result)
        break

    required_fields = [
        ("name", t("recipient_name_missing")),
        ("street", t("recipient_street_missing")),
        ("zip", t("recipient_zip_missing")),
        ("city", t("recipient_city_missing")),
    ]
    for field_name, error_text in required_fields:
        if not state.get(field_name, "").strip():
            message_box(stdscr, t("shipping_label_title"), error_text)
            return

    try:
        weight_grams = int((state.get("weight_grams") or "").strip())
    except ValueError:
        message_box(stdscr, t("shipping_label_title"), t("weight_grams_integer"))
        return
    if weight_grams <= 0:
        message_box(stdscr, t("shipping_label_title"), t("weight_grams_positive"))
        return
    option_mode = _shipping_carrier_option_mode(carrier_key)
    if option_mode == "post_products" and not post_selection:
        message_box(stdscr, t("shipping_label_title"), t("choose_post_product"))
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
    carrier = _shipping_carrier_label(carrier_key)

    try:
        created = create_shipping_label(
            order_stub,
            weight_kg=round(weight_grams / 1000.0, 3),
            shipment_reference=reference,
            service_codes=(
                post_selection if option_mode == "post_products"
                else selected_services if option_mode == "gls_services"
                else None
            ),
            carrier=carrier_key,
        )
    except DatabaseUnavailableError:
        raise
    except Exception as exc:
        PRINT_LOGGER.exception("Manuelles Versandlabel fehlgeschlagen reference=%s", reference)
        message_box(stdscr, t("shipping_label_title"), f"{str(exc)[:28]} {PRINT_LOG_PATH.name}"[:56])
        return

    if print_mode == "print":
        printed = _print_pdf_via_lp(stdscr, created["label_path"], f"{carrier} {created['shipment_reference']}", carrier=carrier_key)
        if printed and created.get("label_id") is not None:
            update_shipping_label_status(created["label_id"], "PRINTED")
        if printed:
            message_box(
                stdscr,
                t("shipping_label_title"),
                t("manual_shipping_label_printed", carrier=carrier, value=_created_label_display_value(carrier_key, created))[:56],
            )
        else:
            message_box(
                stdscr,
                t("shipping_label_title"),
                t("manual_shipping_label_pdf_only", carrier=carrier, value=_created_label_display_value(carrier_key, created))[:56],
            )
    else:
        message_box(
            stdscr,
            t("shipping_label_title"),
            t("manual_shipping_label_pdf_only", carrier=carrier, value=_created_label_display_value(carrier_key, created))[:56],
        )


def _format_gls_history_line(row, width):
    created_at = row.get("created_at")
    if isinstance(created_at, datetime.datetime):
        ts = created_at.strftime("%d.%m %H:%M")
    else:
        ts = "-"
    order_name = (row.get("order_name") or "-").replace("#", "")
    carrier = _shipping_carrier_label(row.get("carrier") or "gls", short=True)
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
    job_rows = {}

    while True:
        if reload_rows:
            filter_order_id = None if show_all else (selected_order["order_id"] if selected_order else None)
            rows = list_shipping_labels(filter_order_id)
            job_rows = get_latest_shopify_jobs_for_labels([row.get("id") for row in rows])
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
        title = t("history_title_all") if show_all else t("history_title_order")
        win.addstr(0, 2, title)

        left_width = max(46, int((width - 3) * 0.57))
        right_width = width - left_width - 3
        list_height = height - 4

        list_win = win.derwin(list_height, left_width, 1, 1)
        details_win = win.derwin(list_height, right_width, 1, 2 + left_width)

        display_lines = [_format_gls_history_line(row, left_width - 2) for row in rows] or [t("history_none")]

        if selected < top_index:
            top_index = selected
        if selected >= top_index + max(1, list_height - 2):
            top_index = selected - max(1, list_height - 2) + 1

        draw_panel(list_win, t("history_label_panel"), display_lines, selected if rows else 0, top_index, True)

        detail_lines = []
        chosen = rows[selected] if rows else None
        if chosen:
            job = job_rows.get(chosen["id"])
            detail_lines.append(_fit(t("history_detail_order", value=chosen.get("order_name") or "-"), right_width - 2))
            detail_lines.append(_fit(t("history_detail_carrier", value=_shipping_carrier_label(chosen.get("carrier") or "gls")), right_width - 2))
            detail_lines.append(_fit(t("history_detail_track_id", value=chosen.get("track_id") or "-"), right_width - 2))
            detail_lines.append(_fit(t("history_detail_shipment_number", value=_shipment_number(chosen)), right_width - 2))
            detail_lines.append(_fit(t("history_detail_status", value=chosen.get("status") or "-"), right_width - 2))
            detail_lines.append(_fit(t("history_detail_source", value=_shipment_source_label(chosen.get("source"))), right_width - 2))
            detail_lines.append(_fit(t("history_detail_reference", value=chosen.get("shipment_reference") or "-"), right_width - 2))
            if chosen.get("shopify_fulfillment_id"):
                detail_lines.append(_fit(t("history_detail_shopify_fulfillment", value=chosen.get("shopify_fulfillment_id")), right_width - 2))
            if job:
                detail_lines.append(_fit(t("history_detail_shopify_job", status=job.get("status") or "-", attempts=job.get("attempts") or 0), right_width - 2))
                if job.get("result_message"):
                    detail_lines.append(_fit(t("history_detail_shopify_message", value=job["result_message"]), right_width - 2))
            else:
                detail_lines.append(_fit(t("history_detail_shopify_empty"), right_width - 2))
            detail_lines.append("")
            detail_lines.append(_fit(t("history_detail_pdf", value=chosen.get("label_path") or "-"), right_width - 2))
            if chosen.get("tracking_url"):
                detail_lines.append(_fit(t("history_detail_tracking_url", value=chosen.get("tracking_url")), right_width - 2))
            if chosen.get("last_error"):
                detail_lines.append(_fit(t("history_detail_error", value=chosen["last_error"]), right_width - 2))
        else:
            detail_lines.append(t("history_none"))

        draw_panel(details_win, t("history_details_panel"), detail_lines, 0, 0, False)

        footer = t("history_footer")
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
                message_box(stdscr, t("history_label_panel"), t("history_pdf_missing"))
                continue
            if _print_pdf_via_lp(
                stdscr,
                chosen["label_path"],
                f"{(chosen.get('carrier') or 'gls').upper()} {chosen['shipment_reference']}",
                carrier=(chosen.get("carrier") or "gls").lower(),
            ):
                message_box(stdscr, t("history_label_panel"), t("history_label_reprinted"))
                reload_rows = True
        elif key == curses.KEY_F7 and chosen:
            try:
                reprint_path = reprint_shipping_label(chosen)
            except Exception as exc:
                PRINT_LOGGER.exception("Versand Reprint fehlgeschlagen carrier=%s track=%s", chosen.get("carrier"), chosen.get("track_id"))
                message_box(stdscr, t("reprint_title"), f"{str(exc)[:28]} {PRINT_LOG_PATH.name}"[:56])
            else:
                if _print_pdf_via_lp(
                    stdscr,
                    reprint_path,
                    f"{(chosen.get('carrier') or 'gls').upper()} {chosen['shipment_reference']}",
                    carrier=(chosen.get("carrier") or "gls").lower(),
                ):
                    message_box(stdscr, t("reprint_title"), t("history_label_reprinted"))
                    reload_rows = True
        elif key == curses.KEY_F6 and chosen:
            if not confirm_box(stdscr, t("cancellation_title"), t("history_cancel_confirm", track_id=chosen["track_id"])):
                continue
            try:
                result = cancel_shipping_label(chosen)
            except Exception as exc:
                PRINT_LOGGER.exception("Versand Storno fehlgeschlagen carrier=%s track=%s", chosen.get("carrier"), chosen.get("track_id"))
                message_box(stdscr, t("cancellation_title"), f"{str(exc)[:28]} {PRINT_LOG_PATH.name}"[:56])
            else:
                message_box(stdscr, t("cancellation_title"), t("history_cancel_result", value=result)[:56])
                reload_rows = True
        elif key == curses.KEY_F10 and chosen:
            if str(chosen.get("order_id") or "").startswith("manual-"):
                message_box(stdscr, t("shopify_queue_title"), t("manual_labels_no_shopify_order"))
                continue
            if (chosen.get("carrier") or "").strip().lower() in {"test", "free"}:
                message_box(stdscr, t("shopify_queue_title"), t("test_and_free_no_shopify"))
                continue
            if (chosen.get("source") or "").strip().lower() == "shopify":
                message_box(stdscr, t("shopify_queue_title"), t("shipment_from_shopify"))
                continue
            if not confirm_box(stdscr, t("shopify_confirm_title"), t("shopify_send_fulfillment_confirm", track_id=chosen["track_id"])):
                continue
            try:
                queue_result = enqueue_shopify_fulfillment_job(chosen, notify_customer=False)
            except Exception as exc:
                PRINT_LOGGER.exception("Shopify Queue fehlgeschlagen track=%s", chosen.get("track_id"))
                update_shipping_label_status(chosen["id"], "SHOPIFY_QUEUE_FAILED", str(exc)[:160])
                message_box(stdscr, t("shopify_queue_title"), f"{str(exc)[:28]} {PRINT_LOG_PATH.name}"[:56])
            else:
                if queue_result.get("created"):
                    update_shipping_label_status(chosen["id"], "SHOPIFY_QUEUED")
                    message_box(stdscr, t("shopify_queue_title"), t("shopify_job_queued", job_id=queue_result["job_id"]))
                else:
                    message_box(stdscr, t("shopify_queue_title"), t("shopify_job_running", job_id=queue_result["job_id"]))
                reload_rows = True


def orders_dialog(stdscr):
    try:
        curses.curs_set(0)
    except curses.error:
        pass
    order_filter = None
    only_pending = False
    fulfillment_filter = "all"
    payment_filter = "all"
    selected = 0
    top_index = 0
    orders_snapshot = []
    last_orders_snapshot_refresh_at = None
    orders = []
    order_items_cache = {}
    order_shipments_cache = {}
    selected_order_ids = set()
    reload_orders_snapshot = True
    orders_snapshot_reload_pending = False
    rebuild_orders_view = True
    prefetch_order_ids = []

    while True:
        for result in _ORDER_ITEMS_LOADER.poll_all():
            if result.get("error") is None and result.get("value") is not None:
                order_items_cache[result["key"]] = result["value"]
        for result in _ORDER_SHIPMENTS_LOADER.poll_all():
            if result.get("error") is None and result.get("value") is not None:
                order_shipments_cache[result["key"]] = result["value"]
        loader_result = _ORDERS_SNAPSHOT_LOADER.poll()
        if loader_result and loader_result.get("key") == "orders_snapshot":
            orders_snapshot_reload_pending = False
            if loader_result.get("error") is None and loader_result.get("value") is not None:
                orders_snapshot = loader_result["value"]
                active_order_ids = {row.get("order_id") for row in orders_snapshot if row.get("order_id")}
                order_items_cache = {key: value for key, value in order_items_cache.items() if key in active_order_ids}
                order_shipments_cache = {key: value for key, value in order_shipments_cache.items() if key in active_order_ids}
                rebuild_orders_view = True
                last_orders_snapshot_refresh_at = loader_result["loaded_at"]

        try:
            if reload_orders_snapshot:
                if not orders_snapshot:
                    orders_snapshot = _load_orders_snapshot()
                    active_order_ids = {row.get("order_id") for row in orders_snapshot if row.get("order_id")}
                    order_items_cache = {key: value for key, value in order_items_cache.items() if key in active_order_ids}
                    order_shipments_cache = {key: value for key, value in order_shipments_cache.items() if key in active_order_ids}
                    last_orders_snapshot_refresh_at = time.monotonic()
                    rebuild_orders_view = True
                elif not orders_snapshot_reload_pending:
                    _ORDERS_SNAPSHOT_LOADER.request("orders_snapshot", _load_orders_snapshot)
                    orders_snapshot_reload_pending = True
                reload_orders_snapshot = False
            if rebuild_orders_view:
                orders = _filter_orders_snapshot(
                    orders_snapshot,
                    order_filter=order_filter,
                    only_pending=only_pending,
                    fulfillment_filter=fulfillment_filter,
                    payment_filter=payment_filter,
                )
                selected_order_ids = {order_id for order_id in selected_order_ids if any(row["order_id"] == order_id for row in orders)}
                rebuild_orders_view = False
        except (DatabaseUnavailableError, DatabaseBusyError) as exc:
            if not database_connection_dialog(stdscr, str(exc)):
                return
            reload_orders_snapshot = True
            continue

        if selected >= len(orders):
            selected = len(orders) - 1
        if selected < 0:
            selected = 0

        selected_order = orders[selected] if orders else None
        selected_order_id = selected_order["order_id"] if selected_order else None

        try:
            prefetch_order_ids = _prefetch_order_ids(orders, selected, ahead=5, behind=1)
            for order_id in prefetch_order_ids:
                if order_id not in order_items_cache:
                    _ORDER_ITEMS_LOADER.request(order_id, get_order_items, order_id)
                if order_id not in order_shipments_cache:
                    _ORDER_SHIPMENTS_LOADER.request(order_id, list_shipping_labels, order_id)
        except (DatabaseUnavailableError, DatabaseBusyError) as exc:
            if not database_connection_dialog(stdscr, str(exc)):
                return
            reload_orders_snapshot = True
            continue

        order_items = order_items_cache.get(selected_order_id, [])
        order_shipments = order_shipments_cache.get(selected_order_id, [])

        h, w = stdscr.getmaxyx()
        width = min(max(88, int(w * 0.84)), w - 6)
        height = min(max(18, int(h * 0.82)), h - 4)
        y = max(1, (h - height) // 2)
        x = max(2, (w - width) // 2)

        win = curses.newwin(height, width, y, x)
        win.keypad(True)
        win.timeout(200)
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        win.addstr(0, 2, t("orders_title"))

        left_width = max(34, int((width - 3) * 0.42))
        right_width = width - left_width - 3
        list_height = height - 4

        orders_win = win.derwin(list_height, left_width, 1, 1)
        details_win = win.derwin(list_height, right_width, 1, 2 + left_width)

        order_lines = []
        for order in orders:
            location_count = int(order.get("shopify_location_count") or 0)
            if location_count > 0:
                open_hint = "[!]" if int(order.get("active_location_remaining_qty") or 0) > 0 else "   "
            else:
                status_value = (order.get("fulfillment_status") or "").strip().lower()
                open_hint = "[!]" if status_value not in {"fulfilled", "cancelled"} else "   "
            mark = "[x]" if order["order_id"] in selected_order_ids else "[ ]"
            order_lines.append(
                f"{mark}{open_hint} {_fit(order['order_name'], 10)} {_fit(format_address(order), left_width - 19)}"
            )
        if not order_lines:
            order_lines = [t("orders_none")]

        if selected < top_index:
            top_index = selected
        if selected >= top_index + max(1, list_height - 2):
            top_index = selected - max(1, list_height - 2) + 1

        draw_panel(orders_win, t("orders_panel_title"), order_lines, selected if orders else 0, top_index, True)

        detail_lines = []
        if selected_order:
            selected_weight_kg, selected_weight_grams = calculate_order_shipping_weight(selected_order, order_items)
            country = _localized_country_display(selected_order.get("shipping_country"))
            created_at = selected_order.get("created_at")
            if isinstance(created_at, datetime.datetime):
                ordered_at_text = created_at.strftime("%d.%m.%Y %H:%M")
            else:
                ordered_at_text = "-"
            detail_lines.append(_fit(t("orders_detail_order", value=selected_order["order_name"]), right_width - 2))
            detail_lines.append(_fit(format_address(selected_order), right_width - 2))
            detail_lines.append(_fit(t("orders_detail_country", value=country), right_width - 2))
            detail_lines.append(_fit(t("orders_detail_email", value=selected_order.get("shipping_email") or "-"), right_width - 2))
            detail_lines.append(_fit(t("orders_detail_phone", value=selected_order.get("shipping_phone") or "-"), right_width - 2))
            detail_lines.append(_fit(t("orders_detail_ordered_at", value=ordered_at_text), right_width - 2))
            status = _localized_fulfillment_status(selected_order["fulfillment_status"])
            payment_status = _localized_payment_status(selected_order["payment_status"])
            internal_qty = selected_order.get("local_internal_qty") or 0
            active_location_label = _ACTIVE_SHOPIFY_LOCATION_NAME or _active_shopify_location_id() or "-"
            active_location_qty = int(selected_order.get("active_location_internal_qty") or 0)
            active_location_remaining_qty = int(selected_order.get("active_location_remaining_qty") or 0)
            shopify_location_count = int(selected_order.get("shopify_location_count") or 0)
            detail_lines.append(_fit(t("orders_detail_status", value=status), right_width - 2))
            detail_lines.append(_fit(t("orders_detail_payment", value=payment_status), right_width - 2))
            detail_lines.append(_fit(t("orders_detail_internal_qty", value=internal_qty), right_width - 2))
            detail_lines.append(_fit(t("orders_detail_shopify_location", value=active_location_label), right_width - 2))
            detail_lines.append(_fit(t("orders_detail_location_qty", value=active_location_qty), right_width - 2))
            detail_lines.append(_fit(t("orders_detail_location_open_qty", value=active_location_remaining_qty), right_width - 2))
            if shopify_location_count > 1:
                detail_lines.append(_fit(t("orders_detail_location_split", value=shopify_location_count), right_width - 2))
            detail_lines.append(_fit(t("orders_detail_shipping_weight", grams=selected_weight_grams, kg=selected_weight_kg), right_width - 2))
            if selected_order_id and selected_order_id not in order_shipments_cache:
                detail_lines.append(_fit(t("orders_detail_shipments_loading"), right_width - 2))
            else:
                detail_lines.extend(_shipment_summary_lines(order_shipments, right_width - 13))
            detail_lines.append("")
            qty_width, sku_width, regal_width, fach_width, platz_width, title_width = format_order_item_header(right_width - 2)
            detail_lines.append(
                t(
                    "orders_detail_items_header",
                    qty=_fit("Off/Ges", qty_width),
                    sku=_fit("SKU", sku_width),
                    item=_fit(t("items_panel"), title_width),
                    regal=_fit(t("field_regal_short"), regal_width),
                    fach=_fit(t("field_fach_short"), fach_width),
                    platz=_fit(t("field_platz_short"), platz_width),
                )
            )
            detail_lines.append("-" * max(1, right_width - 2))

            if selected_order_id and selected_order_id not in order_items_cache:
                detail_lines.append(_fit(t("orders_detail_positions_loading"), right_width - 2))
            else:
                if not order_items and shopify_location_count > 0:
                    detail_lines.append(_fit(t("orders_detail_no_location_positions"), right_width - 2))
                for row in order_items:
                    detail_lines.append(format_order_item_row(row, right_width - 2))
        else:
            detail_lines.append(t("order_not_found"))

        draw_panel(details_win, t("orders_positions_panel"), detail_lines, 0, 0, False)

        footer = t("orders_footer")
        filter_tags = []
        if order_filter:
            filter_tags.append(t("orders_filter_text", value=order_filter))
        if only_pending:
            filter_tags.append(t("orders_filter_only_open"))
        if fulfillment_filter != "all":
            filter_tags.append(_fulfillment_filter_label(fulfillment_filter).replace("Status: ", ""))
        if payment_filter != "all":
            filter_tags.append(_payment_filter_label(payment_filter).replace("Zahlung: ", ""))
        if filter_tags:
            footer = f" Filter[{', '.join(filter_tags)}] " + footer
        win.attrset(curses.color_pair(3))
        draw_footer_line(win, height - 1, 1, width - 2, footer)
        win.refresh()

        try:
            key = win.get_wch()
        except curses.error:
            if should_refresh_orders(last_orders_snapshot_refresh_at):
                reload_orders_snapshot = True
            continue

        if key in (27, curses.KEY_F9):
            try:
                curses.curs_set(1)
            except curses.error:
                pass
            return
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
            rebuild_orders_view = True
        elif key == curses.KEY_F2:
            current_index = FULFILLMENT_FILTER_SEQUENCE.index(fulfillment_filter) if fulfillment_filter in FULFILLMENT_FILTER_SEQUENCE else 0
            fulfillment_filter = FULFILLMENT_FILTER_SEQUENCE[(current_index + 1) % len(FULFILLMENT_FILTER_SEQUENCE)]
            selected = 0
            top_index = 0
            rebuild_orders_view = True
        elif key == curses.KEY_F3:
            current_index = PAYMENT_FILTER_SEQUENCE.index(payment_filter) if payment_filter in PAYMENT_FILTER_SEQUENCE else 0
            payment_filter = PAYMENT_FILTER_SEQUENCE[(current_index + 1) % len(PAYMENT_FILTER_SEQUENCE)]
            selected = 0
            top_index = 0
            rebuild_orders_view = True
        elif key == curses.KEY_F4:
            value = order_jump_dialog(stdscr, order_filter or "")
            try:
                curses.curs_set(0)
            except curses.error:
                pass
            if value is not None:
                order_filter = value or None
                selected = 0
                top_index = 0
                rebuild_orders_view = True
                if value:
                    matched_orders = _filter_orders_snapshot(
                        orders_snapshot,
                        order_filter=order_filter,
                        only_pending=only_pending,
                        fulfillment_filter=fulfillment_filter,
                        payment_filter=payment_filter,
                    )
                    target_index = jump_to_order(matched_orders, value)
                    orders = matched_orders
                    rebuild_orders_view = False
                    if target_index is not None:
                        selected = target_index
        elif key == curses.KEY_F5 and selected_order:
            try:
                create_shipping_label_for_order(stdscr, selected_order)
                order_shipments_cache.pop(selected_order_id, None)
                try:
                    curses.curs_set(0)
                except curses.error:
                    pass
            except DatabaseUnavailableError as exc:
                if not database_connection_dialog(stdscr, str(exc)):
                    try:
                        curses.curs_set(1)
                    except curses.error:
                        pass
                    return
                reload_orders_snapshot = True
        elif key in (curses.KEY_F17, curses.KEY_F20, "m", "M"):
            try:
                create_manual_shipping_label(stdscr)
                try:
                    curses.curs_set(0)
                except curses.error:
                    pass
            except DatabaseUnavailableError as exc:
                if not database_connection_dialog(stdscr, str(exc)):
                    try:
                        curses.curs_set(1)
                    except curses.error:
                        pass
                    return
                reload_orders_snapshot = True
        elif key == curses.KEY_F6 and selected_order:
            try:
                order_items = ensure_order_items_loaded(selected_order_id, order_items_cache)
                run_partial_execution_for_order(stdscr, selected_order, order_items)
                order_shipments_cache.pop(selected_order_id, None)
                order_items_cache.pop(selected_order_id, None)
                reload_orders_snapshot = True
                try:
                    curses.curs_set(0)
                except curses.error:
                    pass
            except DatabaseUnavailableError as exc:
                if not database_connection_dialog(stdscr, str(exc)):
                    try:
                        curses.curs_set(1)
                    except curses.error:
                        pass
                    return
                reload_orders_snapshot = True
        elif key in (curses.KEY_F19, "t", "T") and selected_order:
            try:
                order_items = ensure_order_items_loaded(selected_order_id, order_items_cache)
                run_partial_execution_for_order(stdscr, selected_order, order_items)
                order_shipments_cache.pop(selected_order_id, None)
                order_items_cache.pop(selected_order_id, None)
                reload_orders_snapshot = True
                try:
                    curses.curs_set(0)
                except curses.error:
                    pass
            except DatabaseUnavailableError as exc:
                if not database_connection_dialog(stdscr, str(exc)):
                    try:
                        curses.curs_set(1)
                    except curses.error:
                        pass
                    return
                reload_orders_snapshot = True
        elif key == curses.KEY_F7:
            try:
                run_bulk_execution(stdscr, orders, order_items_cache, selected_order_ids)
                reload_orders_snapshot = True
                order_items_cache = {}
                order_shipments_cache = {}
                try:
                    curses.curs_set(0)
                except curses.error:
                    pass
            except DatabaseUnavailableError as exc:
                if not database_connection_dialog(stdscr, str(exc)):
                    try:
                        curses.curs_set(1)
                    except curses.error:
                        pass
                    return
                reload_orders_snapshot = True
        elif key == curses.KEY_F8:
            try:
                shipping_history_dialog(stdscr, selected_order)
                order_shipments_cache.pop(selected_order_id, None)
                try:
                    curses.curs_set(0)
                except curses.error:
                    pass
            except DatabaseUnavailableError as exc:
                if not database_connection_dialog(stdscr, str(exc)):
                    try:
                        curses.curs_set(1)
                    except curses.error:
                        pass
                    return
                reload_orders_snapshot = True
        elif key == curses.KEY_F11 and selected_order:
            try:
                handle_delivery_note_output(stdscr, selected_order, order_items=None, order_items_cache=order_items_cache)
                try:
                    curses.curs_set(0)
                except curses.error:
                    pass
            except DatabaseUnavailableError as exc:
                if not database_connection_dialog(stdscr, str(exc)):
                    try:
                        curses.curs_set(1)
                    except curses.error:
                        pass
                    return
                reload_orders_snapshot = True
        elif key == curses.KEY_F10 and selected_order:
            order_items = ensure_order_items_loaded(selected_order_id, order_items_cache)
            print_picklist(stdscr, selected_order, order_items)
        elif key == " " and selected_order:
            order_id = selected_order["order_id"]
            if order_id in selected_order_ids:
                selected_order_ids.remove(order_id)
            else:
                selected_order_ids.add(order_id)
        elif key in ("a", "A"):
            if not orders:
                continue
            if len(selected_order_ids) == len(orders):
                selected_order_ids.clear()
            else:
                selected_order_ids = {row["order_id"] for row in orders}


def inventory_count_dialog(stdscr, line):
    res = form_dialog(
        stdscr,
        t("inventory_qty_title"),
        [
            {"name": "soll", "label": t("field_soll_short"), "value": str(line["soll_menge"])},
            {"name": "ist", "label": t("field_ist_short"), "value": "" if line["ist_menge"] is None else str(line["ist_menge"])},
        ],
        initial_active=1,
    )

    if res is None:
            return None

    ist_raw = res["ist"].strip()
    if ist_raw == "":
        return None

    return parse_int_or_error(stdscr, ist_raw, t("field_ist_short"))


def inventory_dialog(stdscr):
    session = get_active_inventory_session()
    if session is None:
        if not confirm_box(stdscr, t("inventory_title"), t("inventory_start_new")):
            return False
        session = run_background_action_dialog(
            stdscr,
            t("inventory_title"),
            create_inventory_session,
            detail=t("inventory_create_detail"),
        )

    selected = 0
    top_index = 0
    show_differences = False
    lines = []
    all_lines = []
    reload_lines = True

    while True:
        if reload_lines:
            all_lines = get_inventory_lines(session["session_id"])
            if show_differences:
                lines = [row for row in all_lines if row["ist_menge"] is not None and row["ist_menge"] != row["soll_menge"]]
            else:
                lines = list(all_lines)
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
            t("inventory_title"),
            display_lines,
            selected + 2 if lines else 0,
            top_index,
            True,
        )

        total, counted, differences = inventory_session_summary(all_lines)
        footer = t("inventory_footer", total=total, counted=counted, differences=differences)
        win.attrset(curses.color_pair(3))
        draw_footer_line(win, height - 1, 1, width - 2, footer)
        win.refresh()

        win.timeout(200)
        try:
            key = win.get_wch()
        except curses.error:
            continue
        finally:
            win.timeout(-1)

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
            if confirm_box(stdscr, t("inventory_title"), t("inventory_new_archive")):
                session = run_background_action_dialog(
                    stdscr,
                    t("inventory_title"),
                    create_inventory_session,
                    detail=t("inventory_create_detail"),
                )
                selected = 0
                top_index = 0
                reload_lines = True
        elif key == curses.KEY_F3 and lines:
            qty = inventory_count_dialog(stdscr, lines[selected])
            if qty is not None:
                run_background_action_dialog(
                    stdscr,
                    t("inventory_title"),
                    lambda: set_inventory_count(session["session_id"], lines[selected]["line_no"], qty),
                    detail=t("inventory_line_save_detail"),
                )
                reload_lines = True
        elif key == curses.KEY_F4:
            path = export_inventory_csv(session, all_lines)
            message_box(stdscr, t("csv_export_title"), path[-56:])
        elif key == curses.KEY_F5:
            print_inventory_list(stdscr, session, all_lines)
        elif key == curses.KEY_F6:
            show_differences = not show_differences
            selected = 0
            top_index = 0
            reload_lines = True
        elif key == curses.KEY_F7:
            counted_now = sum(1 for row in all_lines if row["ist_menge"] is not None)
            if counted_now == 0:
                message_box(stdscr, t("inventory_title"), t("inventory_no_counts"))
            elif confirm_box(stdscr, t("inventory_title"), t("inventory_apply_confirm")):
                run_background_action_dialog(
                    stdscr,
                    t("inventory_title"),
                    lambda: apply_inventory_session(session["session_id"]),
                    detail=t("inventory_apply_detail"),
                )
                message_box(stdscr, t("inventory_title"), t("inventory_applied"))
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
    shopify_locations = []
    active_shopify_location = None
    items_snapshot = []
    last_items_snapshot_refresh_at = None
    items = []
    location_rows = []
    reload_items_snapshot = True
    items_snapshot_reload_pending = False
    rebuild_items_view = True
    sync_state = get_service_runtime_state(max_age_seconds=999999)
    sync_status_refresh_pending = False
    last_sync_status_request_at = 0.0
    transient_notice = None
    transient_notice_until = 0.0

    while True:
        for event in poll_background_ui_events():
            message = (event.get("message") or "").strip()
            if message:
                transient_notice = message
                transient_notice_until = time.monotonic() + (8.0 if event.get("error") is not None else 3.0)
            if event.get("type") == "items_reload":
                reload_items_snapshot = True

        runtime_result = _SERVICE_RUNTIME_LOADER.poll()
        if runtime_result and runtime_result.get("key") == "service_runtime_state":
            sync_status_refresh_pending = False
            if runtime_result.get("error") is None:
                sync_state = runtime_result.get("value")
                last_sync_status_request_at = runtime_result.get("loaded_at") or time.monotonic()
        loader_result = _ITEMS_SNAPSHOT_LOADER.poll()
        if loader_result and loader_result.get("key") == "items_snapshot":
            items_snapshot_reload_pending = False
            if loader_result.get("error") is None and loader_result.get("value") is not None:
                items_snapshot = loader_result["value"]
                shopify_locations = get_shopify_locations_snapshot()
                active_shopify_location = _resolve_active_shopify_location(
                    shopify_locations,
                    active_shopify_location["location_id"] if active_shopify_location else None,
                )
                last_items_snapshot_refresh_at = loader_result["loaded_at"]
                rebuild_items_view = True
        if not sync_status_refresh_pending and (
            sync_state is None or (time.monotonic() - last_sync_status_request_at) >= SERVICE_RUNTIME_CACHE_SECONDS
        ):
            _SERVICE_RUNTIME_LOADER.request(
                "service_runtime_state",
                get_service_runtime_state,
                SHOPIFY_SYNC_SERVICE,
                SERVICE_RUNTIME_CACHE_SECONDS,
                True,
            )
            sync_status_refresh_pending = True

        if reload_items_snapshot:
            try:
                shopify_locations = get_shopify_locations_snapshot()
                active_shopify_location = _resolve_active_shopify_location(
                    shopify_locations,
                    active_shopify_location["location_id"] if active_shopify_location else None,
                )
                if not items_snapshot:
                    items_snapshot = _load_items_snapshot(
                        active_shopify_location["location_id"] if active_shopify_location else None
                    )
                    last_items_snapshot_refresh_at = time.monotonic()
                    rebuild_items_view = True
                elif not items_snapshot_reload_pending:
                    _ITEMS_SNAPSHOT_LOADER.request(
                        "items_snapshot",
                        _load_items_snapshot,
                        active_shopify_location["location_id"] if active_shopify_location else None,
                    )
                    items_snapshot_reload_pending = True
                reload_items_snapshot = False
            except DatabaseUnavailableError as exc:
                if not database_connection_dialog(stdscr, str(exc)):
                    return
                reload_items_snapshot = True
                continue

        if rebuild_items_view:
            items = _filter_items_snapshot(
                items_snapshot,
                filter_text=filter_text,
                filter_no_location=filter_no_location,
                filter_local=filter_local,
                sort_mode=sort_mode,
                external_mode=external_mode,
            )
            location_rows = build_location_rows(items)
            rebuild_items_view = False

        if left_selected >= len(items):
            left_selected = len(items) - 1

        if left_selected < 0:
            left_selected = 0

        if right_selected >= len(location_rows):
            right_selected = len(location_rows) - 1

        if right_selected < 0:
            right_selected = 0

        h, _ = stdscr.getmaxyx()
        left_visible_rows = max(1, h - 5)
        right_visible_rows = max(1, h - 4)

        left_top_index = clamp_top_index(left_selected, left_top_index, left_visible_rows)
        right_top_index = clamp_top_index(right_selected, right_top_index, right_visible_rows)

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
            active_shopify_location,
            format_shopify_sync_status_label(sync_state),
            notice_text=(transient_notice if transient_notice and time.monotonic() < transient_notice_until else None),
        )
        if transient_notice and time.monotonic() >= transient_notice_until:
            transient_notice = None

        stdscr.timeout(200)
        try:
            key = stdscr.get_wch()
        except curses.error:
            if should_refresh_orders(last_items_snapshot_refresh_at, interval_seconds=ITEMS_AUTO_REFRESH_SECONDS):
                reload_items_snapshot = True
            continue
        finally:
            stdscr.timeout(-1)
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
                left_selected = move_selection(items, left_selected, left_visible_rows)
            else:
                right_selected = move_selection(location_rows, right_selected, right_visible_rows)

        elif key == curses.KEY_PPAGE:
            if active_pane == "left":
                left_selected = move_selection(items, left_selected, -left_visible_rows)
            else:
                right_selected = move_selection(location_rows, right_selected, -right_visible_rows)

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
            rebuild_items_view = True

        elif key == curses.KEY_F2:
            if _shopify_location_switch_enabled():
                previous_location = _cycle_shopify_location(
                    shopify_locations,
                    active_shopify_location["location_id"] if active_shopify_location else None,
                    -1,
                )
                if previous_location is not None:
                    active_shopify_location = previous_location
                    left_selected = 0
                    left_top_index = 0
                    right_selected = 0
                    right_top_index = 0
                    reload_items_snapshot = True
            else:
                try:
                    add_item(stdscr)
                    reload_items_snapshot = True
                except DatabaseUnavailableError as exc:
                    if not database_connection_dialog(stdscr, str(exc)):
                        return
                    reload_items_snapshot = True

        elif key == curses.KEY_F3:
            if _shopify_location_switch_enabled():
                next_location = _cycle_shopify_location(
                    shopify_locations,
                    active_shopify_location["location_id"] if active_shopify_location else None,
                    1,
                )
                if next_location is not None:
                    active_shopify_location = next_location
                    left_selected = 0
                    left_top_index = 0
                    right_selected = 0
                    right_top_index = 0
                    reload_items_snapshot = True
            else:
                try:
                    if inventory_dialog(stdscr):
                        reload_items_snapshot = True
                except DatabaseUnavailableError as exc:
                    if not database_connection_dialog(stdscr, str(exc)):
                        return
                    reload_items_snapshot = True

        elif key == curses.KEY_F4 and selected_item:
            item_info_dialog(stdscr, selected_item)

        elif key == curses.KEY_F5:
            try:
                add_item(stdscr)
                reload_items_snapshot = True
            except DatabaseUnavailableError as exc:
                if not database_connection_dialog(stdscr, str(exc)):
                    return
                reload_items_snapshot = True

        elif key == curses.KEY_F6 and selected_item:
            try:
                new_location = change_location(stdscr, selected_item)
                if new_location is not None:
                    updated_row = _update_item_snapshot_location(
                        items_snapshot,
                        selected_item["sku"],
                        new_location["regal"],
                        new_location["fach"],
                        new_location["platz"],
                    )
                    if updated_row is not None:
                        if updated_row is not selected_item:
                            _apply_item_local_location(
                                selected_item,
                                new_location["regal"],
                                new_location["fach"],
                                new_location["platz"],
                            )
                        rebuild_items_view = True
                    transient_notice = t("item_write_pending_location", sku=_display_sku_value(selected_item))
                    transient_notice_until = time.monotonic() + 3.0
            except DatabaseUnavailableError as exc:
                if not database_connection_dialog(stdscr, str(exc)):
                    return
                reload_items_snapshot = True

        elif key == curses.KEY_F7 and selected_item:
            try:
                new_qty = change_qty(stdscr, selected_item)
                if new_qty is not None:
                    updated_row = _update_item_snapshot_quantity(items_snapshot, selected_item["sku"], new_qty)
                    if updated_row is not None:
                        if updated_row is not selected_item:
                            _apply_item_local_quantity(selected_item, new_qty)
                        rebuild_items_view = True
                    transient_notice = t("item_write_pending_qty", sku=_display_sku_value(selected_item))
                    transient_notice_until = time.monotonic() + 3.0
            except DatabaseUnavailableError as exc:
                if not database_connection_dialog(stdscr, str(exc)):
                    return
                reload_items_snapshot = True

        elif key == curses.KEY_F8 and selected_item:
            print_label(stdscr, selected_item)

        elif key == curses.KEY_F1 + 12:
            try:
                if inventory_dialog(stdscr):
                    reload_items_snapshot = True
            except DatabaseUnavailableError as exc:
                if not database_connection_dialog(stdscr, str(exc)):
                    return
                reload_items_snapshot = True

        elif key == curses.KEY_F2 + 12:
            filter_local = not filter_local
            left_selected = 0
            left_top_index = 0
            right_selected = 0
            right_top_index = 0
            rebuild_items_view = True

        elif key == curses.KEY_F3 + 12:
            filter_no_location = not filter_no_location
            left_selected = 0
            left_top_index = 0
            right_selected = 0
            right_top_index = 0
            rebuild_items_view = True

        elif key == curses.KEY_F5 + 12 and selected_item:
            try:
                edit_item(stdscr, selected_item)
                reload_items_snapshot = True
            except DatabaseUnavailableError as exc:
                if not database_connection_dialog(stdscr, str(exc)):
                    return
                reload_items_snapshot = True

        elif key == curses.KEY_F8 + 12 and selected_item:
            print_label_multiple(stdscr, selected_item)

        elif key == curses.KEY_F11 + 12:
            settings_dialog(stdscr)
            shopify_locations = get_shopify_locations_snapshot()
            active_shopify_location = _resolve_active_shopify_location(
                shopify_locations,
                (SETTINGS.get("shopify_active_location_id") or "").strip() or None,
            )
            reload_items_snapshot = True

        elif key == curses.KEY_F9:
            filter_text = None
            filter_no_location = False
            filter_local = False
            external_mode = "hide"
            left_selected = 0
            left_top_index = 0
            right_selected = 0
            right_top_index = 0
            rebuild_items_view = True

        elif key == curses.KEY_F10:
            pending_count = _pending_item_write_count()
            if pending_count > 0:
                decision = pending_item_write_exit_dialog(stdscr)
                if decision == "wait":
                    LOGGER.info(
                        "Programmende wartet auf offene DB-Schreibaktionen anzahl=%s skus=%s",
                        pending_count,
                        _pending_item_write_skus(),
                    )
                    wait_for_pending_item_writes_dialog(stdscr)
                    break
                if decision == "force":
                    LOGGER.warning(
                        "Programmende erzwingt Abbruch mit offenen DB-Schreibaktionen anzahl=%s skus=%s",
                        pending_count,
                        _pending_item_write_skus(),
                    )
                    break
                LOGGER.info(
                    "Programmende abgebrochen wegen offener DB-Schreibaktionen anzahl=%s skus=%s",
                    pending_count,
                    _pending_item_write_skus(),
                )
                continue
            break

        elif key == curses.KEY_F11:
            show_secondary_help = not show_secondary_help

        elif key == curses.KEY_F12:
            try:
                orders_dialog(stdscr)
            except DatabaseUnavailableError as exc:
                if not database_connection_dialog(stdscr, str(exc)):
                    return

        elif key in (curses.KEY_BACKSPACE, 127, 8, '\x7f', '\b'):

            if filter_text:
                filter_text = filter_text[:-1]

            if filter_text == "":
                filter_text = None

            left_selected = 0
            left_top_index = 0
            right_selected = 0
            right_top_index = 0
            rebuild_items_view = True

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
            rebuild_items_view = True


try:
    LOGGER.debug("Starte lager_mc")
    curses.wrapper(main)
except Exception:
    LOGGER.exception("lager_mc Start oder Laufzeitfehler")
    raise
