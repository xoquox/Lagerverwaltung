#!/usr/bin/env python3
import curses
import csv
import datetime
import html
import os
import psycopg2
import psycopg2.extras
import locale
import re
import subprocess
import string
import tempfile
import textwrap
from pathlib import Path
from urllib.parse import urlparse

from app_logging import MAIN_LOG_PATH, PRINT_LOG_PATH, get_logger
from app_settings import DEFAULT_SETTINGS, load_settings, save_settings
from delivery_note import build_delivery_note_pdf, build_delivery_note_rows

locale.setlocale(locale.LC_ALL, "")

SETTINGS = load_settings()
LOGGER = get_logger("lager_mc")
PRINT_LOGGER = get_logger("print")


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
            fulfillment_status text,
            payment_status text,
            updated_at timestamptz NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute("ALTER TABLE shopify_orders ADD COLUMN IF NOT EXISTS shipping_country text")
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
            sku text,
            title text NOT NULL,
            quantity integer NOT NULL,
            PRIMARY KEY (order_id, line_index)
        )
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


def get_orders(order_filter=None):
    con = db()
    cur = con.cursor()

    where = ""
    params = []

    if order_filter:
        where = """
        WHERE REPLACE(order_name, '#', '') ILIKE %s
           OR COALESCE(shipping_name, '') ILIKE %s
           OR COALESCE(shipping_city, '') ILIKE %s
        """
        match = f"%{order_filter.replace('#', '')}%"
        params = [match, f"%{order_filter}%", f"%{order_filter}%"]

    cur.execute(
        f"""
        SELECT
            order_id,
            order_name,
            created_at,
            shipping_name,
            shipping_address1,
            shipping_zip,
            shipping_city,
            shipping_country,
            fulfillment_status,
            payment_status
        FROM shopify_orders
        {where}
        ORDER BY created_at DESC NULLS LAST, order_name DESC
        """,
        params,
    )
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def get_order_items(order_id):
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        SELECT
            oi.line_index,
            oi.sku,
            oi.title,
            oi.quantity,
            i.regal,
            i.fach,
            i.platz,
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
    return rows


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
    cells = [_fit(name, width) for name, width in COLS]
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
    panel_title = " Artikel "

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
    stdscr.addstr(0, 2, " Lagerverwaltung ")
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

    right_lines = [row["label"] for row in location_rows] if location_rows else ["Keine Lagerplaetze"]
    draw_panel(
        right_win,
        "Regale",
        right_lines,
        right_selected if location_rows else 0,
        right_top_index,
        active_pane == "right",
    )

    stdscr.attrset(curses.color_pair(3))

    if show_secondary_help:
        status = " Shift+F1 Inventur  Shift+F5 Bearb.  Shift+F8 Multi-Label  Shift+F11 Einst.  F11 Standard  F12 Auftraege  F10 Ende "
    else:
        status = " Tab Fokus  F1 Sortieren  F2 Lokal  F3 Ohne  F4 Info  F5 Neu  F6 Platz  F7 Menge  F8 Label  F9 Reset  F10 Ende  F11 Mehr  F12 Auftraege "
    focus = " Fokus: Artikel " if active_pane == "left" else " Fokus: Regale "
    if external_mode == "only":
        focus = focus[:-1] + " | Ansicht: Extern "

    stdscr.addstr(h-2, 0, " "*(w-1))

    if filter_text:
        stdscr.addstr(h-2, 0, f" Filter: {filter_text} "[:w-1])
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
    win.addstr(4, 2, "Taste drücken …")

    win.refresh()
    key = stdscr.get_wch()


def confirm_box(stdscr, title, message):

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
    win.addstr(4, 2, "[J]a / [N]ein")

    win.refresh()

    while True:

        key = win.get_wch()

        if key in ("j", "J", "y", "Y", '\n', '\r', 10, 13, curses.KEY_ENTER):
            return True

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

        win.addstr(0, 2, " Suche ")

        win.addstr(2, 2, "Suche:")

        win.attron(curses.color_pair(2))

        field_width = width - 12

        field = value[-field_width:]
        win.addstr(2, 10, field[:field_width].ljust(field_width))
        
        win.attroff(curses.color_pair(2))

        win.addstr(height-1, 2, "Enter suchen  F9 Abbrechen")

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


def get_cups_printers():
    try:
        PRINT_LOGGER.debug("Lade Drucker mit lpstat -p")
        result = subprocess.run(
            ["lpstat", "-p"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
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
        )
        prefix = "system default destination: "
        for line in default_result.stdout.splitlines():
            if line.startswith(prefix):
                default_printer = line[len(prefix):].strip() or None
                break
    except (FileNotFoundError, subprocess.CalledProcessError):
        default_printer = None

    return printers, default_printer, None


def cups_printer_dialog(stdscr, current_printer):
    selected_name = current_printer.strip()

    while True:
        printers, default_printer, error = get_cups_printers()
        if error:
            message_box(stdscr, "Drucker Fehler", error[:56])
            return current_printer

        options = [{"name": "", "detail": "Keinen Drucker auswaehlen"}]
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
            win.addstr(0, 2, " Drucker ")

            visible_rows = max(1, height - 4)
            if selected < top_index:
                top_index = selected
            if selected >= top_index + visible_rows:
                top_index = selected - visible_rows + 1

            lines = []
            for printer in options:
                name = printer["name"] or "(leer)"
                markers = []
                if printer["name"] == current_printer:
                    markers.append("aktiv")
                if printer["name"] and printer["name"] == default_printer:
                    markers.append("default")
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
            footer = "Enter waehlen  F5 Neu laden  F9 Zurueck"
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


def settings_dialog(stdscr):
    global SETTINGS

    values = {
        "db_host": SETTINGS["db_host"],
        "db_name": SETTINGS["db_name"],
        "db_user": SETTINGS["db_user"],
        "db_pass": SETTINGS["db_pass"],
        "printer_uri": SETTINGS["printer_uri"],
        "printer_model": SETTINGS["printer_model"],
        "label_size": SETTINGS["label_size"],
        "location_regex_regal": SETTINGS.get("location_regex_regal", DEFAULT_SETTINGS["location_regex_regal"]),
        "location_regex_fach": SETTINGS.get("location_regex_fach", DEFAULT_SETTINGS["location_regex_fach"]),
        "location_regex_platz": SETTINGS.get("location_regex_platz", DEFAULT_SETTINGS["location_regex_platz"]),
        "picklist_printer": SETTINGS["picklist_printer"],
        "delivery_note_printer": SETTINGS["delivery_note_printer"],
        "pdf_output_dir": SETTINGS["pdf_output_dir"],
        "delivery_note_template_path": SETTINGS.get("delivery_note_template_path", ""),
        "delivery_note_logo_source": SETTINGS.get("delivery_note_logo_source", ""),
        "delivery_note_sender_name": SETTINGS["delivery_note_sender_name"],
        "delivery_note_sender_street": SETTINGS["delivery_note_sender_street"],
        "delivery_note_sender_city": SETTINGS["delivery_note_sender_city"],
        "delivery_note_sender_email": SETTINGS["delivery_note_sender_email"],
    }
    active = 0

    while True:
        res = form_dialog(
            stdscr,
            "Einstellungen",
            [
                {"name": "db_host", "label": "DB Host", "value": values["db_host"]},
                {"name": "db_name", "label": "DB Name", "value": values["db_name"]},
                {"name": "db_user", "label": "DB User", "value": values["db_user"]},
                {"name": "db_pass", "label": "DB Passwort", "value": values["db_pass"]},
                {"name": "printer_uri", "label": "Drucker URI", "value": values["printer_uri"]},
                {"name": "printer_model", "label": "Drucker Modell", "value": values["printer_model"]},
                {"name": "label_size", "label": "Labelformat", "value": values["label_size"]},
                {"name": "location_regex_regal", "label": "Regex Regal", "value": values["location_regex_regal"]},
                {"name": "location_regex_fach", "label": "Regex Fach", "value": values["location_regex_fach"]},
                {"name": "location_regex_platz", "label": "Regex Platz", "value": values["location_regex_platz"]},
                {"name": "picklist_printer", "label": "Pickliste Drucker", "value": values["picklist_printer"]},
                {"name": "delivery_note_printer", "label": "Lieferschein Drucker", "value": values["delivery_note_printer"]},
                {"name": "pdf_output_dir", "label": "PDF Ordner", "value": values["pdf_output_dir"]},
                {"name": "delivery_note_template_path", "label": "LS Vorlage", "value": values["delivery_note_template_path"]},
                {"name": "delivery_note_logo_source", "label": "LS Logo URL/Pfad", "value": values["delivery_note_logo_source"]},
                {"name": "delivery_note_sender_name", "label": "LS Name", "value": values["delivery_note_sender_name"]},
                {"name": "delivery_note_sender_street", "label": "LS Strasse", "value": values["delivery_note_sender_street"]},
                {"name": "delivery_note_sender_city", "label": "LS Ort", "value": values["delivery_note_sender_city"]},
                {"name": "delivery_note_sender_email", "label": "LS E-Mail", "value": values["delivery_note_sender_email"]},
            ],
            initial_active=active,
            footer_text="Enter weiter  ↑↓ wechseln  F2 Speichern  F3 Drucker  F9 Abbrechen",
            extra_actions=[
                {"name": "cups_printer_select", "keys": (curses.KEY_F3,)},
            ],
        )

        if res is None:
            return

        if "__action__" in res:
            values.update(res["__values__"])
            active = res["__active__"]

            if res["__action__"] == "cups_printer_select":
                field_names = [
                    "db_host",
                    "db_name",
                    "db_user",
                    "db_pass",
                    "printer_uri",
                    "printer_model",
                    "label_size",
                    "location_regex_regal",
                    "location_regex_fach",
                    "location_regex_platz",
                    "picklist_printer",
                    "delivery_note_printer",
                    "pdf_output_dir",
                    "delivery_note_template_path",
                    "delivery_note_logo_source",
                    "delivery_note_sender_name",
                    "delivery_note_sender_street",
                    "delivery_note_sender_city",
                    "delivery_note_sender_email",
                ]
                active_field = field_names[active] if 0 <= active < len(field_names) else "picklist_printer"
                if active_field not in {"picklist_printer", "delivery_note_printer"}:
                    active_field = "picklist_printer"
                values[active_field] = cups_printer_dialog(stdscr, values[active_field])

            continue

        break

    updated = {
        "db_host": res["db_host"].strip(),
        "db_name": res["db_name"].strip(),
        "db_user": res["db_user"].strip(),
        "db_pass": res["db_pass"],
        "printer_uri": res["printer_uri"].strip(),
        "printer_model": res["printer_model"].strip(),
        "label_size": res["label_size"].strip(),
        "location_regex_regal": res["location_regex_regal"].strip(),
        "location_regex_fach": res["location_regex_fach"].strip(),
        "location_regex_platz": res["location_regex_platz"].strip(),
        "picklist_printer": res["picklist_printer"].strip(),
        "delivery_note_printer": res["delivery_note_printer"].strip(),
        "pdf_output_dir": os.path.expanduser(res["pdf_output_dir"].strip()),
        "delivery_note_template_path": os.path.expanduser(res["delivery_note_template_path"].strip()),
        "delivery_note_logo_source": res["delivery_note_logo_source"].strip(),
        "delivery_note_sender_name": res["delivery_note_sender_name"].strip(),
        "delivery_note_sender_street": res["delivery_note_sender_street"].strip(),
        "delivery_note_sender_city": res["delivery_note_sender_city"].strip(),
        "delivery_note_sender_email": res["delivery_note_sender_email"].strip(),
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
        message_box(stdscr, "Fehler", f"Felder fehlen: {', '.join(missing)}")
        return

    if updated["pdf_output_dir"] and not os.path.isdir(updated["pdf_output_dir"]):
        message_box(stdscr, "Fehler", "PDF Ordner existiert nicht.")
        return
    if updated["delivery_note_template_path"] and not os.path.isfile(updated["delivery_note_template_path"]):
        message_box(stdscr, "Fehler", "LS Vorlage existiert nicht.")
        return
    for key, label in [
        ("location_regex_regal", "Regex Regal"),
        ("location_regex_fach", "Regex Fach"),
        ("location_regex_platz", "Regex Platz"),
    ]:
        if not updated[key]:
            message_box(stdscr, "Fehler", f"{label} darf nicht leer sein.")
            return
        try:
            re.compile(updated[key])
        except re.error as exc:
            message_box(stdscr, "Fehler", f"{label} ungueltig: {exc}"[:56])
            return
    if updated["delivery_note_logo_source"]:
        logo_source = updated["delivery_note_logo_source"]
        if not is_http_url(logo_source):
            logo_path = os.path.expanduser(logo_source)
            if not os.path.isfile(logo_path):
                message_box(stdscr, "Fehler", "LS Logo Datei existiert nicht.")
                return
            updated["delivery_note_logo_source"] = logo_path

    try:
        test_db_connection(updated)
    except Exception as exc:
        message_box(stdscr, "DB Fehler", str(exc)[:56])
        return

    SETTINGS = save_settings(updated)
    message_box(stdscr, "Gespeichert", "Einstellungen wurden gespeichert.")

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
        subprocess.run(
            [
                "python3",
                "label_print.py",
                item["sku"],
                item["name"],
                str(item["menge"]),
                item["regal"] or "",
                item["fach"] or "",
                item["platz"] or "",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        PRINT_LOGGER.exception("Labeldruck fehlgeschlagen fuer SKU=%s", item["sku"])
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
            subprocess.run(
                [
                    "python3",
                    "label_print.py",
                    item["sku"],
                    item["name"],
                    str(item["menge"]),
                    item["regal"] or "",
                    item["fach"] or "",
                    item["platz"] or "",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            PRINT_LOGGER.exception("Mehrfachdruck fehlgeschlagen fuer SKU=%s", item["sku"])
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
    qty_width = 5
    sku_width = 18
    regal_width = 5
    fach_width = 5
    platz_width = 5
    used = qty_width + sku_width + regal_width + fach_width + platz_width + 8
    title_width = max(10, width - used)

    return qty_width, sku_width, regal_width, fach_width, platz_width, title_width


def format_order_item_row(row, width):
    qty_width, sku_width, regal_width, fach_width, platz_width, title_width = format_order_item_header(width)
    qty = _fit(str(row["quantity"]), qty_width)
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


def orders_dialog(stdscr):
    order_filter = None
    selected = 0
    top_index = 0
    orders = []
    order_items_cache = {}
    reload_orders = True
    current_order_id = None

    while True:
        if reload_orders:
            orders = get_orders(order_filter)
            order_items_cache = {}
            reload_orders = False

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
        win.bkgd(" ", curses.color_pair(1))
        win.erase()
        win.box()
        win.addstr(0, 2, " Bestellungen ")

        left_width = max(34, int((width - 3) * 0.42))
        right_width = width - left_width - 3
        list_height = height - 4

        orders_win = win.derwin(list_height, left_width, 1, 1)
        details_win = win.derwin(list_height, right_width, 1, 2 + left_width)

        order_lines = [
            f"{_fit(order['order_name'], 14)} {_fit(format_address(order), left_width - 16)}"
            for order in orders
        ] or ["Keine Bestellungen"]

        if selected < top_index:
            top_index = selected
        if selected >= top_index + max(1, list_height - 2):
            top_index = selected - max(1, list_height - 2) + 1

        draw_panel(orders_win, "Auftraege", order_lines, selected if orders else 0, top_index, True)

        detail_lines = []
        if selected_order:
            detail_lines.append(_fit(f"Bestellung: {selected_order['order_name']}", right_width - 2))
            detail_lines.append(_fit(format_address(selected_order), right_width - 2))
            status = selected_order["fulfillment_status"] or "-"
            payment_status = selected_order["payment_status"] or "-"
            detail_lines.append(_fit(f"Status: {status}", right_width - 2))
            detail_lines.append(_fit(f"Zahlung: {payment_status}", right_width - 2))
            detail_lines.append("")
            qty_width, sku_width, regal_width, fach_width, platz_width, title_width = format_order_item_header(right_width - 2)
            detail_lines.append(
                f"{_fit('Menge', qty_width)} {_fit('SKU', sku_width)} {_fit('Artikel', title_width)} {_fit('Regal', regal_width)} {_fit('Fach', fach_width)} {_fit('Platz', platz_width)}"
            )
            detail_lines.append("-" * max(1, right_width - 2))

            for row in order_items:
                detail_lines.append(format_order_item_row(row, right_width - 2))
        else:
            detail_lines.append("Keine Bestellung gefunden")

        draw_panel(details_win, "Positionen", detail_lines, 0, 0, False)

        footer = " F3 Springen  F5 Pickliste  F6 PDF  F7 Lieferschein  F9 Zurueck "
        if order_filter:
            footer = f" Filter: {order_filter} " + footer
        win.attrset(curses.color_pair(3))
        win.addstr(height - 1, 1, " " * (width - 2))
        win.addstr(height - 1, 1, footer[:width - 2])
        win.refresh()

        key = win.get_wch()

        if key in (27, curses.KEY_F9, curses.KEY_F12):
            return
        if key == curses.KEY_DOWN:
            selected = move_selection(orders, selected, 1)
        elif key == curses.KEY_UP:
            selected = move_selection(orders, selected, -1)
        elif key == curses.KEY_NPAGE:
            selected = move_selection(orders, selected, max(1, list_height - 2))
        elif key == curses.KEY_PPAGE:
            selected = move_selection(orders, selected, -max(1, list_height - 2))
        elif key == curses.KEY_F3:
            value = order_jump_dialog(stdscr, order_filter or "")
            if value is not None:
                order_filter = value or None
                selected = 0
                top_index = 0
                reload_orders = True
                if value:
                    matched_orders = get_orders(order_filter)
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

    curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLUE)
    curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_CYAN)
    curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_WHITE)

    stdscr.bkgd(" ", curses.color_pair(1))
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
