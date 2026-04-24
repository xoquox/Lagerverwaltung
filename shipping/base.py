"""Gemeinsame carrier-neutrale Hilfen fuer Versandmodule."""

import base64
import binascii
import datetime
import os
import string
from pathlib import Path


COUNTRY_CODE_MAP = {
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


def normalize_country_code(country_value):
    raw = (country_value or "").strip()
    if len(raw) == 2 and raw.isalpha():
        return raw.upper()
    return COUNTRY_CODE_MAP.get(raw.lower(), "")


def sanitize_order_reference(order_name):
    raw = (order_name or "").replace("#", "").strip()
    cleaned = "".join(ch if ch in string.ascii_letters + string.digits + "-_/" else "-" for ch in raw).strip("-")
    return cleaned or f"order-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"


def extract_first_pdf_blob(data):
    candidates = []

    def walk(value):
        if isinstance(value, dict):
            for nested in value.values():
                walk(nested)
        elif isinstance(value, list):
            for nested in value:
                walk(nested)
        elif isinstance(value, str):
            normalized = value.strip()
            if len(normalized) > 200 and normalized.startswith("JVBERi0"):
                candidates.append(normalized)

    walk(data)
    if not candidates:
        return None
    try:
        return base64.b64decode(candidates[0], validate=True)
    except binascii.Error:
        return base64.b64decode(candidates[0])


def validate_shipping_address(order, t, *, require_country=False, country_normalizer=normalize_country_code):
    checks = [
        ("shipping_name", t("recipient_name_missing")),
        ("shipping_address1", t("recipient_street_missing")),
        ("shipping_zip", t("recipient_zip_missing")),
        ("shipping_city", t("recipient_city_missing")),
    ]
    for key, message in checks:
        if not (order.get(key) or "").strip():
            raise ValueError(message)
    if require_country and not country_normalizer(order.get("shipping_country")):
        raise ValueError(t("recipient_country_invalid_or_missing_iso2"))


def label_identifiers(label_row):
    identifiers = []
    for value in (label_row.get("parcel_number"), label_row.get("track_id")):
        normalized = (value or "").strip()
        if normalized and normalized not in identifiers:
            identifiers.append(normalized)
    return identifiers


def build_test_label_pdf(order_name, shipment_reference, track_id):
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


def save_shipping_label_pdf(output_dir, carrier, order_name, track_id, pdf_bytes, suffix=""):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    safe_order = sanitize_order_reference(order_name)
    safe_track = "".join(ch for ch in (track_id or "unknown") if ch.isalnum() or ch in "-_") or "unknown"
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix_part = f"_{suffix}" if suffix else ""
    safe_carrier = "".join(ch for ch in (carrier or "shipping") if ch.isalnum() or ch in "-_") or "shipping"
    filename = f"{safe_carrier}_{safe_order}_{safe_track}_{timestamp}{suffix_part}.pdf"
    output_path = output_dir / filename
    output_path.write_bytes(pdf_bytes)
    os.chmod(output_path, 0o600)
    return str(output_path)
