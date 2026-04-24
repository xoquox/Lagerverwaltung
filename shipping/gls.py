"""GLS carrier runtime."""

import base64
import json
import ssl
import datetime
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .base import extract_first_pdf_blob, label_identifiers, normalize_country_code, sanitize_order_reference, save_shipping_label_pdf, validate_shipping_address


def load_credentials(settings, t):
    creds = {
        "api_url": settings.get("gls_api_url", "").strip(),
        "user": settings.get("gls_user", "").strip(),
        "password": settings.get("gls_password", "").strip(),
        "contact_id": settings.get("gls_contact_id", "").strip(),
    }
    missing = [name for name, value in creds.items() if not value]
    if missing:
        raise RuntimeError(t("gls_missing_data", fields=", ".join(missing)))
    return creds


def api_json_request(url, credentials, payload=None, t=None):
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


def error_summary(data, raw):
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

    if isinstance(data, (dict, list)):
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


def select_options(ctx, stdscr, scope="domestic"):
    return ctx["shipping_services_dialog"](
        stdscr,
        ctx["normalize_shipping_services"](ctx["settings"].get("shipping_services", [])),
        cancel_returns_none=True,
    )


def manual_state_defaults(ctx):
    return {
        "selected_services": ctx["normalize_shipping_services"](ctx["settings"].get("shipping_services", [])),
    }


def manual_fields(ctx, state):
    return [{
        "name": "services_display",
        "label": ctx["t"]("manual_label_field_services"),
        "value": ctx["shipping_services_summary"](state.get("selected_services", [])),
        "read_only": True,
        "action": "carrier_options",
    }]


def manual_handle_action(ctx, stdscr, action_name, state):
    if action_name != "carrier_options":
        return state, False
    chosen = ctx["shipping_services_dialog"](stdscr, state.get("selected_services", []), cancel_returns_none=True)
    if chosen is None:
        return state, True
    updated = dict(state)
    updated["selected_services"] = chosen
    return updated, True


def manual_service_codes(state):
    return state.get("selected_services", [])


def create_label(ctx, order, weight_kg=1.0, shipment_reference=None, service_codes=None):
    t = ctx["t"]
    validate_shipping_address(order, t, require_country=True, country_normalizer=normalize_country_code)
    creds = load_credentials(ctx["settings"], t)
    try:
        weight_value = float(weight_kg)
    except (TypeError, ValueError):
        raise ValueError(t("weight_invalid"))
    if weight_value <= 0:
        raise ValueError(t("weight_positive"))
    weight_value = round(weight_value, 3)

    shipment_reference = sanitize_order_reference(shipment_reference or order["order_name"])
    normalized_services = ctx["normalize_shipping_services"](
        service_codes if service_codes is not None else ctx["settings"].get("shipping_services", [])
    )
    if "service_flexdelivery" in normalized_services and not (order.get("shipping_email") or "").strip():
        raise ValueError(t("flexdelivery_email_missing"))
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
                    "Name2": (order.get("shipping_address2") or "").strip(),
                    "CountryCode": normalize_country_code(order.get("shipping_country")),
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

    status_code, data, raw = api_json_request(creds["api_url"], creds, payload, t=t)
    if status_code >= 400 or not isinstance(data, dict):
        error_detail = error_summary(data, raw)
        ctx["logger"].error(
            "GLS Label-API Fehler status=%s order=%s ref=%s country=%s zip=%s city=%s weight=%.3f detail=%s",
            status_code,
            order.get("order_name"),
            shipment_reference,
            normalize_country_code(order.get("shipping_country")),
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

    pdf_blob = extract_first_pdf_blob(data)
    if not pdf_blob:
        if raw.startswith(b"%PDF-"):
            pdf_blob = raw
        else:
            raise RuntimeError(t("gls_label_response_missing_pdf"))

    label_path = save_shipping_label_pdf(ctx["shipping_label_output_dir"], "gls", order["order_name"], track_id, pdf_blob)
    label_id = ctx["insert_shipping_label_history"](
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


def reprint_label(ctx, label_row):
    t = ctx["t"]
    creds = load_credentials(ctx["settings"], t)
    identifiers = label_identifiers(label_row)
    if not identifiers:
        raise ValueError(t("track_or_parcel_missing"))
    status_code = None
    data = None
    raw = b""
    chosen_identifier = identifiers[0]
    for identifier in identifiers:
        url = f"{creds['api_url'].rstrip('/')}/reprint/{identifier}"
        status_code, data, raw = api_json_request(url, creds, t=t)
        chosen_identifier = identifier
        if status_code < 400 or status_code != 404:
            break
    if status_code is None:
        raise RuntimeError(t("gls_reprint_failed"))
    if status_code >= 400:
        error_detail = error_summary(data, raw)
        ctx["logger"].error("GLS Reprint Fehler status=%s identifiers=%s detail=%s", status_code, ",".join(identifiers), error_detail or "-")
        if error_detail:
            raise RuntimeError(t("gls_reprint_http_error", status_code=status_code, detail=error_detail[:180]))
        raise RuntimeError(t("gls_reprint_http_error_plain", status_code=status_code))

    pdf_blob = extract_first_pdf_blob(data)
    if not pdf_blob and raw.startswith(b"%PDF-"):
        pdf_blob = raw
    if not pdf_blob:
        raise RuntimeError(t("gls_reprint_missing_pdf"))

    label_path = save_shipping_label_pdf(ctx["shipping_label_output_dir"], "gls", label_row["order_name"], chosen_identifier, pdf_blob, suffix="reprint")
    ctx["update_shipping_label_reprint"](label_row["id"], label_path)
    return label_path


def cancel_label(ctx, label_row):
    t = ctx["t"]
    creds = load_credentials(ctx["settings"], t)
    identifiers = label_identifiers(label_row)
    if not identifiers:
        raise ValueError(t("track_or_parcel_missing"))
    status_code = None
    data = None
    raw = b""
    for identifier in identifiers:
        url = f"{creds['api_url'].rstrip('/')}/cancel/{identifier}"
        status_code, data, raw = api_json_request(url, creds, t=t)
        if status_code < 400 or status_code != 404:
            break
    if status_code is None:
        raise RuntimeError(t("gls_cancel_failed"))
    if status_code >= 400:
        error_detail = error_summary(data, raw)
        ctx["update_shipping_label_status"](label_row["id"], "CANCEL_FAILED", f"HTTP {status_code} {error_detail[:120]}".strip())
        ctx["logger"].error("GLS Storno Fehler status=%s identifiers=%s detail=%s", status_code, ",".join(identifiers), error_detail or "-")
        if error_detail:
            raise RuntimeError(t("gls_cancel_http_error", status_code=status_code, detail=error_detail[:180]))
        raise RuntimeError(t("gls_cancel_http_error_plain", status_code=status_code))

    result = ""
    if isinstance(data, dict):
        result = (data.get("result") or "").strip().upper()
    if result == "CANCELLED":
        ctx["update_shipping_label_status"](label_row["id"], "CANCELLED")
    elif result == "CANCELLATION_PENDING":
        ctx["update_shipping_label_status"](label_row["id"], "CANCELLATION_PENDING")
    else:
        ctx["update_shipping_label_status"](label_row["id"], "CANCEL_REQUESTED")
    return result or "CANCEL_REQUESTED"
