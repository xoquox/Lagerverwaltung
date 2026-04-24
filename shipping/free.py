"""Carrier-unabhaengiges lokales Adresslabel."""

import os
import tempfile
from pathlib import Path

from .base import sanitize_order_reference, save_shipping_label_pdf, validate_shipping_address


def create_label(ctx, order, weight_kg=1.0, shipment_reference=None, service_codes=None):
    t = ctx["t"]
    validate_shipping_address(order, t, require_country=False)
    try:
        weight_value = round(float(weight_kg), 3)
    except (TypeError, ValueError):
        weight_value = 0.0
    shipment_reference = sanitize_order_reference(shipment_reference or order["order_name"])
    internal_id = f"FREE{ctx['datetime'].datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    template_path = ctx["get_free_label_template_path"]()
    if template_path and not template_path.exists():
        raise FileNotFoundError(f"Adresslabel Vorlage fehlt: {template_path.name}")

    with tempfile.NamedTemporaryFile(prefix="free-label-", suffix=".pdf", delete=False) as handle:
        temp_path = handle.name
    try:
        ctx["build_address_label_pdf"](
            template_path,
            temp_path,
            sender=ctx["get_free_label_sender"](),
            receiver=ctx["free_label_receiver"](order),
            page_size=ctx["shipping_format_for_carrier"]("free"),
        )
        pdf_blob = Path(temp_path).read_bytes()
    finally:
        try:
            os.unlink(temp_path)
        except OSError:
            pass

    label_path = save_shipping_label_pdf(ctx["shipping_label_output_dir"], "free", order["order_name"], internal_id, pdf_blob)
    label_id = ctx["insert_shipping_label_history"](
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


def select_options(ctx, stdscr, scope="domestic"):
    return []


def manual_state_defaults(ctx):
    return {}


def manual_fields(ctx, state):
    return []


def manual_handle_action(ctx, stdscr, action_name, state):
    return state, False


def manual_service_codes(state):
    return None
