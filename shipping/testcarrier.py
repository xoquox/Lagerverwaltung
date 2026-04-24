"""Test carrier runtime."""

from .base import build_test_label_pdf, sanitize_order_reference, save_shipping_label_pdf, validate_shipping_address


def create_label(ctx, order, weight_kg=1.0, shipment_reference=None, service_codes=None):
    t = ctx["t"]
    validate_shipping_address(order, t, require_country=False)
    shipment_reference = sanitize_order_reference(shipment_reference or order["order_name"])
    track_id = f"TEST{ctx['datetime'].datetime.now().strftime('%Y%m%d%H%M%S')}"
    parcel_number = f"999{ctx['datetime'].datetime.now().strftime('%H%M%S')}"
    pdf_blob = build_test_label_pdf(order.get("order_name") or "TEST", shipment_reference, track_id)
    label_path = save_shipping_label_pdf(ctx["shipping_label_output_dir"], "test", order["order_name"], track_id, pdf_blob)
    label_id = ctx["insert_shipping_label_history"](
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
