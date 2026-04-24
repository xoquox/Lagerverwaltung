"""POST / Internetmarke carrier runtime."""

from .base import normalize_country_code, sanitize_order_reference, save_shipping_label_pdf, validate_shipping_address


def load_credentials(settings, t):
    creds = {
        "api_url": (settings.get("post_api_url") or "").strip(),
        "api_key": (settings.get("post_api_key") or "").strip(),
        "api_secret": (settings.get("post_api_secret") or "").strip(),
        "user": (settings.get("post_user") or "").strip(),
        "password": (settings.get("post_password") or "").strip(),
        "partner_id": (settings.get("post_partner_id") or "").strip(),
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


def normalize_option_codes(option_codes):
    result = []
    for code in option_codes or []:
        normalized = str(code or "").strip().lower()
        if normalized and normalized not in result:
            result.append(normalized)
    return sorted(result)


def selection_summary(selection):
    if not selection:
        return "-"
    label = (selection.get("selection_label") or selection.get("name") or "").strip()
    price = str(selection.get("price_eur") or "").strip()
    if label and price:
        return f"{label} - {price} EUR"
    return label or "-"


def selection_dialog(ctx, stdscr, scope="domestic"):
    current = dict(ctx["post_selection_cache"].get(scope) or {})
    selection = ctx["post_product_dialog"](stdscr, current_selection=current, scope=scope)
    if selection:
        ctx["post_selection_cache"][scope] = dict(selection)
    return selection


def resolve_product_selection(selection, t, *, find_post_product, list_post_base_products):
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
    option_codes = normalize_option_codes(selection.get("option_codes") or [])
    if not base_key:
        raise ValueError(t("post_base_product_missing"))

    for group in list_post_base_products(scope=scope):
        if group.get("base_key") != base_key:
            continue
        for bucket in ("untracked_variants", "tracked_variants"):
            for variant in group.get(bucket, []):
                if normalize_option_codes(variant.get("addons") or []) == option_codes:
                    product = find_post_product(variant["product_code"])
                    if product:
                        return product
        break
    raise ValueError(t("post_product_combination_unavailable"))


def country_to_alpha3(country_value, country_alpha3):
    raw = (country_value or "").strip()
    if not raw:
        return ""
    if len(raw) == 3 and raw.isalpha():
        return raw.upper()
    if len(raw) == 2 and raw.isalpha():
        return country_alpha3.get(raw.upper(), "")
    code2 = normalize_country_code(raw)
    if code2:
        return country_alpha3.get(code2, "")
    return ""


def sender_address(client):
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


def receiver_address(order, t, *, country_alpha3):
    country = country_to_alpha3(order.get("shipping_country"), country_alpha3)
    if not country:
        raise ValueError(t("recipient_country_invalid_or_missing_iso2_iso3"))
    address = {
        "name": (order.get("shipping_name") or "").strip()[:50],
        "addressLine1": (order.get("shipping_address1") or "").strip()[:50],
        "postalCode": (order.get("shipping_zip") or "").strip()[:10],
        "city": (order.get("shipping_city") or "").strip()[:40],
        "country": country,
    }
    company = (order.get("shipping_company") or order.get("shipping_address2") or "").strip()
    if company:
        address["additionalName"] = company[:40]
    address_line2 = (order.get("shipping_address2") or "").strip()
    if address_line2:
        address["addressLine2"] = address_line2[:60]
    return address


def create_label(ctx, order, weight_kg=1.0, shipment_reference=None, service_codes=None):
    t = ctx["t"]
    validate_shipping_address(order, t, require_country=False)
    creds = load_credentials(ctx["settings"], t)
    client = ctx["internetmarke_client_class"](
        api_url=creds["api_url"],
        partner_id=creds["partner_id"],
        api_key=creds["api_key"],
        api_secret=creds["api_secret"],
        user=creds["user"],
        password=creds["password"],
    )
    client.validate()
    try:
        weight_value = float(weight_kg)
    except (TypeError, ValueError):
        raise ValueError(t("weight_invalid"))
    if weight_value <= 0:
        raise ValueError(t("weight_positive"))
    weight_value = round(weight_value, 3)
    reference = sanitize_order_reference(shipment_reference or order["order_name"])
    product = resolve_product_selection(service_codes, t, find_post_product=ctx["find_post_product"], list_post_base_products=ctx["list_post_base_products"])
    page_format_id = ctx["resolve_post_page_format_id"](client, ctx["shipping_format_for_carrier"]("post"))
    sender = sender_address(client)
    receiver = receiver_address(order, t, country_alpha3=ctx["country_alpha3"])
    total_cents = int(product.get("price_cents") or 0)
    if total_cents <= 0:
        raise RuntimeError(t("post_product_price_invalid"))

    position = {
        "productCode": int(product["product_code"]),
        "voucherLayout": "ADDRESS_ZONE",
        "positionType": "AppShoppingCartPDFPosition",
        "position": {"page": 1, "labelX": 1, "labelY": 1},
        "address": {"sender": sender, "receiver": receiver},
    }
    try:
        response, pdf_blob = client.checkout_pdf_binary(
            shop_order_id=reference[:18],
            total_cents=total_cents,
            page_format_id=page_format_id,
            positions=[position],
            create_manifest=False,
            create_shipping_list="0",
            dpi="DPI300",
            direct_checkout=True,
        )
    except Exception:
        ctx["logger"].exception(
            "POST Label-Checkout fehlgeschlagen reference=%s product_code=%s product_name=%s page_format_id=%s country=%s",
            reference,
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
    tracking_url = ctx["tracking_url_for_carrier"]("post", track_id or parcel_number or reference)
    label_path = save_shipping_label_pdf(ctx["shipping_label_output_dir"], "post", order["order_name"], track_id or reference, pdf_blob)
    label_id = ctx["insert_shipping_label_history"](
        order=order,
        shipment_reference=reference,
        track_id=track_id or reference,
        parcel_number=parcel_number,
        label_path=label_path,
        status="CREATED",
        weight_kg=weight_value,
        carrier="post",
        tracking_url=tracking_url,
    )
    return {
        "label_id": label_id,
        "track_id": track_id or reference,
        "parcel_number": parcel_number,
        "label_path": label_path,
        "shipment_reference": reference,
        "post_product_code": product["product_code"],
        "post_product_name": product["name"],
        "tracking_url": tracking_url,
    }


def select_options(ctx, stdscr, scope="domestic"):
    return selection_dialog(ctx, stdscr, scope=scope)


def manual_state_defaults(ctx):
    return {
        "post_selection": dict(ctx["post_selection_cache"].get("domestic") or {}),
    }


def manual_fields(ctx, state):
    return [{
        "name": "post_product",
        "label": ctx["t"]("manual_label_field_post_product"),
        "value": selection_summary(state.get("post_selection")),
        "read_only": True,
        "action": "carrier_options",
    }]


def manual_handle_action(ctx, stdscr, action_name, state):
    if action_name != "carrier_options":
        return state, False
    chosen = selection_dialog(ctx, stdscr, scope="domestic")
    if not chosen:
        return state, True
    updated = dict(state)
    updated["post_selection"] = chosen
    return updated, True


def manual_service_codes(state):
    return state.get("post_selection") or None
