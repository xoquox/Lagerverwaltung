"""Zentrale Carrier-Definitionen und einfache Carrier-Helfer."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ShippingCarrierDefinition:
    code: str
    label: str
    short_label: str
    default_format: str = "A6"
    shopify_allowed: bool = False
    option_mode: str | None = None
    printer_field: str | None = None
    printer_field_label_key: str | None = None
    format_field: str | None = None
    format_field_label_key: str | None = None
    template_field: str | None = None
    template_field_label_key: str | None = None
    tracking_mode_field: str | None = None
    tracking_url_field: str | None = None
    extra_settings_fields: tuple[tuple[str, str], ...] = field(default_factory=tuple)

    def get(self, key, default=None):
        return getattr(self, key, default)


@dataclass(frozen=True)
class ShippingCarrierRuntime:
    create_label: object | None = None
    reprint_label: object | None = None
    cancel_label: object | None = None


SHIPPING_CARRIER_DEFINITIONS = {
    "gls": ShippingCarrierDefinition(
        code="gls",
        label="GLS",
        short_label="GLS",
        default_format="A6",
        shopify_allowed=True,
        option_mode="gls_services",
        printer_field="shipping_label_printer_gls",
        printer_field_label_key="field_shipping_printer_gls",
        format_field="shipping_label_format_gls",
        format_field_label_key="field_shipping_format_gls",
        tracking_mode_field="shopify_tracking_mode_gls",
        tracking_url_field="shopify_tracking_url_gls",
        extra_settings_fields=(
            ("shipping_services_display", "field_shipping_services"),
            ("gls_api_url", "field_gls_api_url"),
            ("gls_user", "field_gls_user"),
            ("gls_password", "field_gls_password"),
            ("gls_contact_id", "field_gls_contact_id"),
        ),
    ),
    "post": ShippingCarrierDefinition(
        code="post",
        label="POST",
        short_label="POST",
        default_format="100x62",
        shopify_allowed=True,
        option_mode="post_products",
        printer_field="shipping_label_printer_post",
        printer_field_label_key="field_shipping_printer_post",
        format_field="shipping_label_format_post",
        format_field_label_key="field_shipping_format_post",
        tracking_mode_field="shopify_tracking_mode_post",
        tracking_url_field="shopify_tracking_url_post",
        extra_settings_fields=(
            ("post_api_url", "field_post_api_url"),
            ("post_api_key", "field_post_api_key"),
            ("post_api_secret", "field_post_api_secret"),
            ("post_user", "field_post_user"),
            ("post_password", "field_post_password"),
            ("post_partner_id", "field_post_partner_id"),
        ),
    ),
    "free": ShippingCarrierDefinition(
        code="free",
        label="Adresslabel",
        short_label="ADR",
        default_format="A6",
        shopify_allowed=False,
        option_mode=None,
        printer_field="shipping_label_printer_free",
        printer_field_label_key="field_shipping_printer_free",
        format_field="shipping_label_format_free",
        format_field_label_key="field_shipping_format_free",
        template_field="free_label_template_path",
        template_field_label_key="field_free_label_template",
    ),
    "test": ShippingCarrierDefinition(
        code="test",
        label="TEST",
        short_label="TEST",
        default_format="A6",
        shopify_allowed=False,
        option_mode=None,
    ),
}

SHIPPING_CARRIER_ORDER = list(SHIPPING_CARRIER_DEFINITIONS.keys())
DEFAULT_ACTIVE_SHIPPING_CARRIERS = ["gls", "post", "free"]


def normalize_carrier_code(carrier):
    return (carrier or "").strip().lower()


def carrier_definition(carrier):
    return SHIPPING_CARRIER_DEFINITIONS.get(normalize_carrier_code(carrier))


def carrier_label(carrier, short=False):
    definition = carrier_definition(carrier)
    normalized = normalize_carrier_code(carrier)
    if definition:
        return definition.short_label if short else definition.label
    return normalized.upper() or "-"


def configurable_carrier_codes(include_test=False):
    codes = []
    for code in SHIPPING_CARRIER_ORDER:
        if code == "test" and not include_test:
            continue
        if SHIPPING_CARRIER_DEFINITIONS.get(code):
            codes.append(code)
    return codes


def carrier_allows_shopify(carrier):
    definition = carrier_definition(carrier)
    return bool(definition and definition.shopify_allowed)


def carrier_option_mode(carrier):
    definition = carrier_definition(carrier)
    return definition.option_mode if definition else None


def carrier_setting_field(carrier, field_kind):
    definition = carrier_definition(carrier)
    return getattr(definition, f"{field_kind}_field", None) if definition else None


def carrier_field_to_code(field_kind, include_test=False):
    mapping = {}
    for code in configurable_carrier_codes(include_test=include_test):
        field_name = carrier_setting_field(code, field_kind)
        if field_name:
            mapping[field_name] = code
    return mapping


def shipping_active_carrier_values(values):
    if isinstance(values, str):
        raw_values = [part.strip().lower() for part in values.split(",")]
    else:
        raw_values = [str(value or "").strip().lower() for value in (values or [])]
    normalized = []
    for code in SHIPPING_CARRIER_ORDER:
        if code in raw_values and code not in normalized:
            normalized.append(code)
    return normalized


def normalize_active_carriers(values, fallback_to_defaults=True):
    normalized = shipping_active_carrier_values(values)
    if normalized:
        return normalized
    if not fallback_to_defaults:
        return []
    return list(DEFAULT_ACTIVE_SHIPPING_CARRIERS)


def shipping_carrier_options(active_carriers, include_test=True):
    allowed = normalize_active_carriers(active_carriers)
    if include_test and "test" not in allowed and "test" in SHIPPING_CARRIER_DEFINITIONS:
        allowed = list(allowed) + ["test"]
    options = []
    for code in allowed:
        definition = carrier_definition(code)
        if definition:
            options.append({"value": code, "label": definition.label})
    return options


def default_tracking_mode_for_carrier(carrier):
    return "company_and_url" if normalize_carrier_code(carrier) == "post" else "company"


def shopify_tracking_company(carrier):
    normalized = normalize_carrier_code(carrier)
    if normalized == "gls":
        return "GLS"
    if normalized == "post":
        return "Deutsche Post"
    return carrier_label(normalized)
