"""Strict transport validation: malformed snapshots never replace warehouse data."""
import hashlib
import json
from decimal import Decimal, InvalidOperation


def json_text(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      default=lambda value: value.isoformat() if hasattr(value, "isoformat") else str(value))


def command_id(kind, entity, revision, payload):
    return hashlib.sha256(json_text([kind, entity, revision, payload]).encode()).hexdigest()


def number(value, *, minimum=0):
    if isinstance(value, bool):
        raise ValueError("Menge darf kein Boolean sein.")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError(f"Ungueltige Menge: {value!r}") from exc
    if not result.is_finite() or result != result.to_integral_value() or result < minimum or result > 2147483647:
        raise ValueError(f"Lager-MC benoetigt ganze Stueckzahlen >= {minimum}: {value!r}")
    return int(result)


def identifier(value):
    if isinstance(value, bool) or not str(value).isdigit() or not 1 <= int(value) <= 9223372036854775807:
        raise ValueError('Ungueltige Ereignis-ID.')
    return int(value)


def wc_id(kind, value):
    return f"wc:{kind}:{identifier(value)}"


def parse_id(kind, value):
    prefix = f"wc:{kind}:"
    if not str(value).startswith(prefix):
        raise ValueError(f"Keine WooCommerce-{kind}-ID: {value!r}")
    return identifier(str(value)[len(prefix):])


def validate_event(event):
    seq = identifier(event["seq"])
    kind = event["kind"]
    if kind not in {"product", "order", "customer"}:
        raise ValueError(f"Unbekannter Ereignistyp {kind!r}")
    entity_id = identifier(event["entity_id"])
    if int(event.get("deleted", 0)):
        return seq, kind, entity_id, None
    payload = event["payload"]
    payload = json.loads(payload) if isinstance(payload, str) else payload
    if not isinstance(payload, dict) or identifier(payload["id"]) != entity_id:
        raise ValueError("Ereignis-ID passt nicht zum Payload.")
    if kind == "product":
        for key in ("name", "fingerprint", "inventory_supported", "stock", "held", "status", "currency"):
            if key not in payload:
                raise ValueError(f"Produktfeld fehlt: {key}")
        if payload["currency"] != "EUR":
            raise ValueError("Lager-MC unterstuetzt derzeit nur EUR.")
        if type(payload['inventory_supported']) is not bool or not isinstance(payload['name'], str):
            raise ValueError('Ungueltiger Produktdatentyp.')
        if payload['status'] not in {'ACTIVE', 'DRAFT', 'ARCHIVED'}:
            raise ValueError('Ungueltiger Produktstatus.')
        for key in ('price', 'compare_at_price', 'unit_cost'):
            value = payload.get(key)
            if value is not None:
                try:
                    amount = Decimal(str(value))
                except InvalidOperation as exc:
                    raise ValueError('Ungueltiger Preis.') from exc
                if not amount.is_finite() or amount < 0:
                    raise ValueError('Ungueltiger Preis.')
        number(payload["stock"], minimum=0)
        number(payload["held"], minimum=0)
        if payload.get("weight_grams") is not None:
            number(payload["weight_grams"])
    if kind == "order":
        if payload.get("currency") != "EUR":
            raise ValueError("Bestellungen mit Fremdwaehrung werden nicht importiert.")
        if not isinstance(payload.get("items"), list):
            raise ValueError("Bestellpositionen fehlen.")
        if payload.get('fulfillment_status') not in {'unfulfilled','partial','fulfilled','cancelled'}:
            raise ValueError('Ungueltiger Versandstatus.')
        if payload.get('payment_status') not in {'paid','pending','refunded','voided'}:
            raise ValueError('Ungueltiger Zahlungsstatus.')
        seen = set()
        for line in payload["items"]:
            line_id = identifier(line["id"])
            if line_id in seen:
                raise ValueError("Doppelte Bestellposition.")
            seen.add(line_id)
            if type(line.get('stock_reduced')) is not bool or not isinstance(line.get('name'), str):
                raise ValueError('Ungueltige Bestellposition.')
            if line.get('product_id'):
                identifier(line['product_id'])
            qty, fulfilled = number(line["quantity"]), number(line["fulfilled"])
            if fulfilled > qty:
                raise ValueError("Erfuellte Menge groesser als Bestellmenge.")
    return seq, kind, entity_id, payload
