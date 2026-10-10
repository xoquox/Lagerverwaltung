"""Shop selection and the shared database boundary; no shop network clients."""

SHOP_SYSTEMS = {"shopify": "Shopify", "woocommerce": "WooCommerce"}


def normalize_shop_system(value=None):
    value = str(value if value is not None else "shopify").strip().lower()
    if value not in SHOP_SYSTEMS:
        raise ValueError(f"Unbekanntes Shopsystem: {value!r}. Erlaubt: shopify, woocommerce.")
    return value


def shop_system(settings):
    return normalize_shop_system(settings.get("shop_system"))


def sync_service(value=None):
    return f"{normalize_shop_system(value)}-sync"


def assert_shop_system(cur, expected):
    """Reject mismatched workers/workstations before accessing shop data."""
    expected = normalize_shop_system(expected)
    cur.execute("SELECT shop_system FROM shop_configuration WHERE id = 1")
    row = cur.fetchone()
    actual = row.get("shop_system") if isinstance(row, dict) else row[0] if row else None
    if actual != expected:
        raise RuntimeError(
            f"Shopsystem passt nicht zur Datenbank: erwartet {expected}, gespeichert {actual or 'keines'}. "
            "Fuer den Shopwechsel eine separate Datenbank verwenden; Datenuebernahme separat durchfuehren."
        )
