import os
import time
import json
import argparse
import datetime
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

try:
    import psycopg2
    import psycopg2.extras
except ModuleNotFoundError:
    psycopg2 = None

try:
    import requests
except ModuleNotFoundError:
    requests = None

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv():
        return None


def resolve_sync_base_dir(script_path=None):
    app_dir = Path(script_path or __file__).resolve().parent
    repo_root = app_dir.parent
    if (repo_root / "shipping").is_dir():
        return repo_root
    return app_dir


BASE_DIR = resolve_sync_base_dir()
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from shipping.history import (
    SHIPPING_LABEL_TABLE,
    claim_shopify_fulfillment_jobs as _claim_shopify_fulfillment_jobs,
    ensure_shipping_history_schema,
    mark_shopify_fulfillment_job_done as _mark_shopify_fulfillment_job_done,
    mark_shopify_fulfillment_job_failed as _mark_shopify_fulfillment_job_failed,
    upsert_shopify_shipment as _upsert_shopify_shipment_record,
)
from sync_version import SYNC_VERSION

load_dotenv()

LOG_DIR = BASE_DIR / "logs"
SYNC_LOG_PATH = LOG_DIR / "shopify-sync.log"

SHOP = os.getenv("SHOP")
TOKEN = os.getenv("TOKEN")

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

API_VERSION = "2026-01"
SHOPIFY_LOCATION_ID = 67402989753
GRAPHQL_URL = f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json"
SYNC_INTERVAL = 60
REQUEST_TIMEOUT_SECONDS = 45
_LOGGER = None


def configure_logging():
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    LOG_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger("lagerverwaltung.shopify_sync")
    logger.setLevel(getattr(logging, os.getenv("LAGERVERWALTUNG_LOG_LEVEL", "INFO").strip().upper(), logging.INFO))
    logger.propagate = False

    if not logger.handlers:
        handler = RotatingFileHandler(SYNC_LOG_PATH, maxBytes=1_000_000, backupCount=5, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
        logger.addHandler(handler)

    _LOGGER = logger
    return logger


def log_info(message, *args):
    rendered = message % args if args else message
    print(rendered)
    configure_logging().info(rendered)


def log_warning(message, *args):
    rendered = message % args if args else message
    print(rendered)
    configure_logging().warning(rendered)


def log_error(message, *args):
    rendered = message % args if args else message
    print(rendered)
    configure_logging().error(rendered)


def log_exception(message, *args):
    rendered = message % args if args else message
    print(rendered)
    configure_logging().exception(rendered)


def shorten_text(value, limit=400):
    text = "" if value is None else str(value).replace("\n", "\\n")
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def summarize_orders(orders):
    count = len(orders or [])
    if not orders:
        return {"count": 0, "latest_name": "-", "latest_created_at": "-", "line_items": 0}

    latest_order = None
    latest_key = None
    line_items = 0
    for order in orders:
        line_items += len(((order.get("lineItems") or {}).get("nodes") or []))
        created_at = order.get("createdAt")
        candidate = (created_at or "", order.get("name") or "")
        if latest_key is None or candidate > latest_key:
            latest_key = candidate
            latest_order = order

    return {
        "count": count,
        "latest_name": (latest_order or {}).get("name") or "-",
        "latest_created_at": (latest_order or {}).get("createdAt") or "-",
        "line_items": line_items,
    }


def build_sync_version_payload():
    return {
        "service": "shopify-sync",
        "version": SYNC_VERSION,
        "reported_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


def update_service_runtime_state(
    *,
    status=None,
    mark_seen=False,
    mark_started=False,
    mark_finished=False,
    mark_pull=False,
    mark_push=False,
    last_error=None,
    clear_error=False,
):
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO service_runtime_state (
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
        )
        VALUES (
            'shopify-sync',
            %s,
            %s,
            CASE WHEN %s THEN NOW() ELSE NULL END,
            CASE WHEN %s THEN NOW() ELSE NULL END,
            CASE WHEN %s THEN NOW() ELSE NULL END,
            CASE WHEN %s THEN NOW() ELSE NULL END,
            CASE WHEN %s THEN NOW() ELSE NULL END,
            %s,
            NOW()
        )
        ON CONFLICT (service) DO UPDATE SET
            version = COALESCE(EXCLUDED.version, service_runtime_state.version),
            status = COALESCE(EXCLUDED.status, service_runtime_state.status),
            last_seen_at = COALESCE(EXCLUDED.last_seen_at, service_runtime_state.last_seen_at),
            last_started_at = COALESCE(EXCLUDED.last_started_at, service_runtime_state.last_started_at),
            last_finished_at = COALESCE(EXCLUDED.last_finished_at, service_runtime_state.last_finished_at),
            last_pull_at = COALESCE(EXCLUDED.last_pull_at, service_runtime_state.last_pull_at),
            last_push_at = COALESCE(EXCLUDED.last_push_at, service_runtime_state.last_push_at),
            last_error = CASE
                WHEN %s THEN NULL
                WHEN EXCLUDED.last_error IS NOT NULL THEN EXCLUDED.last_error
                ELSE service_runtime_state.last_error
            END,
            updated_at = NOW()
        """,
        (
            SYNC_VERSION,
            status,
            bool(mark_seen),
            bool(mark_started),
            bool(mark_finished),
            bool(mark_pull),
            bool(mark_push),
            (last_error or "")[:1000] if last_error else None,
            bool(clear_error),
        ),
    )
    con.commit()
    cur.close()
    con.close()


def ensure_runtime_dependencies():
    missing = []
    if psycopg2 is None:
        missing.append("psycopg2")
    if requests is None:
        missing.append("requests")
    if missing:
        raise RuntimeError(f"Fehlende Python-Abhaengigkeiten: {', '.join(missing)}")


def db():
    ensure_runtime_dependencies()
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def init_db():
    con = db()
    cur = con.cursor()
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS available integer")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS reserved integer DEFAULT 0")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS committed integer DEFAULT 0")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS unavailable integer DEFAULT 0")
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
            shipping_email text,
            shipping_phone text,
            fulfillment_status text,
            payment_status text,
            updated_at timestamptz NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute("ALTER TABLE shopify_orders ADD COLUMN IF NOT EXISTS shipping_country text")
    cur.execute("ALTER TABLE shopify_orders ADD COLUMN IF NOT EXISTS shipping_email text")
    cur.execute("ALTER TABLE shopify_orders ADD COLUMN IF NOT EXISTS shipping_phone text")
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
            order_line_item_id text,
            sku text,
            title text NOT NULL,
            quantity integer NOT NULL,
            fulfilled_quantity integer NOT NULL DEFAULT 0,
            PRIMARY KEY (order_id, line_index)
        )
        """
    )
    cur.execute("ALTER TABLE shopify_order_items ADD COLUMN IF NOT EXISTS order_line_item_id text")
    cur.execute("ALTER TABLE shopify_order_items ADD COLUMN IF NOT EXISTS fulfilled_quantity integer NOT NULL DEFAULT 0")
    ensure_shipping_history_schema(cur)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS service_runtime_state (
            service text PRIMARY KEY,
            version text,
            status text NOT NULL DEFAULT 'unknown',
            last_seen_at timestamptz,
            last_started_at timestamptz,
            last_finished_at timestamptz,
            last_pull_at timestamptz,
            last_push_at timestamptz,
            last_error text,
            updated_at timestamptz NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shopify_customers (
            customer_id text PRIMARY KEY,
            first_name text,
            last_name text,
            display_name text,
            email text,
            phone text,
            default_name text,
            default_address1 text,
            default_zip text,
            default_city text,
            default_country text,
            default_phone text,
            updated_at timestamptz NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute("ALTER TABLE shopify_customers ADD COLUMN IF NOT EXISTS first_name text")
    cur.execute("ALTER TABLE shopify_customers ADD COLUMN IF NOT EXISTS last_name text")
    cur.execute("ALTER TABLE shopify_customers ADD COLUMN IF NOT EXISTS display_name text")
    cur.execute("ALTER TABLE shopify_customers ADD COLUMN IF NOT EXISTS email text")
    cur.execute("ALTER TABLE shopify_customers ADD COLUMN IF NOT EXISTS phone text")
    cur.execute("ALTER TABLE shopify_customers ADD COLUMN IF NOT EXISTS default_name text")
    cur.execute("ALTER TABLE shopify_customers ADD COLUMN IF NOT EXISTS default_address1 text")
    cur.execute("ALTER TABLE shopify_customers ADD COLUMN IF NOT EXISTS default_zip text")
    cur.execute("ALTER TABLE shopify_customers ADD COLUMN IF NOT EXISTS default_city text")
    cur.execute("ALTER TABLE shopify_customers ADD COLUMN IF NOT EXISTS default_country text")
    cur.execute("ALTER TABLE shopify_customers ADD COLUMN IF NOT EXISTS default_phone text")
    cur.execute("ALTER TABLE shopify_customers ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW()")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_shopify_customers_display_name ON shopify_customers(display_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_shopify_customers_email ON shopify_customers(email)")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS version text")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'unknown'")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS last_seen_at timestamptz")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS last_started_at timestamptz")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS last_finished_at timestamptz")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS last_pull_at timestamptz")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS last_push_at timestamptz")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS last_error text")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW()")
    con.commit()
    cur.close()
    con.close()


def graphql_request(query, variables=None):
    ensure_runtime_dependencies()
    headers = {
        "X-Shopify-Access-Token": TOKEN,
        "Content-Type": "application/json",
    }
    payload = {"query": query, "variables": variables or {}}
    try:
        response = requests.post(
            GRAPHQL_URL,
            json=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", "-")
        body = shorten_text(getattr(response, "text", ""))
        log_error("GraphQL HTTP-Fehler status=%s body=%s", status, body or "-")
        raise

    try:
        data = response.json()
    except ValueError as exc:
        body = shorten_text(response.text)
        log_error("GraphQL JSON-Fehler body=%s", body or "-")
        raise RuntimeError("Shopify GraphQL Antwort war kein gueltiges JSON.") from exc
    errors = data.get("errors")

    if errors:
        log_error("Shopify GraphQL Fehler: %s", shorten_text(json.dumps(errors, ensure_ascii=False)))
        raise RuntimeError(f"Shopify GraphQL Fehler: {errors}")

    return data["data"]


def get_products_page(url):
    ensure_runtime_dependencies()
    headers = {
        "X-Shopify-Access-Token": TOKEN,
    }

    response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response


def get_all_products():
    products = []
    url = f"https://{SHOP}/admin/api/{API_VERSION}/products.json?limit=250"

    while url:
        response = get_products_page(url)
        data = response.json()
        products.extend(data["products"])

        log_info("Geladen: %s Produkte", len(products))

        link = response.headers.get("Link")
        next_url = None

        if link:
            parts = link.split(",")

            for part in parts:
                if 'rel="next"' in part:
                    next_url = part.split(";")[0].strip()[1:-1]

        url = next_url
        time.sleep(0.5)

    return products

def push_inventory_changes():

    con = db()
    cur = con.cursor()

    cur.execute(
        """
        SELECT sku, available, shopify_inventory_item_id
        FROM items
        WHERE dirty = TRUE
          AND shopify_inventory_item_id IS NOT NULL
        """
    )

    rows = cur.fetchall()

    if not rows:
        con.close()
        return 0

    log_info("Push %s Lageraenderungen zu Shopify", len(rows))

    headers = {
        "X-Shopify-Access-Token": TOKEN,
        "Content-Type": "application/json",
    }

    pushed_count = 0
    for sku, available_qty, inventory_item_id in rows:
        payload = {
            "location_id": SHOPIFY_LOCATION_ID,
            "inventory_item_id": inventory_item_id,
            "available": available_qty,
        }

        response = requests.post(
            f"https://{SHOP}/admin/api/{API_VERSION}/inventory_levels/set.json",
            json=payload,
            headers=headers,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        if response.status_code != 200:
            log_error("Shopify Fehler sku=%s status=%s body=%s", sku, response.status_code, shorten_text(response.text))
            continue

        log_info("Shopify Update sku=%s available=%s", sku, available_qty)

        cur.execute("""
            UPDATE items
            SET dirty = FALSE,
                sync_status = 'pushed',
                last_sync = NOW()
            WHERE sku = %s
        """,
        (sku,),
        )
        pushed_count += 1

        time.sleep(0.5)

    con.commit()
    con.close()
    return pushed_count


def get_location_inventory_levels():
    query = """
    query LocationInventoryLevels($locationId: ID!, $after: String) {
      location(id: $locationId) {
        inventoryLevels(first: 250, after: $after) {
          nodes {
            item {
              sku
            }
            quantities(
              names: [
                "available",
                "reserved",
                "committed",
                "on_hand",
                "damaged",
                "safety_stock",
                "quality_control"
              ]
            ) {
              name
              quantity
            }
          }
          pageInfo {
            hasNextPage
            endCursor
          }
        }
      }
    }
    """

    location_id = f"gid://shopify/Location/{SHOPIFY_LOCATION_ID}"
    after = None
    inventory_by_sku = {}

    while True:
        data = graphql_request(
            query,
            {
                "locationId": location_id,
                "after": after,
            },
        )

        levels = data["location"]["inventoryLevels"]

        for node in levels["nodes"]:
            item = node["item"] or {}
            sku = item.get("sku")

            if not sku:
                continue

            quantities = {entry["name"]: entry["quantity"] for entry in node["quantities"]}
            unavailable = (
                quantities.get("reserved", 0)
                + quantities.get("damaged", 0)
                + quantities.get("safety_stock", 0)
                + quantities.get("quality_control", 0)
            )
            inventory_by_sku[sku] = {
                "available": quantities.get("available", 0),
                "committed": quantities.get("committed", 0),
                "reserved": quantities.get("reserved", 0),
                "unavailable": unavailable,
                "on_hand": quantities.get("on_hand"),
            }

        page_info = levels["pageInfo"]

        if not page_info["hasNextPage"]:
            return inventory_by_sku

        after = page_info["endCursor"]
        time.sleep(0.5)


def sync_inventory_levels():
    inventory_by_sku = get_location_inventory_levels()

    if not inventory_by_sku:
        log_warning("Keine Inventory-Levels von Shopify geladen")
        return 0

    con = db()
    cur = con.cursor()

    for sku, quantities in inventory_by_sku.items():
        available = quantities["available"]
        committed = quantities["committed"]
        reserved = quantities["reserved"]
        unavailable = quantities["unavailable"]
        on_hand = quantities["on_hand"]

        if on_hand is None:
            on_hand = available + committed + unavailable

        cur.execute(
            """
            UPDATE items
            SET menge = CASE
                    WHEN dirty = TRUE THEN items.menge
                    ELSE %s
                END,
                available = CASE
                    WHEN dirty = TRUE THEN GREATEST(items.menge - %s - %s, 0)
                    ELSE %s
                END,
                committed = %s,
                reserved = %s,
                unavailable = %s,
                sync_status = 'ok',
                last_sync = NOW(),
                updated_at = NOW(),
                dirty = CASE
                    WHEN dirty = TRUE AND GREATEST(items.menge - %s - %s, 0) = %s THEN FALSE
                    ELSE dirty
                END
            WHERE sku = %s
            """,
            (
                on_hand,
                unavailable,
                committed,
                available,
                committed,
                reserved,
                unavailable,
                unavailable,
                committed,
                available,
                sku,
            ),
        )

    con.commit()
    con.close()
    log_info("Inventory-Levels synchronisiert: %s", len(inventory_by_sku))
    return len(inventory_by_sku)


def sync_products():

    products = get_all_products()
    inventory_item_ids = []
    for product in products:
        for variant in product["variants"]:
            inventory_item_id = variant.get("inventory_item_id")
            if inventory_item_id:
                inventory_item_ids.append(inventory_item_id)

    unit_cost_by_inventory_item_id = get_inventory_item_unit_costs(inventory_item_ids)

    con = db()
    cur = con.cursor()

    for product in products:

        product_id = product["id"]
        name = product["title"]

        for variant in product["variants"]:

            sku = variant["sku"]

            if not sku:
                continue

            variant_id = variant["id"]
            inventory_item_id = variant["inventory_item_id"]
            barcode = variant.get("barcode")
            price = variant.get("price")
            compare_at_price = variant.get("compare_at_price")
            weight_grams = variant.get("grams")
            unit_cost = unit_cost_by_inventory_item_id.get(inventory_item_id, {})
            qty = variant["inventory_quantity"]
            log_info("Import sku=%s qty=%s", sku, qty)

            cur.execute("""
            INSERT INTO items(
                sku,
                name,
                menge,
                available,
                unavailable,
                committed,
                reserved,
                shopify_product_id,
                shopify_variant_id,
                shopify_inventory_item_id,
                barcode,
                shopify_product_status,
                shopify_description,
                shopify_price,
                shopify_compare_at_price,
                shopify_unit_cost,
                shopify_unit_cost_currency,
                shopify_weight_grams,
                sync_status,
                last_sync,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'ok',NOW(),NOW())
            ON CONFLICT (sku)
            DO UPDATE SET
                name = EXCLUDED.name,
                menge = CASE
                    WHEN items.dirty = TRUE THEN items.menge
                    ELSE EXCLUDED.menge
                END,
                available = CASE
                    WHEN items.dirty = TRUE THEN items.available
                    ELSE EXCLUDED.available
                END,
                unavailable = COALESCE(items.unavailable, EXCLUDED.unavailable),
                committed = COALESCE(items.committed, EXCLUDED.committed),
                reserved = COALESCE(items.reserved, EXCLUDED.reserved),
                shopify_product_id = EXCLUDED.shopify_product_id,
                shopify_variant_id = EXCLUDED.shopify_variant_id,
                shopify_inventory_item_id = EXCLUDED.shopify_inventory_item_id,
                barcode = EXCLUDED.barcode,
                shopify_product_status = EXCLUDED.shopify_product_status,
                shopify_description = EXCLUDED.shopify_description,
                shopify_price = EXCLUDED.shopify_price,
                shopify_compare_at_price = EXCLUDED.shopify_compare_at_price,
                shopify_unit_cost = EXCLUDED.shopify_unit_cost,
                shopify_unit_cost_currency = EXCLUDED.shopify_unit_cost_currency,
                shopify_weight_grams = EXCLUDED.shopify_weight_grams,
                last_sync = NOW(),
                sync_status = 'ok',
                updated_at = NOW(),
                dirty = CASE
                    WHEN items.dirty = TRUE AND items.available = EXCLUDED.available THEN FALSE
                    ELSE items.dirty
                END
            """,
            (
                sku,
                name,
                qty,
                qty,
                0,
                0,
                0,
                product_id,
                variant_id,
                inventory_item_id,
                barcode,
                product.get("status"),
                product.get("body_html"),
                price,
                compare_at_price,
                unit_cost.get("amount"),
                unit_cost.get("currency"),
                weight_grams,
            ))

    con.commit()
    con.close()
    return len(products)


def _chunks(values, size):
    for index in range(0, len(values), size):
        yield values[index : index + size]


def get_inventory_item_unit_costs(inventory_item_ids):
    ids = sorted({item_id for item_id in inventory_item_ids if item_id})
    if not ids:
        return {}

    query = """
    query InventoryItemUnitCosts($ids: [ID!]!) {
      nodes(ids: $ids) {
        ... on InventoryItem {
          id
          unitCost {
            amount
            currencyCode
          }
        }
      }
    }
    """

    costs = {}
    for chunk in _chunks(ids, 100):
        gid_chunk = [f"gid://shopify/InventoryItem/{item_id}" for item_id in chunk]
        data = graphql_request(query, {"ids": gid_chunk})
        for node in data["nodes"]:
            if not node:
                continue
            gid = node["id"]
            try:
                item_id = int(gid.rsplit("/", 1)[-1])
            except (TypeError, ValueError):
                continue
            unit_cost = node.get("unitCost") or {}
            costs[item_id] = {
                "amount": unit_cost.get("amount"),
                "currency": unit_cost.get("currencyCode"),
            }
        time.sleep(0.2)

    return costs


def get_all_orders():
    query = """
    query OrdersPage($after: String) {
      orders(first: 50, after: $after, reverse: true, sortKey: CREATED_AT) {
        nodes {
          id
          name
          createdAt
          email
          displayFulfillmentStatus
          displayFinancialStatus
          shippingAddress {
            name
            address1
            zip
            city
            country
            phone
          }
          lineItems(first: 100) {
            nodes {
              id
              name
              sku
              quantity
              unfulfilledQuantity
            }
          }
          fulfillments {
            id
            status
            createdAt
            trackingInfo {
              number
              company
              url
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """

    orders = []
    after = None

    while True:
        data = graphql_request(query, {"after": after})
        page = data["orders"]
        orders.extend(page["nodes"])
        log_info("Orders-Seite geladen: gesamt=%s has_next=%s", len(orders), page["pageInfo"]["hasNextPage"])

        if not page["pageInfo"]["hasNextPage"]:
            return orders

        after = page["pageInfo"]["endCursor"]
        time.sleep(0.5)


def get_all_customers():
    query = """
    query CustomersPage($after: String) {
      customers(first: 50, after: $after, sortKey: UPDATED_AT) {
        nodes {
          id
          firstName
          lastName
          displayName
          email
          phone
          defaultAddress {
            name
            address1
            zip
            city
            country
            phone
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """

    customers = []
    after = None
    while True:
        data = graphql_request(query, {"after": after})
        page = data["customers"]
        customers.extend(page["nodes"])
        log_info("Customers-Seite geladen: gesamt=%s has_next=%s", len(customers), page["pageInfo"]["hasNextPage"])
        if not page["pageInfo"]["hasNextPage"]:
            return customers
        after = page["pageInfo"]["endCursor"]
        time.sleep(0.5)


def sync_customers():
    customers = get_all_customers()
    con = db()
    cur = con.cursor()
    cur.execute("TRUNCATE TABLE shopify_customers")

    for customer in customers:
        default_address = customer.get("defaultAddress") or {}
        cur.execute(
            """
            INSERT INTO shopify_customers (
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
                default_phone,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            """,
            (
                customer.get("id"),
                customer.get("firstName"),
                customer.get("lastName"),
                customer.get("displayName"),
                customer.get("email"),
                customer.get("phone"),
                default_address.get("name"),
                default_address.get("address1"),
                default_address.get("zip"),
                default_address.get("city"),
                default_address.get("country"),
                default_address.get("phone"),
            ),
        )

    con.commit()
    cur.close()
    con.close()
    log_info("Kunden synchronisiert: %s", len(customers))
    return len(customers)


def sync_orders():
    orders = get_all_orders()
    stats = summarize_orders(orders)
    log_info(
        "Order-Import gestartet: count=%s latest=%s created_at=%s line_items=%s",
        stats["count"],
        stats["latest_name"],
        stats["latest_created_at"],
        stats["line_items"],
    )

    con = db()
    cur = con.cursor()
    cur.execute("TRUNCATE TABLE shopify_order_items")
    cur.execute("TRUNCATE TABLE shopify_orders")

    for order in orders:
        shipping = order.get("shippingAddress") or {}

        cur.execute(
            """
            INSERT INTO shopify_orders (
                order_id,
                order_name,
                created_at,
                shipping_name,
                shipping_address1,
                shipping_zip,
                shipping_city,
                shipping_country,
                shipping_email,
                shipping_phone,
                fulfillment_status,
                payment_status,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            """,
            (
                order["id"],
                order["name"],
                order["createdAt"],
                shipping.get("name"),
                shipping.get("address1"),
                shipping.get("zip"),
                shipping.get("city"),
                shipping.get("country"),
                order.get("email"),
                shipping.get("phone"),
                order.get("displayFulfillmentStatus"),
                order.get("displayFinancialStatus"),
            ),
        )

        for index, line_item in enumerate(order["lineItems"]["nodes"], start=1):
            cur.execute(
                """
                INSERT INTO shopify_order_items (
                    order_id,
                    line_index,
                    order_line_item_id,
                    sku,
                    title,
                    quantity,
                    fulfilled_quantity
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    order["id"],
                    index,
                    line_item.get("id"),
                    line_item.get("sku"),
                    line_item["name"],
                    line_item["quantity"],
                    max(0, int(line_item.get("quantity") or 0) - int(line_item.get("unfulfilledQuantity") or 0)),
                ),
            )
        sync_order_shipments(cur, order)

    con.commit()
    con.close()
    log_info(
        "Bestellungen synchronisiert: count=%s latest=%s created_at=%s",
        stats["count"],
        stats["latest_name"],
        stats["latest_created_at"],
    )
    return len(orders)


def _normalize_carrier_name(value):
    raw = (value or "").strip()
    if not raw:
        return "shopify"
    normalized = raw.lower()
    if "gls" in normalized:
        return "gls"
    if "post" in normalized:
        return "post"
    return normalized[:32]


def _shopify_tracking_company(value):
    normalized = _normalize_carrier_name(value)
    if normalized == "gls":
        return "GLS"
    if normalized == "post":
        return "Deutsche Post"
    return (value or "").strip() or "GLS"


def _iter_fulfillments(order):
    rows = order.get("fulfillments") or []
    if isinstance(rows, dict):
        rows = rows.get("nodes") or []
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _iter_tracking_rows(fulfillment):
    rows = fulfillment.get("trackingInfo") or []
    if isinstance(rows, dict):
        rows = rows.get("nodes") or []
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def upsert_shopify_shipment(cur, order, fulfillment, tracking):
    tracking_number = (tracking.get("number") or "").strip()
    if not tracking_number:
        return
    fulfillment_id = (fulfillment.get("id") or "").strip() or None
    tracking_url = (tracking.get("url") or "").strip() or None
    status = (fulfillment.get("status") or "SHOPIFY_SYNCED").strip() or "SHOPIFY_SYNCED"
    carrier = _normalize_carrier_name(tracking.get("company"))
    parcel_number = tracking_number if tracking_number.isdigit() else None
    created_at = fulfillment.get("createdAt") or datetime.datetime.now(datetime.timezone.utc)
    _upsert_shopify_shipment_record(
        cur,
        carrier=carrier,
        order_id=order["id"],
        order_name=order["name"],
        shipment_reference=order["name"],
        tracking_number=tracking_number,
        parcel_number=parcel_number,
        status=status,
        fulfillment_id=fulfillment_id,
        tracking_url=tracking_url,
        created_at=created_at,
    )


def sync_order_shipments(cur, order):
    for fulfillment in _iter_fulfillments(order):
        for tracking in _iter_tracking_rows(fulfillment):
            upsert_shopify_shipment(cur, order, fulfillment, tracking)


def get_open_fulfillment_order_targets(order_id):
    query = """
    query FulfillmentOrdersForOrder($orderId: ID!) {
      order(id: $orderId) {
        id
        name
        fulfillmentOrders(first: 50) {
          nodes {
            id
            status
            requestStatus
            supportedActions {
              action
            }
            lineItems(first: 100) {
              nodes {
                id
                remainingQuantity
                lineItem {
                  id
                  sku
                }
              }
            }
          }
        }
      }
    }
    """
    data = graphql_request(query, {"orderId": order_id})
    order = data.get("order")
    if not order:
        raise RuntimeError(f"Order nicht gefunden: {order_id}")

    open_targets = []
    for node in (order.get("fulfillmentOrders", {}) or {}).get("nodes", []):
        status = (node.get("status") or "").upper()
        if status in {"CANCELLED", "CLOSED", "FULFILLED"}:
            continue
        actions = {entry.get("action") for entry in (node.get("supportedActions") or [])}
        if "CREATE_FULFILLMENT" in actions or not actions:
            line_items = []
            for li in (node.get("lineItems") or {}).get("nodes", []):
                line_item = li.get("lineItem") or {}
                line_items.append(
                    {
                        "fulfillment_order_line_item_id": li.get("id"),
                        "order_line_item_id": line_item.get("id"),
                        "sku": line_item.get("sku"),
                        "remaining_quantity": int(li.get("remainingQuantity") or 0),
                    }
                )
            open_targets.append({"fulfillment_order_id": node["id"], "line_items": line_items})

    if not open_targets:
        raise RuntimeError(f"Keine offenen FulfillmentOrders fuer {order.get('name') or order_id}.")
    return open_targets


def _build_line_items_by_fulfillment_order(open_targets, requested_items):
    if not requested_items:
        return [{"fulfillmentOrderId": target["fulfillment_order_id"]} for target in open_targets]

    requests = {}
    for item in requested_items:
        line_item_id = (item.get("order_line_item_id") or "").strip()
        quantity = int(item.get("quantity") or 0)
        if not line_item_id or quantity <= 0:
            continue
        requests[line_item_id] = requests.get(line_item_id, 0) + quantity

    if not requests:
        raise RuntimeError("Keine gueltigen line items fuer Fulfillment uebergeben.")

    by_fo = {}
    for line_item_id, requested_qty in requests.items():
        remaining_request = requested_qty
        for target in open_targets:
            fulfillment_order_id = target["fulfillment_order_id"]
            for source in target["line_items"]:
                if source.get("order_line_item_id") != line_item_id:
                    continue
                available = int(source.get("remaining_quantity") or 0)
                if available <= 0:
                    continue
                take = min(available, remaining_request)
                if take <= 0:
                    continue
                by_fo.setdefault(fulfillment_order_id, []).append(
                    {
                        "id": source["fulfillment_order_line_item_id"],
                        "quantity": take,
                    }
                )
                remaining_request -= take
                if remaining_request <= 0:
                    break
            if remaining_request <= 0:
                break

        if remaining_request > 0:
            raise RuntimeError(f"Menge fuer LineItem {line_item_id} nicht mehr offen (Rest {remaining_request}).")

    payload = []
    for fulfillment_order_id, rows in by_fo.items():
        payload.append(
            {
                "fulfillmentOrderId": fulfillment_order_id,
                "fulfillmentOrderLineItems": rows,
            }
        )
    if not payload:
        raise RuntimeError("Keine offenen FulfillmentOrder-Positionen gefunden.")
    return payload


def create_fulfillment(order_id, tracking_number, company, tracking_url=None, notify_customer=False, line_items=None):
    open_targets = get_open_fulfillment_order_targets(order_id)
    mutation = """
    mutation CreateFulfillment($fulfillment: FulfillmentInput!, $message: String) {
      fulfillmentCreate(fulfillment: $fulfillment, message: $message) {
        fulfillment {
          id
          status
          trackingInfo(first: 5) {
            number
            company
            url
          }
        }
        userErrors {
          field
          message
        }
      }
    }
    """

    line_items_payload = _build_line_items_by_fulfillment_order(open_targets, line_items)
    tracking_info = {
        "number": tracking_number,
        "company": _shopify_tracking_company(company),
    }
    if (tracking_url or "").strip():
        tracking_info["url"] = tracking_url.strip()

    variables = {
        "fulfillment": {
            "notifyCustomer": bool(notify_customer),
            "lineItemsByFulfillmentOrder": line_items_payload,
            "trackingInfo": tracking_info,
        },
        "message": "Lagerverwaltung Versand abgeschlossen",
    }
    data = graphql_request(mutation, variables)
    payload = (data.get("fulfillmentCreate") or {})
    user_errors = payload.get("userErrors") or []
    if user_errors:
        raise RuntimeError(f"Fulfillment userErrors: {user_errors}")
    fulfillment = payload.get("fulfillment")
    if not fulfillment:
        raise RuntimeError("Shopify hat kein Fulfillment zurueckgegeben.")
    return {
        "fulfillment_id": fulfillment.get("id"),
        "status": fulfillment.get("status"),
        "tracking": fulfillment.get("trackingInfo"),
        "fulfillment_order_ids": [entry["fulfillmentOrderId"] for entry in line_items_payload],
    }


def claim_fulfillment_jobs(limit=20):
    return _claim_shopify_fulfillment_jobs(
        db,
        cursor_factory=psycopg2.extras.RealDictCursor,
        limit=limit,
    )


def mark_fulfillment_job_done(job_id, label_id, fulfillment_id, status):
    return _mark_shopify_fulfillment_job_done(db, job_id, label_id, fulfillment_id, status)


def mark_fulfillment_job_failed(job_id, label_id, message):
    return _mark_shopify_fulfillment_job_failed(db, job_id, label_id, message)


def process_fulfillment_jobs(limit=20):
    jobs = claim_fulfillment_jobs(limit=limit)
    if not jobs:
        return 0, 0

    success_count = 0
    failed_count = 0
    for job in jobs:
        try:
            line_items = None
            if job.get("line_items_json"):
                try:
                    line_items = json.loads(job["line_items_json"])
                except json.JSONDecodeError:
                    raise RuntimeError(f"line_items_json ungueltig fuer Job {job['id']}")
            result = create_fulfillment(
                order_id=job["order_id"],
                tracking_number=job["tracking_number"],
                company=job["carrier"],
                tracking_url=job.get("tracking_url"),
                notify_customer=job["notify_customer"],
                line_items=line_items,
            )
            mark_fulfillment_job_done(
                job_id=job["id"],
                label_id=job.get("label_id"),
                fulfillment_id=result.get("fulfillment_id"),
                status=result.get("status") or "OK",
            )
            success_count += 1
        except Exception as exc:
            error_text = str(exc)
            mark_fulfillment_job_failed(job["id"], job.get("label_id"), error_text)
            log_error("Fulfillment Job %s fehlgeschlagen: %s", job["id"], error_text)
            failed_count += 1
    return success_count, failed_count


def run_sync_loop():
    init_db()
    update_service_runtime_state(status="idle", mark_seen=True, clear_error=True)
    while True:
        run_started_at = time.monotonic()
        update_service_runtime_state(status="running", mark_seen=True, mark_started=True, clear_error=True)
        log_info(
            "Starte Shopify Sync version=%s shop=%s db_host=%s interval=%ss log=%s",
            SYNC_VERSION,
            SHOP or "-",
            DB_HOST or "-",
            SYNC_INTERVAL,
            SYNC_LOG_PATH,
        )
        try:
            ok_jobs, failed_jobs = process_fulfillment_jobs(limit=20)
            if ok_jobs or failed_jobs:
                log_info("Fulfillment Jobs verarbeitet: ok=%s failed=%s", ok_jobs, failed_jobs)
            pushed_inventory = push_inventory_changes()
            if ok_jobs > 0 or pushed_inventory > 0:
                update_service_runtime_state(mark_seen=True, mark_push=True)
            sync_products()
            sync_inventory_levels()
            sync_customers()
            sync_orders()
            update_service_runtime_state(status="ok", mark_seen=True, mark_pull=True, mark_finished=True, clear_error=True)
        except Exception as exc:
            update_service_runtime_state(status="error", mark_seen=True, mark_finished=True, last_error=str(exc))
            log_exception("Sync-Fehler: %s", exc)
        else:
            duration = time.monotonic() - run_started_at
            log_info("Sync abgeschlossen in %.2fs", duration)
        log_info("Warte %s Sekunden", SYNC_INTERVAL)
        time.sleep(SYNC_INTERVAL)


def main():
    parser = argparse.ArgumentParser(description="Shopify Sync / Fulfillment Tool")
    parser.add_argument("--version", action="store_true", help="Aktuelle Shopify-Sync-Version ausgeben")
    sub = parser.add_subparsers(dest="command")
    fulfill_cmd = sub.add_parser("fulfill", help="Fulfillment fuer Bestellung erzeugen")
    fulfill_cmd.add_argument("--order-id", required=True, help="Shopify Order GID")
    fulfill_cmd.add_argument("--tracking-number", required=True, help="Trackingnummer")
    fulfill_cmd.add_argument("--company", required=True, help="Versanddienstleister")
    fulfill_cmd.add_argument("--notify-customer", action="store_true", help="Kundenbenachrichtigung aktivieren")
    version_cmd = sub.add_parser("version", help="Shopify-Sync-Version ausgeben")
    version_cmd.add_argument("--json", action="store_true", help="Version als JSON ausgeben")

    args = parser.parse_args()
    if args.version:
        print(SYNC_VERSION)
        return
    if args.command == "version":
        if args.json:
            print(json.dumps(build_sync_version_payload(), ensure_ascii=False))
        else:
            print(SYNC_VERSION)
        return
    if args.command == "fulfill":
        result = create_fulfillment(
            order_id=args.order_id,
            tracking_number=args.tracking_number,
            company=args.company,
            notify_customer=args.notify_customer,
        )
        print(json.dumps(result, ensure_ascii=False))
        return

    run_sync_loop()

if __name__ == "__main__":
    main()
