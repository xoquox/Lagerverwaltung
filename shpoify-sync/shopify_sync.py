import os
import time

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

SHOP = os.getenv("SHOP")
TOKEN = os.getenv("TOKEN")

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

API_VERSION = "2026-01"
SHOPIFY_LOCATION_ID = 67402989753
GRAPHQL_URL = f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json"

def db():
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
    con.commit()
    cur.close()
    con.close()


def graphql_request(query, variables=None):
    headers = {
        "X-Shopify-Access-Token": TOKEN,
        "Content-Type": "application/json",
    }
    payload = {"query": query, "variables": variables or {}}
    response = requests.post(GRAPHQL_URL, json=payload, headers=headers)
    response.raise_for_status()

    data = response.json()
    errors = data.get("errors")

    if errors:
        raise RuntimeError(f"Shopify GraphQL Fehler: {errors}")

    return data["data"]


def get_products_page(url):
    headers = {
        "X-Shopify-Access-Token": TOKEN,
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response


def get_all_products():
    products = []
    url = f"https://{SHOP}/admin/api/{API_VERSION}/products.json?limit=250"

    while url:
        response = get_products_page(url)
        data = response.json()
        products.extend(data["products"])

        print(f"Geladen: {len(products)} Produkte")

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
        return

    print(f"Push {len(rows)} Lageränderungen zu Shopify")

    headers = {
        "X-Shopify-Access-Token": TOKEN,
        "Content-Type": "application/json",
    }

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
        )

        if response.status_code != 200:
            print("Shopify Fehler:", response.text)
            continue

        print("Shopify Update:", sku, available_qty)

        cur.execute("""
            UPDATE items
            SET dirty = FALSE,
                sync_status = 'pushed',
                last_sync = NOW()
            WHERE sku = %s
        """,
        (sku,),
        )

        time.sleep(0.5)

    con.commit()
    con.close()


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
        print("Keine Inventory-Levels von Shopify geladen")
        return

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
    print(f"Inventory-Levels synchronisiert: {len(inventory_by_sku)}")


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
            print("Import:", sku, qty)

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
          displayFulfillmentStatus
          displayFinancialStatus
          shippingAddress {
            name
            address1
            zip
            city
            country
          }
          lineItems(first: 100) {
            nodes {
              name
              sku
              quantity
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

        if not page["pageInfo"]["hasNextPage"]:
            return orders

        after = page["pageInfo"]["endCursor"]
        time.sleep(0.5)


def sync_orders():
    orders = get_all_orders()

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
                fulfillment_status,
                payment_status,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
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
                    sku,
                    title,
                    quantity
                )
                VALUES (%s,%s,%s,%s,%s)
                """,
                (
                    order["id"],
                    index,
                    line_item.get("sku"),
                    line_item["name"],
                    line_item["quantity"],
                ),
            )

    con.commit()
    con.close()
    print(f"Bestellungen synchronisiert: {len(orders)}")


SYNC_INTERVAL = 60

if __name__ == "__main__":
    init_db()

    while True:

        print("Starte Shopify Sync")

        try:
            push_inventory_changes()
            sync_products()
            sync_inventory_levels()
            sync_orders()
        except Exception as e:
            print("Fehler:", e)

        print("Sync abgeschlossen")
        print(f"Warte {SYNC_INTERVAL} Sekunden")

        time.sleep(SYNC_INTERVAL)
