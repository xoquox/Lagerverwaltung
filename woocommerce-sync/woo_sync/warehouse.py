"""Transactional imports and a durable local outbox for WooCommerce."""

import json
import uuid
from .protocol import (
    command_id,
    identifier,
    json_text,
    number,
    parse_id,
    validate_event,
    wc_id,
)
from shipping.shop import assert_shop_system

LOCATION_ID = "wc:location:main"


def migrate(cur):
    assert_shop_system(cur, "woocommerce")
    cur.execute("""CREATE TABLE IF NOT EXISTS woocommerce_sync_state (
        id integer PRIMARY KEY CHECK (id=1), client_id text NOT NULL,
        store_id text, updated_at timestamptz NOT NULL DEFAULT NOW())""")
    cur.execute(
        "INSERT INTO woocommerce_sync_state(id,client_id) VALUES(1,%s) ON CONFLICT DO NOTHING",
        (str(uuid.uuid4()),),
    )
    cur.execute("""CREATE TABLE IF NOT EXISTS woocommerce_sync_entities (
        kind text NOT NULL, entity_id bigint NOT NULL, seq bigint NOT NULL,
        payload jsonb, PRIMARY KEY(kind,entity_id))""")
    cur.execute("""CREATE TABLE IF NOT EXISTS woocommerce_sync_commands (
        command_id text PRIMARY KEY, kind text NOT NULL, entity_id text NOT NULL,
        revision timestamptz, payload jsonb NOT NULL, status text NOT NULL DEFAULT 'pending',
        result jsonb, error text, created_at timestamptz NOT NULL DEFAULT NOW(),
        updated_at timestamptz NOT NULL DEFAULT NOW())""")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_woo_commands_status ON woocommerce_sync_commands(status,created_at)"
    )
    cur.execute("""CREATE TABLE IF NOT EXISTS woocommerce_order_lines (
        order_id text NOT NULL REFERENCES shop_orders(order_id) ON DELETE CASCADE,
        line_id text NOT NULL, product_id bigint, stock_reduced boolean NOT NULL,
        PRIMARY KEY(order_id,line_id))""")
    cur.execute(
        "INSERT INTO shop_locations(location_id,name,fulfills_online_orders) VALUES(%s,'WooCommerce',TRUE) ON CONFLICT DO NOTHING",
        (LOCATION_ID,),
    )
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_woo_variant_unique ON items(shop_variant_id) WHERE shop_variant_id LIKE 'wc:variant:%'"
    )


class Warehouse:
    def __init__(self, connection):
        self.con = connection

    def bind(self, store_id):
        with self.con.cursor() as cur:
            assert_shop_system(cur, "woocommerce")
            cur.execute("SELECT * FROM woocommerce_sync_state WHERE id=1 FOR UPDATE")
            row = cur.fetchone()
            if not row:
                raise RuntimeError("WooCommerce-Schema fehlt. migrate ausfuehren.")
            if row["store_id"] and row["store_id"] != store_id:
                raise RuntimeError(
                    "Lagerdatenbank ist an einen anderen WooCommerce-Shop gebunden."
                )
            cur.execute(
                "UPDATE woocommerce_sync_state SET store_id=%s,updated_at=NOW() WHERE id=1",
                (store_id,),
            )
        self.con.commit()
        return row["client_id"]

    def heartbeat(self, status, *, error=None, pull=False, push=False):
        from . import VERSION

        with self.con.cursor() as cur:
            cur.execute(
                """INSERT INTO service_runtime_state(service,version,status,last_seen_at,last_error,last_pull_at,last_push_at)
                VALUES('woocommerce-sync',%s,%s,NOW(),%s,CASE WHEN %s THEN NOW() END,CASE WHEN %s THEN NOW() END)
                ON CONFLICT(service) DO UPDATE SET version=EXCLUDED.version,status=EXCLUDED.status,
                last_seen_at=NOW(),last_error=EXCLUDED.last_error,
                last_pull_at=COALESCE(EXCLUDED.last_pull_at,service_runtime_state.last_pull_at),
                last_push_at=COALESCE(EXCLUDED.last_push_at,service_runtime_state.last_push_at),updated_at=NOW()""",
                (VERSION, status, str(error)[:1000] if error else None, pull, push),
            )
        self.con.commit()

    def import_events(self, events):
        # Validation precedes all writes. Invalid batches remain visible remotely.
        validated = [validate_event(event) for event in events]
        with self.con.cursor() as cur:
            assert_shop_system(cur, "woocommerce")
            for seq, kind, entity_id, payload in validated:
                cur.execute(
                    """INSERT INTO woocommerce_sync_entities(kind,entity_id,seq,payload) VALUES(%s,%s,%s,%s::jsonb)
                    ON CONFLICT(kind,entity_id) DO UPDATE SET seq=EXCLUDED.seq,payload=EXCLUDED.payload
                    WHERE woocommerce_sync_entities.seq < EXCLUDED.seq RETURNING seq""",
                    (
                        kind,
                        entity_id,
                        seq,
                        json_text(payload) if payload is not None else None,
                    ),
                )
                if not cur.fetchone():
                    continue  # Redelivery or a late commit with an older sequence.
                if payload is None:
                    self._delete(cur, kind, entity_id)
                elif kind == "product":
                    self._product(cur, payload)
                elif kind == "order":
                    self._order(cur, payload)
                else:
                    self._customer(cur, wc_id("customer", entity_id), payload)
            self._inventory(cur)
        self.con.commit()
        return [row[0] for row in validated]

    def _product(self, cur, p):
        variant = wc_id("variant", p["id"])
        cur.execute("SELECT sku FROM items WHERE shop_variant_id=%s", (variant,))
        old = cur.fetchone()
        sku = old["sku"] if old else variant
        cur.execute(
            """INSERT INTO items(sku,display_sku,name,shop_product_id,shop_variant_id,shop_inventory_item_id,
                shop_product_status,shop_description,shop_price,shop_compare_at_price,shop_unit_cost,
                shop_unit_cost_currency,shop_weight_grams,barcode,external_fulfillment,sync_status,last_sync)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'EUR',%s,%s,%s,'synced',NOW())
            ON CONFLICT(sku) DO UPDATE SET
                display_sku=CASE WHEN items.shop_product_dirty THEN items.display_sku ELSE EXCLUDED.display_sku END,
                name=CASE WHEN items.shop_product_dirty THEN items.name ELSE EXCLUDED.name END,
                shop_product_id=EXCLUDED.shop_product_id,shop_variant_id=EXCLUDED.shop_variant_id,
                shop_inventory_item_id=EXCLUDED.shop_inventory_item_id,
                shop_product_status=CASE WHEN items.shop_product_dirty THEN items.shop_product_status ELSE EXCLUDED.shop_product_status END,
                shop_description=CASE WHEN items.shop_product_dirty THEN items.shop_description ELSE EXCLUDED.shop_description END,
                shop_price=CASE WHEN items.shop_product_dirty THEN items.shop_price ELSE EXCLUDED.shop_price END,
                shop_compare_at_price=CASE WHEN items.shop_product_dirty THEN items.shop_compare_at_price ELSE EXCLUDED.shop_compare_at_price END,
                shop_unit_cost=CASE WHEN items.shop_product_dirty THEN items.shop_unit_cost ELSE EXCLUDED.shop_unit_cost END,
                shop_unit_cost_currency='EUR',
                shop_weight_grams=CASE WHEN items.shop_product_dirty THEN items.shop_weight_grams ELSE EXCLUDED.shop_weight_grams END,
                barcode=CASE WHEN items.shop_product_dirty THEN items.barcode ELSE EXCLUDED.barcode END,
                last_sync=NOW(),sync_status=CASE WHEN items.dirty OR items.shop_product_dirty THEN items.sync_status ELSE 'synced' END""",
            (
                sku,
                p.get("sku") or "",
                p["name"],
                wc_id("product", p.get("parent_id") or p["id"]),
                variant,
                wc_id("stock", p["id"]),
                p["status"],
                p.get("description", ""),
                p.get("price"),
                p.get("compare_at_price"),
                p.get("unit_cost"),
                p.get("weight_grams"),
                p.get("barcode"),
                not p.get("requires_shipping", True),
            ),
        )
        cur.execute(
            "INSERT INTO item_location_inventory(sku,location_id) VALUES(%s,%s) ON CONFLICT DO NOTHING",
            (sku, LOCATION_ID),
        )
        if not p["inventory_supported"] and p.get("requires_shipping", True):
            cur.execute(
                "UPDATE items SET sync_status='shop_error',shop_product_sync_error=%s WHERE sku=%s AND NOT shop_product_dirty",
                (
                    "Bestandsmodell nicht unterstuetzt: "
                    + (
                        p.get("inventory_error")
                        or "Keine eigene Stueckzahl-Verwaltung."
                    ),
                    sku,
                ),
            )

    def _customer(self, cur, customer_id, p):
        a = p.get("address") or {}
        cur.execute(
            """INSERT INTO shop_customers(customer_id,first_name,last_name,display_name,email,phone,
                default_name,default_address1,default_zip,default_city,default_country,default_phone)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(customer_id) DO UPDATE SET
                first_name=EXCLUDED.first_name,last_name=EXCLUDED.last_name,display_name=EXCLUDED.display_name,
                email=EXCLUDED.email,phone=EXCLUDED.phone,default_name=EXCLUDED.default_name,
                default_address1=EXCLUDED.default_address1,default_zip=EXCLUDED.default_zip,
                default_city=EXCLUDED.default_city,default_country=EXCLUDED.default_country,
                default_phone=EXCLUDED.default_phone,updated_at=NOW()""",
            (
                customer_id,
                p.get("first_name"),
                p.get("last_name"),
                p.get("name"),
                p.get("email"),
                p.get("phone"),
                a.get("name") or p.get("name"),
                a.get("address1"),
                a.get("zip"),
                a.get("city"),
                a.get("country"),
                a.get("phone") or p.get("phone"),
            ),
        )

    def _order(self, cur, p):
        oid = wc_id("order", p["id"])
        a = p.get("address") or {}
        cur.execute(
            """INSERT INTO shop_orders(order_id,order_name,created_at,shipping_name,shipping_address1,
                shipping_address2,shipping_zip,shipping_city,shipping_country,shipping_email,shipping_phone,
                fulfillment_status,payment_status,source)
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'woocommerce')
            ON CONFLICT(order_id) DO UPDATE SET order_name=EXCLUDED.order_name,created_at=EXCLUDED.created_at,
                shipping_name=EXCLUDED.shipping_name,shipping_address1=EXCLUDED.shipping_address1,
                shipping_address2=EXCLUDED.shipping_address2,shipping_zip=EXCLUDED.shipping_zip,
                shipping_city=EXCLUDED.shipping_city,shipping_country=EXCLUDED.shipping_country,
                shipping_email=EXCLUDED.shipping_email,shipping_phone=EXCLUDED.shipping_phone,
                fulfillment_status=EXCLUDED.fulfillment_status,payment_status=EXCLUDED.payment_status,updated_at=NOW()
            WHERE shop_orders.source='woocommerce'""",
            (
                oid,
                p.get("number") or f"#{p['id']}",
                p.get("created_at"),
                a.get("name"),
                a.get("address1"),
                a.get("address2"),
                a.get("zip"),
                a.get("city"),
                a.get("country"),
                a.get("email"),
                a.get("phone"),
                p["fulfillment_status"],
                p["payment_status"],
            ),
        )
        cur.execute("DELETE FROM shop_fulfillment_orders WHERE order_id=%s", (oid,))
        cur.execute("DELETE FROM shop_order_items WHERE order_id=%s", (oid,))
        cur.execute("DELETE FROM woocommerce_order_lines WHERE order_id=%s", (oid,))
        fid = wc_id("fulfillment", p["id"])
        cur.execute(
            """INSERT INTO shop_fulfillment_orders(fulfillment_order_id,order_id,assigned_location_id,assigned_location_name,status)
            VALUES(%s,%s,%s,'WooCommerce',%s)""",
            (fid, oid, LOCATION_ID, p["fulfillment_status"]),
        )
        for index, line in enumerate(p["items"]):
            lid = wc_id("line", line["id"])
            variant = (
                wc_id("variant", line["product_id"]) if line.get("product_id") else None
            )
            cur.execute("SELECT sku FROM items WHERE shop_variant_id=%s", (variant,))
            product = cur.fetchone()
            sku = product["sku"] if product else variant
            qty, fulfilled = number(line["quantity"]), number(line["fulfilled"])
            cur.execute(
                "INSERT INTO shop_order_items(order_id,line_index,order_line_item_id,sku,title,quantity,fulfilled_quantity) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                (oid, index, lid, sku, line["name"], qty, fulfilled),
            )
            cur.execute(
                """INSERT INTO shop_fulfillment_order_items(fulfillment_order_line_item_id,fulfillment_order_id,
                order_id,order_line_item_id,sku,title,quantity,remaining_quantity,assigned_location_id,assigned_location_name)
                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,'WooCommerce')""",
                (
                    wc_id("fulfillment-line", line["id"]),
                    fid,
                    oid,
                    lid,
                    sku,
                    line["name"],
                    qty,
                    qty - fulfilled,
                    LOCATION_ID,
                ),
            )
            cur.execute(
                "INSERT INTO woocommerce_order_lines(order_id,line_id,product_id,stock_reduced) VALUES(%s,%s,%s,%s)",
                (oid, lid, line.get("product_id"), bool(line.get("stock_reduced"))),
            )
        if not p.get("customer_id"):
            self._customer(
                cur,
                wc_id("guest", p["id"]),
                dict(
                    name=a.get("name"),
                    email=a.get("email"),
                    phone=a.get("phone"),
                    address=a,
                ),
            )
        from shipping.history import upsert_shop_shipment

        for shipment in p.get("shipments", []):
            if shipment.get("tracking_number"):
                upsert_shop_shipment(
                    cur,
                    carrier=shipment.get("carrier", "other"),
                    order_id=oid,
                    order_name=p.get("number") or f"#{p['id']}",
                    shipment_reference=p.get("number") or str(p["id"]),
                    tracking_number=shipment["tracking_number"],
                    parcel_number=None,
                    status="SHOP_FULFILLED",
                    fulfillment_id=str(shipment["id"]),
                    tracking_url=shipment.get("tracking_url"),
                    created_at=shipment.get("created_at"),
                    source="woocommerce",
                )

    def _delete(self, cur, kind, entity_id):
        if kind == "product":
            cur.execute(
                "UPDATE items SET shop_product_status='ARCHIVED' WHERE shop_variant_id=%s AND NOT shop_product_dirty",
                (wc_id("variant", entity_id),),
            )
        elif kind == "customer":
            cur.execute(
                "DELETE FROM shop_customers WHERE customer_id=%s",
                (wc_id("customer", entity_id),),
            )
        else:
            oid = wc_id("order", entity_id)
            cur.execute("DELETE FROM shop_order_items WHERE order_id=%s", (oid,))
            cur.execute(
                "DELETE FROM shop_orders WHERE order_id=%s AND source='woocommerce'",
                (oid,),
            )
            cur.execute(
                "DELETE FROM shop_customers WHERE customer_id=%s",
                (wc_id("guest", entity_id),),
            )

    def _inventory(self, cur):
        cur.execute(
            """SELECT i.sku,e.payload,ili.dirty FROM woocommerce_sync_entities e
            JOIN items i ON i.shop_variant_id='wc:variant:' || e.entity_id::text
            JOIN item_location_inventory ili ON ili.sku=i.sku AND ili.location_id=%s
            WHERE e.kind='product' AND e.payload IS NOT NULL""",
            (LOCATION_ID,),
        )
        for row in cur.fetchall():
            p, sku = row["payload"], row["sku"]
            if not p["inventory_supported"]:
                continue
            committed = self._committed(cur, p["id"])
            manual = self._manual_reserved(cur, sku)
            held, stock = number(p["held"]), number(p["stock"])
            reserved = held + manual
            if row["dirty"]:
                cur.execute(
                    "UPDATE item_location_inventory SET committed=%s,reserved=%s,unavailable=%s,available=GREATEST(menge-%s-%s,0) WHERE sku=%s AND location_id=%s",
                    (
                        committed,
                        reserved,
                        reserved,
                        committed,
                        reserved,
                        sku,
                        LOCATION_ID,
                    ),
                )
                continue
            cur.execute(
                """UPDATE item_location_inventory SET menge=%s,available=%s,reserved=%s,unavailable=%s,
                committed=%s WHERE sku=%s AND location_id=%s AND NOT dirty""",
                (
                    stock + committed + manual,
                    max(0, stock - held),
                    reserved,
                    reserved,
                    committed,
                    sku,
                    LOCATION_ID,
                ),
            )
            # Remote stock already excludes our previously synchronized manual reservation.
            # Initial/single snapshot physical total includes that reservation again.
        self._totals(cur)

    def _committed(self, cur, product_id):
        cur.execute(
            """SELECT COALESCE(SUM(GREATEST(oi.quantity-oi.fulfilled_quantity,0)),0) AS qty
            FROM woocommerce_order_lines w JOIN shop_order_items oi ON oi.order_id=w.order_id AND oi.order_line_item_id=w.line_id
            JOIN shop_orders o ON o.order_id=w.order_id WHERE w.product_id=%s AND w.stock_reduced
            AND o.fulfillment_status NOT IN ('fulfilled','cancelled')""",
            (product_id,),
        )
        return int(cur.fetchone()["qty"])

    def _manual_reserved(self, cur, sku):
        cur.execute(
            """SELECT COALESCE(SUM(f.remaining_quantity),0) AS qty FROM shop_fulfillment_order_items f
            JOIN shop_orders o ON o.order_id=f.order_id WHERE o.source='custom' AND f.sku=%s
            AND f.assigned_location_id=%s AND o.fulfillment_status NOT IN ('fulfilled','cancelled')""",
            (sku, LOCATION_ID),
        )
        return int(cur.fetchone()["qty"])

    def _totals(self, cur):
        cur.execute(
            """UPDATE items i SET menge=t.menge,available=t.available,reserved=t.reserved,
            unavailable=t.unavailable,committed=t.committed,dirty=t.dirty FROM (
                SELECT sku,SUM(menge) menge,SUM(available) available,SUM(reserved) reserved,
                SUM(unavailable) unavailable,SUM(committed) committed,BOOL_OR(dirty) dirty
                FROM item_location_inventory GROUP BY sku) t WHERE i.sku=t.sku"""
        )

    def commands(self):
        with self.con.cursor() as cur:
            cur.execute(
                "SELECT * FROM woocommerce_sync_commands WHERE status IN ('pending','submitted') ORDER BY created_at"
            )
            return cur.fetchall()

    def enqueue_changes(self, capabilities):
        with self.con.cursor() as cur:
            self._inventory(cur)
            cur.execute(
                """SELECT i.*,e.payload remote FROM items i LEFT JOIN woocommerce_sync_entities e
                ON e.kind='product' AND i.shop_variant_id='wc:variant:' || e.entity_id::text
                WHERE i.shop_product_dirty"""
            )
            for row in cur.fetchall():
                remote = row["remote"]
                if row["shop_product_sync_action"] == "create" or not remote:
                    cur.execute(
                        "UPDATE items SET shop_product_sync_error=%s,sync_status='shop_error' WHERE sku=%s",
                        (
                            "Produktanlage/Shopbefuellung ist fuer einen spaeteren Schritt vorgesehen.",
                            row["sku"],
                        ),
                    )
                    continue
                payload = dict(
                    product_id=remote["id"],
                    expected_fingerprint=remote["fingerprint"],
                    fields={
                        "name": row["name"],
                        "sku": row["display_sku"] or "",
                        "description": row["shop_description"] or "",
                        "status": row["shop_product_status"] or "DRAFT",
                        "price": row["shop_price"],
                        "compare_at_price": row["shop_compare_at_price"],
                        "weight_grams": row["shop_weight_grams"],
                        "unit_cost": row["shop_unit_cost"],
                        "barcode": row["barcode"],
                    },
                )
                self._enqueue(
                    cur, "product_update", row["sku"], row["updated_at"], payload
                )
            cur.execute(
                """SELECT ili.*,i.shop_variant_id,e.payload remote FROM item_location_inventory ili
                JOIN items i ON i.sku=ili.sku LEFT JOIN woocommerce_sync_entities e
                ON e.kind='product' AND i.shop_variant_id='wc:variant:' || e.entity_id::text
                WHERE ili.dirty AND ili.location_id=%s""",
                (LOCATION_ID,),
            )
            for row in cur.fetchall():
                remote = row["remote"]
                if not remote or not remote["inventory_supported"]:
                    cur.execute(
                        "UPDATE items SET sync_status='shop_error',shop_product_sync_error=%s WHERE sku=%s",
                        ("Bestandsmodell nicht unterstuetzt.", row["sku"]),
                    )
                    continue
                manual = self._manual_reserved(cur, row["sku"])
                payload = dict(
                    product_id=remote["id"],
                    expected_stock=remote["stock"],
                    stock=max(
                        0, row["menge"] - self._committed(cur, remote["id"]) - manual
                    ),
                )
                self._enqueue(cur, "inventory", row["sku"], row["updated_at"], payload)
            cur.execute(
                "SELECT * FROM shop_fulfillment_jobs WHERE status='pending' FOR UPDATE SKIP LOCKED"
            )
            for row in cur.fetchall():
                try:
                    oid = parse_id("order", row["order_id"])
                    if row["notify_customer"]:
                        raise ValueError(
                            "Separate Versandbenachrichtigung ist in dieser Brueckenversion nicht implementiert."
                        )
                    if not capabilities.get("native_fulfillments"):
                        raise ValueError(
                            "Versandrueckmeldung benoetigt aktivierte native WooCommerce-Fulfillments >= 10.7."
                        )
                    items = (
                        json.loads(row["line_items_json"])
                        if row["line_items_json"]
                        else None
                    )
                    lines = (
                        None
                        if items is None
                        else [
                            dict(
                                item_id=parse_id("line", i["order_line_item_id"]),
                                qty=number(i["quantity"], minimum=1),
                            )
                            for i in items
                        ]
                    )
                    payload = dict(
                        order_id=oid,
                        tracking_number=row["tracking_number"],
                        tracking_url=row["tracking_url"],
                        carrier=row["carrier"],
                        items=lines,
                    )
                    self._enqueue(
                        cur, "fulfillment", str(row["id"]), row["created_at"], payload
                    )
                    cur.execute(
                        "UPDATE shop_fulfillment_jobs SET status='processing',attempts=attempts+1,updated_at=NOW() WHERE id=%s",
                        (row["id"],),
                    )
                except (ValueError, TypeError, KeyError) as exc:
                    cur.execute(
                        "UPDATE shop_fulfillment_jobs SET status='failed',result_message=%s,processed_at=NOW() WHERE id=%s",
                        (str(exc), row["id"]),
                    )
                    if row["label_id"]:
                        cur.execute(
                            "UPDATE shipping_labels SET status='SHOP_FAILED',last_error=%s WHERE id=%s",
                            (str(exc), row["label_id"]),
                        )
        self.con.commit()

    def _enqueue(self, cur, kind, entity_id, revision, payload):
        key = command_id(kind, entity_id, revision, payload)
        cur.execute(
            """SELECT 1 FROM woocommerce_sync_commands WHERE kind=%s AND entity_id=%s
            AND (status IN ('pending','submitted') OR revision=%s)""",
            (kind, entity_id, revision),
        )
        if cur.fetchone():
            return
        cur.execute(
            """INSERT INTO woocommerce_sync_commands(command_id,kind,entity_id,revision,payload)
            VALUES(%s,%s,%s,%s,%s::jsonb) ON CONFLICT DO NOTHING""",
            (key, kind, entity_id, revision, json_text(payload)),
        )

    def submitted(self, key):
        with self.con.cursor() as cur:
            cur.execute(
                "UPDATE woocommerce_sync_commands SET status='submitted',updated_at=NOW() WHERE command_id=%s AND status='pending'",
                (key,),
            )
        self.con.commit()

    def complete(self, command, result):
        # Do not clear dirty flags until the authoritative post-write snapshot
        # has arrived. Otherwise an old baseline could overwrite a local edit.
        if result["status"] == "done":
            data = result.get("result") or {}
            kind = "order" if command["kind"] == "fulfillment" else "product"
            entity = command["payload"].get(
                "order_id" if kind == "order" else "product_id"
            )
            seq = identifier(data["event_seq"])
            with self.con.cursor() as cur:
                cur.execute(
                    "SELECT seq FROM woocommerce_sync_entities WHERE kind=%s AND entity_id=%s",
                    (kind, entity),
                )
                row = cur.fetchone()
                if not row or row["seq"] < seq:
                    self.con.commit()
                    return
        with self.con.cursor() as cur:
            cur.execute(
                "SELECT status FROM woocommerce_sync_commands WHERE command_id=%s FOR UPDATE",
                (command["command_id"],),
            )
            if cur.fetchone()["status"] not in {"pending", "submitted"}:
                self.con.commit()
                return
            ok = result["status"] == "done"
            error = result.get("error") or "WooCommerce-Auftrag fehlgeschlagen."
            cur.execute(
                "UPDATE woocommerce_sync_commands SET status=%s,result=%s::jsonb,error=%s,updated_at=NOW() WHERE command_id=%s",
                (
                    "done" if ok else "failed",
                    json_text(result.get("result")),
                    None if ok else error,
                    command["command_id"],
                ),
            )
            if command["kind"] == "product_update":
                cur.execute(
                    """UPDATE items SET shop_product_dirty=CASE WHEN %s THEN FALSE ELSE shop_product_dirty END,
                    shop_product_sync_error=%s,sync_status=%s WHERE sku=%s AND updated_at=%s""",
                    (
                        ok,
                        None if ok else error,
                        "synced" if ok else "shop_error",
                        command["entity_id"],
                        command["revision"],
                    ),
                )
                if ok:
                    cur.execute(
                        "SELECT payload FROM woocommerce_sync_entities WHERE kind='product' AND entity_id=%s",
                        (command["payload"]["product_id"],),
                    )
                    snapshot = cur.fetchone()
                    if snapshot and snapshot["payload"]:
                        self._product(cur, snapshot["payload"])
            elif command["kind"] == "inventory":
                if ok:
                    cur.execute(
                        "UPDATE item_location_inventory SET dirty=FALSE WHERE sku=%s AND location_id=%s AND updated_at=%s",
                        (command["entity_id"], LOCATION_ID, command["revision"]),
                    )
                    self._inventory(cur)
                else:
                    cur.execute(
                        "UPDATE items SET sync_status='shop_error' WHERE sku=%s",
                        (command["entity_id"],),
                    )
            else:
                fid = (result.get("result") or {}).get("fulfillment_id")
                cur.execute(
                    """UPDATE shop_fulfillment_jobs SET status=%s,shop_fulfillment_id=%s,result_message=%s,
                    processed_at=NOW(),updated_at=NOW() WHERE id=%s RETURNING label_id""",
                    (
                        "done" if ok else "failed",
                        str(fid) if fid else None,
                        "fulfilled" if ok else error,
                        int(command["entity_id"]),
                    ),
                )
                label = cur.fetchone()
                if label and label["label_id"]:
                    cur.execute(
                        "UPDATE shipping_labels SET status=%s,last_error=%s,shop_fulfillment_id=%s,shop_synced_at=CASE WHEN %s THEN NOW() ELSE shop_synced_at END WHERE id=%s",
                        (
                            "SHOP_FULFILLED" if ok else "SHOP_FAILED",
                            None if ok else error,
                            str(fid) if fid else None,
                            ok,
                            label["label_id"],
                        ),
                    )
        self.con.commit()
