"""Gemeinsame SQL-Helfer fuer Versandlabel-History und Shopify-Fulfillment-Jobs."""

import datetime
import json


SHIPPING_LABEL_TABLE = "shipping_labels"
SHOPIFY_FULFILLMENT_JOB_TABLE = "shopify_fulfillment_jobs"


def ensure_shipping_history_schema(cur):
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SHIPPING_LABEL_TABLE} (
            id serial PRIMARY KEY,
            carrier text NOT NULL DEFAULT 'gls',
            order_id text NOT NULL,
            order_name text NOT NULL,
            shipment_reference text NOT NULL,
            track_id text NOT NULL UNIQUE,
            parcel_number text,
            weight_kg numeric(8,3) NOT NULL DEFAULT 1.0,
            status text NOT NULL DEFAULT 'CREATED',
            label_path text NOT NULL DEFAULT '',
            last_error text,
            source text NOT NULL DEFAULT 'local',
            shopify_fulfillment_id text,
            shopify_synced_at timestamptz,
            tracking_url text,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            updated_at timestamptz NOT NULL DEFAULT NOW(),
            cancel_requested_at timestamptz,
            cancelled_at timestamptz
        )
        """
    )
    cur.execute(f"ALTER TABLE {SHIPPING_LABEL_TABLE} ADD COLUMN IF NOT EXISTS carrier text NOT NULL DEFAULT 'gls'")
    cur.execute(f"ALTER TABLE {SHIPPING_LABEL_TABLE} ADD COLUMN IF NOT EXISTS shipment_reference text")
    cur.execute(f"ALTER TABLE {SHIPPING_LABEL_TABLE} ADD COLUMN IF NOT EXISTS parcel_number text")
    cur.execute(f"ALTER TABLE {SHIPPING_LABEL_TABLE} ADD COLUMN IF NOT EXISTS weight_kg numeric(8,3) NOT NULL DEFAULT 1.0")
    cur.execute(f"ALTER TABLE {SHIPPING_LABEL_TABLE} ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'CREATED'")
    cur.execute(f"ALTER TABLE {SHIPPING_LABEL_TABLE} ADD COLUMN IF NOT EXISTS label_path text NOT NULL DEFAULT ''")
    cur.execute(f"ALTER TABLE {SHIPPING_LABEL_TABLE} ADD COLUMN IF NOT EXISTS last_error text")
    cur.execute(f"ALTER TABLE {SHIPPING_LABEL_TABLE} ADD COLUMN IF NOT EXISTS cancel_requested_at timestamptz")
    cur.execute(f"ALTER TABLE {SHIPPING_LABEL_TABLE} ADD COLUMN IF NOT EXISTS cancelled_at timestamptz")
    cur.execute(f"ALTER TABLE {SHIPPING_LABEL_TABLE} ADD COLUMN IF NOT EXISTS source text NOT NULL DEFAULT 'local'")
    cur.execute(f"ALTER TABLE {SHIPPING_LABEL_TABLE} ADD COLUMN IF NOT EXISTS shopify_fulfillment_id text")
    cur.execute(f"ALTER TABLE {SHIPPING_LABEL_TABLE} ADD COLUMN IF NOT EXISTS shopify_synced_at timestamptz")
    cur.execute(f"ALTER TABLE {SHIPPING_LABEL_TABLE} ADD COLUMN IF NOT EXISTS tracking_url text")
    cur.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_shipping_labels_order_created
        ON {SHIPPING_LABEL_TABLE}(order_id, created_at DESC)
        """
    )
    cur.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_shipping_labels_created
        ON {SHIPPING_LABEL_TABLE}(created_at DESC)
        """
    )
    cur.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_shipping_labels_shopify_fulfillment
        ON {SHIPPING_LABEL_TABLE}(shopify_fulfillment_id)
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SHOPIFY_FULFILLMENT_JOB_TABLE} (
            id serial PRIMARY KEY,
            label_id integer,
            order_id text NOT NULL,
            tracking_number text NOT NULL,
            tracking_url text,
            carrier text NOT NULL,
            line_items_json text,
            notify_customer boolean NOT NULL DEFAULT FALSE,
            status text NOT NULL DEFAULT 'pending',
            attempts integer NOT NULL DEFAULT 0,
            result_message text,
            shopify_fulfillment_id text,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            updated_at timestamptz NOT NULL DEFAULT NOW(),
            processed_at timestamptz
        )
        """
    )
    cur.execute(f"ALTER TABLE {SHOPIFY_FULFILLMENT_JOB_TABLE} ADD COLUMN IF NOT EXISTS label_id integer")
    cur.execute(f"ALTER TABLE {SHOPIFY_FULFILLMENT_JOB_TABLE} ADD COLUMN IF NOT EXISTS tracking_url text")
    cur.execute(f"ALTER TABLE {SHOPIFY_FULFILLMENT_JOB_TABLE} ADD COLUMN IF NOT EXISTS line_items_json text")
    cur.execute(f"ALTER TABLE {SHOPIFY_FULFILLMENT_JOB_TABLE} ADD COLUMN IF NOT EXISTS notify_customer boolean NOT NULL DEFAULT FALSE")
    cur.execute(f"ALTER TABLE {SHOPIFY_FULFILLMENT_JOB_TABLE} ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'pending'")
    cur.execute(f"ALTER TABLE {SHOPIFY_FULFILLMENT_JOB_TABLE} ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0")
    cur.execute(f"ALTER TABLE {SHOPIFY_FULFILLMENT_JOB_TABLE} ADD COLUMN IF NOT EXISTS result_message text")
    cur.execute(f"ALTER TABLE {SHOPIFY_FULFILLMENT_JOB_TABLE} ADD COLUMN IF NOT EXISTS shopify_fulfillment_id text")
    cur.execute(f"ALTER TABLE {SHOPIFY_FULFILLMENT_JOB_TABLE} ADD COLUMN IF NOT EXISTS processed_at timestamptz")
    cur.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_shopify_fulfillment_jobs_status_created
        ON {SHOPIFY_FULFILLMENT_JOB_TABLE}(status, created_at)
        """
    )
    cur.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_shopify_fulfillment_jobs_label_created
        ON {SHOPIFY_FULFILLMENT_JOB_TABLE}(label_id, created_at DESC)
        """
    )


def list_shipping_labels(db_factory, order_id=None, limit=400):
    con = db_factory()
    cur = con.cursor()
    if order_id:
        cur.execute(
            f"""
            SELECT
                id,
                carrier,
                order_id,
                order_name,
                shipment_reference,
                track_id,
                parcel_number,
                status,
                weight_kg,
                label_path,
                last_error,
                source,
                shopify_fulfillment_id,
                shopify_synced_at,
                tracking_url,
                created_at,
                updated_at,
                cancel_requested_at,
                cancelled_at
            FROM {SHIPPING_LABEL_TABLE}
            WHERE order_id = %s
            ORDER BY created_at DESC, id DESC
            """,
            (order_id,),
        )
    else:
        cur.execute(
            f"""
            SELECT
                id,
                carrier,
                order_id,
                order_name,
                shipment_reference,
                track_id,
                parcel_number,
                status,
                weight_kg,
                label_path,
                last_error,
                source,
                shopify_fulfillment_id,
                shopify_synced_at,
                tracking_url,
                created_at,
                updated_at,
                cancel_requested_at,
                cancelled_at
            FROM {SHIPPING_LABEL_TABLE}
            ORDER BY created_at DESC, id DESC
            LIMIT {int(limit)}
            """
        )
    rows = cur.fetchall()
    cur.close()
    con.close()
    return rows


def get_latest_shipping_label_for_order(db_factory, order_id):
    if not order_id:
        return None
    con = db_factory()
    cur = con.cursor()
    cur.execute(
        f"""
        SELECT
            id,
            carrier,
            track_id,
            parcel_number,
            status,
            created_at
        FROM {SHIPPING_LABEL_TABLE}
        WHERE order_id = %s
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (order_id,),
    )
    row = cur.fetchone()
    cur.close()
    con.close()
    return row


def insert_shipping_label_history(
    db_factory,
    order,
    shipment_reference,
    track_id,
    parcel_number,
    label_path,
    status,
    weight_kg=1.0,
    carrier="gls",
    source="local",
    shopify_fulfillment_id=None,
    tracking_url=None,
):
    con = db_factory()
    cur = con.cursor()
    cur.execute(
        f"""
        INSERT INTO {SHIPPING_LABEL_TABLE} (
            carrier,
            order_id,
            order_name,
            shipment_reference,
            track_id,
            parcel_number,
            weight_kg,
            status,
            label_path,
            source,
            shopify_fulfillment_id,
            shopify_synced_at,
            tracking_url
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (track_id)
        DO UPDATE
           SET carrier = EXCLUDED.carrier,
               order_id = EXCLUDED.order_id,
               order_name = EXCLUDED.order_name,
               shipment_reference = EXCLUDED.shipment_reference,
               parcel_number = EXCLUDED.parcel_number,
               weight_kg = EXCLUDED.weight_kg,
               status = EXCLUDED.status,
               label_path = COALESCE(NULLIF(EXCLUDED.label_path, ''), {SHIPPING_LABEL_TABLE}.label_path),
               source = CASE
                   WHEN {SHIPPING_LABEL_TABLE}.source = 'local' AND EXCLUDED.source = 'shopify' THEN {SHIPPING_LABEL_TABLE}.source
                   ELSE EXCLUDED.source
               END,
               shopify_fulfillment_id = COALESCE(EXCLUDED.shopify_fulfillment_id, {SHIPPING_LABEL_TABLE}.shopify_fulfillment_id),
               shopify_synced_at = COALESCE(EXCLUDED.shopify_synced_at, {SHIPPING_LABEL_TABLE}.shopify_synced_at),
               tracking_url = COALESCE(EXCLUDED.tracking_url, {SHIPPING_LABEL_TABLE}.tracking_url),
               updated_at = NOW(),
               last_error = NULL
        RETURNING id
        """,
        (
            carrier,
            order["order_id"],
            order["order_name"],
            shipment_reference,
            track_id,
            parcel_number,
            weight_kg,
            status,
            label_path,
            (source or "local").strip().lower() or "local",
            shopify_fulfillment_id,
            datetime.datetime.now() if shopify_fulfillment_id else None,
            (tracking_url or "").strip() or None,
        ),
    )
    row = cur.fetchone()
    con.commit()
    cur.close()
    con.close()
    return row["id"] if row else None


def update_shipping_label_status(db_factory, label_id, status, last_error=None):
    con = db_factory()
    cur = con.cursor()
    if status == "CANCELLED":
        cur.execute(
            f"""
            UPDATE {SHIPPING_LABEL_TABLE}
            SET status = %s,
                last_error = %s,
                cancelled_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
            """,
            (status, last_error, label_id),
        )
    elif status == "CANCELLATION_PENDING":
        cur.execute(
            f"""
            UPDATE {SHIPPING_LABEL_TABLE}
            SET status = %s,
                last_error = %s,
                cancel_requested_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
            """,
            (status, last_error, label_id),
        )
    else:
        cur.execute(
            f"""
            UPDATE {SHIPPING_LABEL_TABLE}
            SET status = %s,
                last_error = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (status, last_error, label_id),
        )
    con.commit()
    cur.close()
    con.close()


def update_shipping_label_reprint(db_factory, label_id, label_path):
    con = db_factory()
    cur = con.cursor()
    cur.execute(
        f"""
        UPDATE {SHIPPING_LABEL_TABLE}
        SET label_path = %s,
            status = 'REPRINTED',
            last_error = NULL,
            updated_at = NOW()
        WHERE id = %s
        """,
        (label_path, label_id),
    )
    con.commit()
    cur.close()
    con.close()


def get_latest_shopify_job_for_label(db_factory, label_id):
    con = db_factory()
    cur = con.cursor()
    cur.execute(
        f"""
        SELECT
            id,
            order_id,
            tracking_number,
            carrier,
            line_items_json,
            notify_customer,
            status,
            attempts,
            result_message,
            shopify_fulfillment_id,
            created_at,
            updated_at,
            processed_at
        FROM {SHOPIFY_FULFILLMENT_JOB_TABLE}
        WHERE label_id = %s
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (label_id,),
    )
    row = cur.fetchone()
    cur.close()
    con.close()
    return row


def find_or_create_shopify_fulfillment_job(
    db_factory,
    *,
    label_id,
    order_id,
    tracking_number,
    tracking_url,
    carrier,
    line_items_json=None,
    notify_customer=False,
):
    con = db_factory()
    cur = con.cursor()
    cur.execute(
        f"""
        SELECT id, status
        FROM {SHOPIFY_FULFILLMENT_JOB_TABLE}
        WHERE label_id = %s
          AND status IN ('pending', 'processing')
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (label_id,),
    )
    existing = cur.fetchone()
    if existing:
        cur.close()
        con.close()
        return {"job_id": existing["id"], "status": existing["status"], "created": False}

    cur.execute(
        f"""
        INSERT INTO {SHOPIFY_FULFILLMENT_JOB_TABLE} (
            label_id,
            order_id,
            tracking_number,
            tracking_url,
            carrier,
            line_items_json,
            notify_customer,
            status,
            attempts,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'pending', 0, NOW(), NOW())
        RETURNING id, status
        """,
        (label_id, order_id, tracking_number, tracking_url, carrier, line_items_json, bool(notify_customer)),
    )
    row = cur.fetchone()
    con.commit()
    cur.close()
    con.close()
    return {"job_id": row["id"], "status": row["status"], "created": True}


def claim_shopify_fulfillment_jobs(db_factory, *, cursor_factory=None, limit=20):
    con = db_factory()
    cur = con.cursor(cursor_factory=cursor_factory) if cursor_factory is not None else con.cursor()
    cur.execute(
        f"""
        WITH claimed AS (
            SELECT id
            FROM {SHOPIFY_FULFILLMENT_JOB_TABLE}
            WHERE status = 'pending'
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT %s
        )
        UPDATE {SHOPIFY_FULFILLMENT_JOB_TABLE} j
        SET status = 'processing',
            attempts = COALESCE(j.attempts, 0) + 1,
            updated_at = NOW()
        FROM claimed
        WHERE j.id = claimed.id
        RETURNING
            j.id,
            j.label_id,
            j.order_id,
            j.tracking_number,
            j.tracking_url,
            j.carrier,
            j.line_items_json,
            j.notify_customer,
            j.attempts
        """,
        (limit,),
    )
    rows = cur.fetchall()
    con.commit()
    cur.close()
    con.close()
    return rows


def upsert_shopify_shipment(
    cur,
    *,
    carrier,
    order_id,
    order_name,
    shipment_reference,
    tracking_number,
    parcel_number,
    status,
    fulfillment_id=None,
    tracking_url=None,
    created_at=None,
):
    cur.execute(
        f"""
        INSERT INTO {SHIPPING_LABEL_TABLE} (
            carrier,
            order_id,
            order_name,
            shipment_reference,
            track_id,
            parcel_number,
            weight_kg,
            status,
            label_path,
            last_error,
            source,
            shopify_fulfillment_id,
            shopify_synced_at,
            tracking_url,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, 1.0, %s, '', NULL, 'shopify', %s, NOW(), %s, %s, NOW())
        ON CONFLICT (track_id)
        DO UPDATE
           SET carrier = EXCLUDED.carrier,
               order_id = EXCLUDED.order_id,
               order_name = EXCLUDED.order_name,
               shipment_reference = EXCLUDED.shipment_reference,
               parcel_number = COALESCE(EXCLUDED.parcel_number, {SHIPPING_LABEL_TABLE}.parcel_number),
               status = EXCLUDED.status,
               label_path = COALESCE(NULLIF(EXCLUDED.label_path, ''), {SHIPPING_LABEL_TABLE}.label_path),
               source = CASE
                   WHEN {SHIPPING_LABEL_TABLE}.source = 'local' THEN {SHIPPING_LABEL_TABLE}.source
                   ELSE 'shopify'
               END,
               shopify_fulfillment_id = COALESCE(EXCLUDED.shopify_fulfillment_id, {SHIPPING_LABEL_TABLE}.shopify_fulfillment_id),
               shopify_synced_at = NOW(),
               tracking_url = COALESCE(EXCLUDED.tracking_url, {SHIPPING_LABEL_TABLE}.tracking_url),
               updated_at = NOW()
        """,
        (
            carrier,
            order_id,
            order_name,
            shipment_reference,
            tracking_number,
            parcel_number,
            status,
            fulfillment_id,
            tracking_url,
            created_at,
        ),
    )


def _update_label_status_from_job(cur, label_id, status, message=None):
    if not label_id:
        return
    cur.execute(
        f"""
        UPDATE {SHIPPING_LABEL_TABLE}
        SET status = %s,
            last_error = %s,
            updated_at = NOW()
        WHERE id = %s
        """,
        (status, message, label_id),
    )


def mark_shopify_fulfillment_job_done(db_factory, job_id, label_id, fulfillment_id, status):
    con = db_factory()
    cur = con.cursor()
    cur.execute(f"SELECT order_id, line_items_json FROM {SHOPIFY_FULFILLMENT_JOB_TABLE} WHERE id = %s", (job_id,))
    job_row = cur.fetchone()
    cur.execute(
        f"""
        UPDATE {SHOPIFY_FULFILLMENT_JOB_TABLE}
        SET status = 'done',
            shopify_fulfillment_id = %s,
            result_message = %s,
            processed_at = NOW(),
            updated_at = NOW()
        WHERE id = %s
        """,
        (fulfillment_id, status, job_id),
    )
    if label_id:
        cur.execute(
            f"""
            UPDATE {SHIPPING_LABEL_TABLE}
            SET shopify_fulfillment_id = COALESCE(%s, shopify_fulfillment_id),
                shopify_synced_at = NOW(),
                updated_at = NOW()
            WHERE id = %s
            """,
            (fulfillment_id, label_id),
        )
    if job_row and job_row[1]:
        order_id = job_row[0]
        try:
            payload = json.loads(job_row[1])
        except json.JSONDecodeError:
            payload = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            line_item_id = (item.get("order_line_item_id") or "").strip()
            if not line_item_id:
                continue
            try:
                qty = int(item.get("quantity") or 0)
            except (TypeError, ValueError):
                continue
            if qty <= 0:
                continue
            cur.execute(
                """
                UPDATE shopify_order_items
                SET fulfilled_quantity = LEAST(quantity, COALESCE(fulfilled_quantity, 0) + %s)
                WHERE order_id = %s
                  AND order_line_item_id = %s
                """,
                (qty, order_id, line_item_id),
            )
    _update_label_status_from_job(cur, label_id, "SHOPIFY_FULFILLED", None)
    con.commit()
    cur.close()
    con.close()


def mark_shopify_fulfillment_job_failed(db_factory, job_id, label_id, message):
    con = db_factory()
    cur = con.cursor()
    cur.execute(
        f"""
        UPDATE {SHOPIFY_FULFILLMENT_JOB_TABLE}
        SET status = 'failed',
            result_message = %s,
            processed_at = NOW(),
            updated_at = NOW()
        WHERE id = %s
        """,
        (message[:1000], job_id),
    )
    _update_label_status_from_job(cur, label_id, "SHOPIFY_FAILED", message[:160])
    con.commit()
    cur.close()
    con.close()
