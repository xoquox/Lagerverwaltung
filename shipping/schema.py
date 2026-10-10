"""Gemeinsame Datenbank-Schemafunktionen fuer Lager MC."""

from shipping.history import ensure_shipping_history_schema
from shipping.shop import normalize_shop_system


REQUIRED_TABLE_COLUMNS = {
    "shop_configuration": {"id", "shop_system"},
    "shipping_labels": {
        "id", "carrier", "order_id", "order_name", "shipment_reference", "track_id",
        "parcel_number", "weight_kg", "status", "label_path", "last_error", "source",
        "shop_fulfillment_id", "shop_synced_at", "tracking_url", "created_at", "updated_at",
        "cancel_requested_at", "cancelled_at",
    },
    "shop_fulfillment_jobs": {
        "id", "label_id", "order_id", "tracking_number", "tracking_url", "carrier",
        "line_items_json", "notify_customer", "status", "attempts", "result_message",
        "shop_fulfillment_id", "created_at", "updated_at", "processed_at",
    },
    "items": {
        "sku",
        "display_sku",
        "name",
        "regal",
        "fach",
        "platz",
        "menge",
        "available",
        "reserved",
        "committed",
        "unavailable",
        "dirty",
        "external_fulfillment",
        "shop_product_id",
        "shop_variant_id",
        "shop_inventory_item_id",
        "barcode",
        "shop_product_status",
        "shop_description",
        "shop_price",
        "shop_compare_at_price",
        "shop_unit_cost",
        "shop_unit_cost_currency",
        "shop_weight_grams",
        "shop_product_dirty",
        "shop_product_sync_action",
        "shop_product_sync_error",
        "sync_status",
        "last_sync",
        "updated_at",
    },
    "shop_orders": {
        "order_id",
        "order_name",
        "created_at",
        "shipping_name",
        "shipping_address1",
        "shipping_address2",
        "shipping_zip",
        "shipping_city",
        "shipping_country",
        "shipping_email",
        "shipping_phone",
        "fulfillment_status",
        "payment_status",
        "source",
        "updated_at",
    },
    "shop_order_items": {
        "order_id",
        "line_index",
        "order_line_item_id",
        "sku",
        "title",
        "quantity",
        "fulfilled_quantity",
    },
    "shop_fulfillment_orders": {
        "fulfillment_order_id",
        "order_id",
        "assigned_location_id",
        "assigned_location_name",
        "status",
        "request_status",
        "updated_at",
    },
    "shop_fulfillment_order_items": {
        "fulfillment_order_line_item_id",
        "fulfillment_order_id",
        "order_id",
        "order_line_item_id",
        "sku",
        "title",
        "quantity",
        "remaining_quantity",
        "assigned_location_id",
        "assigned_location_name",
        "updated_at",
    },
    "service_runtime_state": {
        "service",
        "version",
        "status",
        "last_seen_at",
        "last_started_at",
        "last_finished_at",
        "last_pull_at",
        "last_push_at",
        "last_error",
        "updated_at",
    },
    "shop_customers": {
        "customer_id",
        "first_name",
        "last_name",
        "display_name",
        "email",
        "phone",
        "default_name",
        "default_address1",
        "default_zip",
        "default_city",
        "default_country",
        "default_phone",
        "updated_at",
    },
    "inventory_sessions": {
        "session_id",
        "session_name",
        "created_at",
        "status",
    },
    "inventory_lines": {
        "session_id",
        "line_no",
        "sku",
        "name",
        "regal",
        "fach",
        "platz",
        "soll_menge",
        "ist_menge",
    },
    "shop_locations": {
        "location_id",
        "name",
        "fulfills_online_orders",
        "is_active",
        "updated_at",
    },
    "item_location_inventory": {
        "sku",
        "location_id",
        "regal",
        "fach",
        "platz",
        "menge",
        "available",
        "reserved",
        "committed",
        "unavailable",
        "dirty",
        "updated_at",
    },
}



def migrate_shop_schema(cur, shop_system):
    """Transactional, repeatable migration of the former Shopify database contract.

    Never merge two schemas or relabel Shopify data as WooCommerce data.
    Callers commit the entire migration only after it succeeds.
    """
    cur.execute("SELECT pg_advisory_xact_lock(178245901)")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS shop_configuration (
            id integer PRIMARY KEY CHECK (id = 1),
            shop_system text NOT NULL CHECK (shop_system IN ('shopify', 'woocommerce'))
        )
    """)
    cur.execute("""
        INSERT INTO shop_configuration(id, shop_system) VALUES (1, %s)
        ON CONFLICT (id) DO NOTHING
    """, (shop_system,))
    cur.execute("""
        DO $$ BEGIN
            IF (SELECT shop_system FROM shop_configuration WHERE id = 1) <> %s THEN
                RAISE EXCEPTION 'Shopsystem passt nicht zur Datenbank. Separate Datenbank fuer Shopwechsel verwenden.';
            END IF;
            IF (SELECT shop_system FROM shop_configuration WHERE id = 1) = 'woocommerce'
               AND (EXISTS (SELECT 1 FROM information_schema.tables
                            WHERE table_schema = 'public' AND table_name IN (
                                'shopify_orders', 'shopify_order_items', 'shopify_customers',
                                'shopify_locations', 'shopify_fulfillment_orders',
                                'shopify_fulfillment_order_items', 'shopify_fulfillment_jobs'))
                    OR EXISTS (SELECT 1 FROM information_schema.columns
                               WHERE table_schema = 'public' AND table_name = 'items'
                                 AND column_name = 'shopify_product_id')) THEN
                RAISE EXCEPTION 'Shopify-Altbestand darf nicht als WooCommerce-Datenbank verwendet werden.';
            END IF;
        END $$
    """, (shop_system,))
    table_renames = [
        ('shopify_orders', 'shop_orders'),
        ('shopify_order_items', 'shop_order_items'),
        ('shopify_fulfillment_orders', 'shop_fulfillment_orders'),
        ('shopify_fulfillment_order_items', 'shop_fulfillment_order_items'),
        ('shopify_customers', 'shop_customers'),
        ('shopify_locations', 'shop_locations'),
        ('shopify_fulfillment_jobs', 'shop_fulfillment_jobs'),
    ]
    column_renames = [
        ('items', 'shopify_product_id', 'shop_product_id'),
        ('items', 'shopify_variant_id', 'shop_variant_id'),
        ('items', 'shopify_inventory_item_id', 'shop_inventory_item_id'),
        ('items', 'shopify_product_status', 'shop_product_status'),
        ('items', 'shopify_description', 'shop_description'),
        ('items', 'shopify_price', 'shop_price'),
        ('items', 'shopify_compare_at_price', 'shop_compare_at_price'),
        ('items', 'shopify_unit_cost', 'shop_unit_cost'),
        ('items', 'shopify_unit_cost_currency', 'shop_unit_cost_currency'),
        ('items', 'shopify_weight_grams', 'shop_weight_grams'),
        ('items', 'shopify_product_dirty', 'shop_product_dirty'),
        ('items', 'shopify_product_sync_action', 'shop_product_sync_action'),
        ('items', 'shopify_product_sync_error', 'shop_product_sync_error'),
        ('shipping_labels', 'shopify_fulfillment_id', 'shop_fulfillment_id'),
        ('shipping_labels', 'shopify_synced_at', 'shop_synced_at'),
        ('shop_fulfillment_jobs', 'shopify_fulfillment_id', 'shop_fulfillment_id'),
    ]
    for old, new in table_renames:
        # Names come exclusively from the static mapping above.
        cur.execute(f"""
            DO $$ BEGIN
                IF to_regclass('public.{old}') IS NOT NULL THEN
                    IF to_regclass('public.{new}') IS NOT NULL THEN
                        RAISE EXCEPTION 'Parallele Shop-Tabellen: {old} und {new}. Migration abgebrochen.';
                    END IF;
                    ALTER TABLE {old} RENAME TO {new};
                END IF;
            END $$
        """)
    cur.execute("""
        DO $$ DECLARE idx record; target text; BEGIN
            FOR idx IN SELECT indexname FROM pg_indexes WHERE schemaname = 'public'
                       AND (indexname LIKE 'idx_shopify_%' OR indexname = 'idx_shipping_labels_shopify_fulfillment') LOOP
                target := replace(idx.indexname, 'shopify_', 'shop_');
                IF to_regclass('public.' || target) IS NULL THEN
                    EXECUTE format('ALTER INDEX public.%I RENAME TO %I', idx.indexname, target);
                END IF;
            END LOOP;
        END $$
    """)
    for table, old, new in column_renames:
        cur.execute(f"""
            DO $$ BEGIN
                IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public'
                           AND table_name = '{table}' AND column_name = '{old}') THEN
                    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'public'
                               AND table_name = '{table}' AND column_name = '{new}') THEN
                        RAISE EXCEPTION 'Parallele Shop-Spalten: {table}.{old} und {new}.';
                    END IF;
                    ALTER TABLE {table} RENAME COLUMN {old} TO {new};
                END IF;
            END $$
        """)


def apply_app_schema(cur, shop_system="shopify"):
    shop_system = normalize_shop_system(shop_system)
    migrate_shop_schema(cur, shop_system)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
            sku text PRIMARY KEY,
            display_sku text,
            name text NOT NULL,
            regal text,
            fach text,
            platz text,
            menge integer NOT NULL DEFAULT 0,
            available integer,
            reserved integer NOT NULL DEFAULT 0,
            committed integer NOT NULL DEFAULT 0,
            unavailable integer NOT NULL DEFAULT 0,
            dirty boolean NOT NULL DEFAULT FALSE,
            shop_product_id text,
            shop_variant_id text,
            shop_inventory_item_id text,
            barcode text,
            shop_product_status text,
            shop_description text,
            shop_price text,
            shop_compare_at_price text,
            shop_unit_cost text,
            shop_unit_cost_currency text,
            shop_weight_grams integer,
            shop_product_dirty boolean NOT NULL DEFAULT FALSE,
            shop_product_sync_action text,
            shop_product_sync_error text,
            sync_status text NOT NULL DEFAULT 'local',
            last_sync timestamptz,
            updated_at timestamptz NOT NULL DEFAULT NOW(),
            external_fulfillment boolean NOT NULL DEFAULT FALSE
        )
        """
    )
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS display_sku text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS available integer")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS reserved integer DEFAULT 0")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS committed integer DEFAULT 0")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS unavailable integer DEFAULT 0")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS dirty boolean NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS external_fulfillment boolean NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shop_product_id text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shop_variant_id text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shop_inventory_item_id text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS barcode text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shop_product_status text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shop_description text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shop_price text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shop_compare_at_price text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shop_unit_cost text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shop_unit_cost_currency text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shop_weight_grams integer")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shop_product_dirty boolean NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shop_product_sync_action text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shop_product_sync_error text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS sync_status text NOT NULL DEFAULT 'local'")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS last_sync timestamptz")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW()")
    for column in ("shop_product_id", "shop_variant_id", "shop_inventory_item_id"):
        cur.execute(f"ALTER TABLE items ALTER COLUMN {column} TYPE text USING {column}::text")
    cur.execute("UPDATE items SET sync_status = 'shop_pending' WHERE sync_status = 'shopify_pending'")
    cur.execute("UPDATE items SET sync_status = 'shop_error' WHERE sync_status = 'shopify_error'")
    cur.execute("UPDATE items SET reserved = COALESCE(reserved, 0)")
    cur.execute("UPDATE items SET committed = COALESCE(committed, 0)")
    cur.execute("UPDATE items SET display_sku = sku WHERE display_sku IS NULL")
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
        CREATE TABLE IF NOT EXISTS shop_orders (
            order_id text PRIMARY KEY,
            order_name text NOT NULL,
            created_at timestamptz,
            shipping_name text,
            shipping_address1 text,
            shipping_address2 text,
            shipping_zip text,
            shipping_city text,
            shipping_country text,
            shipping_email text,
            shipping_phone text,
            fulfillment_status text,
            payment_status text,
            source text NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute("ALTER TABLE shop_orders ADD COLUMN IF NOT EXISTS shipping_country text")
    cur.execute("ALTER TABLE shop_orders ADD COLUMN IF NOT EXISTS shipping_address2 text")
    cur.execute("ALTER TABLE shop_orders ADD COLUMN IF NOT EXISTS shipping_email text")
    cur.execute("ALTER TABLE shop_orders ADD COLUMN IF NOT EXISTS shipping_phone text")
    cur.execute("ALTER TABLE shop_orders ADD COLUMN IF NOT EXISTS payment_status text")
    cur.execute("ALTER TABLE shop_orders ADD COLUMN IF NOT EXISTS source text NOT NULL DEFAULT %s", (shop_system,))
    cur.execute("ALTER TABLE shop_orders ALTER COLUMN source DROP DEFAULT")
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_shop_orders_name
        ON shop_orders(order_name)
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shop_order_items (
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
    cur.execute("ALTER TABLE shop_order_items ADD COLUMN IF NOT EXISTS order_line_item_id text")
    cur.execute("ALTER TABLE shop_order_items ADD COLUMN IF NOT EXISTS fulfilled_quantity integer NOT NULL DEFAULT 0")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shop_fulfillment_orders (
            fulfillment_order_id text PRIMARY KEY,
            order_id text NOT NULL REFERENCES shop_orders(order_id) ON DELETE CASCADE,
            assigned_location_id text,
            assigned_location_name text,
            status text,
            request_status text,
            updated_at timestamptz NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute("ALTER TABLE shop_fulfillment_orders ADD COLUMN IF NOT EXISTS assigned_location_id text")
    cur.execute("ALTER TABLE shop_fulfillment_orders ADD COLUMN IF NOT EXISTS assigned_location_name text")
    cur.execute("ALTER TABLE shop_fulfillment_orders ADD COLUMN IF NOT EXISTS status text")
    cur.execute("ALTER TABLE shop_fulfillment_orders ADD COLUMN IF NOT EXISTS request_status text")
    cur.execute("ALTER TABLE shop_fulfillment_orders ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW()")
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_shop_fulfillment_orders_order
        ON shop_fulfillment_orders(order_id)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_shop_fulfillment_orders_location
        ON shop_fulfillment_orders(assigned_location_id)
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shop_fulfillment_order_items (
            fulfillment_order_line_item_id text PRIMARY KEY,
            fulfillment_order_id text NOT NULL REFERENCES shop_fulfillment_orders(fulfillment_order_id) ON DELETE CASCADE,
            order_id text NOT NULL REFERENCES shop_orders(order_id) ON DELETE CASCADE,
            order_line_item_id text,
            sku text,
            title text NOT NULL,
            quantity integer NOT NULL,
            remaining_quantity integer NOT NULL DEFAULT 0,
            assigned_location_id text,
            assigned_location_name text,
            updated_at timestamptz NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute("ALTER TABLE shop_fulfillment_order_items ADD COLUMN IF NOT EXISTS order_line_item_id text")
    cur.execute("ALTER TABLE shop_fulfillment_order_items ADD COLUMN IF NOT EXISTS sku text")
    cur.execute("ALTER TABLE shop_fulfillment_order_items ADD COLUMN IF NOT EXISTS title text")
    cur.execute("ALTER TABLE shop_fulfillment_order_items ADD COLUMN IF NOT EXISTS quantity integer NOT NULL DEFAULT 0")
    cur.execute("ALTER TABLE shop_fulfillment_order_items ADD COLUMN IF NOT EXISTS remaining_quantity integer NOT NULL DEFAULT 0")
    cur.execute("ALTER TABLE shop_fulfillment_order_items ADD COLUMN IF NOT EXISTS assigned_location_id text")
    cur.execute("ALTER TABLE shop_fulfillment_order_items ADD COLUMN IF NOT EXISTS assigned_location_name text")
    cur.execute("ALTER TABLE shop_fulfillment_order_items ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW()")
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_shop_fulfillment_order_items_order
        ON shop_fulfillment_order_items(order_id)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_shop_fulfillment_order_items_location
        ON shop_fulfillment_order_items(assigned_location_id)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_shop_fulfillment_order_items_order_line
        ON shop_fulfillment_order_items(order_line_item_id)
        """
    )
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
        CREATE TABLE IF NOT EXISTS shop_customers (
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
    cur.execute("ALTER TABLE shop_customers ADD COLUMN IF NOT EXISTS first_name text")
    cur.execute("ALTER TABLE shop_customers ADD COLUMN IF NOT EXISTS last_name text")
    cur.execute("ALTER TABLE shop_customers ADD COLUMN IF NOT EXISTS display_name text")
    cur.execute("ALTER TABLE shop_customers ADD COLUMN IF NOT EXISTS email text")
    cur.execute("ALTER TABLE shop_customers ADD COLUMN IF NOT EXISTS phone text")
    cur.execute("ALTER TABLE shop_customers ADD COLUMN IF NOT EXISTS default_name text")
    cur.execute("ALTER TABLE shop_customers ADD COLUMN IF NOT EXISTS default_address1 text")
    cur.execute("ALTER TABLE shop_customers ADD COLUMN IF NOT EXISTS default_zip text")
    cur.execute("ALTER TABLE shop_customers ADD COLUMN IF NOT EXISTS default_city text")
    cur.execute("ALTER TABLE shop_customers ADD COLUMN IF NOT EXISTS default_country text")
    cur.execute("ALTER TABLE shop_customers ADD COLUMN IF NOT EXISTS default_phone text")
    cur.execute("ALTER TABLE shop_customers ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW()")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_shop_customers_display_name ON shop_customers(display_name)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_shop_customers_email ON shop_customers(email)")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS version text")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'unknown'")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS last_seen_at timestamptz")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS last_started_at timestamptz")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS last_finished_at timestamptz")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS last_pull_at timestamptz")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS last_push_at timestamptz")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS last_error text")
    cur.execute("ALTER TABLE service_runtime_state ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW()")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory_sessions (
            session_id serial PRIMARY KEY,
            session_name text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT NOW(),
            status text NOT NULL DEFAULT 'active'
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory_lines (
            session_id integer NOT NULL REFERENCES inventory_sessions(session_id) ON DELETE CASCADE,
            line_no integer NOT NULL,
            sku text NOT NULL,
            name text NOT NULL,
            regal text,
            fach text,
            platz text,
            soll_menge integer NOT NULL,
            ist_menge integer,
            PRIMARY KEY (session_id, line_no)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shop_locations (
            location_id text PRIMARY KEY,
            name text,
            fulfills_online_orders boolean NOT NULL DEFAULT FALSE,
            is_active boolean NOT NULL DEFAULT TRUE,
            updated_at timestamptz NOT NULL DEFAULT NOW()
        )
        """
    )
    cur.execute("ALTER TABLE shop_locations ADD COLUMN IF NOT EXISTS name text")
    cur.execute("ALTER TABLE shop_locations ADD COLUMN IF NOT EXISTS fulfills_online_orders boolean NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE shop_locations ADD COLUMN IF NOT EXISTS is_active boolean NOT NULL DEFAULT TRUE")
    cur.execute("ALTER TABLE shop_locations ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW()")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS item_location_inventory (
            sku text NOT NULL REFERENCES items(sku) ON DELETE CASCADE,
            location_id text NOT NULL REFERENCES shop_locations(location_id) ON DELETE CASCADE,
            regal text,
            fach text,
            platz text,
            menge integer NOT NULL DEFAULT 0,
            available integer,
            reserved integer NOT NULL DEFAULT 0,
            committed integer NOT NULL DEFAULT 0,
            unavailable integer NOT NULL DEFAULT 0,
            dirty boolean NOT NULL DEFAULT FALSE,
            updated_at timestamptz NOT NULL DEFAULT NOW(),
            PRIMARY KEY (sku, location_id)
        )
        """
    )
    cur.execute("ALTER TABLE item_location_inventory ADD COLUMN IF NOT EXISTS regal text")
    cur.execute("ALTER TABLE item_location_inventory ADD COLUMN IF NOT EXISTS fach text")
    cur.execute("ALTER TABLE item_location_inventory ADD COLUMN IF NOT EXISTS platz text")
    cur.execute("ALTER TABLE item_location_inventory ADD COLUMN IF NOT EXISTS menge integer NOT NULL DEFAULT 0")
    cur.execute("ALTER TABLE item_location_inventory ADD COLUMN IF NOT EXISTS available integer")
    cur.execute("ALTER TABLE item_location_inventory ADD COLUMN IF NOT EXISTS reserved integer NOT NULL DEFAULT 0")
    cur.execute("ALTER TABLE item_location_inventory ADD COLUMN IF NOT EXISTS committed integer NOT NULL DEFAULT 0")
    cur.execute("ALTER TABLE item_location_inventory ADD COLUMN IF NOT EXISTS unavailable integer NOT NULL DEFAULT 0")
    cur.execute("ALTER TABLE item_location_inventory ADD COLUMN IF NOT EXISTS dirty boolean NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE item_location_inventory ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW()")
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_item_location_inventory_location
        ON item_location_inventory(location_id)
        """
    )


def collect_schema_issues(cur):
    def _row_value(row, key):
        if isinstance(row, dict):
            return row.get(key)
        return row[0]

    table_names = list(REQUIRED_TABLE_COLUMNS.keys())
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        """
    )
    existing_tables = {_row_value(row, "table_name") for row in cur.fetchall()}

    issues = []
    for table_name in table_names:
        if table_name not in existing_tables:
            issues.append(f"Tabelle fehlt: {table_name}")
            continue
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table_name,),
        )
        existing_columns = {_row_value(row, "column_name") for row in cur.fetchall()}
        missing_columns = sorted(REQUIRED_TABLE_COLUMNS[table_name] - existing_columns)
        for column_name in missing_columns:
            issues.append(f"Spalte fehlt: {table_name}.{column_name}")
    return issues
