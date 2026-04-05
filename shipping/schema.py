"""Gemeinsame Datenbank-Schemafunktionen fuer Lager MC."""

from shipping.history import ensure_shipping_history_schema


REQUIRED_TABLE_COLUMNS = {
    "items": {
        "sku",
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
        "shopify_product_id",
        "shopify_variant_id",
        "shopify_inventory_item_id",
        "barcode",
        "shopify_product_status",
        "shopify_description",
        "shopify_price",
        "shopify_compare_at_price",
        "shopify_unit_cost",
        "shopify_unit_cost_currency",
        "shopify_weight_grams",
        "sync_status",
        "last_sync",
        "updated_at",
    },
    "shopify_orders": {
        "order_id",
        "order_name",
        "created_at",
        "shipping_name",
        "shipping_address1",
        "shipping_zip",
        "shipping_city",
        "shipping_country",
        "shipping_email",
        "shipping_phone",
        "fulfillment_status",
        "payment_status",
        "updated_at",
    },
    "shopify_order_items": {
        "order_id",
        "line_index",
        "order_line_item_id",
        "sku",
        "title",
        "quantity",
        "fulfilled_quantity",
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
    "shopify_customers": {
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
}


def apply_app_schema(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
            sku text PRIMARY KEY,
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
            shopify_product_id text,
            shopify_variant_id text,
            shopify_inventory_item_id text,
            barcode text,
            shopify_product_status text,
            shopify_description text,
            shopify_price text,
            shopify_compare_at_price text,
            shopify_unit_cost text,
            shopify_unit_cost_currency text,
            shopify_weight_grams integer,
            sync_status text NOT NULL DEFAULT 'local',
            last_sync timestamptz,
            updated_at timestamptz NOT NULL DEFAULT NOW(),
            external_fulfillment boolean NOT NULL DEFAULT FALSE
        )
        """
    )
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS available integer")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS reserved integer DEFAULT 0")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS committed integer DEFAULT 0")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS unavailable integer DEFAULT 0")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS dirty boolean NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS external_fulfillment boolean NOT NULL DEFAULT FALSE")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_product_id text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_variant_id text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_inventory_item_id text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS barcode text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_product_status text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_description text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_price text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_compare_at_price text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_unit_cost text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_unit_cost_currency text")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS shopify_weight_grams integer")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS sync_status text NOT NULL DEFAULT 'local'")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS last_sync timestamptz")
    cur.execute("ALTER TABLE items ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT NOW()")
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
