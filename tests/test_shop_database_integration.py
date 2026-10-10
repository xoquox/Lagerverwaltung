"""Run separately against a disposable PostgreSQL instance via LAGER_MC_TEST_DSN.

Creates and removes only randomly named test databases. No production DSN default.
"""

import os
import unittest
import uuid


@unittest.skipUnless(
    os.environ.get("LAGER_MC_TEST_DSN"), "LAGER_MC_TEST_DSN not configured"
)
class ShopDatabaseIntegrationTests(unittest.TestCase):
    def setUp(self):
        import psycopg2
        from psycopg2 import sql

        self.sql = sql
        self.psycopg2 = psycopg2
        self.admin = psycopg2.connect(os.environ["LAGER_MC_TEST_DSN"])
        self.admin.autocommit = True
        self.name = "lager_shop_test_" + uuid.uuid4().hex
        with self.admin.cursor() as cursor:
            cursor.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(self.name))
            )
        params = self.admin.get_dsn_parameters()
        params["dbname"] = self.name
        self.con = psycopg2.connect(**params)

    def tearDown(self):
        self.con.close()
        with self.admin.cursor() as cursor:
            cursor.execute(
                self.sql.SQL("DROP DATABASE {}").format(self.sql.Identifier(self.name))
            )
        self.admin.close()

    def migrate(self, provider):
        from shipping.schema import apply_app_schema, collect_schema_issues

        with self.con.cursor() as cursor:
            apply_app_schema(cursor, provider)
            self.assertEqual(collect_schema_issues(cursor), [])
        self.con.commit()

    def test_fresh_woocommerce_schema_is_repeatable(self):
        from shipping.shop import assert_shop_system

        self.migrate("woocommerce")
        self.migrate("woocommerce")
        with self.con.cursor() as cursor:
            assert_shop_system(cursor, "woocommerce")
            with self.assertRaises(RuntimeError):
                assert_shop_system(cursor, "shopify")
            cursor.execute("SELECT to_regclass('public.shopify_orders')")
            self.assertIsNone(cursor.fetchone()[0])

    def test_fresh_shopify_schema_is_repeatable(self):
        self.migrate("shopify")
        self.migrate("shopify")

    def seed_legacy(self):
        with self.con.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE items (sku text PRIMARY KEY, name text NOT NULL,
                    regal text, fach text, platz text, menge integer DEFAULT 0,
                    shopify_product_id bigint, shopify_product_dirty boolean DEFAULT false,
                    sync_status text DEFAULT 'local');
                INSERT INTO items(sku, name, shopify_product_id, shopify_product_dirty, sync_status)
                    VALUES ('SKU1', 'Produkt', 42, true, 'shopify_pending');
                CREATE TABLE shopify_orders (order_id text PRIMARY KEY, order_name text NOT NULL,
                    created_at timestamptz, shipping_name text, shipping_address1 text,
                    shipping_zip text, shipping_city text, fulfillment_status text,
                    updated_at timestamptz DEFAULT now(), source text DEFAULT 'shopify');
                CREATE UNIQUE INDEX idx_shopify_orders_name ON shopify_orders(order_name);
                INSERT INTO shopify_orders(order_id, order_name, source) VALUES ('gid://shopify/Order/1', '#1', 'shopify');
                CREATE TABLE shopify_locations (location_id text PRIMARY KEY);
                INSERT INTO shopify_locations VALUES ('gid://shopify/Location/1');
                CREATE TABLE item_location_inventory (sku text REFERENCES items(sku),
                    location_id text REFERENCES shopify_locations(location_id), PRIMARY KEY(sku, location_id));
                INSERT INTO item_location_inventory VALUES ('SKU1', 'gid://shopify/Location/1');
                CREATE TABLE shopify_fulfillment_jobs (id serial PRIMARY KEY,
                    order_id text NOT NULL, tracking_number text NOT NULL, carrier text NOT NULL,
                    shopify_fulfillment_id text, created_at timestamptz DEFAULT now(),
                    updated_at timestamptz DEFAULT now());
                INSERT INTO shopify_fulfillment_jobs(order_id, tracking_number, carrier)
                    VALUES ('gid://shopify/Order/1', 'TRACK1', 'GLS');
            """)
        self.con.commit()

    def test_legacy_data_ids_jobs_and_foreign_keys_survive(self):
        self.seed_legacy()
        self.migrate("shopify")
        self.migrate("shopify")
        with self.con.cursor() as cursor:
            cursor.execute(
                "SELECT shop_product_id, shop_product_dirty, sync_status FROM items WHERE sku='SKU1'"
            )
            self.assertEqual(cursor.fetchone(), ("42", True, "shop_pending"))
            cursor.execute("SELECT order_id, source FROM shop_orders")
            self.assertEqual(cursor.fetchone(), ("gid://shopify/Order/1", "shopify"))
            cursor.execute("SELECT tracking_number, status FROM shop_fulfillment_jobs")
            self.assertEqual(cursor.fetchone(), ("TRACK1", "pending"))
            cursor.execute(
                "SELECT count(*) FROM item_location_inventory JOIN shop_locations USING(location_id)"
            )
            self.assertEqual(cursor.fetchone()[0], 1)
            cursor.execute(
                "SELECT to_regclass('public.idx_shop_orders_name'), to_regclass('public.idx_shopify_orders_name')"
            )
            self.assertEqual(cursor.fetchone(), ("idx_shop_orders_name", None))

    def test_legacy_shopify_cannot_be_relabelled_as_woocommerce(self):
        self.seed_legacy()
        with self.assertRaises(self.psycopg2.Error):
            self.migrate("woocommerce")
        self.con.rollback()
        with self.con.cursor() as cursor:
            cursor.execute(
                "SELECT to_regclass('public.shop_configuration'), to_regclass('public.shopify_orders')"
            )
            self.assertEqual(cursor.fetchone(), (None, "shopify_orders"))

    def test_bound_database_cannot_switch_provider(self):
        self.migrate("shopify")
        with self.assertRaises(self.psycopg2.Error):
            self.migrate("woocommerce")
        self.con.rollback()
        with self.con.cursor() as cursor:
            cursor.execute("SELECT shop_system FROM shop_configuration")
            self.assertEqual(cursor.fetchone()[0], "shopify")

    def test_parallel_legacy_and_neutral_tables_abort_atomically(self):
        self.seed_legacy()
        with self.con.cursor() as cursor:
            cursor.execute("CREATE TABLE shop_orders (id integer)")
        self.con.commit()
        with self.assertRaises(self.psycopg2.Error):
            self.migrate("shopify")
        self.con.rollback()
        with self.con.cursor() as cursor:
            cursor.execute("SELECT shopify_product_id FROM items")
            self.assertEqual(cursor.fetchone()[0], 42)

    def test_woocommerce_shipping_job_roundtrip_uses_database_only(self):
        import psycopg2.extras
        from shipping.history import (
            find_or_create_shop_fulfillment_job,
            claim_shop_fulfillment_jobs,
            mark_shop_fulfillment_job_done,
            upsert_shop_shipment,
        )

        self.migrate("woocommerce")
        params = self.con.get_dsn_parameters()

        def factory():
            return self.psycopg2.connect(
                **params, cursor_factory=psycopg2.extras.RealDictCursor
            )

        # Completion helper uses tuple results internally; mirror the actual worker.
        def worker_factory():
            return self.psycopg2.connect(**params)

        queued = find_or_create_shop_fulfillment_job(
            factory,
            label_id=1,
            order_id="wc:order:123",
            tracking_number="TRACK",
            tracking_url=None,
            carrier="GLS",
        )
        duplicate = find_or_create_shop_fulfillment_job(
            factory,
            label_id=1,
            order_id="wc:order:123",
            tracking_number="TRACK",
            tracking_url=None,
            carrier="GLS",
        )
        self.assertEqual(queued["job_id"], duplicate["job_id"])
        jobs = claim_shop_fulfillment_jobs(
            worker_factory, cursor_factory=psycopg2.extras.RealDictCursor
        )
        self.assertEqual(jobs[0]["order_id"], "wc:order:123")
        mark_shop_fulfillment_job_done(
            worker_factory, queued["job_id"], None, "wc:shipment:1", "fulfilled"
        )
        with self.con.cursor() as cursor:
            cursor.execute(
                "SELECT status, shop_fulfillment_id FROM shop_fulfillment_jobs"
            )
            self.assertEqual(cursor.fetchone(), ("done", "wc:shipment:1"))
            upsert_shop_shipment(
                cursor,
                carrier="gls",
                order_id="wc:order:123",
                order_name="#123",
                shipment_reference="#123",
                tracking_number="TRACK",
                parcel_number=None,
                status="fulfilled",
                fulfillment_id="wc:shipment:1",
                source="woocommerce",
            )
            cursor.execute("SELECT source, shop_fulfillment_id FROM shipping_labels")
            self.assertEqual(cursor.fetchone(), ("woocommerce", "wc:shipment:1"))

    def test_warehouse_reads_woocommerce_orders_stock_and_customers(self):
        import importlib.util
        from pathlib import Path
        from unittest import mock
        from app_settings import DEFAULT_SETTINGS

        self.migrate("woocommerce")
        with self.con.cursor() as cursor:
            cursor.execute("""
                INSERT INTO items(sku, name, menge, available, shop_product_id, shop_weight_grams)
                    VALUES ('WC1', 'Woo Produkt', 10, 8, 'wc:product:42', 250);
                INSERT INTO shop_locations(location_id, name) VALUES ('wc:location:main', 'Hauptlager');
                INSERT INTO item_location_inventory(sku, location_id, menge, available)
                    VALUES ('WC1', 'wc:location:main', 10, 8);
                INSERT INTO shop_orders(order_id, order_name, source, fulfillment_status, payment_status)
                    VALUES ('wc:order:123', '#123', 'woocommerce', 'unfulfilled', 'paid');
                INSERT INTO shop_order_items(order_id, line_index, order_line_item_id, sku, title, quantity)
                    VALUES ('wc:order:123', 0, 'wc:line:123:7', 'WC1', 'Woo Produkt', 2);
                INSERT INTO shop_fulfillment_orders(fulfillment_order_id, order_id, assigned_location_id)
                    VALUES ('wc:fulfillment:123', 'wc:order:123', 'wc:location:main');
                INSERT INTO shop_fulfillment_order_items(fulfillment_order_line_item_id,
                    fulfillment_order_id, order_id, order_line_item_id, sku, title, quantity,
                    remaining_quantity, assigned_location_id)
                    VALUES ('wc:fulfillment-line:7', 'wc:fulfillment:123', 'wc:order:123',
                        'wc:line:123:7', 'WC1', 'Woo Produkt', 2, 2, 'wc:location:main');
                INSERT INTO shop_customers(customer_id, display_name) VALUES ('wc:customer:8', 'Woo Kunde');
            """)
        self.con.commit()
        params = self.con.get_dsn_parameters()
        settings = dict(
            DEFAULT_SETTINGS,
            shop_system="woocommerce",
            db_host=params["host"],
            db_port=int(params["port"]),
            db_name=self.name,
            db_user=params["user"],
            db_pass="",
            shop_active_location_id="wc:location:main",
        )
        spec = importlib.util.spec_from_file_location(
            "lager_mc_integration",
            Path(__file__).resolve().parent.parent / "lager_mc.py",
        )
        app = importlib.util.module_from_spec(spec)
        with mock.patch("curses.wrapper"), mock.patch(
            "app_settings.load_settings", return_value=settings
        ):
            spec.loader.exec_module(app)
        ready, message = app._probe_database_ready()
        self.assertTrue(ready, message)
        self.assertEqual(app.SHOP_SYNC_SERVICE, "woocommerce-sync")
        self.assertEqual(app.get_orders()[0]["source"], "woocommerce")
        self.assertEqual(app._load_orders_snapshot()[0]["shop_location_count"], 1)
        self.assertEqual(app.get_order_items("wc:order:123")[0]["quantity"], 2)
        self.assertEqual(
            app._load_items_snapshot("wc:location:main")[0]["available"], 8
        )
        self.assertEqual(
            app.get_shop_customers_snapshot(force=True)[0]["customer_id"],
            "wc:customer:8",
        )
        with mock.patch.object(app, "save_settings", side_effect=lambda value: value):
            saved = app._save_settings_checked({"language": "en"})
            self.assertEqual(saved["shop_system"], "woocommerce")
        app.SETTINGS["shop_system"] = "shopify"
        ready, message = app._probe_database_ready()
        self.assertFalse(ready)
        self.assertIn("Shopsystem passt nicht", message)
        with mock.patch.object(app, "save_settings") as save:
            with self.assertRaisesRegex(RuntimeError, "Shopsystem passt nicht"):
                app._save_settings_checked({"language": "en"})
            save.assert_not_called()

    def test_parallel_workstations_create_only_one_job_per_label(self):
        from concurrent.futures import ThreadPoolExecutor
        from psycopg2.extras import RealDictCursor
        from shipping.history import find_or_create_shop_fulfillment_job

        self.migrate("woocommerce")
        params = self.con.get_dsn_parameters()

        def factory():
            return self.psycopg2.connect(**params, cursor_factory=RealDictCursor)

        def create(_):
            return find_or_create_shop_fulfillment_job(
                factory,
                label_id=123,
                order_id="wc:order:123",
                tracking_number="TRACK",
                tracking_url=None,
                carrier="gls",
                line_items_json='[{"order_line_item_id":"wc:line:7","quantity":1}]',
            )

        with ThreadPoolExecutor(max_workers=8) as pool:
            jobs = list(pool.map(create, range(8)))
        self.assertEqual(len({job["job_id"] for job in jobs}), 1)
        self.assertEqual(sum(job["created"] for job in jobs), 1)

    def test_legacy_orders_without_source_receive_shopify_source(self):
        self.seed_legacy()
        with self.con.cursor() as cursor:
            cursor.execute("ALTER TABLE shopify_orders DROP COLUMN source")
        self.con.commit()
        self.migrate("shopify")
        with self.con.cursor() as cursor:
            cursor.execute("SELECT source FROM shop_orders")
            self.assertEqual(cursor.fetchone()[0], "shopify")

    def test_parallel_legacy_and_neutral_columns_abort_atomically(self):
        self.seed_legacy()
        with self.con.cursor() as cursor:
            cursor.execute("ALTER TABLE items ADD COLUMN shop_product_id text")
        self.con.commit()
        with self.assertRaises(self.psycopg2.Error):
            self.migrate("shopify")
        self.con.rollback()
        with self.con.cursor() as cursor:
            cursor.execute(
                "SELECT to_regclass('public.shopify_orders'), to_regclass('public.shop_orders')"
            )
            self.assertEqual(cursor.fetchone(), ("shopify_orders", None))
