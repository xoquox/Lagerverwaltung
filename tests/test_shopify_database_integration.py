"""Shopify concurrency tests use disposable databases and stub every remote operation."""

import importlib.util
import os
from pathlib import Path
import unittest
from unittest import mock

import test_shop_database_integration as pg_fixture


@unittest.skipUnless(
    os.environ.get("LAGER_MC_TEST_DSN"), "LAGER_MC_TEST_DSN not configured"
)
class ShopifyDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.base = pg_fixture.ShopDatabaseIntegrationTests()
        self.base.setUp()
        self.base.migrate("shopify")
        self.con = self.base.con
        path = Path(__file__).resolve().parents[1] / "shopify-sync/shopify_sync.py"
        spec = importlib.util.spec_from_file_location("shopify_pg_test", path)
        self.sync = importlib.util.module_from_spec(spec)
        import sys

        sys.path.insert(0, str(path.parent))
        try:
            spec.loader.exec_module(self.sync)
        finally:
            sys.path.remove(str(path.parent))
        self.sync.SHOPIFY_LOCATION_ID = "123"
        self.location = self.sync._location_gid()
        with self.con.cursor() as cur:
            cur.execute(
                "INSERT INTO items(sku,display_sku,name,menge,available,shop_product_id,shop_variant_id,shop_inventory_item_id,shop_product_status) VALUES('stable','SKU','Original',10,10,'9','11','77','ACTIVE')"
            )
            cur.execute(
                "INSERT INTO shop_locations(location_id,name) VALUES(%s,'Fixture')",
                (self.location,),
            )
            cur.execute(
                "INSERT INTO item_location_inventory(sku,location_id,menge,available,dirty) VALUES('stable',%s,10,10,TRUE)",
                (self.location,),
            )
        self.con.commit()

    def tearDown(self):
        self.base.tearDown()

    def worker_connection(self):
        import psycopg2

        return psycopg2.connect(**self.con.get_dsn_parameters())

    def update(self, sql):
        with self.con.cursor() as cur:
            cur.execute(sql)
        self.con.commit()

    def row(self, sql):
        with self.con.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()

    def test_inventory_ack_preserves_change_saved_during_remote_request(self):
        def remote(*args, **kwargs):
            self.update(
                "UPDATE item_location_inventory SET menge=25,available=25,dirty=TRUE,updated_at=NOW()"
            )
            return {"inventorySetQuantities": {"userErrors": []}}

        with (
            mock.patch.object(self.sync, "db", side_effect=self.worker_connection),
            mock.patch.object(self.sync, "graphql_request", side_effect=remote),
            mock.patch.object(self.sync.time, "sleep"),
        ):
            self.assertEqual(self.sync.push_inventory_changes(), 1)
        self.assertEqual(
            self.row("SELECT menge,available,dirty FROM item_location_inventory"),
            (25, 25, True),
        )
        self.assertEqual(self.row("SELECT menge,dirty FROM items"), (25, True))

    def test_inventory_ack_clears_the_unchanged_revision(self):
        with (
            mock.patch.object(self.sync, "db", side_effect=self.worker_connection),
            mock.patch.object(
                self.sync,
                "graphql_request",
                return_value={"inventorySetQuantities": {"userErrors": []}},
            ),
            mock.patch.object(self.sync.time, "sleep"),
        ):
            self.assertEqual(self.sync.push_inventory_changes(), 1)
        self.assertEqual(
            self.row("SELECT dirty FROM item_location_inventory"), (False,)
        )

    def test_product_ack_preserves_newer_name_status_and_dirty_flag(self):
        self.update(
            "UPDATE items SET shop_product_dirty=TRUE,shop_product_sync_action='update'"
        )

        def remote(*args, **kwargs):
            self.update(
                "UPDATE items SET name='Newer',shop_product_status='DRAFT',shop_product_dirty=TRUE,updated_at=NOW()"
            )
            return dict(
                product_id="9", variant_id="11", inventory_item_id="77", status="ACTIVE"
            )

        with (
            mock.patch.object(self.sync, "db", side_effect=self.worker_connection),
            mock.patch.object(self.sync, "_update_shopify_product", side_effect=remote),
            mock.patch.object(self.sync.time, "sleep"),
        ):
            self.assertEqual(self.sync.push_product_changes(), 1)
        self.assertEqual(
            self.row(
                "SELECT name,shop_product_status,shop_product_dirty,shop_product_sync_action FROM items"
            ),
            ("Newer", "DRAFT", True, "update"),
        )

    def test_old_product_failure_does_not_mark_newer_edit_as_failed(self):
        self.update(
            "UPDATE items SET shop_product_dirty=TRUE,shop_product_sync_action='update'"
        )

        def remote(*args, **kwargs):
            self.update(
                "UPDATE items SET name='Newer',sync_status='shop_pending',updated_at=NOW()"
            )
            raise RuntimeError("old request failed")

        with (
            mock.patch.object(self.sync, "db", side_effect=self.worker_connection),
            mock.patch.object(self.sync, "_update_shopify_product", side_effect=remote),
            mock.patch.object(self.sync.time, "sleep"),
        ):
            self.assertEqual(self.sync.push_product_changes(), 0)
        self.assertEqual(
            self.row("SELECT name,sync_status,shop_product_sync_error FROM items"),
            ("Newer", "shop_pending", None),
        )

    def test_sku_reconciliation_preserves_fulfillment_assignment(self):
        self.update("INSERT INTO items(sku,name) VALUES('renamed','Target')")
        self.update(
            "INSERT INTO shop_orders(order_id,order_name,source) VALUES('order-fixture','Fixture','shopify')"
        )
        self.update(
            "INSERT INTO shop_fulfillment_orders(fulfillment_order_id,order_id) VALUES('fulfillment-fixture','order-fixture')"
        )
        self.update(
            "INSERT INTO shop_fulfillment_order_items(fulfillment_order_line_item_id,fulfillment_order_id,order_id,sku,title,quantity) VALUES('line-fixture','fulfillment-fixture','order-fixture','stable','Fixture',1)"
        )
        with self.con.cursor() as cur:
            self.sync._update_item_sku_references(cur, "stable", "renamed")
        self.con.commit()
        self.assertEqual(
            self.row("SELECT sku FROM shop_fulfillment_order_items"), ("renamed",)
        )
