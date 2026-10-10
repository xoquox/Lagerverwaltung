"""Opt-in actual WordPress + MariaDB + PostgreSQL test, using an explicit /tmp fixture."""

import dataclasses
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "woocommerce-sync"))
import test_shop_database_integration as pg_fixture
from woo_sync.config import Config
from woo_sync.bridge import Bridge
from woo_sync.worker import cycle, acquire_lock
from woo_sync.warehouse import migrate, LOCATION_ID

READY = bool(os.getenv("LAGER_MC_TEST_DSN") and os.getenv("LAGER_MC_WOO_TEST_ROOT"))


@unittest.skipUnless(READY, "Isolated WordPress/PostgreSQL fixture not configured")
class WooEndToEndTests(unittest.TestCase):
    def test_actual_database_transport_stock_product_partial_shipment_and_conflict(
        self,
    ):
        root = Path(os.environ["LAGER_MC_WOO_TEST_ROOT"]).resolve()
        if (
            not str(root).startswith("/tmp/")
            or not (root / ".lager-mc-test-instance").is_file()
        ):
            raise RuntimeError("Explicit disposable /tmp fixture required")
        import pymysql
        from psycopg2.extras import RealDictCursor

        base = pg_fixture.ShopDatabaseIntegrationTests()
        base.setUp()
        mysql = admin = None
        try:
            base.migrate("woocommerce")
            pg = base.con
            pg.cursor_factory = RealDictCursor
            with pg.cursor() as cur:
                migrate(cur)
            pg.commit()
            acquire_lock(pg)
            mysql = pymysql.connect(
                unix_socket="/tmp/lager-woo-mysql/test.sock",
                user="root",
                database="lager_woo_test",
                autocommit=True,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
            )
            admin = mysql
            with admin.cursor() as cur:
                cur.execute(
                    "CREATE USER IF NOT EXISTS 'lager_test'@'localhost' IDENTIFIED BY 'fixture-password'"
                )
                cur.execute(
                    "GRANT SELECT,UPDATE ON lager_woo_test.test_lager_mc_info TO 'lager_test'@'localhost'"
                )
                cur.execute(
                    "GRANT SELECT,UPDATE ON lager_woo_test.test_lager_mc_events TO 'lager_test'@'localhost'"
                )
                cur.execute(
                    "GRANT SELECT,INSERT ON lager_woo_test.test_lager_mc_commands TO 'lager_test'@'localhost'"
                )
            mysql = pymysql.connect(
                unix_socket="/tmp/lager-woo-mysql/test.sock",
                user="lager_test",
                password="fixture-password",
                database="lager_woo_test",
                autocommit=True,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
            )
            with mysql.cursor() as cur:
                cur.execute("UPDATE test_lager_mc_info SET client_id=NULL WHERE id=1")
                cur.execute("UPDATE test_lager_mc_events SET acknowledged=0")
            ids = json.loads((root / ".lager-mc-fixture.json").read_text())["yes"]
            config = Config({}, {}, prefix="test_", mode="read-only", batch=1000)

            def step():
                cycle(config, pg, mysql)

            def row(sql, args=()):
                with pg.cursor() as cur:
                    cur.execute(sql, args)
                    return cur.fetchone()

            def edit(sql, args=()):
                with pg.cursor() as cur:
                    cur.execute(sql, args)
                pg.commit()

            def tick():
                php = os.environ["LAGER_MC_TEST_PHP"]
                fixture = Path(__file__).parent / "woocommerce_bridge_integration.php"
                subprocess.run(
                    [php, str(fixture), str(root), "tick"],
                    check=True,
                    capture_output=True,
                    text=True,
                )

            tick()
            step()
            sku = "wc:variant:" + str(ids["product"])
            oid = "wc:order:" + str(ids["order"])
            item = row("SELECT * FROM items WHERE sku=%s", (sku,))
            self.assertEqual(
                (item["menge"], item["available"], item["committed"]), (10, 8, 2)
            )
            self.assertEqual(
                row(
                    "SELECT source,payment_status FROM shop_orders WHERE order_id=%s",
                    (oid,),
                ),
                dict(source="woocommerce", payment_status="paid"),
            )
            # Write inventory and product fields, preserving stock bound to paid orders.
            edit(
                "UPDATE item_location_inventory SET menge=15,dirty=true,updated_at=now() WHERE sku=%s AND location_id=%s",
                (sku, LOCATION_ID),
            )
            edit(
                "UPDATE items SET name='Edited by Lager',shop_product_dirty=true,shop_unit_cost='3.25',updated_at=now() WHERE sku=%s",
                (sku,),
            )
            config = dataclasses.replace(config, mode="read-write")
            step()
            tick()
            step()
            commands = row(
                "SELECT count(*) n FROM woocommerce_sync_commands WHERE status='failed'"
            )
            with mysql.cursor() as cur:
                cur.execute(
                    "SELECT kind,status,error FROM test_lager_mc_commands ORDER BY created_at"
                )
                remote = cur.fetchall()
            self.assertEqual(commands["n"], 0, remote)
            item = row("SELECT * FROM items WHERE sku=%s", (sku,))
            self.assertEqual(
                (
                    item["menge"],
                    item["dirty"],
                    item["shop_product_dirty"],
                    item["name"],
                ),
                (15, False, False, "Edited by Lager"),
            )
            # Partial native fulfillment via database command; no REST request.
            edit(
                "INSERT INTO shop_fulfillment_jobs(order_id,tracking_number,carrier,line_items_json) VALUES(%s,'TRACK-ONE','gls',%s)",
                (
                    oid,
                    json.dumps(
                        [
                            dict(
                                order_line_item_id="wc:line:" + str(ids["line"]),
                                quantity=1,
                            )
                        ]
                    ),
                ),
            )
            step()
            tick()
            step()
            job = row("SELECT status,result_message FROM shop_fulfillment_jobs")
            self.assertEqual(job["status"], "done", job)
            order = row(
                "SELECT fulfillment_status FROM shop_orders WHERE order_id=%s", (oid,)
            )
            self.assertEqual(order["fulfillment_status"], "partial")
            self.assertEqual(
                row(
                    "SELECT fulfilled_quantity FROM shop_order_items WHERE order_id=%s",
                    (oid,),
                )["fulfilled_quantity"],
                1,
            )
            # Replay the same remote command. It must neither duplicate nor ship more.
            with admin.cursor() as cur:
                cur.execute(
                    "SELECT command_id FROM test_lager_mc_commands WHERE kind='fulfillment' ORDER BY created_at DESC LIMIT 1"
                )
                key = cur.fetchone()["command_id"]
                cur.execute(
                    "UPDATE test_lager_mc_commands SET status='pending' WHERE command_id=%s",
                    (key,),
                )
            tick()
            step()
            self.assertEqual(
                row(
                    "SELECT fulfilled_quantity FROM shop_order_items WHERE order_id=%s",
                    (oid,),
                )["fulfilled_quantity"],
                1,
            )
            # Simulate checkout changing Woo stock after a local command was queued.
            edit(
                "UPDATE item_location_inventory SET menge=20,dirty=true,updated_at=now() WHERE sku=%s",
                (sku,),
            )
            step()
            with admin.cursor() as cur:
                # Test-only direct mutation. Production worker never accesses this table.
                cur.execute(
                    "UPDATE test_postmeta SET meta_value=12 WHERE post_id=%s AND meta_key='_stock'",
                    (ids["product"],),
                )
            tick()
            step()
            self.assertEqual(
                row("SELECT dirty FROM item_location_inventory WHERE sku=%s", (sku,))[
                    "dirty"
                ],
                True,
            )
            self.assertGreater(
                row(
                    "SELECT count(*) n FROM woocommerce_sync_commands WHERE status='failed'"
                )["n"],
                0,
            )
        finally:
            if mysql is not None:
                mysql.close()
            if admin is not None and admin is not mysql:
                admin.close()
            base.tearDown()


if __name__ == "__main__":
    unittest.main()
