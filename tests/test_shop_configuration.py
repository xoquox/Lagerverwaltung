import ast
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from app_settings import load_settings, save_settings, migrate_shop_settings
from shipping.shop import assert_shop_system, normalize_shop_system, sync_service

ROOT = Path(__file__).resolve().parent.parent


class ShopConfigurationTests(unittest.TestCase):
    def test_selection_validates_and_selects_worker(self):
        self.assertEqual(normalize_shop_system(None), "shopify")
        self.assertEqual(sync_service(" WooCommerce "), "woocommerce-sync")
        for invalid in ("", "woo", "something-else"):
            with self.assertRaises(ValueError):
                normalize_shop_system(invalid)

    def test_local_legacy_settings_override_project_defaults(self):
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory) / "settings.json"
            local = Path(directory) / "settings.local.json"
            project.write_text(json.dumps({"shop_location_mode": "single"}))
            local.write_text(
                json.dumps(
                    {
                        "shop_system": "woocommerce",
                        "shopify_location_mode": "multi",
                        "shopify_active_location_id": "wc:location:main",
                    }
                )
            )
            with mock.patch("app_settings.SETTINGS_PATH", project), mock.patch(
                "app_settings.LOCAL_SETTINGS_PATH", local
            ):
                settings = load_settings()
                self.assertEqual(settings["shop_location_mode"], "multi")
                self.assertEqual(
                    settings["shop_active_location_id"], "wc:location:main"
                )
                self.assertEqual(settings["shop_system"], "woocommerce")
                self.assertNotIn("shopify_location_mode", settings)
                save_settings(settings)
                self.assertEqual(load_settings(), settings)
                self.assertNotIn("shopify_location_mode", json.loads(local.read_text()))

    def test_explicit_neutral_key_wins_over_legacy_key(self):
        self.assertEqual(
            migrate_shop_settings(
                {
                    "shopify_tracking_mode_post": "company",
                    "shop_tracking_mode_post": "company_and_url",
                }
            ),
            {"shop_tracking_mode_post": "company_and_url"},
        )

    def test_database_guard_rejects_wrong_or_missing_provider(self):
        cursor = mock.Mock()
        for row in (("woocommerce",), {"shop_system": "woocommerce"}, None):
            cursor.fetchone.return_value = row
            with self.assertRaises(RuntimeError):
                assert_shop_system(cursor, "shopify")
        cursor.fetchone.return_value = {"shop_system": "woocommerce"}
        assert_shop_system(cursor, "woocommerce")

    def test_warehouse_has_no_shop_network_client_or_sync_import(self):
        for filename in (
            "lager_mc.py",
            "ui_orders.py",
            "ui_settings.py",
            "ui_menu.py",
            "ui_mouse.py",
            "shipping/shop.py",
        ):
            source = (ROOT / filename).read_text()
            tree = ast.parse(source)
            imports = [
                name.name
                for node in ast.walk(tree)
                if isinstance(node, ast.Import)
                for name in node.names
            ] + [
                node.module or ""
                for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom)
            ]
            self.assertFalse(
                any(
                    name.startswith(("shopify", "woocommerce", "requests"))
                    for name in imports
                ),
                filename,
            )
            for token in (
                "myshopify.com",
                "/admin/api/",
                "/wp-json/wc/",
                "SHOPIFY_APP_CLIENT_SECRET",
            ):
                self.assertNotIn(token, source, filename)

    def test_shared_contract_is_identical_in_sync_container(self):
        for name in ("shop.py", "history.py", "schema.py"):
            self.assertEqual(
                (ROOT / "shipping" / name).read_bytes(),
                (ROOT / "shopify-sync" / "shipping" / name).read_bytes(),
            )
