import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parent.parent
MODULE_PATH = ROOT / "shopify-sync" / "shopify_sync.py"


def load_shopify_sync_module():
    psycopg2_module = types.ModuleType("psycopg2")
    extras_module = types.ModuleType("psycopg2.extras")
    extras_module.RealDictCursor = object
    psycopg2_module.extras = extras_module
    psycopg2_module.connect = lambda *args, **kwargs: None

    requests_module = types.ModuleType("requests")
    requests_module.RequestException = Exception
    requests_module.post = lambda *args, **kwargs: None
    requests_module.get = lambda *args, **kwargs: None

    dotenv_module = types.ModuleType("dotenv")
    dotenv_module.load_dotenv = lambda *args, **kwargs: None

    sys.modules["psycopg2"] = psycopg2_module
    sys.modules["psycopg2.extras"] = extras_module
    sys.modules["requests"] = requests_module
    sys.modules["dotenv"] = dotenv_module

    if str(MODULE_PATH.parent) not in sys.path:
        sys.path.insert(0, str(MODULE_PATH.parent))

    spec = importlib.util.spec_from_file_location("shopify_sync_test_module", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    if str(MODULE_PATH.parent) in sys.path:
        sys.path.remove(str(MODULE_PATH.parent))
    for module_name in ("shipping", "shipping.history", "shipping.schema"):
        sys.modules.pop(module_name, None)
    return module


class ShopifySyncLoggingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.shopify_sync = load_shopify_sync_module()

    def test_resolve_sync_base_dir_uses_app_dir_in_repo_layout(self):
        script_path = ROOT / "shopify-sync" / "shopify_sync.py"

        base_dir = self.shopify_sync.resolve_sync_base_dir(script_path)

        self.assertEqual(base_dir, ROOT / "shopify-sync")

    def test_resolve_sync_base_dir_falls_back_to_app_dir_for_standalone_layout(self):
        script_path = Path("/app/shopify_sync.py")

        base_dir = self.shopify_sync.resolve_sync_base_dir(script_path)

        self.assertEqual(base_dir, Path("/app"))

    def test_shorten_text_truncates_and_flattens_newlines(self):
        value = "abc\ndefghijkl"
        shortened = self.shopify_sync.shorten_text(value, limit=8)
        self.assertEqual(shortened, "abc\\n...")

    def test_summarize_orders_returns_latest_and_line_item_count(self):
        orders = [
            {
                "name": "2026-2675",
                "createdAt": "2026-03-24T14:39:07Z",
                "lineItems": {"nodes": [{"id": "1"}, {"id": "2"}]},
            },
            {
                "name": "2026-2676",
                "createdAt": "2026-03-25T14:07:00Z",
                "lineItems": {"nodes": [{"id": "3"}]},
            },
        ]

        summary = self.shopify_sync.summarize_orders(orders)

        self.assertEqual(summary["count"], 2)
        self.assertEqual(summary["latest_name"], "2026-2676")
        self.assertEqual(summary["latest_created_at"], "2026-03-25T14:07:00Z")
        self.assertEqual(summary["line_items"], 3)

    def test_summarize_orders_handles_empty_list(self):
        summary = self.shopify_sync.summarize_orders([])
        self.assertEqual(
            summary,
            {"count": 0, "latest_name": "-", "latest_created_at": "-", "line_items": 0},
        )

    def test_build_sync_version_payload_contains_service_and_version(self):
        with mock.patch.object(
            self.shopify_sync.datetime,
            "datetime",
            wraps=self.shopify_sync.datetime.datetime,
        ) as datetime_mock:
            datetime_mock.now.return_value = self.shopify_sync.datetime.datetime(2026, 3, 25, 21, 0, tzinfo=self.shopify_sync.datetime.timezone.utc)
            payload = self.shopify_sync.build_sync_version_payload()

        self.assertEqual(payload["service"], "shopify-sync")
        self.assertEqual(payload["version"], self.shopify_sync.SYNC_VERSION)
        self.assertEqual(payload["reported_at"], "2026-03-25T21:00:00+00:00")

    def test_update_service_runtime_state_writes_version_and_status(self):
        executed = []

        class FakeCursor:
            def execute(self, query, params=None):
                executed.append((" ".join(query.split()), params))

            def close(self):
                return None

        class FakeConnection:
            def cursor(self):
                return FakeCursor()

            def commit(self):
                return None

            def close(self):
                return None

        with mock.patch.object(self.shopify_sync, "db", return_value=FakeConnection()):
            self.shopify_sync.update_service_runtime_state(status="running", mark_seen=True, mark_started=True, clear_error=True)

        self.assertTrue(executed)
        query, params = executed[0]
        self.assertIn("INSERT INTO service_runtime_state", query)
        self.assertEqual(params[0], self.shopify_sync.SYNC_VERSION)
        self.assertEqual(params[1], "running")

    def test_iter_fulfillments_accepts_plain_list_shape(self):
        order = {
            "fulfillments": [
                {"id": "f1", "trackingInfo": []},
                {"id": "f2", "trackingInfo": []},
                None,
            ]
        }

        rows = self.shopify_sync._iter_fulfillments(order)

        self.assertEqual([row["id"] for row in rows], ["f1", "f2"])

    def test_iter_tracking_rows_accepts_plain_list_shape(self):
        fulfillment = {
            "trackingInfo": [
                {"number": "123"},
                {"number": "456"},
                "bad",
            ]
        }

        rows = self.shopify_sync._iter_tracking_rows(fulfillment)

        self.assertEqual([row["number"] for row in rows], ["123", "456"])

    def test_iter_helpers_also_accept_nodes_shape(self):
        order = {
            "fulfillments": {
                "nodes": [
                    {
                        "id": "f1",
                        "trackingInfo": {
                            "nodes": [{"number": "123"}],
                        },
                    }
                ]
            }
        }

        fulfillments = self.shopify_sync._iter_fulfillments(order)
        tracking_rows = self.shopify_sync._iter_tracking_rows(fulfillments[0])

        self.assertEqual(fulfillments[0]["id"], "f1")
        self.assertEqual(tracking_rows[0]["number"], "123")

    def test_iter_fulfillment_orders_accepts_nodes_shape(self):
        order = {
            "fulfillmentOrders": {
                "nodes": [
                    {"id": "fo1"},
                    {"id": "fo2"},
                    "bad",
                ]
            }
        }

        rows = self.shopify_sync._iter_fulfillment_orders(order)

        self.assertEqual([row["id"] for row in rows], ["fo1", "fo2"])

    def test_build_fulfillment_order_records_extracts_location_and_line_items(self):
        order = {
            "id": "gid://shopify/Order/1",
            "fulfillmentOrders": {
                "nodes": [
                    {
                        "id": "gid://shopify/FulfillmentOrder/10",
                        "status": "OPEN",
                        "requestStatus": "UNSUBMITTED",
                        "assignedLocation": {
                            "location": {
                                "id": "gid://shopify/Location/100",
                                "name": "Werkstatt",
                                "fulfillsOnlineOrders": True,
                                "isActive": True,
                            }
                        },
                        "lineItems": {
                            "nodes": [
                                {
                                    "id": "gid://shopify/FulfillmentOrderLineItem/55",
                                    "sku": "ABC-1",
                                    "productTitle": "Artikel A",
                                    "remainingQuantity": 2,
                                    "totalQuantity": 5,
                                    "lineItem": {
                                        "id": "gid://shopify/LineItem/9",
                                        "name": "Artikel A",
                                        "sku": "ABC-1",
                                    },
                                }
                            ]
                        },
                    }
                ]
            },
        }

        orders_payload, items_payload = self.shopify_sync._build_fulfillment_order_records(order)

        self.assertEqual(len(orders_payload), 1)
        self.assertEqual(orders_payload[0]["assigned_location_id"], "gid://shopify/Location/100")
        self.assertEqual(orders_payload[0]["assigned_location_name"], "Werkstatt")
        self.assertEqual(len(items_payload), 1)
        self.assertEqual(items_payload[0]["order_line_item_id"], "gid://shopify/LineItem/9")
        self.assertEqual(items_payload[0]["sku"], "ABC-1")
        self.assertEqual(items_payload[0]["quantity"], 5)
        self.assertEqual(items_payload[0]["remaining_quantity"], 2)

    def test_main_prints_version_for_flag(self):
        with mock.patch.object(self.shopify_sync.argparse.ArgumentParser, "parse_args", return_value=types.SimpleNamespace(version=True, command=None)):
            with mock.patch("builtins.print") as print_mock:
                self.shopify_sync.main()

        print_mock.assert_called_once_with(self.shopify_sync.SYNC_VERSION)

    def test_main_prints_version_payload_for_subcommand(self):
        args = types.SimpleNamespace(version=False, command="version", json=True)
        with mock.patch.object(self.shopify_sync.argparse.ArgumentParser, "parse_args", return_value=args):
            with mock.patch("builtins.print") as print_mock:
                self.shopify_sync.main()

        payload = json.loads(print_mock.call_args.args[0])
        self.assertEqual(payload["service"], "shopify-sync")
        self.assertEqual(payload["version"], self.shopify_sync.SYNC_VERSION)

    def test_format_locations_text_renders_name_id_and_status(self):
        text = self.shopify_sync.format_locations_text(
            [
                {
                    "location_id": "gid://shopify/Location/100",
                    "location_name": "Werkstatt",
                    "fulfills_online_orders": True,
                    "is_active": True,
                },
                {
                    "location_id": "gid://shopify/Location/200",
                    "location_name": "Lager Nord",
                    "fulfills_online_orders": False,
                    "is_active": False,
                },
            ]
        )

        self.assertIn("1. Werkstatt [gid://shopify/Location/100] (active, online)", text)
        self.assertIn("2. Lager Nord [gid://shopify/Location/200] (inactive)", text)

    def test_main_prints_location_payload_for_subcommand(self):
        args = types.SimpleNamespace(version=False, command="list-locations", json=True)
        locations = [
            {
                "location_id": "gid://shopify/Location/100",
                "location_name": "Werkstatt",
                "fulfills_online_orders": True,
                "is_active": True,
            }
        ]
        with mock.patch.object(self.shopify_sync.argparse.ArgumentParser, "parse_args", return_value=args):
            with mock.patch.object(self.shopify_sync, "build_locations_payload", return_value=locations):
                with mock.patch("builtins.print") as print_mock:
                    self.shopify_sync.main()

        payload = json.loads(print_mock.call_args.args[0])
        self.assertEqual(payload["locations"][0]["location_name"], "Werkstatt")

    def test_sync_customers_truncates_and_inserts_default_address(self):
        executed = []

        class FakeCursor:
            def execute(self, query, params=None):
                executed.append((" ".join(query.split()), params))

            def close(self):
                return None

        class FakeConnection:
            def cursor(self):
                return FakeCursor()

            def commit(self):
                return None

            def close(self):
                return None

        customers = [
            {
                "id": "gid://shopify/Customer/1",
                "firstName": "Max",
                "lastName": "Mustermann",
                "displayName": "Max Mustermann",
                "email": "max@example.com",
                "phone": "01234",
                "defaultAddress": {
                    "name": "Max Mustermann",
                    "address1": "Musterstr. 1",
                    "zip": "12345",
                    "city": "Berlin",
                    "country": "Germany",
                    "phone": "01234",
                },
            }
        ]

        with mock.patch.object(self.shopify_sync, "get_all_customers", return_value=customers):
            with mock.patch.object(self.shopify_sync, "db", return_value=FakeConnection()):
                count = self.shopify_sync.sync_customers()

        self.assertEqual(count, 1)
        self.assertTrue(any("TRUNCATE TABLE shopify_customers" in query for query, _ in executed))
        insert_query, insert_params = next((q, p) for q, p in executed if "INSERT INTO shopify_customers" in q)
        self.assertIn("INSERT INTO shopify_customers", insert_query)
        self.assertEqual(insert_params[0], "gid://shopify/Customer/1")
        self.assertEqual(insert_params[3], "Max Mustermann")
        self.assertEqual(insert_params[7], "Musterstr. 1")
        self.assertEqual(insert_params[10], "Germany")

    def test_sync_orders_preserves_custom_orders(self):
        executed = []

        class FakeCursor:
            def execute(self, query, params=None):
                executed.append((" ".join(query.split()), params))

        class FakeConnection:
            def cursor(self):
                return FakeCursor()

            def commit(self):
                return None

            def close(self):
                return None

        orders = [
            {
                "id": "gid://shopify/Order/1",
                "name": "2026-3000",
                "createdAt": "2026-05-21T10:00:00Z",
                "email": "kunde@example.test",
                "displayFulfillmentStatus": "UNFULFILLED",
                "displayFinancialStatus": "PAID",
                "shippingAddress": {
                    "name": "Max Mustermann",
                    "address1": "Musterstr. 1",
                    "address2": "Firma",
                    "zip": "12345",
                    "city": "Berlin",
                    "country": "Germany",
                    "phone": "01234",
                },
                "lineItems": {"nodes": []},
                "fulfillmentOrders": {"nodes": []},
                "fulfillments": {"nodes": []},
            }
        ]

        with mock.patch.object(self.shopify_sync, "get_all_orders", return_value=orders):
            with mock.patch.object(self.shopify_sync, "db", return_value=FakeConnection()):
                count = self.shopify_sync.sync_orders()

        self.assertEqual(count, 1)
        self.assertFalse(any("TRUNCATE TABLE shopify_fulfillment_order_items" in query for query, _ in executed))
        self.assertTrue(any("DELETE FROM shopify_orders WHERE COALESCE(source, 'shopify') = 'shopify'" in query for query, _ in executed))
        insert_query, insert_params = next((q, p) for q, p in executed if "INSERT INTO shopify_orders" in q)
        self.assertIn("source", insert_query)
        self.assertEqual(insert_params[5], "Firma")

    def test_get_all_product_variants_paginates_graphql_connection(self):
        responses = [
            {
                "productVariants": {
                    "nodes": [
                        {"id": "gid://shopify/ProductVariant/1", "sku": "SKU-1"},
                    ],
                    "pageInfo": {"hasNextPage": True, "endCursor": "cursor-1"},
                }
            },
            {
                "productVariants": {
                    "nodes": [
                        {"id": "gid://shopify/ProductVariant/2", "sku": "SKU-2"},
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                }
            },
        ]

        with mock.patch.object(self.shopify_sync, "graphql_request", side_effect=responses) as graphql_mock:
            variants = self.shopify_sync.get_all_product_variants()

        self.assertEqual([row["sku"] for row in variants], ["SKU-1", "SKU-2"])
        self.assertEqual(graphql_mock.call_count, 2)
        self.assertEqual(graphql_mock.call_args_list[0].args[1], {"after": None})
        self.assertEqual(graphql_mock.call_args_list[1].args[1], {"after": "cursor-1"})

    def test_push_inventory_changes_uses_inventory_set_quantities_mutation(self):
        executed = []

        class FakeCursor:
            def execute(self, query, params=None):
                executed.append((" ".join(query.split()), params))

            def fetchall(self):
                return [("SKU 1", "SKU 1", "gid://shopify/Location/67402989753", 7, "12345")]

        class FakeConnection:
            def cursor(self):
                return FakeCursor()

            def commit(self):
                return None

            def close(self):
                return None

        graphql_response = {
            "inventorySetQuantities": {
                "inventoryAdjustmentGroup": {"createdAt": "2026-04-04T18:00:00Z"},
                "userErrors": [],
            }
        }

        with mock.patch.object(self.shopify_sync, "db", return_value=FakeConnection()):
            with mock.patch.object(self.shopify_sync, "graphql_request", return_value=graphql_response) as graphql_mock:
                count = self.shopify_sync.push_inventory_changes()

        self.assertEqual(count, 1)
        self.assertIn("inventorySetQuantities", graphql_mock.call_args.args[0])
        self.assertIn("@idempotent(key:", graphql_mock.call_args.args[0])
        variables = graphql_mock.call_args.args[1]
        self.assertEqual(variables["input"]["name"], "available")
        self.assertEqual(variables["input"]["referenceDocumentUri"], "gid://lager-mc/InventorySync/SKU%201")
        self.assertEqual(variables["input"]["quantities"][0]["inventoryItemId"], "gid://shopify/InventoryItem/12345")
        self.assertEqual(variables["input"]["quantities"][0]["locationId"], "gid://shopify/Location/67402989753")
        self.assertEqual(variables["input"]["quantities"][0]["quantity"], 7)
        self.assertTrue(any("UPDATE item_location_inventory SET dirty = FALSE" in query for query, _ in executed))

    def test_push_product_changes_creates_local_item_as_draft(self):
        executed = []

        class FakeCursor:
            def __init__(self):
                self._rows = [
                    (
                        "SKU-LOCAL",
                        "SKU-LOCAL",
                        "Lokales Produkt",
                        "4012345678901",
                        None,
                        None,
                        None,
                        "ACTIVE",
                        "Beschreibung",
                        "19.99",
                        None,
                        "8.50",
                        250,
                        "create",
                        3,
                        3,
                    )
                ]

            def execute(self, query, params=None):
                executed.append((" ".join(query.split()), params))

            def fetchall(self):
                rows = self._rows
                self._rows = []
                return rows

        class FakeConnection:
            def cursor(self):
                return FakeCursor()

            def commit(self):
                return None

            def close(self):
                return None

        responses = [
            {
                "productCreate": {
                    "product": {
                        "id": "gid://shopify/Product/99",
                        "status": "DRAFT",
                        "variants": {
                            "nodes": [
                                {
                                    "id": "gid://shopify/ProductVariant/88",
                                    "inventoryItem": {"id": "gid://shopify/InventoryItem/77"},
                                }
                            ]
                        },
                    },
                    "userErrors": [],
                }
            },
            {"productUpdate": {"product": {"id": "gid://shopify/Product/99", "status": "DRAFT"}, "userErrors": []}},
            {
                "productVariantsBulkUpdate": {
                    "productVariants": [
                        {
                            "id": "gid://shopify/ProductVariant/88",
                            "inventoryItem": {"id": "gid://shopify/InventoryItem/77"},
                        }
                    ],
                    "userErrors": [],
                }
            },
        ]

        with mock.patch.object(self.shopify_sync, "db", return_value=FakeConnection()):
            with mock.patch.object(self.shopify_sync, "graphql_request", side_effect=responses) as graphql_mock:
                count = self.shopify_sync.push_product_changes()

        self.assertEqual(count, 1)
        self.assertIn("productCreate", graphql_mock.call_args_list[0].args[0])
        self.assertEqual(graphql_mock.call_args_list[0].args[1]["product"]["status"], "DRAFT")
        self.assertIn("productVariantsBulkUpdate", graphql_mock.call_args_list[2].args[0])
        variant = graphql_mock.call_args_list[2].args[1]["variants"][0]
        self.assertEqual(variant["inventoryItem"]["sku"], "SKU-LOCAL")
        self.assertEqual(variant["inventoryItem"]["tracked"], True)
        self.assertTrue(any("shopify_product_dirty = FALSE" in query for query, _ in executed))
        self.assertTrue(any("INSERT INTO item_location_inventory" in query for query, _ in executed))

    def test_sync_products_writes_graphql_variant_fields(self):
        executed = []

        class FakeCursor:
            def __init__(self):
                self._rows = []

            def execute(self, query, params=None):
                executed.append((" ".join(query.split()), params))
                if "SELECT sku, dirty, menge, available" in query:
                    self._rows = []

            def fetchall(self):
                return list(self._rows)

        class FakeConnection:
            def cursor(self):
                return FakeCursor()

            def commit(self):
                return None

            def close(self):
                return None

        variants = [
            {
                "id": "gid://shopify/ProductVariant/11",
                "sku": "SKU-11",
                "barcode": "BAR-11",
                "price": "19.99",
                "compareAtPrice": "24.99",
                "inventoryQuantity": 5,
                "product": {
                    "id": "gid://shopify/Product/9",
                    "title": "Produkt A",
                    "status": "ACTIVE",
                    "descriptionHtml": "<p>Beschreibung</p>",
                },
                "inventoryItem": {
                    "id": "gid://shopify/InventoryItem/77",
                    "sku": "SKU-11",
                    "unitCost": {"amount": "12.50", "currencyCode": "EUR"},
                    "measurement": {"weight": {"unit": "KILOGRAMS", "value": 0.25}},
                },
            }
        ]

        with mock.patch.object(self.shopify_sync, "get_all_product_variants", return_value=variants):
            with mock.patch.object(self.shopify_sync, "db", return_value=FakeConnection()):
                count = self.shopify_sync.sync_products()

        self.assertEqual(count, 1)
        insert_query, insert_params = next((q, p) for q, p in executed if "INSERT INTO items(" in q)
        self.assertIn("INSERT INTO items(", insert_query)
        self.assertEqual(insert_params[0], "SKU-11")
        self.assertEqual(insert_params[1], "Produkt A")
        self.assertEqual(insert_params[2], "SKU-11")
        self.assertEqual(insert_params[8], "gid://shopify/Product/9")
        self.assertEqual(insert_params[9], "gid://shopify/ProductVariant/11")
        self.assertEqual(insert_params[10], "gid://shopify/InventoryItem/77")
        self.assertEqual(insert_params[16], "12.50")
        self.assertEqual(insert_params[17], "EUR")
        self.assertEqual(insert_params[18], 250)
        self.assertIn("WHEN COALESCE(items.shopify_product_dirty, FALSE) = TRUE THEN items.name", insert_query)
        self.assertIn("WHEN COALESCE(items.shopify_product_dirty, FALSE) = TRUE THEN items.shopify_price", insert_query)

    def test_sync_products_reconciles_existing_variant_row_before_upsert(self):
        executed = []

        class FakeCursor:
            def __init__(self):
                self._rows = []

            def execute(self, query, params=None):
                compact = " ".join(query.split())
                executed.append((compact, params))
                if "SELECT sku, dirty, menge, available" in compact:
                    self._rows = [
                        ("__shopify_variant__11", False, 5, 5, 0, 0, 0, "", "", ""),
                    ]
                else:
                    self._rows = []

            def fetchall(self):
                return list(self._rows)

        class FakeConnection:
            def cursor(self):
                return FakeCursor()

            def commit(self):
                return None

            def close(self):
                return None

        variants = [
            {
                "id": "gid://shopify/ProductVariant/11",
                "sku": "SKU-11",
                "barcode": "BAR-11",
                "price": "19.99",
                "compareAtPrice": "24.99",
                "inventoryQuantity": 5,
                "product": {
                    "id": "gid://shopify/Product/9",
                    "title": "Produkt A",
                    "status": "ACTIVE",
                    "descriptionHtml": "<p>Beschreibung</p>",
                },
                "inventoryItem": {
                    "id": "gid://shopify/InventoryItem/77",
                    "sku": "SKU-11",
                    "unitCost": {"amount": "12.50", "currencyCode": "EUR"},
                    "measurement": {"weight": {"unit": "KILOGRAMS", "value": 0.25}},
                },
            }
        ]

        with mock.patch.object(self.shopify_sync, "get_all_product_variants", return_value=variants):
            with mock.patch.object(self.shopify_sync, "db", return_value=FakeConnection()):
                count = self.shopify_sync.sync_products()

        self.assertEqual(count, 1)
        self.assertTrue(any("UPDATE items SET sku = %s" in query for query, _ in executed))
        update_query, update_params = next((q, p) for q, p in executed if "UPDATE items SET sku = %s" in q)
        self.assertEqual(update_params, ("SKU-11", "__shopify_variant__11"))
        insert_query, insert_params = next((q, p) for q, p in executed if "INSERT INTO items(" in q)
        self.assertEqual(insert_params[0], "SKU-11")

    def test_sync_products_merges_duplicate_identity_rows_into_target_sku(self):
        executed = []

        class FakeCursor:
            def __init__(self):
                self._rows = []

            def execute(self, query, params=None):
                compact = " ".join(query.split())
                executed.append((compact, params))
                if "SELECT sku, dirty, menge, available" in compact:
                    self._rows = [
                        ("SKU-11", False, 5, 5, 0, 0, 0, "", "", ""),
                        ("__shopify_variant__11", True, 7, 7, 0, 0, 0, "F", "2", "3"),
                    ]
                else:
                    self._rows = []

            def fetchall(self):
                return list(self._rows)

        class FakeConnection:
            def cursor(self):
                return FakeCursor()

            def commit(self):
                return None

            def close(self):
                return None

        variants = [
            {
                "id": "gid://shopify/ProductVariant/11",
                "sku": "SKU-11",
                "barcode": "BAR-11",
                "price": "19.99",
                "compareAtPrice": "24.99",
                "inventoryQuantity": 5,
                "product": {
                    "id": "gid://shopify/Product/9",
                    "title": "Produkt A",
                    "status": "ACTIVE",
                    "descriptionHtml": "<p>Beschreibung</p>",
                },
                "inventoryItem": {
                    "id": "gid://shopify/InventoryItem/77",
                    "sku": "SKU-11",
                    "unitCost": {"amount": "12.50", "currencyCode": "EUR"},
                    "measurement": {"weight": {"unit": "KILOGRAMS", "value": 0.25}},
                },
            }
        ]

        with mock.patch.object(self.shopify_sync, "get_all_product_variants", return_value=variants):
            with mock.patch.object(self.shopify_sync, "db", return_value=FakeConnection()):
                count = self.shopify_sync.sync_products()

        self.assertEqual(count, 1)
        self.assertTrue(any("UPDATE item_location_inventory AS dest" in query for query, _ in executed))
        self.assertTrue(any("DELETE FROM items WHERE sku = %s" in query for query, _ in executed))
        delete_query, delete_params = next((q, p) for q, p in executed if "DELETE FROM items WHERE sku = %s" in q)
        self.assertEqual(delete_params, ("__shopify_variant__11",))

    def test_upsert_shopify_shipment_writes_shipping_labels_table(self):
        executed = []

        class FakeCursor:
            def execute(self, query, params=None):
                executed.append((" ".join(query.split()), params))

        order = {"id": "gid://shopify/Order/1", "name": "#1001"}
        fulfillment = {"id": "gid://shopify/Fulfillment/1", "status": "SUCCESS", "createdAt": "2026-03-31T10:00:00Z"}
        tracking = {"number": "1234567890", "url": "https://example.invalid/track/1234567890", "company": "GLS"}

        self.shopify_sync.upsert_shopify_shipment(FakeCursor(), order, fulfillment, tracking)

        query, params = executed[0]
        self.assertIn("INSERT INTO shipping_labels", query)
        self.assertEqual(params[0], "gls")
        self.assertEqual(params[4], "1234567890")

    def test_run_connect_flow_writes_expiring_token_bundle(self):
        env_path = ROOT / "shopify-sync" / ".env.test-connect"
        if env_path.exists():
            env_path.unlink()

        callback_payload = {
            "shop": "example-shop.myshopify.com",
            "token": "shpat_access",
            "refresh_token": "shprt_refresh",
            "token_expires_at": 1712443600,
            "refresh_token_expires_at": 1715040000,
            "scope": "read_products",
        }

        with mock.patch.object(self.shopify_sync, "_wait_for_local_oauth_callback", return_value=callback_payload):
            original_write_sync_env_values = self.shopify_sync.write_sync_env_values
            target_env_path = env_path
            with mock.patch.object(
                self.shopify_sync,
                "write_sync_env_values",
                side_effect=lambda updates, env_path=None: original_write_sync_env_values(updates, env_path=env_path or target_env_path),
            ):
                result = self.shopify_sync.run_connect_flow(
                    "example-shop.myshopify.com",
                    relay_base_url="https://relay.example.test",
                    port=3459,
                    timeout_seconds=30,
                    open_browser=False,
                )

        self.assertEqual(result["token"], "shpat_access")
        env_text = env_path.read_text(encoding="utf-8")
        self.assertIn("SHOP=example-shop.myshopify.com", env_text)
        self.assertIn("TOKEN=shpat_access", env_text)
        self.assertIn("REFRESH_TOKEN=shprt_refresh", env_text)
        self.assertIn("TOKEN_EXPIRES_AT=1712443600", env_text)
        env_path.unlink()

    def test_manual_oauth_callback_from_url_parses_shop_and_code(self):
        callback = (
            "https://sync-auth.lagerverwaltung.org/manual-oauth-callback"
            "?code=abc123&shop=example-shop.myshopify.com&state=expected"
        )

        payload = self.shopify_sync._manual_oauth_callback_from_url(callback, expected_state="expected")

        self.assertEqual(payload["shop"], "example-shop.myshopify.com")
        self.assertEqual(payload["code"], "abc123")

    def test_refresh_access_token_updates_runtime_and_env(self):
        env_path = ROOT / "shopify-sync" / ".env.test-refresh"
        if env_path.exists():
            env_path.unlink()

        class FakeResponse:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "ok": True,
                    "shop": "example-shop.myshopify.com",
                    "token": "shpat_new",
                    "refresh_token": "shprt_new",
                    "token_expires_at": 1712447200,
                    "refresh_token_expires_at": 1717640000,
                    "scope": "read_products",
                }

        original_write_sync_env_values = self.shopify_sync.write_sync_env_values
        target_env_path = env_path
        with mock.patch.object(
            self.shopify_sync,
            "write_sync_env_values",
            side_effect=lambda updates, env_path=None: original_write_sync_env_values(updates, env_path=env_path or target_env_path),
        ):
            with mock.patch.object(self.shopify_sync.requests, "post", return_value=FakeResponse()) as post_mock:
                with mock.patch.object(self.shopify_sync.time, "time", return_value=1712443500):
                    old_values = (
                        self.shopify_sync.SHOP,
                        self.shopify_sync.TOKEN,
                        self.shopify_sync.REFRESH_TOKEN,
                        self.shopify_sync.TOKEN_EXPIRES_AT,
                        self.shopify_sync.REFRESH_TOKEN_EXPIRES_AT,
                        self.shopify_sync.SHOPIFY_CONNECT_BASE_URL,
                    )
                    self.shopify_sync.SHOP = "example-shop.myshopify.com"
                    self.shopify_sync.TOKEN = "shpat_old"
                    self.shopify_sync.REFRESH_TOKEN = "shprt_old"
                    self.shopify_sync.TOKEN_EXPIRES_AT = 1712443600
                    self.shopify_sync.REFRESH_TOKEN_EXPIRES_AT = 1717640000
                    self.shopify_sync.SHOPIFY_CONNECT_BASE_URL = "https://relay.example.test"
                    refreshed = self.shopify_sync._refresh_access_token()

        self.assertTrue(refreshed)
        post_mock.assert_called_once()
        self.assertEqual(self.shopify_sync.TOKEN, "shpat_new")
        self.assertEqual(self.shopify_sync.REFRESH_TOKEN, "shprt_new")
        env_text = env_path.read_text(encoding="utf-8")
        self.assertIn("TOKEN=shpat_new", env_text)
        self.assertIn("REFRESH_TOKEN=shprt_new", env_text)
        env_path.unlink()
        (
            self.shopify_sync.SHOP,
            self.shopify_sync.TOKEN,
            self.shopify_sync.REFRESH_TOKEN,
            self.shopify_sync.TOKEN_EXPIRES_AT,
            self.shopify_sync.REFRESH_TOKEN_EXPIRES_AT,
            self.shopify_sync.SHOPIFY_CONNECT_BASE_URL,
        ) = old_values

    def test_post_refresh_request_follows_redirect_with_post(self):
        class FakeResponse:
            def __init__(self, status_code, location=None, url="https://relay.example.test/shopify/refresh", text=""):
                self.status_code = status_code
                self.url = url
                self.text = text
                self.headers = {}
                if location:
                    self.headers["Location"] = location

        calls = []

        def fake_post(url, json=None, timeout=None, allow_redirects=None):
            calls.append((url, json, timeout, allow_redirects))
            if len(calls) == 1:
                return FakeResponse(301, location="/shopify/refresh/", url=url)
            return FakeResponse(200, url=url)

        with mock.patch.object(self.shopify_sync.requests, "post", side_effect=fake_post):
            response = self.shopify_sync._post_refresh_request(
                "https://relay.example.test",
                {"shop": "example-shop.myshopify.com", "refresh_token": "shprt_old"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][0], "https://relay.example.test/shopify/refresh")
        self.assertEqual(calls[1][0], "https://relay.example.test/shopify/refresh/")
        self.assertFalse(calls[0][3])
        self.assertFalse(calls[1][3])

    def test_get_location_inventory_levels_raises_clear_error_for_missing_location(self):
        old_location_id = self.shopify_sync.SHOPIFY_LOCATION_ID
        self.shopify_sync.SHOPIFY_LOCATION_ID = "67402989753"
        try:
            with mock.patch.object(self.shopify_sync, "graphql_request", return_value={"location": None}):
                with self.assertRaises(RuntimeError) as raised:
                    self.shopify_sync.get_location_inventory_levels()
        finally:
            self.shopify_sync.SHOPIFY_LOCATION_ID = old_location_id

        self.assertIn("Shopify-Location nicht gefunden oder nicht lesbar", str(raised.exception))
        self.assertIn("SHOPIFY_LOCATION_ID", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
