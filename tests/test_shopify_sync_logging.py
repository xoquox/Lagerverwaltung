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
    return module


class ShopifySyncLoggingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.shopify_sync = load_shopify_sync_module()

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


if __name__ == "__main__":
    unittest.main()
