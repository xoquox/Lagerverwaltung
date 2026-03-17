import importlib
import json
import os
import sys
import tempfile
import types
import unittest
import datetime
import struct
import zlib
from pathlib import Path
from unittest import mock
import subprocess


ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def install_psycopg2_stub():
    extras_module = types.ModuleType("psycopg2.extras")
    extras_module.RealDictCursor = object

    psycopg2_module = types.ModuleType("psycopg2")
    psycopg2_module.extras = extras_module
    bootstrap_cursor = FakeCursor()
    bootstrap_connection = FakeConnection(bootstrap_cursor)
    psycopg2_module.connect = lambda *args, **kwargs: bootstrap_connection

    sys.modules["psycopg2"] = psycopg2_module
    sys.modules["psycopg2.extras"] = extras_module


def load_lager_mc():
    install_psycopg2_stub()
    sys.modules.pop("lager_mc", None)
    with mock.patch("curses.wrapper", lambda func: None):
        return importlib.import_module("lager_mc")


class FakeCursor:
    def __init__(self, fetchone_results=None, fetchall_results=None):
        self.fetchone_results = list(fetchone_results or [])
        self.fetchall_results = list(fetchall_results or [])
        self.executed = []
        self.closed = False

    def execute(self, query, params=None):
        self.executed.append((" ".join(query.split()), params))

    def fetchone(self):
        if not self.fetchone_results:
            return None
        return self.fetchone_results.pop(0)

    def fetchall(self):
        if not self.fetchall_results:
            return []
        return self.fetchall_results.pop(0)

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
        self.closed = False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True


class AppSettingsTests(unittest.TestCase):
    def test_load_settings_creates_defaults_and_normalizes_existing_file(self):
        import app_settings

        with tempfile.TemporaryDirectory() as tmpdir:
            settings_path = Path(tmpdir) / "settings.json"
            local_settings_path = Path(tmpdir) / "settings.local.json"
            settings_path.write_text(json.dumps({"db_host": "localhost"}), encoding="utf-8")

            with mock.patch.object(app_settings, "SETTINGS_PATH", settings_path):
                with mock.patch.object(app_settings, "LOCAL_SETTINGS_PATH", local_settings_path):
                    loaded = app_settings.load_settings()

            self.assertEqual(loaded["db_host"], "localhost")
            self.assertEqual(loaded["db_name"], app_settings.DEFAULT_SETTINGS["db_name"])
            self.assertEqual(loaded["delivery_note_printer"], app_settings.DEFAULT_SETTINGS["delivery_note_printer"])
            self.assertEqual(loaded["pdf_output_dir"], app_settings.DEFAULT_SETTINGS["pdf_output_dir"])
            self.assertEqual(loaded["delivery_note_logo_source"], app_settings.DEFAULT_SETTINGS["delivery_note_logo_source"])

            self.assertFalse(local_settings_path.exists())


class LagerMcLogicTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lager_mc = load_lager_mc()

    def test_normalize_regal_accepts_single_letter_only(self):
        self.assertEqual(self.lager_mc.normalize_regal(" a "), "A")
        self.assertEqual(self.lager_mc.normalize_regal(""), "")
        self.assertIsNone(self.lager_mc.normalize_regal("AA"))
        self.assertIsNone(self.lager_mc.normalize_regal("1"))

    def test_build_location_rows_groups_and_sorts_locations(self):
        items = [
            {"sku": "SKU-3", "name": "Gamma", "regal": "", "fach": "", "platz": "", "sync_status": "synced", "dirty": False, "menge": 1, "unavailable": 0, "committed": 0, "available": 1},
            {"sku": "SKU-2", "name": "Beta", "regal": "A", "fach": "10", "platz": "2", "sync_status": "synced", "dirty": False, "menge": 1, "unavailable": 0, "committed": 0, "available": 1},
            {"sku": "SKU-1", "name": "Alpha", "regal": "A", "fach": "2", "platz": "1", "sync_status": "synced", "dirty": False, "menge": 1, "unavailable": 0, "committed": 0, "available": 1},
        ]

        rows = self.lager_mc.build_location_rows(items)
        labels = [row["label"] for row in rows]

        self.assertEqual(labels[0], "Regal A")
        self.assertEqual(labels[1], "  Fach 2")
        self.assertEqual(labels[3], "  Fach 10")
        self.assertEqual(labels[-3], "Ohne Regal")
        self.assertEqual(labels[-2], "  Ohne Fach")
        self.assertEqual(rows[2]["item"]["sku"], "SKU-1")
        self.assertEqual(rows[4]["item"]["sku"], "SKU-2")

    def test_sort_order_items_for_picklist_excludes_external_and_sorts_by_location(self):
        rows = [
            {"sku": "B", "title": "Beta", "quantity": 1, "regal": "A", "fach": "2", "platz": "5", "external_fulfillment": False},
            {"sku": "C", "title": "Extern", "quantity": 1, "regal": "A", "fach": "1", "platz": "1", "external_fulfillment": True},
            {"sku": "A", "title": "Alpha", "quantity": 1, "regal": "A", "fach": "2", "platz": "1", "external_fulfillment": False},
        ]

        sorted_rows = self.lager_mc.sort_order_items_for_picklist(rows)

        self.assertEqual([row["sku"] for row in sorted_rows], ["A", "B"])

    def test_get_items_uses_filter_text_for_barcode_search(self):
        cursor = FakeCursor(fetchall_results=[[{"sku": "A"}]])
        connection = FakeConnection(cursor)

        with mock.patch.object(self.lager_mc, "db", return_value=connection):
            rows = self.lager_mc.get_items(filter_text="4012345678901")

        self.assertEqual(rows, [{"sku": "A"}])
        query, params = cursor.executed[0]
        self.assertIn("COALESCE(barcode, '') ILIKE %s", query)
        self.assertEqual(params[2], "%4012345678901%")

    def test_build_picklist_text_contains_shipping_address_and_position_count(self):
        order = {
            "order_name": "#1001",
            "shipping_name": "Max Mustermann",
            "shipping_address1": "Musterstr. 1",
            "shipping_zip": "12345",
            "shipping_city": "Berlin",
        }
        order_items = [
            {"sku": "A", "title": "Alpha", "quantity": 2, "regal": "A", "fach": "1", "platz": "3", "external_fulfillment": False},
            {"sku": "B", "title": "Extern", "quantity": 1, "regal": "Z", "fach": "9", "platz": "9", "external_fulfillment": True},
        ]

        document = self.lager_mc.build_picklist_text(order, order_items)

        self.assertIn("Pickliste #1001", document)
        self.assertIn("Max Mustermann, Musterstr. 1, 12345 Berlin", document)
        self.assertIn("Positionen: 1", document)
        self.assertNotIn("Extern", document)

    def test_build_delivery_note_rows_excludes_external_items(self):
        rows = [
            {"sku": "A", "title": "Alpha", "quantity": 1, "external_fulfillment": False},
            {"sku": "B", "title": "Extern", "quantity": 1, "external_fulfillment": True},
        ]

        filtered = self.lager_mc.build_delivery_note_rows(rows)

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["sku"], "A")

    def test_create_delivery_note_pdf_uses_template_and_omits_gls_line(self):
        import delivery_note

        order = {
            "order_name": "#2001",
            "created_at": datetime.datetime(2026, 3, 16, 12, 0, 0),
            "shipping_name": "Jörg Märtens",
            "shipping_address1": "Musterweg 4",
            "shipping_zip": "12345",
            "shipping_city": "Berlin",
            "shipping_country": "Deutschland",
        }
        order_items = [
            {"sku": "A-1", "title": "Alpha Teil", "quantity": 2, "external_fulfillment": False},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(self.lager_mc.SETTINGS, {"delivery_note_logo_source": ""}, clear=False):
                path, rows = self.lager_mc.create_delivery_note_pdf(order, order_items, output_dir=tmpdir)

            self.assertEqual(len(rows), 1)
            self.assertTrue(path.endswith(".pdf"))
            content = Path(path).read_bytes()
            self.assertTrue(content.startswith(b"%PDF-1.4"))
            self.assertNotIn(b"GLS", content)
            objects = delivery_note._parse_pdf_objects(content)
            stream_start = objects[7].index(b"stream\n") + len(b"stream\n")
            stream_end = objects[7].rindex(b"\nendstream")
            stream = zlib.decompress(objects[7][stream_start:stream_end]).decode("cp1252")
            self.assertIn("Bestellung: #2001", stream)
            self.assertIn("Jörg Märtens", stream)
            self.assertIn("Vielen Dank für Ihre Bestellung!", stream)

    def test_create_delivery_note_pdf_uses_configured_output_dir(self):
        order = {
            "order_name": "#2003",
            "created_at": datetime.datetime(2026, 3, 16, 12, 0, 0),
            "shipping_name": "Max Mustermann",
            "shipping_address1": "Musterweg 4",
            "shipping_zip": "12345",
            "shipping_city": "Berlin",
            "shipping_country": "Deutschland",
        }
        order_items = [
            {"sku": "A-1", "title": "Alpha Teil", "quantity": 2, "external_fulfillment": False},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(self.lager_mc.SETTINGS, {"pdf_output_dir": tmpdir}, clear=False):
                with mock.patch.dict(self.lager_mc.SETTINGS, {"delivery_note_logo_source": ""}, clear=False):
                    path, _ = self.lager_mc.create_delivery_note_pdf(order, order_items)

            self.assertTrue(path.startswith(tmpdir))

    def test_build_item_info_lines_includes_shopify_fields(self):
        item = {
            "sku": "A-1",
            "name": "Alpha",
            "barcode": "4012345678901",
            "shopify_product_status": "active",
            "shopify_price": "12.95",
            "shopify_compare_at_price": "14.95",
            "shopify_unit_cost": "6.20",
            "shopify_unit_cost_currency": "EUR",
            "shopify_weight_grams": 380,
            "sync_status": "ok",
            "regal": "A",
            "fach": "1",
            "platz": "3",
            "shopify_description": "Beschreibung",
        }

        lines = self.lager_mc.build_item_info_lines(item)

        self.assertIn("Barcode/GTIN: 4012345678901", lines)
        self.assertIn("EK Kosten: 6.20 EUR", lines)
        self.assertIn("Gewicht: 380 g", lines)

    def test_clean_shopify_description_strips_html(self):
        html_text = "<p>Text&nbsp;A</p><p>Text<br>B</p><ul><li>Punkt</li></ul>"

        cleaned = self.lager_mc.clean_shopify_description(html_text)

        self.assertEqual(cleaned, "Text A\nText\nB\nPunkt")

    def test_create_delivery_note_pdf_splits_multiple_pages(self):
        order = {
            "order_name": "#2002",
            "created_at": datetime.datetime(2026, 3, 16, 12, 0, 0),
            "shipping_name": "Max Mustermann",
            "shipping_address1": "Musterweg 4",
            "shipping_zip": "12345",
            "shipping_city": "Berlin",
            "shipping_country": "Deutschland",
        }
        order_items = [
            {"sku": f"A-{index}", "title": f"Alpha Teil {index}", "quantity": 1, "external_fulfillment": False}
            for index in range(1, 13)
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(self.lager_mc.SETTINGS, {"delivery_note_logo_source": ""}, clear=False):
                path, rows = self.lager_mc.create_delivery_note_pdf(order, order_items, output_dir=tmpdir)

            self.assertEqual(len(rows), 12)
            content = Path(path).read_bytes()
            self.assertIn(b"/Count 2", content)
            self.assertGreaterEqual(content.count(b"/Type /Page"), 2)

    def test_create_delivery_note_pdf_includes_configured_logo(self):
        import delivery_note

        order = {
            "order_name": "#2004",
            "created_at": datetime.datetime(2026, 3, 16, 12, 0, 0),
            "shipping_name": "Max Mustermann",
            "shipping_address1": "Musterweg 4",
            "shipping_zip": "12345",
            "shipping_city": "Berlin",
            "shipping_country": "Deutschland",
        }
        order_items = [
            {"sku": "A-1", "title": "Alpha Teil", "quantity": 1, "external_fulfillment": False},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch.dict(
                self.lager_mc.SETTINGS,
                {"delivery_note_logo_source": "https://example.invalid/logo.png"},
                clear=False,
            ):
                with mock.patch.object(delivery_note, "_load_binary_source", return_value=self._tiny_rgb_png_bytes()):
                    path, _ = self.lager_mc.create_delivery_note_pdf(order, order_items, output_dir=tmpdir)

            content = Path(path).read_bytes()
            self.assertIn(b"/Subtype /Image", content)
            self.assertIn(b"/L1", content)
            objects = delivery_note._parse_pdf_objects(content)
            stream_start = objects[7].index(b"stream\n") + len(b"stream\n")
            stream_end = objects[7].rindex(b"\nendstream")
            stream = zlib.decompress(objects[7][stream_start:stream_end]).decode("cp1252")
            self.assertIn("/L1 Do", stream)

    @staticmethod
    def _tiny_rgb_png_bytes():
        signature = b"\x89PNG\r\n\x1a\n"
        ihdr_data = struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0)
        raw_scanline = b"\x00\x1a\x7f\xc8"
        idat_data = zlib.compress(raw_scanline)

        def chunk(chunk_type, data):
            crc = zlib.crc32(chunk_type + data) & 0xFFFFFFFF
            return struct.pack(">I", len(data)) + chunk_type + data + struct.pack(">I", crc)

        return signature + chunk(b"IHDR", ihdr_data) + chunk(b"IDAT", idat_data) + chunk(b"IEND", b"")

    def test_inventory_export_text_reports_summary(self):
        session = {"session_name": "Inventur Test", "session_id": 7}
        lines = [
            {"line_no": 1, "sku": "A", "name": "Alpha", "regal": "A", "fach": "1", "platz": "1", "soll_menge": 5, "ist_menge": 4},
            {"line_no": 2, "sku": "B", "name": "Beta", "regal": None, "fach": None, "platz": None, "soll_menge": 3, "ist_menge": None},
        ]

        text = self.lager_mc.build_inventory_export_text(session, lines)

        self.assertIn("Inventur Test", text)
        self.assertIn("Regal A", text)
        self.assertIn("Regal Ohne Regal", text)
        self.assertIn("Positionen: 2  Gezaehlt: 1  Abweichungen: 1", text)

    def test_jump_to_order_matches_exact_before_partial(self):
        orders = [
            {"order_name": "#10012"},
            {"order_name": "#1001"},
        ]

        self.assertEqual(self.lager_mc.jump_to_order(orders, "1001"), 1)
        self.assertEqual(self.lager_mc.jump_to_order(orders, "12"), 0)
        self.assertIsNone(self.lager_mc.jump_to_order(orders, "9999"))

    def test_parse_lpstat_printers_extracts_names_and_details(self):
        output = (
            "printer OFFICE_A4 is idle. enabled since Sun 16 Mar 2026 09:00:00 CET\n"
            "printer OFFICE_BACKUP accepting requests since Sun 16 Mar 2026 09:01:00 CET\n"
        )

        printers = self.lager_mc._parse_lpstat_printers(output)

        self.assertEqual(printers[0]["name"], "OFFICE_A4")
        self.assertIn("is idle.", printers[0]["detail"])
        self.assertEqual(printers[1]["name"], "OFFICE_BACKUP")

    def test_summarize_subprocess_error_prefers_stderr_lines(self):
        exc = subprocess.CalledProcessError(
            1,
            ["python3", "label_print.py"],
            stderr="Traceback\nModuleNotFoundError: No module named 'barcode'\n",
            output="ignored\n",
        )

        self.assertEqual(
            self.lager_mc.summarize_subprocess_error(exc),
            "Traceback",
        )


class LagerMcWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lager_mc = load_lager_mc()

    def test_create_inventory_session_archives_previous_session_and_inserts_snapshot_lines(self):
        cursor = FakeCursor(
            fetchone_results=[
                {"session_id": 42, "session_name": "Inventur 2026-03-15 10:00", "created_at": "now", "status": "active"},
            ],
            fetchall_results=[
                [
                    {"sku": "A-1", "name": "Alpha", "regal": "A", "fach": "1", "platz": "1", "menge": 4},
                    {"sku": "B-2", "name": "Beta", "regal": "B", "fach": "2", "platz": "3", "menge": 7},
                ]
            ],
        )
        connection = FakeConnection(cursor)

        class FixedDateTime:
            @staticmethod
            def now():
                class FixedNow:
                    def strftime(self, fmt):
                        return "2026-03-15 10:00"
                return FixedNow()

        with mock.patch.object(self.lager_mc, "db", return_value=connection):
            with mock.patch.object(self.lager_mc.datetime, "datetime", FixedDateTime):
                session = self.lager_mc.create_inventory_session()

        self.assertEqual(session["session_id"], 42)
        self.assertTrue(connection.committed)
        self.assertTrue(connection.closed)
        self.assertEqual(sum(1 for query, _ in cursor.executed if "INSERT INTO inventory_lines" in query), 2)
        self.assertIn("UPDATE inventory_sessions SET status = 'archived' WHERE status = 'active'", cursor.executed[0][0])

    def test_apply_inventory_session_updates_items_and_marks_session_applied(self):
        cursor = FakeCursor()
        connection = FakeConnection(cursor)

        with mock.patch.object(self.lager_mc, "db", return_value=connection):
            self.lager_mc.apply_inventory_session(42)

        self.assertTrue(connection.committed)
        self.assertTrue(connection.closed)
        self.assertEqual(len(cursor.executed), 2)
        self.assertIn("UPDATE items i SET menge = l.ist_menge", cursor.executed[0][0])
        self.assertEqual(cursor.executed[0][1], (42,))
        self.assertIn("UPDATE inventory_sessions SET status = 'applied' WHERE session_id = %s", cursor.executed[1][0])

    def test_export_inventory_csv_writes_semicolon_file(self):
        session = {"session_id": 9, "session_name": "Inventur Test"}
        lines = [
            {"line_no": 1, "sku": "A", "name": "Alpha", "regal": "A", "fach": "1", "platz": "2", "soll_menge": 4, "ist_menge": 3},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch("os.getcwd", return_value=tmpdir):
                path = self.lager_mc.export_inventory_csv(session, lines)

            self.assertTrue(path.startswith(tmpdir))
            content = Path(path).read_text(encoding="utf-8")
            self.assertIn("session_id;session_name;line_no;sku;name;regal;fach;platz;soll_menge;ist_menge", content)
            self.assertIn("9;Inventur Test;1;A;Alpha;A;1;2;4;3", content)


if __name__ == "__main__":
    unittest.main(verbosity=2)
