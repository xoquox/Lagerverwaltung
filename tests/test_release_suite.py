import importlib
import json
import os
import sys
import tempfile
import types
import unittest
import datetime
import time
import struct
import zlib
from pathlib import Path
from unittest import mock
import subprocess
from urllib.parse import parse_qs, urlparse


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
            self.assertEqual(loaded["location_regex_regal"], app_settings.DEFAULT_SETTINGS["location_regex_regal"])
            self.assertEqual(loaded["location_regex_fach"], app_settings.DEFAULT_SETTINGS["location_regex_fach"])
            self.assertEqual(loaded["location_regex_platz"], app_settings.DEFAULT_SETTINGS["location_regex_platz"])
            self.assertEqual(loaded["language"], app_settings.DEFAULT_SETTINGS["language"])
            self.assertEqual(loaded["color_theme"], app_settings.DEFAULT_SETTINGS["color_theme"])
            self.assertEqual(loaded["color_theme_file"], app_settings.DEFAULT_SETTINGS["color_theme_file"])
            self.assertEqual(loaded["label_font_regular"], app_settings.DEFAULT_SETTINGS["label_font_regular"])
            self.assertEqual(loaded["label_font_condensed"], app_settings.DEFAULT_SETTINGS["label_font_condensed"])

            self.assertFalse(local_settings_path.exists())


class InternetmarkeClientTests(unittest.TestCase):
    def _client(self):
        from post.internetmarke_client import InternetmarkeClient

        return InternetmarkeClient(
            api_url="https://api-eu.dhl.com/post/de/shipping/im/v1",
            partner_id="A00629B8F2",
            api_key="key123",
            api_secret="secret456",
            user="post@example.com",
            password="secretpw",
        )

    def test_authorize_uses_form_payload_and_stores_token(self):
        client = self._client()

        class FakeResponse:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps({"access_token": "tok123", "expires_in": 3000}).encode("utf-8")

        with mock.patch("post.internetmarke_client.urlopen", return_value=FakeResponse()) as urlopen_mock:
            result = client.authorize(force=True)

        self.assertEqual(result["access_token"], "tok123")
        request = urlopen_mock.call_args.args[0]
        body = request.data.decode("utf-8")
        parsed = parse_qs(body)
        self.assertEqual(parsed["grant_type"], ["client_credentials"])
        self.assertEqual(parsed["client_id"], ["key123"])
        self.assertEqual(parsed["client_secret"], ["secret456"])
        self.assertEqual(parsed["username"], ["post@example.com"])
        self.assertEqual(parsed["password"], ["secretpw"])
        self.assertEqual(request.headers["Content-type"], "application/x-www-form-urlencoded; charset=UTF-8")

    def test_get_catalog_repeats_types_query_parameter(self):
        client = self._client()
        client._access_token = "tok123"
        client._token_expires_at = time.time() + 3600

        class FakeResponse:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'{"pageFormats":[]}'

        with mock.patch("post.internetmarke_client.urlopen", return_value=FakeResponse()) as urlopen_mock:
            client.get_catalog(types=("PUBLIC", "PAGE_FORMATS"))

        request = urlopen_mock.call_args.args[0]
        parsed = parse_qs(urlparse(request.full_url).query)
        self.assertEqual(parsed["types"], ["PUBLIC", "PAGE_FORMATS"])
        self.assertEqual(request.headers["Authorization"], "Bearer tok123")
        self.assertEqual(request.headers["X-partner-id"], "A00629B8F2")

    def test_preview_pdf_builds_validate_request(self):
        client = self._client()
        client._access_token = "tok123"
        client._token_expires_at = time.time() + 3600

        class FakeResponse:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'{"link":"https://example.invalid/preview.pdf"}'

        with mock.patch("post.internetmarke_client.urlopen", return_value=FakeResponse()) as urlopen_mock:
            result = client.preview_pdf(product_code=101, page_format_id=7, voucher_layout="FRANKING_ZONE", dpi="DPI203")

        self.assertEqual(result["link"], "https://example.invalid/preview.pdf")
        request = urlopen_mock.call_args.args[0]
        self.assertIn("validate=true", request.full_url)
        payload = json.loads(request.data.decode("utf-8"))
        self.assertEqual(payload["type"], "AppShoppingCartPreviewPDFRequest")
        self.assertEqual(payload["productCode"], 101)
        self.assertEqual(payload["pageFormatId"], 7)
        self.assertEqual(payload["voucherLayout"], "FRANKING_ZONE")
        self.assertEqual(payload["dpi"], "DPI203")


class PostProductImportTests(unittest.TestCase):
    def test_import_post_ppl_creates_structured_json_mapping(self):
        from scripts import import_post_ppl

        sample_csv = """PROD_GUEAB;T&T;PROD_ID;PROD_AUSR;PROD_NAME;PROD_BRPREIS;BP_NAME;BP_BRPREIS;ADD_NAME;ADD_BRPREIS;MINL;MINB;MINH;MAXL;MAXB;MAXH;MING;MAXG;MIND;MAXD;PROD_ANM;INTMA_HINWTEXT;INTMA_PROD_URL;INTMA_VERTRAG;INTMA_ZOLLERKL
01.01.2025;;31;N;Maxibrief;2,90;Maxibrief;2,90;;;100;70;0;353;250;50;0;1000;;;;;Beschreibung;https://example.invalid/maxi;nein;nein
01.01.2025;;41;N;Maxibrief bis 2000 g + Zusatzentgelt MBf;5,10;Maxibrief bis 2000 g;2,90;Zusatzentgelt MBf;2,20;100;70;0;600;300;150;0;2000;;;;;MBf;https://example.invalid/maxi2;nein;nein
01.07.2025;;290;N;Warensendung;2,70;Warensendung;2,70;;;100;70;0;353;250;50;0;1000;;;;;Waren;https://example.invalid/ware;nein;nein
01.07.2025;;331;N;Warensendung 2.000 + Gewichtszuschlag;3,55;Warensendung 1.000 zzgl. Gewichtszuschlag;2,70;Warensendung 2.000 Gewichtszuschlag;0,85;100;70;0;353;250;50;1001;2000;;;;;Waren2;https://example.invalid/ware2;nein;nein
01.01.2025;1;1037;N;Maxibrief Integral + EINSCHREIBEN;5,55;Maxibrief BZL GK;2,90;EINSCHREIBEN;2,65;100;70;0;353;250;50;0;1000;;;;;Tracked;https://example.invalid/reg;nein;nein
"""

        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "sample.csv"
            target = Path(tmpdir) / "post_products.json"
            source.write_text(sample_csv, encoding="latin-1")

            payload = import_post_ppl.import_csv(source, target)

            self.assertEqual(payload["meta"]["product_count"], 5)
            self.assertTrue(target.exists())
            written = json.loads(target.read_text(encoding="utf-8"))
            by_code = {item["product_code"]: item for item in written["products"]}
            self.assertEqual(by_code["31"]["base_product"], "maxibrief")
            self.assertEqual(by_code["31"]["price_cents"], 290)
            self.assertEqual(by_code["31"]["base_label"], "Maxibrief")
            self.assertEqual(by_code["41"]["addons"], ["mbf"])
            self.assertEqual(by_code["41"]["base_label"], "Maxibrief")
            self.assertEqual(by_code["331"]["addons"], ["gewichtszuschlag"])
            self.assertEqual(by_code["331"]["base_label"], "Warensendung")
            self.assertTrue(by_code["1037"]["tracked"])
            self.assertEqual(by_code["1037"]["addons"], ["einschreiben"])
            self.assertEqual(by_code["1037"]["category"], "registered")
            self.assertEqual(by_code["1037"]["addon_labels"], ["EINSCHREIBEN"])
            self.assertTrue(written["selection"]["base_products"])
            maxibrief_group = next(item for item in written["selection"]["base_products"] if item["base_key"] == "maxibrief")
            self.assertIn("einschreiben", maxibrief_group["option_codes"])
            self.assertIn("mbf", maxibrief_group["option_codes"])
            warensendung_group = next(item for item in written["selection"]["base_products"] if item["base_key"] == "warensendung")
            self.assertIn("gewichtszuschlag", warensendung_group["option_codes"])

    def test_product_catalog_loader_and_lookup(self):
        from post.product_catalog import find_post_product, list_post_products, list_post_base_products, list_post_options

        with tempfile.TemporaryDirectory() as tmpdir:
            source = Path(tmpdir) / "post_products.json"
            source.write_text(
                json.dumps(
                    {
                        "meta": {"product_count": 2},
                        "selection": {
                            "base_products": [{"base_key": "maxibrief", "scope": "domestic"}],
                            "options": [{"option_code": "einschreiben", "label": "EINSCHREIBEN"}],
                        },
                        "products": [
                            {"product_code": "31", "category": "letter", "scope": "domestic", "tracked": False},
                            {"product_code": "1032", "category": "registered", "scope": "domestic", "tracked": True},
                        ],
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(find_post_product("1032", path=source)["product_code"], "1032")
            self.assertEqual(len(list_post_products(path=source, domestic_only=True)), 2)
            self.assertEqual(len(list_post_products(path=source, tracked_only=True)), 1)
            self.assertEqual(list_post_products(path=source, category="registered")[0]["product_code"], "1032")
            self.assertEqual(list_post_base_products(path=source, scope="domestic")[0]["base_key"], "maxibrief")
            self.assertEqual(list_post_options(path=source)[0]["option_code"], "einschreiben")


class LagerMcLogicTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lager_mc = load_lager_mc()

    def test_normalize_regal_accepts_single_letter_only(self):
        self.assertEqual(self.lager_mc.normalize_regal("A"), "A")
        self.assertIsNone(self.lager_mc.normalize_regal(" a "))
        self.assertEqual(self.lager_mc.normalize_regal(""), "")
        self.assertIsNone(self.lager_mc.normalize_regal("AA"))
        self.assertIsNone(self.lager_mc.normalize_regal("1"))

    def test_resolve_post_product_selection_uses_base_and_options(self):
        product = self.lager_mc._resolve_post_product_selection(
            {"scope": "domestic", "base_key": "maxibrief", "option_codes": ["einschreiben_einwurf"]}
        )
        self.assertEqual(product["product_code"], "1032")

        base_product = self.lager_mc._resolve_post_product_selection(
            {"scope": "domestic", "base_key": "warensendung", "option_codes": []}
        )
        self.assertEqual(base_product["product_code"], "290")

    def test_normalize_fach_and_platz_default_regex(self):
        self.assertEqual(self.lager_mc.normalize_fach("1"), "1")
        self.assertEqual(self.lager_mc.normalize_fach("99"), "99")
        self.assertIsNone(self.lager_mc.normalize_fach("0"))
        self.assertIsNone(self.lager_mc.normalize_fach("100"))
        self.assertEqual(self.lager_mc.normalize_platz("7"), "7")
        self.assertIsNone(self.lager_mc.normalize_platz("07"))

    def test_normalize_location_uses_configured_regex(self):
        with mock.patch.dict(self.lager_mc.SETTINGS, {"location_regex_regal": "^[A-Z0-9]{2}$"}, clear=False):
            self.assertEqual(self.lager_mc.normalize_regal("A1"), "A1")
            self.assertIsNone(self.lager_mc.normalize_regal("A"))

    def test_is_location_input_allowed_blocks_invalid_chars(self):
        self.assertTrue(self.lager_mc.is_location_input_allowed("regal", "A"))
        self.assertFalse(self.lager_mc.is_location_input_allowed("regal", "a"))
        self.assertFalse(self.lager_mc.is_location_input_allowed("regal", "%"))
        self.assertTrue(self.lager_mc.is_location_input_allowed("fach", "99"))
        self.assertFalse(self.lager_mc.is_location_input_allowed("fach", "100"))

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
            with mock.patch.object(delivery_note, "WEASYPRINT_AVAILABLE", False):
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
        import delivery_note

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
            with mock.patch.object(delivery_note, "WEASYPRINT_AVAILABLE", False):
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
        import delivery_note

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
            with mock.patch.object(delivery_note, "WEASYPRINT_AVAILABLE", False):
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
            with mock.patch.object(delivery_note, "WEASYPRINT_AVAILABLE", False):
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

    def test_create_delivery_note_pdf_uses_html_renderer_when_available(self):
        import delivery_note

        order = {
            "order_name": "#2005",
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
            html_template = Path(tmpdir) / "template.html"
            html_template.write_text("<html><body>$order_name<table>$items_html</table></body></html>", encoding="utf-8")

            fake_writer = mock.Mock()
            fake_html_instance = mock.Mock(write_pdf=fake_writer)
            fake_html_class = mock.Mock(return_value=fake_html_instance)

            with mock.patch.object(delivery_note, "WEASYPRINT_AVAILABLE", True):
                with mock.patch.object(delivery_note, "HTML", fake_html_class):
                    with mock.patch.dict(
                        self.lager_mc.SETTINGS,
                        {"delivery_note_template_path": str(html_template), "delivery_note_logo_source": ""},
                        clear=False,
                    ):
                        path, _ = self.lager_mc.create_delivery_note_pdf(order, order_items, output_dir=tmpdir)

            fake_html_class.assert_called_once()
            fake_writer.assert_called_once_with(path)

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

    def test_should_refresh_orders_when_never_loaded(self):
        self.assertTrue(self.lager_mc.should_refresh_orders(None, now=100.0, interval_seconds=10.0))

    def test_should_refresh_orders_respects_interval(self):
        self.assertFalse(self.lager_mc.should_refresh_orders(100.0, now=109.9, interval_seconds=10.0))
        self.assertTrue(self.lager_mc.should_refresh_orders(100.0, now=110.0, interval_seconds=10.0))

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

    def test_parse_cups_media_options_extracts_values_and_labels(self):
        output = (
            "PageSize/Media Size: *A4/A4 A6/A6 PCard100x148/Postcard\n"
            "PageRegion/PageRegion: *A4/A4\n"
            "media/Media: Custom.100x62mm/100x62mm\n"
        )

        values = self.lager_mc._parse_cups_media_options(output)

        self.assertEqual(
            values,
            [
                {"value": "A4", "label": "A4"},
                {"value": "A6", "label": "A6"},
                {"value": "PCard100x148", "label": "Postcard"},
                {"value": "Custom.100x62mm", "label": "100x62mm"},
            ],
        )

    def test_get_cups_printer_media_options_uses_c_locale_and_parses_lpoptions(self):
        completed = subprocess.CompletedProcess(
            ["lpoptions", "-p", "Xerox", "-l"],
            0,
            stdout="PageSize/Media Size: *A4/A4 A6/A6\n",
            stderr="",
        )

        with mock.patch.object(self.lager_mc.subprocess, "run", return_value=completed) as run_mock:
            values, error = self.lager_mc.get_cups_printer_media_options("Xerox")

        self.assertIsNone(error)
        self.assertEqual(values, [{"value": "A4", "label": "A4"}, {"value": "A6", "label": "A6"}])
        self.assertEqual(run_mock.call_args.args[0], ["lpoptions", "-p", "Xerox", "-l"])
        self.assertEqual(run_mock.call_args.kwargs["env"]["LC_ALL"], "C")
        self.assertEqual(run_mock.call_args.kwargs["env"]["LANG"], "C")

    def test_effective_shipping_carrier_falls_back_to_gls_for_unknown_values(self):
        self.lager_mc._SHIPPING_CARRIER_CACHE = None
        self.assertEqual(self.lager_mc.effective_shipping_carrier("kaputt"), "gls")

    def test_shipping_printer_for_carrier_uses_specific_private_and_fallback(self):
        with mock.patch.dict(
            self.lager_mc.SETTINGS,
            {
                "shipping_label_printer": "FALLBACK",
                "shipping_label_printer_dhl": "DHL-LEGACY",
                "shipping_label_printer_dhl_private": "DHL-PRIVATE",
            },
            clear=False,
        ):
            self.assertEqual(self.lager_mc._shipping_printer_for_carrier("dhl_private"), "DHL-PRIVATE")

        with mock.patch.dict(
            self.lager_mc.SETTINGS,
            {
                "shipping_label_printer": "FALLBACK",
                "shipping_label_printer_gls": "",
                "shipping_label_printer_dhl": "DHL-LEGACY",
                "shipping_label_printer_dhl_private": "",
            },
            clear=False,
        ):
            self.assertEqual(self.lager_mc._shipping_printer_for_carrier("dhl_private"), "DHL-LEGACY")
            self.assertEqual(self.lager_mc._shipping_printer_for_carrier("gls"), "FALLBACK")

    def test_shipping_format_for_carrier_uses_private_fallback_and_normalizes(self):
        with mock.patch.dict(
            self.lager_mc.SETTINGS,
            {
                "shipping_label_format": "A6",
                "shipping_label_format_dhl": "A5",
                "shipping_label_format_dhl_private": "",
                "shipping_label_format_post": "62x100",
            },
            clear=False,
        ):
            self.assertEqual(self.lager_mc._shipping_format_for_carrier("dhl_private"), "A5")
            self.assertEqual(self.lager_mc._shipping_format_for_carrier("post"), "100x62")

    def test_cups_label_print_options_include_media_pagesize_and_scaling_flags(self):
        self.assertEqual(
            self.lager_mc._cups_label_print_options("100x62"),
            [
                "-o",
                "media=Custom.100x62mm",
                "-o",
                "PageSize=Custom.100x62mm",
                "-o",
                "print-scaling=none",
                "-o",
                "fit-to-page=false",
                "-o",
                "scaling=100",
                "-o",
                "page-border=none",
                "-o",
                "number-up=1",
            ],
        )

    def test_enqueue_shopify_fulfillment_job_blocks_test_carrier(self):
        with self.assertRaisesRegex(RuntimeError, "Test-Labels duerfen nicht an Shopify uebertragen werden"):
            self.lager_mc.enqueue_shopify_fulfillment_job(
                {
                    "id": 7,
                    "order_id": "gid://shopify/Order/1",
                    "track_id": "TEST123",
                    "carrier": "test",
                }
            )

    def test_create_shipping_label_routes_to_requested_carrier(self):
        order = {
            "order_id": "gid://shopify/Order/1",
            "order_name": "#1001",
            "shipping_name": "Max Mustermann",
            "shipping_address1": "Musterstr. 1",
            "shipping_zip": "12345",
            "shipping_city": "Berlin",
            "shipping_country": "DE",
        }

        with mock.patch.object(self.lager_mc, "dhl_private_create_label", return_value={"ok": True}) as handler:
            result = self.lager_mc.create_shipping_label(order, weight_kg=1.2, carrier="dhl_private")

        self.assertEqual(result, {"ok": True})
        handler.assert_called_once_with(
            order,
            weight_kg=1.2,
            shipment_reference=None,
            service_codes=None,
        )

    def test_format_shopify_sync_status_label_prefers_pull_and_push_times(self):
        row = {
            "status": "ok",
            "last_seen_at": datetime.datetime(2026, 3, 25, 20, 5, tzinfo=datetime.timezone.utc),
            "last_pull_at": datetime.datetime(2026, 3, 25, 20, 4, tzinfo=datetime.timezone.utc),
            "last_push_at": datetime.datetime(2026, 3, 25, 20, 3, tzinfo=datetime.timezone.utc),
        }

        label = self.lager_mc.format_shopify_sync_status_label(
            row=row,
            now=datetime.datetime(2026, 3, 25, 20, 5, tzinfo=datetime.timezone.utc),
        )

        self.assertIn("Sync:", label)
        self.assertIn("In", label)
        self.assertIn("Out", label)

    def test_format_shopify_sync_status_label_marks_stale_and_error(self):
        row = {
            "status": "error",
            "last_seen_at": datetime.datetime(2026, 3, 25, 20, 0, tzinfo=datetime.timezone.utc),
            "last_pull_at": None,
            "last_push_at": None,
        }

        label = self.lager_mc.format_shopify_sync_status_label(
            row=row,
            now=datetime.datetime(2026, 3, 25, 20, 5, tzinfo=datetime.timezone.utc),
        )

        self.assertTrue(label.startswith("Sync!"))
        self.assertTrue(label.endswith("ERR"))


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
