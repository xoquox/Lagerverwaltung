import base64
import address_label
import app_logging
import importlib
import importlib.util
import json
import os
import shutil
import sys
import tempfile
import types
import unittest
import datetime
import time
import struct
import zlib
import zipfile
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
    psycopg2_module.Error = type("Psycopg2Error", (Exception,), {})
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
        self.isolation_level = None

    def cursor(self, *args, **kwargs):
        return self._cursor

    def commit(self):
        self.committed = True

    def set_isolation_level(self, level):
        self.isolation_level = level

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


class BundleScriptTests(unittest.TestCase):
    def _install_bundle_scripts(self, root):
        scripts_dir = Path(root) / "scripts"
        scripts_dir.mkdir(parents=True, exist_ok=True)
        for name in ("create_local_bundle.py", "apply_local_bundle.py", "local_bundle.py"):
            source = ROOT / "scripts" / name
            target = scripts_dir / name
            shutil.copy2(source, target)
            target.chmod(0o755)

    def _zip_directory(self, source_dir, zip_path):
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in Path(source_dir).rglob("*"):
                if path.is_file():
                    archive.write(path, path.relative_to(source_dir))

    def test_apply_local_bundle_respects_protected_keys(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            target_root = tmp_path / "target"
            target_root.mkdir()
            self._install_bundle_scripts(target_root)

            (target_root / "settings.local.json").write_text(
                json.dumps(
                    {
                        "db_host": "old-db",
                        "shipping_label_printer_gls": "LOCAL-PRINTER",
                    }
                ),
                encoding="utf-8",
            )

            bundle_stage = tmp_path / "bundle"
            (bundle_stage / "files").mkdir(parents=True)
            (bundle_stage / "bundle_manifest.json").write_text(
                json.dumps(
                    {
                        "bundle_version": 2,
                        "non_overwritten_local_keys": ["shipping_label_printer_gls"],
                        "setting_file_mappings": [],
                    }
                ),
                encoding="utf-8",
            )
            (bundle_stage / "settings.bundle.json").write_text(
                json.dumps(
                    {
                        "db_host": "bundle-db",
                        "shipping_label_printer_gls": "BUNDLE-PRINTER",
                    }
                ),
                encoding="utf-8",
            )

            bundle_zip = tmp_path / "bundle.zip"
            self._zip_directory(bundle_stage, bundle_zip)

            subprocess.run(
                [sys.executable, str(target_root / "scripts" / "apply_local_bundle.py"), str(bundle_zip)],
                check=True,
                cwd=target_root,
            )

            imported = json.loads((target_root / "settings.local.json").read_text(encoding="utf-8"))
            self.assertEqual(imported["db_host"], "bundle-db")
            self.assertEqual(imported["shipping_label_printer_gls"], "LOCAL-PRINTER")

    def test_bundle_roundtrip_imports_free_template_and_active_carriers(self):
        with tempfile.TemporaryDirectory() as source_tmpdir, tempfile.TemporaryDirectory() as target_tmpdir:
            source_root = Path(source_tmpdir)
            target_root = Path(target_tmpdir)
            self._install_bundle_scripts(source_root)
            self._install_bundle_scripts(target_root)

            template_dir = source_root / "custom"
            template_dir.mkdir(parents=True, exist_ok=True)
            free_template = template_dir / "free_label.html"
            free_template.write_text("<html><body>$receiver_html</body></html>", encoding="utf-8")

            (source_root / "settings.local.json").write_text(
                json.dumps(
                    {
                        "db_host": "bundle-db",
                        "shipping_active_carriers": ["free", "post"],
                        "free_label_template_path": str(free_template),
                        "shopify_tracking_mode_post": "company_and_url",
                        "shopify_tracking_url_post": "https://post.example/{tracking_number}",
                        "shipping_label_printer_gls": "SOURCE-PRINTER",
                    }
                ),
                encoding="utf-8",
            )

            create_result = subprocess.run(
                [sys.executable, str(source_root / "scripts" / "create_local_bundle.py")],
                check=True,
                cwd=source_root,
                text=True,
                capture_output=True,
            )
            bundle_zip = Path(create_result.stdout.strip())
            self.assertTrue(bundle_zip.is_file())

            (target_root / "settings.local.json").write_text(
                json.dumps(
                    {
                        "shipping_label_printer_gls": "LOCAL-PRINTER",
                    }
                ),
                encoding="utf-8",
            )

            subprocess.run(
                [sys.executable, str(target_root / "scripts" / "apply_local_bundle.py"), str(bundle_zip)],
                check=True,
                cwd=target_root,
            )

            imported = json.loads((target_root / "settings.local.json").read_text(encoding="utf-8"))
            self.assertEqual(imported["db_host"], "bundle-db")
            self.assertEqual(imported["shipping_active_carriers"], ["free", "post"])
            self.assertEqual(imported["shopify_tracking_mode_post"], "company_and_url")
            self.assertEqual(imported["shopify_tracking_url_post"], "https://post.example/{tracking_number}")
            self.assertEqual(imported["shipping_label_printer_gls"], "LOCAL-PRINTER")
            self.assertTrue((target_root / "templates" / "free_label_template.html").is_file())
            self.assertEqual(
                imported["free_label_template_path"],
                str(target_root / "templates" / "free_label_template.html"),
            )


class ShippingHistorySchemaTests(unittest.TestCase):
    def test_ensure_shipping_history_schema_uses_only_shipping_labels(self):
        from shipping.history import ensure_shipping_history_schema

        cursor = FakeCursor()

        ensure_shipping_history_schema(cursor)

        queries = "\n".join(query for query, _ in cursor.executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS shipping_labels", queries)


class DatabaseSchemaTests(unittest.TestCase):
    def test_apply_app_schema_adds_required_items_and_inventory_sql(self):
        from shipping.schema import apply_app_schema

        cursor = FakeCursor()

        apply_app_schema(cursor)

        queries = "\n".join(query for query, _ in cursor.executed)
        self.assertIn("CREATE TABLE IF NOT EXISTS items", queries)
        self.assertIn("ALTER TABLE items ADD COLUMN IF NOT EXISTS external_fulfillment", queries)
        self.assertIn("CREATE TABLE IF NOT EXISTS inventory_sessions", queries)
        self.assertIn("CREATE TABLE IF NOT EXISTS inventory_lines", queries)

    def test_collect_schema_issues_reports_missing_tables(self):
        from shipping.schema import collect_schema_issues

        cursor = FakeCursor(fetchall_results=[[]])

        issues = collect_schema_issues(cursor)

        self.assertIn("Tabelle fehlt: items", issues)
        self.assertIn("Tabelle fehlt: inventory_sessions", issues)

    def test_collect_schema_issues_accepts_dict_rows(self):
        from shipping.schema import collect_schema_issues

        cursor = FakeCursor(
            fetchall_results=[
                [{"table_name": "items"}],
                [{"column_name": "sku"}, {"column_name": "name"}],
            ]
        )

        issues = collect_schema_issues(cursor)

        self.assertIn("Tabelle fehlt: inventory_sessions", issues)
        self.assertIn("Spalte fehlt: items.regal", issues)

    def test_probe_database_ready_requires_migration_when_schema_is_incomplete(self):
        lager_mc = load_lager_mc()
        cursor = FakeCursor()
        con = FakeConnection(cursor)

        with (
            mock.patch.object(lager_mc, "_is_default_db_settings", return_value=False),
            mock.patch.object(lager_mc, "db", return_value=con),
            mock.patch.object(lager_mc, "collect_schema_issues", return_value=["Spalte fehlt: items.available"]),
            mock.patch.object(lager_mc, "init_db", side_effect=AssertionError("init_db darf nicht laufen")),
        ):
            ready, message = lager_mc._probe_database_ready()

        self.assertFalse(ready)
        self.assertIn("DB Migration noetig", message)

    def test_probe_database_ready_verbose_reports_progress(self):
        lager_mc = load_lager_mc()
        progress_calls = []
        cursor = FakeCursor()
        con = FakeConnection(cursor)

        with (
            mock.patch.object(lager_mc, "_is_default_db_settings", return_value=False),
            mock.patch.object(lager_mc, "db", return_value=con),
            mock.patch.object(lager_mc, "collect_schema_issues", return_value=[]),
        ):
            ready, message, lines = lager_mc._probe_database_ready_verbose(progress_calls.append)

        self.assertTrue(ready)
        self.assertEqual(message, "")
        self.assertIn("DB Verbindung erfolgreich.", lines)
        self.assertIn("Pruefe Schema ...", lines)
        self.assertTrue(progress_calls)

    def test_probe_database_ready_verbose_reports_failure(self):
        lager_mc = load_lager_mc()

        with (
            mock.patch.object(lager_mc, "_is_default_db_settings", return_value=False),
            mock.patch.object(lager_mc, "db", side_effect=RuntimeError("timeout")),
        ):
            ready, message, lines = lager_mc._probe_database_ready_verbose()

        self.assertFalse(ready)
        self.assertIn("timeout", message)
        self.assertIn("DB Verbindung fehlgeschlagen.", lines)

    def test_ensure_database_ready_does_not_block_on_sync_probe(self):
        lager_mc = load_lager_mc()

        with (
            mock.patch.object(lager_mc, "_is_default_db_settings", return_value=False),
            mock.patch.object(lager_mc, "_probe_database_ready", side_effect=AssertionError("sync probe darf nicht laufen")),
            mock.patch.object(lager_mc, "database_connection_dialog", return_value=True) as dialog_mock,
        ):
            ready = lager_mc.ensure_database_ready(object())

        self.assertTrue(ready)
        dialog_mock.assert_called_once()


class MigrationScriptTests(unittest.TestCase):
    def _load_module(self, path, name):
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_run_db_migrations_script_applies_schema_and_commits(self):
        script = self._load_module(ROOT / "scripts" / "run_db_migrations.py", "run_db_migrations_test")
        cursor = FakeCursor()
        connection = FakeConnection(cursor)

        with (
            mock.patch.object(script, "load_settings", return_value={
                "db_host": "db",
                "db_name": "lagerdb",
                "db_user": "lager",
                "db_pass": "secret",
            }),
            mock.patch.object(script, "ensure_database_exists", return_value=False),
            mock.patch.object(script, "connect_from_settings", return_value=connection),
            mock.patch.object(script, "apply_app_schema") as apply_mock,
            mock.patch.object(script, "collect_schema_issues", return_value=[]),
        ):
            result = script.main()

        self.assertEqual(result, 0)
        apply_mock.assert_called_once_with(cursor)
        self.assertTrue(connection.committed)

    def test_ensure_database_exists_creates_missing_database(self):
        script = self._load_module(ROOT / "scripts" / "run_db_migrations.py", "run_db_migrations_create_test")
        settings = {
            "db_host": "db",
            "db_name": "lagerdb",
            "db_user": "lager",
            "db_pass": "secret",
        }

        missing_error = script.psycopg2.Error()
        missing_error.pgcode = "3D000"
        maintenance_cursor = FakeCursor(fetchone_results=[None])
        maintenance_connection = FakeConnection(maintenance_cursor)

        connect_calls = []

        def fake_connect(**kwargs):
            connect_calls.append(kwargs)
            if kwargs["database"] == "lagerdb":
                raise missing_error
            return maintenance_connection

        with mock.patch.object(script.psycopg2, "connect", side_effect=fake_connect):
            created = script.ensure_database_exists(settings)

        self.assertTrue(created)
        self.assertEqual(connect_calls[0]["database"], "lagerdb")
        self.assertIn("CREATE DATABASE", maintenance_cursor.executed[1][0])


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

    def test_filter_items_snapshot_filters_locally_and_sorts(self):
        rows = [
            {
                "sku": "B",
                "name": "Beta",
                "barcode": "222",
                "regal": "A",
                "fach": "2",
                "platz": "3",
                "sync_status": "synced",
                "external_fulfillment": False,
            },
            {
                "sku": "A",
                "name": "Alpha",
                "barcode": "111",
                "regal": "A",
                "fach": "1",
                "platz": "1",
                "sync_status": "local",
                "external_fulfillment": False,
            },
            {
                "sku": "X",
                "name": "Extern",
                "barcode": "999",
                "regal": "Z",
                "fach": "9",
                "platz": "9",
                "sync_status": "local",
                "external_fulfillment": True,
            },
        ]

        filtered = self.lager_mc._filter_items_snapshot(
            rows,
            filter_text="1",
            filter_local=True,
            sort_mode="location",
            external_mode="hide",
        )

        self.assertEqual([row["sku"] for row in filtered], ["A"])

    def test_filter_orders_snapshot_filters_locally(self):
        rows = [
            {
                "order_id": "1",
                "order_name": "#1002",
                "created_at": datetime.datetime(2026, 4, 2, 10, 0, 0),
                "shipping_name": "Max Mustermann",
                "shipping_city": "Berlin",
                "fulfillment_status": "unfulfilled",
                "payment_status": "paid",
            },
            {
                "order_id": "2",
                "order_name": "#1001",
                "created_at": datetime.datetime(2026, 4, 1, 10, 0, 0),
                "shipping_name": "Erika Muster",
                "shipping_city": "Bamberg",
                "fulfillment_status": "fulfilled",
                "payment_status": "paid",
            },
        ]

        filtered = self.lager_mc._filter_orders_snapshot(
            rows,
            order_filter="1002",
            only_pending=False,
            fulfillment_filter="open",
            payment_filter="paid",
        )

        self.assertEqual([row["order_id"] for row in filtered], ["1"])

    def test_prefetch_order_ids_returns_selected_and_neighbors(self):
        rows = [{"order_id": f"OID-{index}"} for index in range(8)]

        result = self.lager_mc._prefetch_order_ids(rows, 3, ahead=4, behind=1)

        self.assertEqual(result, ["OID-2", "OID-3", "OID-4", "OID-5", "OID-6", "OID-7"])

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
        with mock.patch.dict(self.lager_mc.SETTINGS, {"shipping_active_carriers": ["post", "free"]}, clear=False):
            self.assertEqual(self.lager_mc.effective_shipping_carrier("kaputt"), "post")

    def test_shipping_printer_for_carrier_uses_specific_and_fallback(self):
        with mock.patch.dict(
            self.lager_mc.SETTINGS,
            {
                "shipping_label_printer": "FALLBACK",
                "shipping_label_printer_free": "FREE-PRINTER",
            },
            clear=False,
        ):
            self.assertEqual(self.lager_mc._shipping_printer_for_carrier("free"), "FREE-PRINTER")

        with mock.patch.dict(
            self.lager_mc.SETTINGS,
            {
                "shipping_label_printer": "FALLBACK",
                "shipping_label_printer_gls": "",
                "shipping_label_printer_free": "",
            },
            clear=False,
        ):
            self.assertEqual(self.lager_mc._shipping_printer_for_carrier("free"), "FALLBACK")
            self.assertEqual(self.lager_mc._shipping_printer_for_carrier("gls"), "FALLBACK")

    def test_shipping_format_for_carrier_uses_specific_fallback_and_normalizes(self):
        with mock.patch.dict(
            self.lager_mc.SETTINGS,
            {
                "shipping_label_format": "A6",
                "shipping_label_format_free": "A5",
                "shipping_label_format_post": "62x100",
            },
            clear=False,
        ):
            self.assertEqual(self.lager_mc._shipping_format_for_carrier("free"), "A5")
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

    def test_build_simple_test_page_pdf_returns_pdf_bytes(self):
        pdf_bytes = self.lager_mc._build_simple_test_page_pdf(
            "Versandlabel Testseite",
            lines=["Drucker: Xerox", "Format: 100x62"],
            page_size="100x62",
        )

        self.assertTrue(pdf_bytes.startswith(b"%PDF-1.4"))
        self.assertIn(b"Helvetica", pdf_bytes)

    def test_safe_addstr_ignores_tight_window_errors(self):
        class TightWindow:
            def getmaxyx(self):
                return (1, 1)

            def addstr(self, y, x, text, *args):
                raise curses.error("too small")

        self.lager_mc._safe_addstr(TightWindow(), 0, 2, "Titel")

    def test_settings_print_test_context_for_carrier_printer_uses_selected_values(self):
        values = {
            "shipping_label_printer": "Fallback",
            "shipping_label_format": "A6",
            "shipping_label_printer_gls": "GLS-Printer",
            "shipping_label_format_gls": "100x62",
        }

        context = self.lager_mc._settings_print_test_context(
            "shipping_label_printer_gls",
            values,
            self.lager_mc._shipping_printer_field_map(),
            self.lager_mc._shipping_format_field_map(),
        )

        self.assertEqual(context["printer"], "GLS-Printer")
        self.assertEqual(context["page_size"], "100x62")
        self.assertIn("GLS", context["title"])

    def test_print_log_path_uses_print_log_name(self):
        self.assertEqual(app_logging.PRINT_LOG_PATH.name, "print.log")

    def test_enqueue_shopify_fulfillment_job_blocks_test_and_free_carriers(self):
        with self.assertRaisesRegex(RuntimeError, "Test- und Adresslabels duerfen nicht an Shopify uebertragen werden"):
            self.lager_mc.enqueue_shopify_fulfillment_job(
                {
                    "id": 7,
                    "order_id": "gid://shopify/Order/1",
                    "track_id": "TEST123",
                    "carrier": "test",
                }
            )
        with self.assertRaisesRegex(RuntimeError, "Test- und Adresslabels duerfen nicht an Shopify uebertragen werden"):
            self.lager_mc.enqueue_shopify_fulfillment_job(
                {
                    "id": 8,
                    "order_id": "gid://shopify/Order/1",
                    "track_id": "FREE123",
                    "carrier": "free",
                }
            )

    def test_tracking_url_for_carrier_uses_settings_template(self):
        with mock.patch.dict(
            self.lager_mc.SETTINGS,
            {
                "shopify_tracking_url_gls": "https://gls.example/track/{tracking_number}",
                "shopify_tracking_url_post": "https://post.example/{number}",
                "shopify_tracking_mode_gls": "company_and_url",
                "shopify_tracking_mode_post": "company_and_url",
            },
            clear=False,
        ):
            self.assertEqual(
                self.lager_mc._tracking_url_for_carrier("gls", "ABC123"),
                "https://gls.example/track/ABC123",
            )
            self.assertEqual(
                self.lager_mc._tracking_url_for_carrier("post", "XYZ789"),
                "https://post.example/XYZ789",
            )
            self.assertIsNone(
                self.lager_mc._effective_tracking_url_for_shopify("free", "FREE123")
            )

    def test_partial_execution_skips_shopify_queue_for_free_labels(self):
        order = {"order_id": "OID-1", "order_name": "#1001"}
        selected_items = [{"selected_quantity": 1, "order_line_item_id": "line-1"}]
        created = {
            "label_id": 77,
            "label_path": "/tmp/free.pdf",
            "shipment_reference": "ADR-1",
        }

        with (
            mock.patch.object(self.lager_mc, "select_partial_items_dialog", return_value=selected_items),
            mock.patch.object(self.lager_mc, "_execution_carrier_dialog", return_value="free"),
            mock.patch.object(self.lager_mc, "_select_shipping_carrier_options", return_value=[]),
            mock.patch.object(self.lager_mc, "_bulk_print_mode_dialog", return_value="none"),
            mock.patch.object(self.lager_mc, "calculate_selected_shipping_weight", return_value=(0.4, 400)),
            mock.patch.object(self.lager_mc, "create_shipping_label", return_value=created),
            mock.patch.object(self.lager_mc, "create_delivery_note_pdf", return_value=("/tmp/note.pdf", [])),
            mock.patch.object(self.lager_mc, "list_shipping_labels") as list_mock,
            mock.patch.object(self.lager_mc, "enqueue_shopify_fulfillment_job_for_items") as queue_mock,
            mock.patch.object(self.lager_mc, "message_box") as message_mock,
        ):
            self.lager_mc.run_partial_execution_for_order(None, order, [{"sku": "A"}])

        list_mock.assert_not_called()
        queue_mock.assert_not_called()
        message_mock.assert_called_once()
        self.assertIn("OK:", message_mock.call_args.args[2])

    def test_bulk_execution_skips_shopify_queue_for_free_labels(self):
        orders = [{"order_id": "OID-1", "order_name": "#1001"}]
        created = {
            "label_id": 55,
            "label_path": "/tmp/free.pdf",
            "shipment_reference": "ADR-1",
        }

        with (
            mock.patch.object(self.lager_mc, "_execution_carrier_dialog", return_value="free"),
            mock.patch.object(self.lager_mc, "_select_shipping_carrier_options", return_value=[]),
            mock.patch.object(self.lager_mc, "_bulk_print_mode_dialog", return_value="none"),
            mock.patch.object(self.lager_mc, "_bulk_shopify_queue_mode_dialog") as queue_mode_mock,
            mock.patch.object(self.lager_mc, "calculate_order_shipping_weight", return_value=(0.5, 500)),
            mock.patch.object(self.lager_mc, "create_shipping_label", return_value=created),
            mock.patch.object(self.lager_mc, "create_delivery_note_pdf", return_value=("/tmp/note.pdf", [])),
            mock.patch.object(self.lager_mc, "list_shipping_labels") as list_mock,
            mock.patch.object(self.lager_mc, "enqueue_shopify_fulfillment_job") as queue_mock,
            mock.patch.object(self.lager_mc, "message_box") as message_mock,
        ):
            self.lager_mc.run_bulk_execution(None, orders, {"OID-1": []}, {"OID-1"})

        queue_mode_mock.assert_not_called()
        list_mock.assert_not_called()
        queue_mock.assert_not_called()
        message_mock.assert_called_once()
        self.assertIn("OK:", message_mock.call_args.args[2])

    def test_get_shopify_customers_snapshot_queries_local_table_and_caches_rows(self):
        fake_rows = [[{"customer_id": "gid://shopify/Customer/1", "display_name": "Max Mustermann"}]]
        cursor = FakeCursor(fetchall_results=fake_rows)
        con = FakeConnection(cursor)

        with mock.patch.object(self.lager_mc, "db", return_value=con):
            self.lager_mc._SHOPIFY_CUSTOMER_CACHE = {"loaded_at": 0.0, "rows": []}
            rows = self.lager_mc.get_shopify_customers_snapshot(force=True)
            cached_rows = self.lager_mc.get_shopify_customers_snapshot()

        self.assertEqual(len(rows), 1)
        self.assertIn("FROM shopify_customers", cursor.executed[0][0])
        self.assertEqual(cached_rows, rows)
        self.assertEqual(rows[0]["display_name"], "Max Mustermann")
        self.assertEqual(len(cursor.executed), 1)

    def test_search_shopify_customers_filters_snapshot_locally(self):
        self.lager_mc._SHOPIFY_CUSTOMER_CACHE = {
            "loaded_at": time.monotonic(),
            "rows": [
                {
                    "customer_id": "gid://shopify/Customer/1",
                    "display_name": "Max Mustermann",
                    "email": "max@example.com",
                    "default_name": "Max Mustermann",
                    "default_address1": "Musterstr. 1",
                    "default_zip": "12345",
                    "default_city": "Berlin",
                },
                {
                    "customer_id": "gid://shopify/Customer/2",
                    "display_name": "Erika Musterfrau",
                    "email": "erika@example.com",
                    "default_name": "Erika Musterfrau",
                    "default_address1": "Ring 5",
                    "default_zip": "80331",
                    "default_city": "Muenchen",
                },
            ],
        }

        with mock.patch.object(self.lager_mc, "_load_shopify_customers_snapshot") as load_mock:
            rows = self.lager_mc.search_shopify_customers("80331", limit=25)

        load_mock.assert_not_called()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["display_name"], "Erika Musterfrau")

    def test_ensure_order_items_loaded_uses_cache_before_db(self):
        cached = [{"sku": "ABC"}]
        cache = {"OID-1": cached}

        with mock.patch.object(self.lager_mc, "get_order_items") as get_mock:
            rows = self.lager_mc.ensure_order_items_loaded("OID-1", cache)

        get_mock.assert_not_called()
        self.assertIs(rows, cached)

    def test_ensure_order_items_loaded_fetches_and_caches_missing_rows(self):
        cache = {}

        with mock.patch.object(self.lager_mc, "get_order_items", return_value=[{"sku": "ABC"}]) as get_mock:
            rows = self.lager_mc.ensure_order_items_loaded("OID-1", cache)

        get_mock.assert_called_once_with("OID-1")
        self.assertEqual(cache["OID-1"], rows)

    def test_list_shipping_labels_queries_shipping_labels_table(self):
        cursor = FakeCursor(fetchall_results=[[{"id": 1}]])
        connection = FakeConnection(cursor)

        with mock.patch.object(self.lager_mc, "db", return_value=connection):
            rows = self.lager_mc.list_shipping_labels("OID-1")

        self.assertEqual(rows, [{"id": 1}])
        self.assertIn("FROM shipping_labels", cursor.executed[0][0])
        self.assertEqual(cursor.executed[0][1], ("OID-1",))

    def test_get_latest_shopify_jobs_for_labels_queries_once(self):
        cursor = FakeCursor(fetchall_results=[[{"label_id": 4, "status": "done"}]])
        connection = FakeConnection(cursor)

        with mock.patch.object(self.lager_mc, "db", return_value=connection):
            rows = self.lager_mc.get_latest_shopify_jobs_for_labels([4, 9])

        self.assertEqual(rows[4]["status"], "done")
        self.assertIn("FROM shopify_fulfillment_jobs", cursor.executed[0][0])
        self.assertIn("DISTINCT ON (label_id)", cursor.executed[0][0])

    def test_apply_shopify_customer_to_manual_state_fills_address(self):
        state = {
            "name": "",
            "street": "",
            "zip": "",
            "city": "",
            "reference": "",
            "weight_grams": "400",
        }
        customer = {
            "display_name": "Max Mustermann",
            "default_name": "Max Mustermann",
            "default_address1": "Musterstr. 1",
            "default_zip": "12345",
            "default_city": "Berlin",
            "default_country": "Germany",
        }

        updated, country = self.lager_mc._apply_shopify_customer_to_manual_state(state, customer, "DE")

        self.assertEqual(updated["name"], "Max Mustermann")
        self.assertEqual(updated["street"], "Musterstr. 1")
        self.assertEqual(updated["zip"], "12345")
        self.assertEqual(updated["city"], "Berlin")
        self.assertEqual(country, "DE")

    def test_handle_delivery_note_output_routes_by_mode(self):
        order = {"order_name": "#1001", "order_id": "OID-1"}
        items = [{"sku": "ABC"}]

        with mock.patch.object(self.lager_mc, "delivery_note_output_mode_dialog", return_value="print"):
            with mock.patch.object(self.lager_mc, "print_delivery_note") as print_mock:
                self.lager_mc.handle_delivery_note_output(None, order, items)
        print_mock.assert_called_once_with(None, order, items)

        with mock.patch.object(self.lager_mc, "delivery_note_output_mode_dialog", return_value="pdf"):
            with mock.patch.object(self.lager_mc, "export_delivery_note_pdf") as export_mock:
                self.lager_mc.handle_delivery_note_output(None, order, items)
        export_mock.assert_called_once_with(None, order, items)

        with mock.patch.object(self.lager_mc, "delivery_note_output_mode_dialog", return_value="print_pdf"):
            with mock.patch.object(self.lager_mc, "create_delivery_note_pdf", return_value=("/tmp/note.pdf", items)) as create_mock:
                with mock.patch.object(self.lager_mc, "_print_delivery_note_pdf_path") as print_path_mock:
                    with mock.patch.object(self.lager_mc, "message_box") as message_mock:
                        self.lager_mc.handle_delivery_note_output(None, order, items)
        create_mock.assert_called_once_with(order, items)
        print_path_mock.assert_called_once_with(order, "/tmp/note.pdf")
        message_mock.assert_called_once()

    def test_handle_delivery_note_output_loads_missing_items_from_cache(self):
        order = {"order_name": "#1001", "order_id": "OID-1"}
        items = [{"sku": "ABC"}]

        with (
            mock.patch.object(self.lager_mc, "delivery_note_output_mode_dialog", return_value="print"),
            mock.patch.object(self.lager_mc, "ensure_order_items_loaded", return_value=items) as ensure_mock,
            mock.patch.object(self.lager_mc, "print_delivery_note") as print_mock,
        ):
            self.lager_mc.handle_delivery_note_output(None, order, order_items=None, order_items_cache={})

        ensure_mock.assert_called_once_with("OID-1", {})
        print_mock.assert_called_once_with(None, order, items)

    def test_bulk_execution_prints_single_delivery_note_from_temp_path(self):
        orders = [{"order_id": "OID-1", "order_name": "#1001"}]
        created = {"label_id": 55, "label_path": "", "shipment_reference": "ADR-1"}

        created_note_paths = []

        def fake_create_note(order, order_items, output_dir=None):
            path = os.path.join(output_dir or "/tmp", "note.pdf")
            created_note_paths.append(path)
            return path, order_items

        with (
            mock.patch.object(self.lager_mc, "_execution_carrier_dialog", return_value="free"),
            mock.patch.object(self.lager_mc, "_select_shipping_carrier_options", return_value=[]),
            mock.patch.object(self.lager_mc, "_bulk_print_mode_dialog", return_value="note"),
            mock.patch.object(self.lager_mc, "calculate_order_shipping_weight", return_value=(0.5, 500)),
            mock.patch.object(self.lager_mc, "create_shipping_label", return_value=created),
            mock.patch.object(self.lager_mc, "create_delivery_note_pdf", side_effect=fake_create_note) as note_mock,
            mock.patch.object(self.lager_mc, "_print_merged_delivery_note_pdf") as print_note_mock,
            mock.patch.object(self.lager_mc, "message_box") as message_mock,
        ):
            self.lager_mc.run_bulk_execution(None, orders, {"OID-1": [{"sku": "ABC"}]}, {"OID-1"})

        note_mock.assert_called_once()
        self.assertTrue(created_note_paths[0].endswith("note.pdf"))
        print_note_mock.assert_called_once_with(created_note_paths[0], title="Lieferschein #1001")
        message_mock.assert_called_once()

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

        with mock.patch.object(self.lager_mc, "free_create_label", return_value={"ok": True}) as handler:
            result = self.lager_mc.create_shipping_label(order, weight_kg=1.2, carrier="free")

        self.assertEqual(result, {"ok": True})
        handler.assert_called_once_with(
            order,
            weight_kg=1.2,
            shipment_reference=None,
            service_codes=None,
        )

    def test_free_create_label_persists_local_address_label(self):
        order = {
            "order_id": "manual-1",
            "order_name": "ADR-1",
            "shipping_name": "Max Mustermann",
            "shipping_address1": "Musterstr. 1",
            "shipping_zip": "12345",
            "shipping_city": "Berlin",
            "shipping_country": "DE",
        }

        def fake_build(_template, output_path, sender, receiver, page_size):
            self.assertTrue(sender["name"])
            self.assertEqual(receiver["name"], "Max Mustermann")
            self.assertEqual(page_size, "A6")
            Path(output_path).write_bytes(b"%PDF-1.4 free label")

        with (
            mock.patch.object(self.lager_mc, "build_address_label_pdf", side_effect=fake_build),
            mock.patch.object(self.lager_mc, "_save_shipping_label_pdf", return_value="/tmp/free.pdf") as save_mock,
            mock.patch.object(self.lager_mc, "insert_shipping_label_history", return_value=77) as insert_mock,
        ):
            result = self.lager_mc.free_create_label(order, weight_kg=0.4, shipment_reference="ADR-1")

        self.assertEqual(result["label_id"], 77)
        self.assertEqual(result["label_path"], "/tmp/free.pdf")
        self.assertIsNone(result["parcel_number"])
        self.assertTrue(result["track_id"].startswith("FREE"))
        save_mock.assert_called_once()
        insert_mock.assert_called_once()

    def test_address_label_pdf_fallback_works_without_weasyprint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "address_label.pdf"
            with mock.patch.object(address_label, "WEASYPRINT_AVAILABLE", False):
                address_label.build_address_label_pdf(
                    None,
                    output_path,
                    sender={"name": "Absender", "street": "Strasse 1", "zip_city": "12345 Ort"},
                    receiver={"name": "Empfaenger", "street": "Zielweg 2", "zip_city": "54321 Zielort"},
                    page_size="A6",
                )
            self.assertTrue(output_path.is_file())
            self.assertTrue(output_path.read_bytes().startswith(b"%PDF-1.4"))

    def test_address_label_custom_html_template_requires_weasyprint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "address_label.pdf"
            template_path = Path(tmpdir) / "custom.html"
            template_path.write_text("<html><body>$receiver_html</body></html>", encoding="utf-8")
            with mock.patch.object(address_label, "WEASYPRINT_AVAILABLE", False):
                with self.assertRaisesRegex(RuntimeError, "Adresslabel HTML-Vorlage benoetigt WeasyPrint"):
                    address_label.build_address_label_pdf(
                        template_path,
                        output_path,
                        sender={"name": "Absender", "street": "Strasse 1", "zip_city": "12345 Ort"},
                        receiver={"name": "Empfaenger", "street": "Zielweg 2", "zip_city": "54321 Zielort"},
                        page_size="A6",
                    )

    def test_active_shipping_carriers_normalize_to_known_order(self):
        self.assertEqual(
            self.lager_mc._normalize_active_shipping_carriers(["free", "kaputt", "gls", "post", "free"]),
            ["gls", "post", "free"],
        )

    def test_active_shipping_carriers_can_be_empty_for_settings_validation(self):
        self.assertEqual(
            self.lager_mc._normalize_active_shipping_carriers([], fallback_to_defaults=False),
            [],
        )
        self.assertEqual(
            self.lager_mc._shipping_active_carriers_summary([], fallback_to_defaults=False),
            "Keine",
        )

    def test_shipment_number_hides_internal_free_tracking_id(self):
        self.assertEqual(
            self.lager_mc._shipment_number({"carrier": "free", "track_id": "FREE20260330120000"}),
            "-",
        )

    def test_gls_reprint_tries_parcel_number_before_track_id(self):
        label_row = {
            "id": 9,
            "order_name": "#1001",
            "parcel_number": "1234567890",
            "track_id": "Z8ZRLZDW",
        }
        responses = [
            (200, {}, b"%PDF-1.4 test"),
        ]

        with (
            mock.patch.object(self.lager_mc, "load_gls_credentials", return_value={"api_url": "https://example.test/api"}) as creds_mock,
            mock.patch.object(self.lager_mc, "_gls_api_json_request", side_effect=responses) as api_mock,
            mock.patch.object(self.lager_mc, "_save_shipping_label_pdf", return_value="/tmp/reprint.pdf") as save_mock,
            mock.patch.object(self.lager_mc, "update_shipping_label_reprint") as update_mock,
        ):
            result = self.lager_mc.gls_reprint_label(label_row)

        self.assertEqual(result, "/tmp/reprint.pdf")
        creds_mock.assert_called_once_with()
        api_mock.assert_called_once()
        self.assertTrue(api_mock.call_args.args[0].endswith("/reprint/1234567890"))
        save_mock.assert_called_once()
        update_mock.assert_called_once_with(9, "/tmp/reprint.pdf")

    def test_gls_sporadic_collection_url_uses_backend_base(self):
        creds = {"api_url": "https://example.invalid/backend/rs/shipments"}
        self.assertEqual(
            self.lager_mc._gls_sporadic_collection_url(creds),
            "https://example.invalid/backend/rs/sporadiccollection",
        )

    def test_gls_order_sporadic_collection_builds_expected_payload(self):
        with (
            mock.patch.object(
                self.lager_mc,
                "load_gls_credentials",
                return_value={
                    "api_url": "https://example.invalid/backend/rs/shipments",
                    "user": "u",
                    "password": "p",
                    "contact_id": "CID123",
                },
            ),
            mock.patch.object(
                self.lager_mc,
                "_gls_api_json_request",
                return_value=(200, {"EstimatedPickUpDate": "2026-03-31"}, b""),
            ) as api_mock,
        ):
            result = self.lager_mc.gls_order_sporadic_collection(
                preferred_pickup_date="2026-03-30",
                number_of_parcels="2",
                product="PARCEL",
                expected_total_weight="12.5",
                contains_haz_goods=True,
                additional_information="Rampe hinten",
            )

        self.assertEqual(result["estimated_date"], "2026-03-31")
        self.assertEqual(
            api_mock.call_args.args[0],
            "https://example.invalid/backend/rs/sporadiccollection",
        )
        self.assertEqual(
            api_mock.call_args.args[2],
            {
                "ContactID": "CID123",
                "PreferredPickUpDate": "2026-03-30",
                "NumberOfParcels": 2,
                "Product": "PARCEL",
                "ExpectedTotalWeight": 12.5,
                "ContainsHazGoods": True,
                "AdditionalInformation": "Rampe hinten",
            },
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
