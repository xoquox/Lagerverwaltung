import tempfile
from contextlib import ExitStack
import unittest
from pathlib import Path
from unittest.mock import patch

import address_label
import delivery_note
from tests.test_release_suite import load_lager_mc, FakeCursor, FakeConnection


class DeliverySortingTests(unittest.TestCase):
    def setUp(self):
        self.rows = [
            {
                "sku": "SKU10",
                "title": "Zahnrad",
                "regal": "A",
                "fach": "10",
                "platz": "1",
                "shop_weight_grams": 100,
            },
            {
                "sku": "SKU2",
                "title": "Achse",
                "regal": "A",
                "fach": "2",
                "platz": "1",
                "shop_weight_grams": 20,
            },
            {"sku": "EXTERNAL", "title": "Extern", "external_fulfillment": True},
            {
                "sku": "SKU3",
                "title": "Bolzen",
                "regal": "B",
                "fach": "1",
                "platz": "1",
                "shop_weight_grams": 50,
            },
        ]

    def test_shop_sequence_and_source_rows_are_preserved(self):
        before = list(self.rows)
        self.assertEqual(
            [r["sku"] for r in delivery_note.build_delivery_note_rows(self.rows)],
            ["SKU10", "SKU2", "SKU3"],
        )
        self.assertEqual(self.rows, before)
        self.assertEqual(
            delivery_note.normalize_delivery_note_sort("order_desc"), "order"
        )

    def test_all_sort_modes_both_directions(self):
        expected = {
            "location": ["SKU2", "SKU10", "SKU3"],
            "name": ["SKU2", "SKU3", "SKU10"],
            "sku": ["SKU2", "SKU3", "SKU10"],
            "weight": ["SKU2", "SKU3", "SKU10"],
        }
        for field, skus in expected.items():
            for direction in ("asc", "desc"):
                with self.subTest(field=field, direction=direction):
                    rows = delivery_note.build_delivery_note_rows(
                        self.rows, f"{field}_{direction}"
                    )
                    self.assertEqual(
                        [r["sku"] for r in rows],
                        skus if direction == "asc" else list(reversed(skus)),
                    )

    def test_unknown_weights_last_and_equal_weights_keep_order(self):
        rows = [
            {"sku": "unknown"},
            {"sku": "first", "shop_weight_grams": 0},
            {"sku": "second", "shop_weight_grams": "0"},
            {"sku": "invalid", "shop_weight_grams": "NaN"},
        ]
        for direction in ("asc", "desc"):
            self.assertEqual(
                [
                    r["sku"]
                    for r in delivery_note.build_delivery_note_rows(
                        rows, f"weight_{direction}"
                    )
                ],
                ["first", "second", "unknown", "invalid"],
            )

    def test_media_dimensions_and_orientation(self):
        self.assertAlmostEqual(
            address_label._page_dimensions_points("PCard100x148")[0] * 25.4 / 72, 100
        )
        self.assertAlmostEqual(
            address_label._page_dimensions_points("62x100")[0] * 25.4 / 72, 62
        )
        self.assertAlmostEqual(
            address_label._page_dimensions_points("62x100")[1] * 25.4 / 72, 100
        )
        with self.assertRaises(ValueError):
            address_label._page_dimensions_points("UnknownDriverMedia")

    def test_delivery_html_receives_selected_page_size_and_sorted_rows(self):
        with tempfile.TemporaryDirectory() as folder, patch.object(
            delivery_note, "WEASYPRINT_AVAILABLE", True
        ), patch.object(delivery_note, "HTML") as html:
            rows = delivery_note.build_delivery_note_rows(self.rows, "sku_asc")
            delivery_note.build_delivery_note_pdf(
                None,
                Path(folder) / "note.pdf",
                {"order_name": "#1"},
                rows,
                page_size="A5",
            )
            content = html.call_args.kwargs["string"]
            self.assertIn(address_label._css_page_size("A5"), content)
            self.assertLess(content.index("SKU2"), content.index("SKU3"))
            self.assertLess(content.index("SKU3"), content.index("SKU10"))
            self.assertNotIn("EXTERNAL", content)

    def test_legacy_small_page_scales_complete_content(self):
        transformed = delivery_note._fit_page_content("original content", "A5")
        self.assertTrue(transformed.startswith("q\n"))
        self.assertIn(" cm\noriginal content\nQ", transformed)
        page = delivery_note._build_page_object(7, "A5")
        self.assertIn(b"419.530 595.280", page)


class PrintSettingsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_lager_mc()

    def test_save_preserves_shipping_scale_and_new_choices(self):
        with patch.dict(
            self.app.SETTINGS,
            {
                "shipping_label_scale_mode": "fit",
                "picklist_duplex": "short",
                "delivery_note_sort": "weight_desc",
            },
        ):
            values = self.app._settings_dialog_initial_values(self.app.SETTINGS)
            saved = self.app._settings_dialog_updated_values(values)
        self.assertEqual(saved["shipping_label_scale_mode"], "fit")
        self.assertEqual(saved["picklist_duplex"], "short")
        self.assertEqual(saved["delivery_note_sort"], "weight_desc")

    def test_unsaved_test_options_and_two_sided_page(self):
        values = {
            "delivery_note_printer": "Example",
            "delivery_note_format": "A5",
            "delivery_note_scale_mode": "fit",
            "delivery_note_duplex": "long",
            "delivery_note_color_mode": "grayscale",
        }
        context = self.app._settings_print_test_context(
            "delivery_note_duplex", values, {}, {}, {}
        )
        captured = {}

        def capture(cmd, **kwargs):
            captured["cmd"] = cmd
            captured["pdf"] = Path(cmd[-1]).read_bytes()

        with patch.object(
            self.app, "_run_lp_command", side_effect=capture
        ), patch.object(self.app, "message_box"):
            self.assertTrue(
                self.app._print_test_page_to_printer(
                    None,
                    context["printer"],
                    context["title"],
                    page_size=context["page_size"],
                    lines=context["lines"],
                    print_options=context["print_options"],
                )
            )
        self.assertIn("print-scaling=fit", captured["cmd"])
        self.assertIn("sides=two-sided-long-edge", captured["cmd"])
        self.assertIn("print-color-mode=monochrome", captured["cmd"])
        self.assertIn(b"/Count 2", captured["pdf"])
        self.assertIn(b"419.53 595.28", captured["pdf"])

    def test_shipping_test_keeps_postcard_and_unsaved_fit(self):
        values = {
            "shipping_label_printer_gls": "Example",
            "shipping_label_format_gls": "PCard100x148",
            "shipping_label_scale_mode_gls": "fit",
        }
        context = self.app._settings_print_test_context(
            "shipping_label_printer_gls",
            values,
            {"shipping_label_printer_gls": "gls"},
            {},
            {},
        )
        captured = {}

        def capture(cmd, **kwargs):
            captured["cmd"] = cmd
            captured["pdf"] = Path(cmd[-1]).read_bytes()

        with patch.object(
            self.app, "_run_lp_command", side_effect=capture
        ), patch.object(self.app, "message_box"):
            self.app._print_test_page_to_printer(
                None,
                context["printer"],
                context["title"],
                page_size=context["page_size"],
                print_options=context["print_options"],
            )
        self.assertIn("media=PCard100x148", captured["cmd"])
        self.assertIn("print-scaling=fit", captured["cmd"])
        self.assertIn("sides=one-sided", captured["cmd"])
        self.assertIn(b"283.46 419.53", captured["pdf"])

    def test_picklist_duplex_reaches_actual_queue_command(self):
        with patch.dict(
            self.app.SETTINGS,
            {"picklist_printer": "Example", "picklist_duplex": "short"},
        ), patch.object(self.app, "_run_lp_command") as run, patch.object(
            self.app, "message_box"
        ):
            self.app.print_picklist(
                None,
                {
                    "order_name": "#1",
                    "shipping_name": "Example",
                    "shipping_address1": "",
                    "shipping_zip": "",
                    "shipping_city": "",
                },
                [],
            )
        self.assertIn("sides=two-sided-short-edge", run.call_args.args[0])

    def test_single_and_batch_delivery_use_same_duplex(self):
        with patch.dict(
            self.app.SETTINGS,
            {"delivery_note_printer": "Example", "delivery_note_duplex": "long"},
        ), patch.object(self.app, "_run_lp_command") as run:
            self.app._print_delivery_note_pdf_path({"order_name": "#1"}, "example.pdf")
            self.app._print_merged_delivery_note_pdf("example.pdf")
        for call in run.call_args_list:
            self.assertIn("sides=two-sided-long-edge", call.args[0])

    def test_delivery_export_uses_saved_sort_and_format(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(
            self.app.SETTINGS,
            {"delivery_note_sort": "sku_desc", "delivery_note_format": "A5"},
        ), patch.object(
            self.app, "get_delivery_note_template_path", return_value=None
        ), patch.object(
            self.app, "get_delivery_note_logo_source", return_value=""
        ), patch.object(
            self.app, "build_delivery_note_pdf"
        ) as build:
            self.app.create_delivery_note_pdf(
                {"order_name": "#1"},
                [{"sku": "SKU2"}, {"sku": "SKU10"}],
                output_dir=folder,
            )
        self.assertEqual([r["sku"] for r in build.call_args.args[3]], ["SKU10", "SKU2"])
        self.assertEqual(build.call_args.kwargs["page_size"], "A5")

    def test_missing_pdf_does_not_silently_drop_a_batch_page(self):
        with tempfile.TemporaryDirectory() as folder:
            good = Path(folder) / "good.pdf"
            good.write_bytes(b"example")
            with self.assertRaises(FileNotFoundError):
                self.app._merge_pdf_files(
                    [good, Path(folder) / "missing.pdf"], Path(folder) / "batch.pdf"
                )

    def test_fulfillment_query_keeps_original_shop_line_order(self):
        cur = FakeCursor(fetchone_results=[{"count": 1}], fetchall_results=[[]])
        with patch.object(
            self.app, "db", return_value=FakeConnection(cur)
        ), patch.object(
            self.app, "_active_shop_location_id", return_value=""
        ), patch.object(
            self.app, "get_local_fulfilled_quantities_for_order", return_value={}
        ):
            self.app.get_order_items("example")
        self.assertIn("ORDER BY MIN(oi.line_index) NULLS LAST", cur.executed[1][0])

    def test_duplex_bulk_keeps_orders_on_separate_sheets(self):
        orders = [
            {"order_id": "1", "order_name": "#1"},
            {"order_id": "2", "order_name": "#2"},
        ]
        mocks = {
            "_execution_carrier_dialog": "free",
            "_select_shipping_carrier_options": [],
            "_bulk_print_mode_dialog": "note",
            "ensure_order_items_loaded": [],
            "calculate_order_shipping_weight": (1.0, 1000),
            "create_shipping_label": {},
            "apply_custom_order_execution": None,
            "message_box": None,
        }
        with ExitStack() as stack:
            stack.enter_context(
                patch.dict(self.app.SETTINGS, {"delivery_note_duplex": "long"})
            )
            for name, result in mocks.items():
                stack.enter_context(patch.object(self.app, name, return_value=result))
            stack.enter_context(
                patch.object(
                    self.app,
                    "create_delivery_note_pdf",
                    side_effect=[("one.pdf", []), ("two.pdf", [])],
                )
            )
            merge = stack.enter_context(patch.object(self.app, "_merge_pdf_files"))
            printed = stack.enter_context(
                patch.object(self.app, "_print_merged_delivery_note_pdf")
            )
            self.app.run_bulk_execution(None, orders, {}, {"1", "2"})
        merge.assert_not_called()
        self.assertEqual(
            [c.args[0] for c in printed.call_args_list], ["one.pdf", "two.pdf"]
        )

    def test_selected_sort_persists_to_settings_file(self):
        import app_settings

        with tempfile.TemporaryDirectory() as folder, patch.object(
            app_settings, "SETTINGS_PATH", Path(folder) / "settings.json"
        ), patch.object(
            app_settings, "LOCAL_SETTINGS_PATH", Path(folder) / "settings.local.json"
        ):
            for mode in delivery_note.DELIVERY_NOTE_SORT_MODES:
                with patch.dict(self.app.SETTINGS, {"delivery_note_sort": mode}):
                    values = self.app._settings_dialog_initial_values(self.app.SETTINGS)
                    updated = self.app._settings_dialog_updated_values(values)
                    app_settings.save_settings(updated)
                self.assertEqual(
                    app_settings.load_settings()["delivery_note_sort"], mode
                )
