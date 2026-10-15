import curses
from contextlib import ExitStack
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, Mock

import app_settings
import ui_menu
from tests.test_release_suite import load_lager_mc


class MenuModelTests(unittest.TestCase):
    def test_every_default_key_is_unique_and_resolves(self):
        for context, bindings in ui_menu.DEFAULT_BINDINGS.items():
            keys = [key for key in bindings.values() if key]
            self.assertEqual(len(keys), len(set(keys)))
            for action, key in bindings.items():
                if key:
                    number = int(key.split("F")[-1]) + (
                        12 if key.startswith("Shift+") else 0
                    )
                    self.assertEqual(
                        ui_menu.resolve_action(curses.KEY_F1 + number - 1, context),
                        ui_menu.ALIASES.get(action, action),
                    )
            self.assertIsNone(ui_menu.resolve_action(curses.KEY_F10, context))

    def test_rebinding_removes_old_key_and_updates_menu_and_footer(self):
        saved = ui_menu.assign_binding({}, "items", "new_item_alt", "")
        saved = ui_menu.assign_binding(saved, "items", "quantity", "F2")
        self.assertEqual(
            ui_menu.resolve_action(curses.KEY_F2, "items", saved), "quantity"
        )
        self.assertIsNone(ui_menu.resolve_action(curses.KEY_F7, "items", saved))
        self.assertEqual(ui_menu.action_key("quantity", "items", saved), "F2")
        self.assertEqual(
            ui_menu.remap_footer("F2 New F7 Qty F10 Menu", "items", saved),
            "— New F2 Qty F10 Menu",
        )
        self.assertEqual(
            ui_menu.resolve_action(curses.KEY_F2, "orders", saved), "status_filter"
        )

    def test_conflicts_and_reserved_menu_key_are_rejected(self):
        with self.assertRaises(ValueError):
            ui_menu.assign_binding({}, "items", "quantity", "F1")
        with self.assertRaises(ValueError):
            ui_menu.assign_binding({}, "items", "quantity", "F10")
        self.assertEqual(
            ui_menu.effective_bindings("items", {"items": {"quantity": "F1"}}),
            ui_menu.DEFAULT_BINDINGS["items"],
        )

    def test_context_sensitive_actions_and_custom_order_guard(self):
        self.assertFalse(ui_menu.action_available("quantity", "items"))
        self.assertTrue(
            ui_menu.action_available("quantity", "items", {"sku": "EXAMPLE"})
        )
        self.assertFalse(
            ui_menu.action_available("quantity", "orders", {"order_id": "1"})
        )
        self.assertFalse(ui_menu.action_available("ship_label", "orders"))
        self.assertFalse(
            ui_menu.action_available("edit_order", "orders", {"order_id": "wc:order:1"})
        )
        self.assertTrue(
            ui_menu.action_available("edit_order", "orders", {"order_id": "custom-1"})
        )
        self.assertTrue(ui_menu.action_available("quit", "orders"))

    def test_configurable_exit_and_single_multi_location_defaults(self):
        saved = ui_menu.assign_binding({}, "items", "quit", "Shift+F10")
        self.assertEqual(ui_menu.resolve_action(curses.KEY_F22, "items", saved), "quit")
        self.assertEqual(ui_menu.resolve_action(curses.KEY_F2, "items"), "new_item")
        self.assertEqual(
            ui_menu.resolve_action(curses.KEY_F2, "items_multi"), "previous_location"
        )
        self.assertEqual(ui_menu.resolve_action(curses.KEY_F3, "items"), "inventory")
        self.assertEqual(
            ui_menu.resolve_action(curses.KEY_F3, "items_multi"), "next_location"
        )

    def test_keyboard_configuration_roundtrip(self):
        saved = ui_menu.assign_binding({}, "orders", "picklist", "")
        saved = ui_menu.assign_binding(saved, "orders", "pdf_export", "Shift+F10")
        with tempfile.TemporaryDirectory() as folder, patch.object(
            app_settings, "SETTINGS_PATH", Path(folder) / "settings.json"
        ), patch.object(
            app_settings, "LOCAL_SETTINGS_PATH", Path(folder) / "settings.local.json"
        ):
            app_settings.save_settings({"keyboard_bindings": saved})
            loaded = app_settings.load_settings()["keyboard_bindings"]
        self.assertEqual(loaded, saved)
        self.assertEqual(
            ui_menu.resolve_action(curses.KEY_F22, "orders", loaded), "pdf_export"
        )


class FakeWindow:
    def __init__(self, keys, height=24, width=100):
        self.keys = iter(keys)
        self.size = (height, width)
        self.drawn = []

    def getmaxyx(self):
        return self.size

    def addstr(self, *args):
        self.drawn.append(args)

    def get_wch(self):
        return next(self.keys)

    def __getattr__(self, name):
        return lambda *args, **kwargs: None


class MenuDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_lager_mc()

    def run_menu(self, keys, context="items", selected=None, group=0):
        screen = FakeWindow([])
        window = FakeWindow(keys)
        with patch("curses.color_pair", side_effect=lambda value: value), patch(
            "curses.panel.update_panels"
        ), patch.object(self.app, "_new_dialog_window", return_value=window):
            result = self.app.menu_bar_dialog(
                screen, context, selected, initial_menu=group
            )
        return result, window

    def test_file_exit_keyboard_navigation(self):
        result, _ = self.run_menu([curses.KEY_UP, "\n"])
        self.assertEqual(result, "quit")

    def test_item_menu_selects_quantity_and_disabled_entries_do_not_execute(self):
        result, _ = self.run_menu(
            [curses.KEY_DOWN, curses.KEY_DOWN, "\n"],
            selected={"sku": "EXAMPLE"},
            group=1,
        )
        self.assertEqual(result, "quantity")
        result, _ = self.run_menu(
            ["\n", curses.KEY_RIGHT, "\n"], context="orders", group=1
        )
        self.assertEqual(result, "show_items")

    def test_escape_cancels_and_menu_has_current_shortcut(self):
        saved = ui_menu.assign_binding({}, "items", "new_item_alt", "")
        saved = ui_menu.assign_binding(saved, "items", "quantity", "F2")
        with patch.dict(self.app.SETTINGS, {"keyboard_bindings": saved}):
            result, window = self.run_menu(
                ["\x1b"], selected={"sku": "EXAMPLE"}, group=1
            )
        self.assertIsNone(result)
        quantity = [
            args for args in window.drawn if len(args) > 2 and "Menge" in str(args[2])
        ]
        self.assertTrue(any("F2" in args[2] for args in quantity))

    def test_key_editor_cancel_discards_and_conflicts_keep_draft(self):
        screen = FakeWindow([])
        window = FakeWindow([curses.KEY_ENTER, curses.KEY_F2])
        with patch("curses.color_pair", side_effect=lambda value: value), patch.object(
            self.app, "_new_dialog_window", return_value=window
        ), patch.object(self.app, "choice_dialog", return_value="F1"), patch.object(
            self.app, "message_box"
        ) as message:
            draft = self.app.keybindings_dialog(screen, {})
        self.assertEqual(draft, {})
        message.assert_called_once()
        window = FakeWindow([27])
        with patch("curses.color_pair", side_effect=lambda value: value), patch.object(
            self.app, "_new_dialog_window", return_value=window
        ):
            self.assertIsNone(self.app.keybindings_dialog(screen, {}))

    def test_settings_save_preserves_keybindings(self):
        saved = ui_menu.assign_binding({}, "items", "quit", "Shift+F10")
        with patch.dict(self.app.SETTINGS, {"keyboard_bindings": saved}):
            initial = self.app._settings_dialog_initial_values(self.app.SETTINGS)
            self.assertEqual(
                self.app._settings_dialog_updated_values(initial)["keyboard_bindings"],
                saved,
            )
        fields = {
            name
            for tab in self.app._settings_dialog_tabs()
            for name, _ in tab["fields"]
        }
        self.assertIn("keyboard_bindings_display", fields)

    def test_all_menu_labels_exist_in_both_languages(self):
        from languages import TRANSLATIONS

        for language in ("de", "en"):
            translations = TRANSLATIONS[language]
            for group, actions in ui_menu.MENUS:
                self.assertIn("menu_" + group, translations)
                for action in actions:
                    if action:
                        self.assertIn("menu_action_" + action, translations)
            for context in ui_menu.CONTEXTS:
                for action in ui_menu.DEFAULT_BINDINGS[context]:
                    self.assertIn("menu_action_" + action, translations)

    def _run_main(self, keys, bindings=None, pending=0, items=None):
        screen = FakeWindow(keys)
        item = {
            "sku": "EXAMPLE",
            "name": "Example",
            "menge": 1,
            "regal": "A",
            "fach": "1",
            "platz": "1",
            "external_fulfillment": False,
        }
        dataset = items if items is not None else [item]
        with ExitStack() as stack:
            for name in ("curs_set", "start_color", "use_default_colors", "mousemask"):
                stack.enter_context(patch("curses." + name, return_value=(0, 0) if name == "mousemask" else None))
            stack.enter_context(
                patch.dict(self.app.SETTINGS, {"keyboard_bindings": bindings or {}})
            )
            functions = {
                "apply_color_theme": None,
                "ensure_database_ready": True,
                "get_service_runtime_state": {},
                "get_shop_locations_snapshot": [],
                "_load_items_snapshot": dataset,
                "_filter_items_snapshot": dataset,
                "build_location_rows": [],
                "poll_background_ui_events": [],
                "_shop_location_switch_enabled": False,
                "draw": None,
                "_pending_item_write_count": pending,
                "_pending_item_write_skus": [],
                "pending_item_write_exit_dialog": "wait",
                "_update_item_snapshot_quantity": None,
            }
            mocks = {
                name: stack.enter_context(
                    patch.object(self.app, name, return_value=result)
                )
                for name, result in functions.items()
            }
            for name in ("_SERVICE_RUNTIME_LOADER", "_ITEMS_SNAPSHOT_LOADER"):
                loader = stack.enter_context(patch.object(self.app, name))
                loader.poll.return_value = None
            menu = stack.enter_context(
                patch.object(self.app, "menu_bar_dialog", return_value="quit")
            )
            qty = stack.enter_context(
                patch.object(self.app, "change_qty", return_value=2)
            )
            wait = stack.enter_context(
                patch.object(self.app, "wait_for_pending_item_writes_dialog")
            )
            self.app.main(screen)
        return qty, menu, wait, mocks

    def test_reassigned_key_executes_actual_main_quantity_action(self):
        bindings = ui_menu.assign_binding({}, "items", "new_item_alt", "")
        bindings = ui_menu.assign_binding(bindings, "items", "quantity", "F2")
        qty, menu, _, _ = self._run_main(
            [curses.KEY_F7, curses.KEY_F2, curses.KEY_F10], bindings
        )
        qty.assert_called_once()
        menu.assert_called_once()

    def test_exit_from_menu_preserves_pending_write_guard(self):
        _, menu, wait, mocks = self._run_main([curses.KEY_F10], pending=1)
        menu.assert_called_once()
        mocks["pending_item_write_exit_dialog"].assert_called_once()
        wait.assert_called_once()
