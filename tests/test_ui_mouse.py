import curses
import unittest
from contextlib import ExitStack
from unittest.mock import patch

import ui_mouse
from tests.test_release_suite import load_lager_mc
import tests.test_ui_menu as menu_tests

FakeWindow = menu_tests.FakeWindow


class MouseModelTests(unittest.TestCase):
    def test_click_double_click_wheel_and_drag_decode(self):
        for flag, kind, step in (
            ("BUTTON1_CLICKED", "click", 0),
            ("BUTTON1_DOUBLE_CLICKED", "double", 0),
            ("BUTTON4_PRESSED", "wheel", -3),
            ("BUTTON5_PRESSED", "wheel", 3),
        ):
            if not hasattr(curses, flag):
                continue
            event = ui_mouse.decode_event((0, 5, 6, 0, getattr(curses, flag)))
            self.assertEqual(
                (event.x, event.y, event.kind, event.step), (5, 6, kind, step)
            )
        self.assertIsNone(ui_mouse.decode_event((0, 5, 6, 0, curses.BUTTON1_PRESSED)))

    def test_rows_follow_scroll_and_exclude_headers_borders_and_empty_rows(self):
        args = dict(y=1, x=2, width=40, first_row=3, rows=10, top=7, count=20)
        self.assertEqual(
            ui_mouse.list_index(ui_mouse.MouseEvent(3, 4, "click"), **args), 7
        )
        self.assertEqual(
            ui_mouse.list_index(ui_mouse.MouseEvent(3, 6, "click"), **args), 9
        )
        for x, y in ((2, 4), (42, 4), (3, 3), (3, 14)):
            self.assertIsNone(
                ui_mouse.list_index(ui_mouse.MouseEvent(x, y, "click"), **args)
            )
        self.assertIsNone(
            ui_mouse.list_index(
                ui_mouse.MouseEvent(3, 6, "click"), **dict(args, count=8)
            )
        )
        self.assertIsNone(
            ui_mouse.list_index(ui_mouse.MouseEvent(3, 6, "wheel", 3), **args)
        )

    def test_function_buttons_use_same_function_keys_as_keyboard(self):
        self.assertEqual(ui_mouse.footer_key("Shift+F10"), curses.KEY_F22)
        self.assertEqual(ui_mouse.footer_key("Shift+Tab"), curses.KEY_BTAB)
        self.assertEqual(ui_mouse.footer_key("Esc"), "\x1b")
        self.assertIsNone(ui_mouse.footer_key("Ctrl+F7"))


class MouseDialogTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_lager_mc()

    def test_unavailable_mouse_support_and_disabled_setting_keep_keyboard(self):
        with patch("curses.mousemask", return_value=(0, 0)):
            self.assertFalse(self.app._configure_mouse())
        with patch("curses.mousemask", side_effect=curses.error("unsupported")):
            self.assertFalse(self.app._configure_mouse())
        with patch.dict(self.app.SETTINGS, {"mouse_enabled": False}), patch(
            "curses.mousemask", return_value=(0, 0)
        ) as mask:
            self.assertFalse(self.app._configure_mouse())
            mask.assert_called_once_with(0)

    def test_choice_click_picks_correct_row(self):
        screen = FakeWindow([])
        window = FakeWindow([curses.KEY_MOUSE])
        with patch("curses.color_pair", return_value=1), patch.object(
            self.app, "_new_dialog_window", return_value=window
        ), patch.object(
            ui_mouse, "read_event", return_value=ui_mouse.MouseEvent(16, 10, "click")
        ):
            self.assertEqual(
                self.app.choice_dialog(
                    screen,
                    "Example",
                    [{"value": "first"}, {"value": "second"}],
                    "first",
                ),
                "second",
            )

    def test_choice_wheel_and_keyboard_confirmation(self):
        screen = FakeWindow([])
        window = FakeWindow([curses.KEY_MOUSE, "\n"])
        with patch("curses.color_pair", return_value=1), patch.object(
            self.app, "_new_dialog_window", return_value=window
        ), patch.object(
            ui_mouse, "read_event", return_value=ui_mouse.MouseEvent(16, 10, "wheel", 3)
        ):
            options = [{"value": str(i)} for i in range(8)]
            self.assertEqual(
                self.app.choice_dialog(screen, "Example", options, "0"), "3"
            )

    def test_mouse_preference_survives_settings_save(self):
        with patch.dict(self.app.SETTINGS, {"mouse_enabled": False}):
            initial = self.app._settings_dialog_initial_values(self.app.SETTINGS)
            self.assertFalse(
                self.app._settings_dialog_updated_values(initial)["mouse_enabled"]
            )

    def test_clipped_shift_shortcut_does_not_activate_plain_function_key(self):
        window = FakeWindow([])
        window.getbegyx = lambda: (0, 0)
        with patch("curses.color_pair", return_value=1), patch.object(
            self.app.time, "monotonic", return_value=12
        ):
            self.app.draw_footer_line(window, 0, 0, 16, "Shift+F7 Menge   F2 Speichern")
        self.assertNotIn(
            curses.KEY_F7, [entry[3] for entry in self.app._MOUSE_FOOTER_TARGETS]
        )
        self.assertIn(
            curses.KEY_F2, [entry[3] for entry in self.app._MOUSE_FOOTER_TARGETS]
        )

    def _items(self, count):
        return [
            {
                "sku": str(i),
                "name": "Example",
                "menge": 1,
                "regal": "A",
                "fach": "1",
                "platz": str(i + 1),
                "external_fulfillment": False,
            }
            for i in range(count)
        ]

    def test_main_click_selects_row_before_quantity_action(self):
        with patch.object(
            ui_mouse, "read_event", return_value=ui_mouse.MouseEvent(5, 5, "click")
        ):
            qty, _, _, _ = menu_tests.MenuDialogTests._run_main(
                self,
                [curses.KEY_MOUSE, curses.KEY_F7, curses.KEY_F10],
                items=self._items(2),
            )
        self.assertEqual(qty.call_args.args[1]["sku"], "1")

    def test_main_double_click_opens_info_without_changing_quantity(self):
        with patch.object(
            ui_mouse, "read_event", return_value=ui_mouse.MouseEvent(5, 5, "double")
        ), patch.object(self.app, "item_info_dialog", return_value=None) as info:
            qty, _, _, _ = menu_tests.MenuDialogTests._run_main(
                self, [curses.KEY_MOUSE, curses.KEY_F10], items=self._items(2)
            )
        self.assertEqual(info.call_args.args[1]["sku"], "1")
        qty.assert_not_called()

    def test_main_wheel_moves_selection_under_mouse(self):
        with patch.object(
            ui_mouse, "read_event", return_value=ui_mouse.MouseEvent(5, 5, "wheel", 3)
        ):
            qty, _, _, _ = menu_tests.MenuDialogTests._run_main(
                self,
                [curses.KEY_MOUSE, curses.KEY_F7, curses.KEY_F10],
                items=self._items(5),
            )
        self.assertEqual(qty.call_args.args[1]["sku"], "3")

    def test_main_footer_click_uses_actual_action_dispatch(self):
        with patch.object(
            ui_mouse, "read_event", return_value=ui_mouse.MouseEvent(5, 23, "click")
        ), patch.object(
            self.app, "_MOUSE_FOOTER_TARGETS", [(23, 0, 12, curses.KEY_F7)]
        ):
            qty, _, _, _ = menu_tests.MenuDialogTests._run_main(
                self, [curses.KEY_MOUSE, curses.KEY_F10]
            )
        qty.assert_called_once()

    def test_last_menu_row_can_be_clicked(self):
        screen = FakeWindow([])
        window = FakeWindow([curses.KEY_MOUSE])
        row = self.app.menu_model.MENUS[0][1].index("quit") + 2
        with patch("curses.color_pair", return_value=1), patch(
            "curses.panel.update_panels"
        ), patch.object(
            self.app, "_new_dialog_window", return_value=window
        ), patch.object(
            ui_mouse, "read_event", return_value=ui_mouse.MouseEvent(3, row, "click")
        ):
            self.assertEqual(self.app.menu_bar_dialog(screen, "items"), "quit")

    def _run_orders(self, event):
        from ui_orders import OrdersDialogState

        state = OrdersDialogState()
        screen = FakeWindow([])
        window = FakeWindow([curses.KEY_MOUSE, curses.KEY_F9])
        orders = [{"order_id": str(i), "order_name": f"#{i}"} for i in range(5)]
        with ExitStack() as stack:
            stack.enter_context(patch("curses.curs_set"))
            stack.enter_context(patch("curses.panel.update_panels"))
            stack.enter_context(patch("curses.doupdate"))
            stack.enter_context(patch("curses.color_pair", return_value=1))
            stack.enter_context(
                patch.object(self.app, "OrdersDialogState", return_value=state)
            )
            stack.enter_context(
                patch.object(self.app, "_new_dialog_window", return_value=window)
            )
            stack.enter_context(
                patch.object(ui_mouse, "read_event", return_value=event)
            )
            mocks = {
                "_load_orders_snapshot": orders,
                "_filter_orders_snapshot": orders,
                "_active_shop_location_id": "",
                "build_order_list_lines": ["example"] * 5,
                "build_order_detail_lines": ["detail"] * 30,
            }
            for name, value in mocks.items():
                stack.enter_context(patch.object(self.app, name, return_value=value))
            drawn = stack.enter_context(patch.object(self.app, "draw_panel"))
            for name in (
                "_ORDER_ITEMS_LOADER",
                "_ORDER_SHIPMENTS_LOADER",
                "_ORDERS_SNAPSHOT_LOADER",
            ):
                loader = stack.enter_context(patch.object(self.app, name))
                loader.poll_all.return_value = []
                loader.poll.return_value = None
            self.app.orders_dialog(screen)
        return state, drawn

    def test_order_checkbox_click_marks_correct_order(self):
        state, _ = self._run_orders(ui_mouse.MouseEvent(8, 6, "click"))
        self.assertEqual(state.selected, 1)
        self.assertEqual(state.selected_order_ids, {"1"})

    def test_order_list_wheel_moves_selected_order(self):
        state, _ = self._run_orders(ui_mouse.MouseEvent(9, 5, "wheel", 3))
        self.assertEqual(state.selected, 3)
        self.assertEqual(state.selected_order_ids, set())

    def test_order_details_wheel_scrolls_without_changing_order(self):
        state, drawn = self._run_orders(ui_mouse.MouseEvent(45, 5, "wheel", 3))
        self.assertEqual(state.selected, 0)
        details = [
            call
            for call in drawn.call_args_list
            if call.args[1] == self.app.t("orders_positions_panel")
        ]
        self.assertEqual(details[-1].args[4], 3)
