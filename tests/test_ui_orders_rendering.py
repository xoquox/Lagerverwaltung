import curses
import unittest
from contextlib import ExitStack
from unittest.mock import patch

from tests.test_release_suite import load_lager_mc
from tests.test_ui_menu import FakeWindow


class OrdersRenderingTests(unittest.TestCase):
    def test_each_order_frame_is_complete_before_terminal_update(self):
        app = load_lager_mc()
        frames = []
        current = set()

        class Window(FakeWindow):
            def __init__(self, keys=(), name="screen", height=24, width=100):
                super().__init__(keys, height, width)
                self.name = name
                self.children = 0

            def derwin(self, height, width, *_):
                self.children += 1
                return Window(name=f"child{self.children}", height=height, width=width)

            def addstr(self, *args):
                super().addstr(*args)
                current.add(self.name)

            def refresh(self):
                frames.append(set(current))

        keys = iter([curses.KEY_DOWN, curses.KEY_DOWN, curses.KEY_F9])

        def new_window(height, width, *_):
            current.clear()
            window = Window(name="orders", height=height, width=width)
            window.get_wch = lambda: next(keys)
            return window

        with ExitStack() as stack:
            stack.enter_context(patch("curses.isendwin", return_value=True))
            stack.enter_context(patch("curses.curs_set"))
            stack.enter_context(patch("curses.color_pair", return_value=1))
            stack.enter_context(patch("curses.panel.update_panels"))
            stack.enter_context(
                patch(
                    "curses.doupdate", side_effect=lambda: frames.append(set(current))
                )
            )
            stack.enter_context(
                patch.object(app, "_new_dialog_window", side_effect=new_window)
            )
            orders = [{"order_id": str(i), "order_name": f"#{i}"} for i in range(5)]
            for name, value in {
                "_load_orders_snapshot": orders,
                "_filter_orders_snapshot": orders,
                "_active_shop_location_id": "",
                "build_order_list_lines": ["example"] * 5,
                "build_order_detail_lines": ["detail"] * 30,
            }.items():
                stack.enter_context(patch.object(app, name, return_value=value))
            for name in (
                "_ORDER_ITEMS_LOADER",
                "_ORDER_SHIPMENTS_LOADER",
                "_ORDERS_SNAPSHOT_LOADER",
            ):
                loader = stack.enter_context(patch.object(app, name))
                loader.poll_all.return_value = []
                loader.poll.return_value = None
            app.orders_dialog(Window())

        self.assertTrue(frames)
        for frame in frames:
            self.assertEqual(frame, {"orders", "child1", "child2"})
        self.assertEqual(len(frames), 3)
