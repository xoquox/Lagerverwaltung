import curses
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tests.test_release_suite import load_lager_mc


class UiColorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = load_lager_mc()

    def test_bright_colors_use_standard_palette_without_reprogramming(self):
        with mock.patch.object(curses, "COLORS", 16, create=True), mock.patch.object(
            curses, "init_color"
        ) as init:
            self.assertEqual(self.app._color_from_name("brightwhite"), 15)
            self.assertEqual(self.app._color_from_name("brightyellow"), 11)
            init.assert_not_called()

    def test_eight_color_terminal_has_readable_pairs_for_every_theme(self):
        for name in self.app.BASE_THEMES:
            with self.subTest(theme=name), mock.patch.dict(
                self.app.SETTINGS, {"color_theme": name}
            ), mock.patch.object(curses, "COLORS", 8, create=True), mock.patch.object(
                curses, "COLOR_PAIRS", 64, create=True
            ), mock.patch.object(
                curses, "can_change_color", return_value=False
            ), mock.patch.object(
                curses, "init_pair"
            ) as init, mock.patch.object(
                curses, "color_pair", return_value=0
            ):
                self.app.apply_color_theme(mock.Mock())
                for call in init.call_args_list:
                    _, fg, bg = call.args
                    self.assertNotEqual(fg, bg)
                    self.assertTrue(0 <= fg < 8 and 0 <= bg < 8)

    def test_mono_theme_keeps_semantic_roles_monochrome(self):
        with mock.patch.dict(
            self.app.SETTINGS, {"color_theme": "mono"}
        ), mock.patch.object(curses, "COLORS", 16, create=True), mock.patch.object(
            curses, "COLOR_PAIRS", 64, create=True
        ), mock.patch.object(
            curses, "init_pair"
        ) as init, mock.patch.object(
            curses, "color_pair", return_value=0
        ):
            self.app.apply_color_theme(mock.Mock())
            for call in init.call_args_list[3:]:
                self.assertEqual(
                    call.args[1:], (curses.COLOR_WHITE, curses.COLOR_BLACK)
                )

    def test_legacy_theme_and_optional_roles_are_loaded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "themes.json"
            path.write_text(
                json.dumps(
                    {
                        "legacy": self.app.BASE_THEMES["green"],
                        "custom": dict(self.app.BASE_THEMES["blue"], heading_fg="cyan"),
                    }
                )
            )
            themes = self.app.load_custom_themes_from_file(path)
        self.assertIn("legacy", themes)
        self.assertEqual(themes["custom"]["heading_fg"], "cyan")

    def test_footer_preserves_labels_and_highlights_full_modified_keys(self):
        win = mock.Mock()
        text = " Shift+F11 Einst.  F1 Sortieren  F10 Ende "
        with mock.patch.object(
            curses, "color_pair", side_effect=lambda pair: pair << 8
        ):
            self.app.draw_footer_line(win, 2, 0, len(text), text)
        self.assertEqual(win.addstr.call_args_list[0].args[2], text)
        self.assertEqual(
            [call.args[2] for call in win.addstr.call_args_list[1:]],
            ["Shift+F11", "F1", "F10"],
        )

    def test_inactive_panel_does_not_use_active_selection_background(self):
        win = mock.Mock()
        win.getmaxyx.return_value = (12, 30)
        with mock.patch.object(
            curses, "color_pair", side_effect=lambda pair: pair << 8
        ):
            self.app.draw_panel(win, "Lagerorte", ["A01"], 0, 0, False)
        self.assertNotIn(mock.call(2 << 8), win.attrset.call_args_list)
