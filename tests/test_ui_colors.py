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
                    set(call.args[1:]), {curses.COLOR_WHITE, curses.COLOR_BLACK}
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

    def test_every_builtin_theme_defines_all_color_roles(self):
        for theme in self.app.BASE_THEMES.values():
            for role in self.app.UI_COLOR_ROLES:
                self.assertIn(role + "_fg", theme)
                self.assertIn(role + "_bg", theme)

    def test_preview_changes_all_pairs_without_changing_saved_selection(self):
        with mock.patch.dict(
            self.app.SETTINGS, {"color_theme": "blue"}
        ), mock.patch.object(curses, "COLORS", 16, create=True), mock.patch.object(
            curses, "COLOR_PAIRS", 64, create=True
        ), mock.patch.object(
            curses, "init_pair"
        ) as init, mock.patch.object(
            curses, "color_pair", return_value=0
        ):
            self.app.apply_color_theme(mock.Mock(), theme_name="green")
            pairs = {call.args[0]: call.args[1:] for call in init.call_args_list}
            self.assertEqual(pairs[1], (10, 0))
            self.assertEqual(pairs[12], (0, 2))
            self.assertEqual(self.app.SETTINGS["color_theme"], "blue")

    def test_leaving_settings_restores_saved_theme_after_preview(self):
        screen = mock.Mock()
        with mock.patch.object(
            self.app, "_settings_dialog", return_value=None
        ), mock.patch.object(self.app, "apply_color_theme") as apply:
            self.app.settings_dialog(screen)
        apply.assert_called_once_with(screen)

    def test_color_selection_applies_preview_immediately(self):
        values = {"color_theme": "blue"}
        screen = mock.Mock()
        with mock.patch.object(
            self.app, "choice_dialog", return_value="norton"
        ), mock.patch.object(self.app, "apply_color_theme") as apply:
            self.app._settings_context_select(
                screen, "color_theme", values, {}, {}, {}, {}, {}
            )
        self.assertEqual(values["color_theme"], "norton")
        apply.assert_called_once_with(screen, theme_name="norton")

    def test_fixed_256_palette_uses_custom_palette_without_init_color(self):
        with mock.patch.object(curses, "COLORS", 256, create=True), mock.patch.object(
            curses, "can_change_color", return_value=False
        ), mock.patch.object(curses, "init_color") as init:
            self.assertEqual(self.app._color_from_name("monokai-background"), 235)
            self.assertGreaterEqual(self.app._color_from_name("monokai-pink"), 16)
        init.assert_not_called()

    def test_saved_theme_survives_settings_exit_and_list_movement(self):
        for name in self.app.BASE_THEMES:
            with self.subTest(theme=name):
                screen = mock.Mock()
                screen.getmaxyx.return_value = (30, 120)
                window = mock.Mock()
                window.getmaxyx.return_value = (20, 60)
                window.get_wch.side_effect = [curses.KEY_DOWN] * 9 + [
                    curses.KEY_F6,
                    curses.KEY_F2,
                ]
                screen.derwin.return_value = window
                loader = mock.Mock()
                loader.poll.return_value = None
                with mock.patch.object(
                    self.app, "SETTINGS", self.app.DEFAULT_SETTINGS.copy()
                ), mock.patch.object(
                    self.app, "_new_dialog_window", return_value=window
                ), mock.patch.object(
                    self.app, "get_service_runtime_state", return_value=None
                ), mock.patch.object(
                    self.app, "get_shop_locations_snapshot", return_value=[]
                ), mock.patch.object(
                    self.app, "_SERVICE_RUNTIME_LOADER", loader
                ), mock.patch.object(
                    self.app, "choice_dialog", return_value=name
                ), mock.patch.object(
                    self.app, "_settings_validate_updated_values", return_value=True
                ), mock.patch.object(
                    self.app,
                    "_save_settings_checked",
                    side_effect=lambda updated: dict(updated),
                ), mock.patch.object(
                    self.app, "_set_active_shop_location"
                ), mock.patch.object(
                    self.app, "message_box"
                ), mock.patch.object(
                    self.app,
                    "run_background_action_dialog",
                    side_effect=lambda screen, title, action, **kw: action(),
                ), mock.patch.object(
                    self.app, "apply_color_theme"
                ) as apply, mock.patch.object(
                    curses, "color_pair", return_value=0
                ):
                    self.app.settings_dialog(screen)
                    self.assertEqual(self.app.SETTINGS["color_theme"], name)
                    apply.reset_mock()
                    for selected in [0, 1]:
                        self.app.draw(
                            screen,
                            [],
                            selected,
                            0,
                            [],
                            0,
                            0,
                            "left",
                            None,
                            False,
                            "hide",
                        )
                    self.assertEqual(self.app.get_active_theme_name(), name)
                    apply.assert_not_called()

    def test_modal_restores_parent_after_success_and_exception(self):
        for fails in [False, True]:
            with self.subTest(fails=fails):
                screen = mock.Mock()
                panel = mock.Mock()

                @self.app.modal_dialog
                def dialog(screen):
                    self.app._new_dialog_window(6, 30, 2, 2)
                    if fails:
                        raise ValueError("test failure")
                    return "saved"

                with mock.patch.object(
                    curses, "isendwin", return_value=False
                ), mock.patch.object(curses, "newwin"), mock.patch.object(
                    curses.panel, "new_panel", return_value=panel
                ), mock.patch.object(
                    curses.panel, "update_panels"
                ) as update, mock.patch.object(
                    curses, "doupdate"
                ) as flush:
                    if fails:
                        with self.assertRaises(ValueError):
                            dialog(screen)
                    else:
                        self.assertEqual(dialog(screen), "saved")
                panel.hide.assert_called_once()
                screen.touchwin.assert_called_once()
                screen.noutrefresh.assert_called_once()
                update.assert_called_once()
                flush.assert_called_once()
                self.assertEqual(self.app._DIALOG_FRAMES, [])

    def test_nested_popups_leave_parent_registered(self):
        screen = mock.Mock()
        parent_panel, child_panel = mock.Mock(), mock.Mock()

        @self.app.modal_dialog
        def child(screen):
            self.app._new_dialog_window(6, 30, 2, 2)

        @self.app.modal_dialog
        def parent(screen):
            self.app._new_dialog_window(20, 70, 1, 1)
            child(screen)
            child_panel.hide.assert_called_once()
            parent_panel.hide.assert_not_called()
            self.assertIs(self.app._DIALOG_FRAMES[-1]["panel"], parent_panel)

        with mock.patch.object(
            curses, "isendwin", return_value=False
        ), mock.patch.object(curses, "newwin"), mock.patch.object(
            curses.panel, "new_panel", side_effect=[parent_panel, child_panel]
        ), mock.patch.object(
            curses.panel, "update_panels"
        ), mock.patch.object(
            curses, "doupdate"
        ):
            parent(screen)
        parent_panel.hide.assert_called_once()
        self.assertEqual(self.app._DIALOG_FRAMES, [])

    def test_monokai_uses_fixed_palette_even_if_terminal_claims_color_changes(self):
        with mock.patch.object(curses, "COLORS", 256, create=True), mock.patch.object(
            curses, "can_change_color", return_value=True
        ), mock.patch.object(curses, "init_color") as init:
            self.assertEqual(self.app._color_from_name("monokai-background"), 235)
            self.assertNotIn(self.app._color_from_name("monokai-text"), [16, 17])
            init.assert_not_called()
