"""Terminal mouse events and hit testing, independent of database actions."""

import curses
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class MouseEvent:
    x: int
    y: int
    kind: str
    step: int = 0

    @property
    def clicked(self):
        return self.kind in {"click", "double"}


def mouse_mask():
    return sum(
        getattr(curses, name, 0)
        for name in (
            "BUTTON1_CLICKED",
            "BUTTON1_DOUBLE_CLICKED",
            "BUTTON4_PRESSED",
            "BUTTON5_PRESSED",
        )
    )


def decode_event(raw):
    _, x, y, _, buttons = raw
    if buttons & getattr(curses, "BUTTON4_PRESSED", 0):
        return MouseEvent(x, y, "wheel", -3)
    if buttons & getattr(curses, "BUTTON5_PRESSED", 0):
        return MouseEvent(x, y, "wheel", 3)
    if buttons & getattr(curses, "BUTTON1_DOUBLE_CLICKED", 0):
        return MouseEvent(x, y, "double")
    if buttons & (
        getattr(curses, "BUTTON1_CLICKED", 0) | getattr(curses, "BUTTON1_RELEASED", 0)
    ):
        return MouseEvent(x, y, "click")
    return None


def read_event():
    try:
        return decode_event(curses.getmouse())
    except curses.error:
        return None


def inside(event, y, x, height, width):
    return bool(event and x <= event.x < x + width and y <= event.y < y + height)


def list_index(event, *, y, x, width, first_row, rows, top, count):
    if (
        not event
        or not event.clicked
        or not inside(event, y + first_row, x + 1, rows, max(0, width - 2))
    ):
        return None
    index = top + event.y - y - first_row
    return index if 0 <= index < count else None


def item_panels(height, width):
    inner_width = width - 4
    left_width = min(max(40, int(inner_width * 0.62)), inner_width - 24)
    right_width = inner_width - left_width - 1
    if right_width < 20:
        left_width = max(30, inner_width - 21)
        right_width = inner_width - left_width - 1
    panel_height = max(6, height - 4)
    return (1, 2, panel_height, left_width), (
        1,
        3 + left_width,
        panel_height,
        right_width,
    )


def footer_key(token):
    match = re.fullmatch(r"(Shift\+)?F(1[0-2]|[1-9])", token)
    if match:
        return curses.KEY_F1 + int(match.group(2)) - 1 + (12 if match.group(1) else 0)
    return {
        "Tab": "\t",
        "Shift+Tab": curses.KEY_BTAB,
        "Enter": "\n",
        "Esc": "\x1b",
        "ESC": "\x1b",
        "Space": " ",
    }.get(token)
