"""Menu actions and configurable function keys for the two main views."""

import curses
import re
from copy import deepcopy

CONTEXTS = ("items", "items_multi", "orders")
MENU_KEY = curses.KEY_F10
KEY_NAMES = tuple(
    [f"F{i}" for i in range(1, 13) if i != 10] + [f"Shift+F{i}" for i in range(1, 13)]
)
COMMON = {
    "inventory": "",
    "settings": "Shift+F11",
    "print_settings": "",
    "keybindings": "",
    "sync_status": "",
    "help": "",
    "usage": "",
    "about": "",
    "quit": "",
}
ITEMS = {
    "sort": "F1",
    "new_item_alt": "F2",
    "inventory_alt": "F3",
    "info": "F4",
    "new_item": "F5",
    "location": "F6",
    "quantity": "F7",
    "print_label": "F8",
    "filter_clear": "F9",
    "footer_help": "F11",
    "show_orders": "F12",
    "inventory": "Shift+F1",
    "filter_local": "Shift+F2",
    "filter_missing": "Shift+F3",
    "edit_item": "Shift+F5",
    "print_many": "Shift+F8",
    "external": "",
    "external_view": "",
    "new_order": "",
    "manual_label": "",
    "history": "",
}
DEFAULT_BINDINGS = {
    "items": {**COMMON, **ITEMS},
    "items_multi": {
        **COMMON,
        **{
            k: v for k, v in ITEMS.items() if k not in {"new_item_alt", "inventory_alt"}
        },
        "previous_location": "F2",
        "next_location": "F3",
        "select_location": "",
    },
    "orders": {
        **COMMON,
        "pending_filter": "F1",
        "status_filter": "F2",
        "payment_filter": "F3",
        "jump_order": "F4",
        "ship_label": "F5",
        "manual_label": "Shift+F5",
        "manual_label_alt": "Shift+F8",
        "partial": "F6",
        "partial_alt": "Shift+F7",
        "bulk": "F7",
        "history": "F8",
        "show_items": "F9",
        "picklist": "Shift+F10",
        "delivery_note": "F11",
        "new_order": "F12",
        "edit_order": "Shift+F12",
        "delete_order": "",
        "pdf_export": "",
    },
}
MENUS = (
    ("file", ("new_item", "new_order", None, "pdf_export", None, "quit")),
    (
        "article",
        (
            "info",
            "edit_item",
            "quantity",
            "location",
            "external",
            None,
            "print_label",
            "print_many",
        ),
    ),
    (
        "view",
        (
            "show_items",
            "show_orders",
            None,
            "sort",
            "filter_local",
            "filter_missing",
            "external_view",
            "filter_clear",
            "previous_location",
            "next_location",
            "select_location",
            None,
            "pending_filter",
            "status_filter",
            "payment_filter",
            "jump_order",
        ),
    ),
    (
        "orders",
        (
            "ship_label",
            "manual_label",
            "delivery_note",
            "picklist",
            "pdf_export",
            None,
            "partial",
            "bulk",
            "history",
            None,
            "edit_order",
            "delete_order",
        ),
    ),
    (
        "extras",
        (
            "inventory",
            None,
            "settings",
            "print_settings",
            "keybindings",
            None,
            "sync_status",
        ),
    ),
    ("help", ("help", "usage", "about")),
)
ITEM_ACTIONS = {
    "info",
    "edit_item",
    "quantity",
    "location",
    "external",
    "print_label",
    "print_many",
}
ORDER_ACTIONS = {
    "ship_label",
    "delivery_note",
    "picklist",
    "pdf_export",
    "partial",
    "edit_order",
    "delete_order",
}
ALIASES = {
    "new_item_alt": "new_item",
    "inventory_alt": "inventory",
    "manual_label_alt": "manual_label",
    "partial_alt": "partial",
}


def key_name(key):
    if isinstance(key, int) and curses.KEY_F1 <= key <= curses.KEY_F1 + 23:
        number = key - curses.KEY_F1 + 1
        return f"F{number}" if number <= 12 else f"Shift+F{number - 12}"
    return None


def effective_bindings(context, saved=None):
    result = DEFAULT_BINDINGS[context].copy()
    raw = (saved or {}).get(context, {}) if isinstance(saved, dict) else {}
    if not isinstance(raw, dict):
        return result
    for action, value in raw.items():
        if action in result and (value == "" or value in KEY_NAMES):
            result[action] = value
    # Malformed external configuration cannot make an action ambiguous.
    used = [value for value in result.values() if value]
    return result if len(used) == len(set(used)) else DEFAULT_BINDINGS[context].copy()


def assign_binding(saved, context, action, value):
    bindings = effective_bindings(context, saved)
    if action not in bindings or (value and value not in KEY_NAMES):
        raise ValueError("invalid")
    occupied = next(
        (
            name
            for name, key in bindings.items()
            if key == value and value and name != action
        ),
        None,
    )
    if occupied:
        raise ValueError(occupied)
    result = deepcopy(saved) if isinstance(saved, dict) else {}
    result[context] = {**bindings, action: value}
    return result


def resolve_action(key, context, saved=None):
    name = key_name(key)
    action = next(
        (
            action
            for action, value in effective_bindings(context, saved).items()
            if name and value == name
        ),
        None,
    )
    return ALIASES.get(action, action)


def action_key(action, context, saved=None):
    bindings = effective_bindings(context, saved)
    return bindings.get(action) or next(
        (
            value
            for alias, value in bindings.items()
            if ALIASES.get(alias) == action and value
        ),
        "",
    )


def action_available(action, context, selected=None):
    if action not in DEFAULT_BINDINGS[context] and action not in {
        "show_items",
        "show_orders",
    }:
        return False
    if action in ITEM_ACTIONS | ORDER_ACTIONS:
        if not selected:
            return False
    if action in {"edit_order", "delete_order"}:
        return bool(
            selected
            and (
                str(selected.get("order_id") or "").startswith("custom-")
                or str(selected.get("source") or "").lower() == "custom"
            )
        )
    return True


def remap_footer(text, context, saved=None):
    defaults = DEFAULT_BINDINGS[context]
    current = effective_bindings(context, saved)
    inverse = {key: action for action, key in defaults.items() if key}

    def replace(match):
        old = match.group()
        if old == "F10":
            return old
        return current.get(inverse.get(old), old) or "—"

    return re.sub(
        r"Shift\+F(?:1[0-2]|[1-9])(?!\d)|(?<![\w+])F(?:1[0-2]|[1-9])(?!\d)",
        replace,
        text,
    )
