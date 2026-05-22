from dataclasses import dataclass, field


def collect_editable_field_names(tabs):
    return {
        name
        for tab in tabs
        for name, _label_key in tab.get("fields", [])
        if not str(name).startswith("_heading_")
    }


def editable_indices(tab_fields):
    return [idx for idx, (name, _label_key) in enumerate(tab_fields) if not str(name).startswith("_heading_")]


def resolve_active_field(tabs, active_tab, active_field_by_tab):
    tab_fields = tabs[active_tab]["fields"]
    indices = editable_indices(tab_fields)
    if not indices:
        return 0, "", indices

    current_pos = active_field_by_tab[active_tab]
    if current_pos >= len(indices):
        current_pos = len(indices) - 1
    if current_pos < 0:
        current_pos = 0
    active_field_by_tab[active_tab] = current_pos
    active_index = indices[current_pos]
    return active_index, tab_fields[active_index][0], indices


@dataclass
class TextInputState:
    cursor_positions: dict = field(default_factory=dict)
    scroll_offsets: dict = field(default_factory=dict)

    @classmethod
    def from_values(cls, field_names, values):
        return cls(
            cursor_positions={name: len(str(values.get(name, ""))) for name in field_names},
            scroll_offsets={name: 0 for name in field_names},
        )

    def ensure_field(self, field_name, values):
        if field_name not in self.cursor_positions:
            self.cursor_positions[field_name] = len(str(values.get(field_name, "")))
        if field_name not in self.scroll_offsets:
            self.scroll_offsets[field_name] = 0

    def normalize_view(self, field_name, field_width, values):
        self.ensure_field(field_name, values)
        field_width = max(1, field_width)
        value = str(values.get(field_name, ""))
        value_len = len(value)
        max_scroll = max(0, value_len - field_width)
        cursor = max(0, min(self.cursor_positions.get(field_name, 0), value_len))
        self.cursor_positions[field_name] = cursor
        scroll = max(0, min(self.scroll_offsets.get(field_name, 0), max_scroll))
        if cursor < scroll:
            scroll = cursor
        elif cursor > scroll + field_width - 1:
            scroll = cursor - field_width + 1
        self.scroll_offsets[field_name] = max(0, min(scroll, max_scroll))

    def visible_text(self, field_name, field_width, values):
        self.normalize_view(field_name, field_width, values)
        value = str(values.get(field_name, ""))
        start = self.scroll_offsets[field_name]
        return value[start:start + field_width]

    def cursor_x(self, field_name, field_x, field_width, values):
        self.normalize_view(field_name, field_width, values)
        return field_x + min(
            max(0, self.cursor_positions[field_name] - self.scroll_offsets[field_name]),
            max(0, field_width - 1),
        )

    def set_to_end(self, field_name, values):
        self.ensure_field(field_name, values)
        self.cursor_positions[field_name] = len(str(values.get(field_name, "")))

    def backspace(self, field_name, values):
        self.ensure_field(field_name, values)
        pos = self.cursor_positions[field_name]
        if pos <= 0:
            return
        value = str(values.get(field_name, ""))
        values[field_name] = value[:pos - 1] + value[pos:]
        self.cursor_positions[field_name] = pos - 1

    def delete(self, field_name, values):
        self.ensure_field(field_name, values)
        pos = self.cursor_positions[field_name]
        value = str(values.get(field_name, ""))
        if pos < len(value):
            values[field_name] = value[:pos] + value[pos + 1:]

    def move_left(self, field_name, values):
        self.ensure_field(field_name, values)
        self.cursor_positions[field_name] = max(0, self.cursor_positions[field_name] - 1)

    def move_right(self, field_name, values):
        self.ensure_field(field_name, values)
        self.cursor_positions[field_name] = min(
            len(str(values.get(field_name, ""))),
            self.cursor_positions[field_name] + 1,
        )

    def move_home(self, field_name, values):
        self.ensure_field(field_name, values)
        self.cursor_positions[field_name] = 0

    def move_end(self, field_name, values):
        self.set_to_end(field_name, values)

    def insert_text(self, field_name, values, text):
        self.ensure_field(field_name, values)
        value = str(values.get(field_name, ""))
        pos = self.cursor_positions[field_name]
        values[field_name] = value[:pos] + text + value[pos:]
        self.cursor_positions[field_name] = pos + len(text)
