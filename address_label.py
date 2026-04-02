#!/usr/bin/env python3
from html import escape
from pathlib import Path
from string import Template
import re

try:
    from weasyprint import HTML
except Exception:  # pragma: no cover - optional dependency
    HTML = None


WEASYPRINT_AVAILABLE = HTML is not None


def build_address_label_pdf(template_path, output_path, sender, receiver, page_size="A6"):
    sender_lines = _normalized_address_lines(sender)
    receiver_lines = _normalized_address_lines(receiver)
    template_html, uses_custom_template = _load_html_template(template_path)
    rendered = Template(template_html).safe_substitute(
        page_size=_css_page_size(page_size),
        sender_html="".join(f"<div>{escape(line)}</div>" for line in sender_lines),
        receiver_html="".join(f"<div>{escape(line)}</div>" for line in receiver_lines),
    )
    if not WEASYPRINT_AVAILABLE:
        if uses_custom_template:
            raise RuntimeError("Adresslabel HTML-Vorlage benoetigt WeasyPrint.")
        Path(output_path).write_bytes(_build_simple_address_label_pdf(sender_lines, receiver_lines, page_size))
        return Path(output_path)
    HTML(string=rendered, base_url=str(Path(__file__).resolve().parent)).write_pdf(output_path)
    return Path(output_path)


def _normalized_address_lines(address):
    data = address or {}
    lines = []
    for key in ("name", "additional_name", "street", "address_line_2", "zip_city", "country"):
        value = (data.get(key) or "").strip()
        if value:
            lines.append(value)
    return lines or ["-"]


def _css_page_size(page_size):
    normalized = (page_size or "A6").strip()
    compact = normalized.upper().replace(" ", "")
    if compact in {"A4", "A5", "A6"}:
        return compact
    if compact in {"100X62", "62X100"}:
        return "100mm 62mm"
    if compact.endswith("MM") and "X" in compact:
        width, height = compact.replace("MM", "").split("X", 1)
        if width.isdigit() and height.isdigit():
            return f"{width}mm {height}mm"
    return normalized


def _load_html_template(template_path):
    if template_path:
        path = Path(template_path)
        if path.is_file() and path.suffix.lower() in {".html", ".htm"}:
            return path.read_text(encoding="utf-8"), True
    return _default_address_label_template(), False


def _default_address_label_template():
    return """<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <style>
    @page {
      size: $page_size;
      margin: 6mm;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: #111111;
      font-family: "DejaVu Sans", "Arial", sans-serif;
      font-size: 10pt;
      line-height: 1.25;
    }
    .sheet {
      border: 0.35mm solid #202020;
      min-height: calc(100vh - 1mm);
      padding: 5mm;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      gap: 6mm;
    }
    .sender {
      font-size: 7.5pt;
      color: #333333;
      border-bottom: 0.25mm solid #9a9a9a;
      padding-bottom: 2mm;
    }
    .sender div { margin-bottom: 0.5mm; }
    .receiver {
      margin-top: 2mm;
      flex: 1;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .receiver-box {
      width: 100%;
      min-height: 32mm;
      border: 0.35mm solid #202020;
      padding: 4mm;
      display: flex;
      flex-direction: column;
      justify-content: center;
      font-size: 12pt;
      font-weight: 600;
    }
    .receiver-box div { margin-bottom: 1mm; }
  </style>
</head>
<body>
  <div class="sheet">
    <div class="sender">$sender_html</div>
    <div class="receiver">
      <div class="receiver-box">$receiver_html</div>
    </div>
  </div>
</body>
</html>
"""


def _page_dimensions_points(page_size):
    normalized = (page_size or "A6").strip().upper().replace(" ", "")
    predefined = {
        "A4": (595.28, 841.89),
        "A5": (419.53, 595.28),
        "A6": (297.64, 419.53),
        "100X62": (_mm_to_pt(100), _mm_to_pt(62)),
        "62X100": (_mm_to_pt(100), _mm_to_pt(62)),
    }
    if normalized in predefined:
        return predefined[normalized]
    match = re.fullmatch(r"(\d+)X(\d+)(MM)?", normalized)
    if match:
        width_mm = int(match.group(1))
        height_mm = int(match.group(2))
        return (_mm_to_pt(width_mm), _mm_to_pt(height_mm))
    return predefined["A6"]


def _mm_to_pt(value_mm):
    return float(value_mm) * 72.0 / 25.4


def _pdf_escape(value):
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _build_simple_address_label_pdf(sender_lines, receiver_lines, page_size):
    width, height = _page_dimensions_points(page_size)
    margin = 18
    sender_font = 8
    receiver_font = 14
    receiver_box_height = min(150, max(110, height * 0.38))
    receiver_box_y = margin + 18
    sender_start_y = height - margin - 10
    sender_gap = 10
    receiver_start_y = receiver_box_y + receiver_box_height - 26
    receiver_gap = 17

    content_lines = [
        "BT",
        "/F1 8 Tf",
    ]
    current_y = sender_start_y
    for line in sender_lines:
        content_lines.append(f"1 0 0 1 {margin:.2f} {current_y:.2f} Tm ({_pdf_escape(line)}) Tj")
        current_y -= sender_gap
    content_lines.extend(
        [
            "ET",
            f"{margin:.2f} {receiver_box_y + receiver_box_height:.2f} m",
            f"{width - margin:.2f} {receiver_box_y + receiver_box_height:.2f} l",
            f"{width - margin:.2f} {receiver_box_y:.2f} l",
            f"{margin:.2f} {receiver_box_y:.2f} l",
            "h S",
            "BT",
            f"/F1 {receiver_font} Tf",
        ]
    )
    current_y = receiver_start_y
    for line in receiver_lines:
        content_lines.append(f"1 0 0 1 {margin + 14:.2f} {current_y:.2f} Tm ({_pdf_escape(line)}) Tj")
        current_y -= receiver_gap
    content_lines.append("ET")
    content = "\n".join(content_lines).encode("latin-1", errors="replace")

    objects = []
    offsets = []

    def add_object(obj):
        objects.append(obj)

    add_object("<< /Type /Catalog /Pages 2 0 R >>")
    add_object("<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    add_object(
        f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {width:.2f} {height:.2f}] "
        "/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>"
    )
    add_object("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    add_object(f"<< /Length {len(content)} >>\nstream\n{content.decode('latin-1')}\nendstream")

    pdf = "%PDF-1.4\n"
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(pdf.encode("latin-1")))
        pdf += f"{index} 0 obj\n{obj}\nendobj\n"
    xref_start = len(pdf.encode("latin-1"))
    pdf += f"xref\n0 {len(objects) + 1}\n"
    pdf += "0000000000 65535 f \n"
    for offset in offsets:
        pdf += f"{offset:010d} 00000 n \n"
    pdf += f"trailer << /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_start}\n%%EOF\n"
    return pdf.encode("latin-1")
