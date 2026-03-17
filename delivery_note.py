#!/usr/bin/env python3
import base64
import datetime
from html import escape
import math
import re
import struct
from string import Template
import urllib.request
import zlib
from pathlib import Path

try:
    from weasyprint import HTML
except Exception:  # pragma: no cover - optional dependency
    HTML = None


MAX_DELIVERY_NOTE_ITEMS = 10
MEDIA_BOX = "[0.000 0.000 595.280 841.890]"
LOCAL_TEMPLATE_PATH = Path(__file__).resolve().parent / "local_only" / "delivery_note_template.pdf"
WEASYPRINT_AVAILABLE = HTML is not None

DEFAULT_SENDER = {
    "name": "Firmenname",
    "street": "Strasse 1",
    "city": "12345 Musterstadt",
    "email": "info@example.com",
}


def build_delivery_note_rows(order_items):
    return [row for row in order_items if not row.get("external_fulfillment")]


def format_delivery_address_lines(order):
    lines = []

    if order.get("shipping_name"):
        lines.append(order["shipping_name"])
    if order.get("shipping_address1"):
        lines.append(order["shipping_address1"])

    zip_city = " ".join(
        part for part in [order.get("shipping_zip") or "", order.get("shipping_city") or ""] if part
    ).strip()
    if zip_city:
        lines.append(zip_city)

    if order.get("shipping_country"):
        lines.append(order["shipping_country"])

    return lines or ["Keine Lieferadresse"]


def build_delivery_note_pdf(template_path, output_path, order, order_items, sender=None, logo_source=""):
    rows = build_delivery_note_rows(order_items)
    if _should_use_html_renderer(template_path):
        _build_delivery_note_pdf_html(template_path, output_path, order, rows, sender=sender, logo_source=logo_source)
        return Path(output_path)
    return _build_delivery_note_pdf_legacy(template_path, output_path, order, rows, sender=sender, logo_source=logo_source)


def _should_use_html_renderer(template_path):
    if not WEASYPRINT_AVAILABLE:
        return False
    if not template_path:
        return True
    suffix = Path(template_path).suffix.lower()
    if suffix == ".pdf":
        return False
    return suffix in {"", ".html", ".htm"}


def _build_delivery_note_pdf_html(template_path, output_path, order, rows, sender=None, logo_source=""):
    sender_data = _normalized_sender(sender)
    order_name = order.get("order_name") or "-"
    created_at = order.get("created_at")
    if hasattr(created_at, "strftime"):
        order_date = created_at.strftime("%d.%m.%Y")
    else:
        order_date = str(created_at or "")

    if logo_source:
        logo_data_uri = _build_logo_data_uri(logo_source)
        logo_html = f'<img class="logo" src="{logo_data_uri}" alt="Logo">'
    else:
        logo_html = ""

    items_html = _build_order_rows_html(rows)
    template_html = _load_html_template(template_path)
    rendered = Template(template_html).safe_substitute(
        logo_html=logo_html,
        order_name=escape(order_name),
        order_date=escape(order_date),
        sender_line=escape(_build_sender_line(sender_data)),
        sender_block_html="".join(f"<div>{escape(line)}</div>" for line in _build_sender_block_lines(sender_data)),
        address_html="".join(f"<div>{escape(line)}</div>" for line in format_delivery_address_lines(order)),
        items_html=items_html,
    )
    HTML(string=rendered, base_url=str(Path(__file__).resolve().parent)).write_pdf(output_path)


def _build_order_rows_html(rows):
    if not rows:
        return "<tr><td>1</td><td>-<br><span class='sku'>-</span></td><td class='qty'>0</td></tr>"

    html_rows = []
    for index, row in enumerate(rows, start=1):
        title = escape(row.get("title") or "-")
        sku = escape(row.get("sku") or "-")
        qty = escape(str(row.get("quantity") or 0))
        html_rows.append(
            "<tr>"
            f"<td>{index}</td>"
            f"<td>{title}<br><span class='sku'>{sku}</span></td>"
            f"<td class='qty'>{qty}</td>"
            "</tr>"
        )
    return "".join(html_rows)


def _load_html_template(template_path):
    if template_path and Path(template_path).suffix.lower() in {".html", ".htm"}:
        return Path(template_path).read_text(encoding="utf-8")
    return _default_delivery_note_html_template()


def _default_delivery_note_html_template():
    return """<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8">
  <style>
    @page {
      size: A4;
      margin: 24mm 15mm 20mm 15mm;
      @bottom-right {
        content: "Seite " counter(page) " von " counter(pages);
        font-size: 10pt;
        color: #21303f;
      }
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: #21303f;
      font-family: "DejaVu Sans", "Arial", sans-serif;
      font-size: 10.5pt;
      line-height: 1.3;
    }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      margin-bottom: 16mm;
    }
    .logo {
      max-width: 150px;
      max-height: 44px;
      object-fit: contain;
    }
    .doc-head {
      text-align: right;
      min-width: 260px;
    }
    .doc-head .title {
      font-size: 20pt;
      font-weight: 700;
      margin-bottom: 4mm;
    }
    .meta-row { margin-bottom: 1.5mm; }
    .address {
      display: flex;
      justify-content: space-between;
      gap: 12mm;
      margin-bottom: 10mm;
    }
    .address-left {
      flex: 1;
      min-width: 0;
    }
    .sender-line {
      font-size: 7.5pt;
      margin-bottom: 1.5mm;
      border-bottom: 1px solid #21303f;
      padding-bottom: 1mm;
    }
    .ship-lines > div { margin-bottom: 1.2mm; }
    .sender-right {
      width: 230px;
      text-align: left;
      white-space: pre-line;
    }
    table {
      width: 100%;
      border-collapse: collapse;
    }
    thead { display: table-header-group; }
    tr { page-break-inside: avoid; }
    th {
      text-align: left;
      border-bottom: 1px solid #21303f;
      padding: 2.5mm 2mm;
      font-weight: 700;
    }
    td {
      border-bottom: 1px solid #d4dce5;
      padding: 3mm 2mm;
      vertical-align: top;
    }
    th:nth-child(1), td:nth-child(1) { width: 70px; }
    th:nth-child(3), td:nth-child(3) { width: 70px; text-align: right; }
    .sku {
      display: inline-block;
      margin-top: 1mm;
      color: #415161;
      font-size: 9.5pt;
    }
    .qty { text-align: right; }
    .thanks {
      margin-top: 14mm;
    }
  </style>
</head>
<body>
  <div class="header">
    <div>$logo_html</div>
    <div class="doc-head">
      <div class="title">Lieferschein</div>
      <div class="meta-row"><strong>Bestellung:</strong> $order_name</div>
      <div class="meta-row"><strong>Datum:</strong> $order_date</div>
    </div>
  </div>

  <div class="address">
    <div class="address-left">
      <div class="sender-line">$sender_line</div>
      <div class="ship-lines">$address_html</div>
    </div>
    <div class="sender-right">$sender_block_html</div>
  </div>

  <table>
    <thead>
      <tr>
        <th>Position</th>
        <th>Artikel</th>
        <th>Anzahl</th>
      </tr>
    </thead>
    <tbody>$items_html</tbody>
  </table>

  <div class="thanks">Vielen Dank für Ihre Bestellung!</div>
</body>
</html>
"""


def _build_logo_data_uri(source):
    image_bytes = _load_binary_source(source)
    mime = _detect_image_mime(image_bytes)
    encoded = base64.b64encode(image_bytes).decode("ascii")
    return f"data:{mime};base64,{encoded}"


def _detect_image_mime(image_bytes):
    if image_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if image_bytes.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if image_bytes.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if image_bytes.startswith(b"RIFF") and image_bytes[8:12] == b"WEBP":
        return "image/webp"
    raise ValueError("Logo-Format nicht unterstuetzt (erwartet PNG/JPG/GIF/WEBP).")


def _build_delivery_note_pdf_legacy(template_path, output_path, order, rows, sender=None, logo_source=""):
    template_bytes = _load_template_bytes(template_path)
    objects = _parse_pdf_objects(template_bytes)
    regular_font_obj_id = max(objects) + 1
    bold_font_obj_id = regular_font_obj_id + 1
    objects[regular_font_obj_id] = _build_builtin_font_object("Helvetica")
    objects[bold_font_obj_id] = _build_builtin_font_object("Helvetica-Bold")
    logo_info = None
    if logo_source:
        logo_image_obj_id = bold_font_obj_id + 1
        logo_info = _load_logo_image_for_pdf(logo_source)
        objects[logo_image_obj_id] = _build_image_xobject(*logo_info)

    resources_body = _augment_resources_with_builtin_fonts(
        _extract_page_resources(objects[3]),
        regular_font_obj_id,
        bold_font_obj_id,
        logo_image_obj_id if logo_info else None,
    )
    objects[3] = _build_pages_object([6], resources_body)
    objects[5] = _build_info_object()
    objects[6] = _build_page_object(7)
    objects[7] = _build_stream_object(
        build_delivery_note_content_stream(order, rows, 1, 1, sender=sender, has_logo=bool(logo_info), logo_info=logo_info)
    )

    if rows:
        page_count = math.ceil(len(rows) / MAX_DELIVERY_NOTE_ITEMS)
    else:
        page_count = 1

    if page_count > 1:
        next_obj_id = max(objects) + 1
        page_ids = []
        for page_index in range(page_count):
            page_obj_id = 6 if page_index == 0 else next_obj_id
            content_obj_id = 7 if page_index == 0 else next_obj_id + 1
            page_ids.append(page_obj_id)
            page_rows = rows[page_index * MAX_DELIVERY_NOTE_ITEMS : (page_index + 1) * MAX_DELIVERY_NOTE_ITEMS]
            objects[page_obj_id] = _build_page_object(content_obj_id)
            objects[content_obj_id] = _build_stream_object(
                build_delivery_note_content_stream(
                    order,
                    page_rows,
                    page_index + 1,
                    page_count,
                    sender=sender,
                    has_logo=bool(logo_info),
                    logo_info=logo_info,
                )
            )
            if page_index > 0:
                next_obj_id += 2

        objects[3] = _build_pages_object(page_ids, resources_body)

    pdf_bytes = _assemble_pdf(objects)
    Path(output_path).write_bytes(pdf_bytes)
    return Path(output_path)


def build_delivery_note_content_stream(order, rows, page_number=1, page_count=1, sender=None, has_logo=False, logo_info=None):
    order_name = order.get("order_name") or "-"
    created_at = order.get("created_at")
    sender = _normalized_sender(sender)

    if hasattr(created_at, "strftime"):
        order_date = created_at.strftime("%d.%m.%Y")
    else:
        order_date = str(created_at or "")

    commands = [
        "1.000 1.000 1.000 rg",
        "60.000 75.000 490.280 732.874 re f",
        "0.129 0.169 0.212 rg",
        _text_cmd(441.652, 778.669, "F4", 15.8, "Lieferschein"),
        _text_cmd(416.426, 750.808, "F4", 10.5, f"Bestellung: {order_name}"),
        _text_cmd(447.779, 737.363, "F3", 10.5, f"Datum: {order_date}"),
        _text_cmd(447.779, 723.919, "F3", 10.5, f"Seite: {page_number} von {page_count}"),
    ]
    if has_logo:
        commands.extend(_build_logo_draw_commands(logo_info))

    sender_line_y = 697.548
    commands.append(_text_cmd(60.000, sender_line_y, "F3", 7.5, _build_sender_line(sender)))
    commands.append("0.129 0.169 0.212 RG")
    commands.append("0.33 w 0 J [  ] 0 d")
    commands.append("60.000 696.037 m 272.273 696.037 l S")

    address_y = 677.372
    for index, line in enumerate(format_delivery_address_lines(order)):
        commands.append(_text_cmd(60.000, address_y - (index * 13.444), "F3", 10.5, line))

    sender_block_y = 694.475
    for index, line in enumerate(_build_sender_block_lines(sender)):
        commands.append(_text_cmd(437.856, sender_block_y - (index * 13.444), "F3", 10.5, line))

    commands.extend(
        [
            "q",
            "60.000 565.712 m 60.000 564.712 l 132.535 564.712 l 132.535 565.712 l 132.535 566.462 l 60.000 566.462 l W n",
            "0.75 w 0 J [  ] 0 d",
            "60.000 566.087 m 132.535 566.087 l S",
            "Q",
            "0.129 0.169 0.212 rg",
            _text_cmd(65.000, 574.151, "F4", 10.5, "Position"),
            "q",
            "132.535 565.712 m 132.535 564.712 l 487.975 564.712 l 487.975 565.712 l 487.975 566.462 l 132.535 566.462 l W n",
            "0.129 0.169 0.212 RG",
            "0.75 w 0 J [  ] 0 d",
            "132.535 566.087 m 487.975 566.087 l S",
            "Q",
            "0.129 0.169 0.212 rg",
            _text_cmd(137.535, 574.151, "F4", 10.5, "Artikel"),
            "q",
            "487.975 565.712 m 487.975 564.712 l 550.280 564.712 l 550.280 565.712 l 550.280 566.462 l 487.975 566.462 l W n",
            "0.129 0.169 0.212 RG",
            "0.75 w 0 J [  ] 0 d",
            "487.975 566.087 m 550.280 566.087 l S",
            "Q",
            "0.129 0.169 0.212 rg",
            _text_cmd(505.401, 574.151, "F4", 10.5, "Anzahl"),
        ]
    )

    base_y = 543.235
    row_step = 36.889
    position_offset = (page_number - 1) * MAX_DELIVERY_NOTE_ITEMS
    for index, row in enumerate(rows, start=1):
        row_y = base_y - ((index - 1) * row_step)
        title = _truncate_text(row.get("title") or "-", 56)
        sku = _truncate_text(row.get("sku") or "-", 32)
        qty = str(row.get("quantity") or 0)
        commands.append(_text_cmd(65.000, row_y, "F3", 10.5, str(position_offset + index)))
        commands.append(_text_cmd(137.535, row_y + 6.722, "F3", 10.5, title))
        commands.append(_text_cmd(137.535, row_y - 6.722, "F3", 10.5, sku))
        commands.append(_text_cmd(538.602, row_y, "F3", 10.5, qty))

    commands.append(_text_cmd(60.000, 156.073, "F3", 10.5, "Vielen Dank für Ihre Bestellung!"))
    return "\n".join(commands) + "\n"


def _truncate_text(value, length):
    if len(value) <= length:
        return value
    if length <= 3:
        return value[:length]
    return value[: length - 3] + "..."


def _text_cmd(x, y, font, size, text):
    escaped = (
        text.replace("\\", "\\\\")
        .replace("(", "\\(")
        .replace(")", "\\)")
    )
    return f"BT {x:.3f} {y:.3f} Td /{font} {size} Tf ({escaped}) Tj ET"


def _normalized_sender(sender):
    normalized = DEFAULT_SENDER.copy()
    normalized.update(sender or {})
    return normalized


def _build_sender_line(sender):
    parts = [sender["name"], sender["street"], sender["city"]]
    return " - ".join(part for part in parts if part)


def _build_sender_block_lines(sender):
    return [part for part in [sender["name"], sender["street"], sender["city"], sender["email"]] if part]


def _extract_page_resources(pages_object_body):
    resources_start = pages_object_body.index(b"/Resources <<")
    media_box_start = pages_object_body.index(b"/MediaBox", resources_start)
    return pages_object_body[resources_start:media_box_start].rstrip()


def _load_template_bytes(template_path):
    if template_path:
        return Path(template_path).read_bytes()
    if LOCAL_TEMPLATE_PATH.exists():
        return LOCAL_TEMPLATE_PATH.read_bytes()
    return _build_fallback_template_pdf()


def _build_fallback_template_pdf():
    objects = {
        1: b"<< /Type /Catalog\n/Pages 3 0 R\n>>",
        2: b"<< /Type /Outlines\n/Count 0\n>>",
        3: (
            b"<< /Type /Pages\n"
            b"/Kids [6 0 R]\n"
            b"/Count 1\n"
            b"/Resources << /Font << >> >>\n"
            + f"/MediaBox {MEDIA_BOX}\n".encode("ascii")
            + b">>"
        ),
        4: b"<<>>",
        5: b"<<>>",
        6: _build_page_object(7),
        7: _build_stream_object(""),
    }
    return _assemble_pdf(objects)


def _augment_resources_with_builtin_fonts(resources_body, regular_font_obj_id, bold_font_obj_id, logo_image_obj_id=None):
    marker = b"/Font <<"
    insert_at = resources_body.index(b">>", resources_body.index(marker))
    font_refs = (
        f"\n/F3 {regular_font_obj_id} 0 R\n/F4 {bold_font_obj_id} 0 R".encode("ascii")
    )
    updated = resources_body[:insert_at] + font_refs + resources_body[insert_at:]
    if not logo_image_obj_id:
        return updated
    return _augment_resources_with_logo_xobject(updated, logo_image_obj_id)


def _augment_resources_with_logo_xobject(resources_body, logo_image_obj_id):
    marker = b"/XObject <<"
    logo_ref = f"\n/L1 {logo_image_obj_id} 0 R".encode("ascii")
    if marker in resources_body:
        insert_at = resources_body.index(b">>", resources_body.index(marker))
        return resources_body[:insert_at] + logo_ref + resources_body[insert_at:]
    insert_at = resources_body.rfind(b">>")
    if insert_at == -1:
        raise ValueError("Ungueltige PDF-Ressourcenstruktur.")
    xobject_block = f"\n/XObject << /L1 {logo_image_obj_id} 0 R >>".encode("ascii")
    return resources_body[:insert_at] + xobject_block + resources_body[insert_at:]


def _build_logo_draw_commands(logo_info):
    width, height = logo_info[0], logo_info[1]
    max_width = 150.0
    max_height = 44.0
    scale = min(max_width / width, max_height / height, 1.0)
    draw_width = width * scale
    draw_height = height * scale
    x = 60.0
    y = 764.374
    return [
        "q",
        f"{draw_width:.3f} 0 0 {draw_height:.3f} {x:.3f} {y:.3f} cm /L1 Do",
        "Q",
    ]


def _load_logo_image_for_pdf(source):
    try:
        image_bytes = _load_binary_source(source)
        return _decode_png_to_pdf_rgb(image_bytes)
    except Exception as exc:
        raise ValueError(f"Logo konnte nicht geladen werden ({source}): {exc}") from exc


def _load_binary_source(source):
    source_text = str(source).strip()
    if source_text.startswith(("http://", "https://")):
        request = urllib.request.Request(source_text, headers={"User-Agent": "Lagerverwaltung/1.0"})
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.read()
    return Path(source_text).read_bytes()


def _decode_png_to_pdf_rgb(png_bytes):
    signature = b"\x89PNG\r\n\x1a\n"
    if not png_bytes.startswith(signature):
        raise ValueError("Datei ist kein PNG.")

    width = height = bit_depth = color_type = interlace_method = None
    idat_chunks = []
    offset = len(signature)

    while offset < len(png_bytes):
        if offset + 8 > len(png_bytes):
            raise ValueError("PNG ist unvollstaendig.")
        chunk_length = struct.unpack(">I", png_bytes[offset:offset + 4])[0]
        chunk_type = png_bytes[offset + 4:offset + 8]
        chunk_data_start = offset + 8
        chunk_data_end = chunk_data_start + chunk_length
        chunk_crc_end = chunk_data_end + 4
        if chunk_crc_end > len(png_bytes):
            raise ValueError("PNG-Chunk ist unvollstaendig.")
        chunk_data = png_bytes[chunk_data_start:chunk_data_end]
        offset = chunk_crc_end

        if chunk_type == b"IHDR":
            width, height, bit_depth, color_type, _, _, interlace_method = struct.unpack(">IIBBBBB", chunk_data)
        elif chunk_type == b"IDAT":
            idat_chunks.append(chunk_data)
        elif chunk_type == b"IEND":
            break

    if not width or not height:
        raise ValueError("PNG ohne IHDR.")
    if bit_depth != 8:
        raise ValueError("PNG Bit-Tiefe wird nicht unterstuetzt (nur 8).")
    if interlace_method != 0:
        raise ValueError("Interlaced PNG wird nicht unterstuetzt.")
    if color_type not in {2, 6}:
        raise ValueError("PNG Farbtyp wird nicht unterstuetzt (nur RGB/RGBA).")
    if not idat_chunks:
        raise ValueError("PNG ohne IDAT-Daten.")

    decompressed = zlib.decompress(b"".join(idat_chunks))
    bytes_per_pixel = 3 if color_type == 2 else 4
    row_width = width * bytes_per_pixel
    expected_size = height * (1 + row_width)
    if len(decompressed) != expected_size:
        raise ValueError("PNG-Datenlaenge passt nicht zu den Bilddaten.")

    prev_row = b"\x00" * row_width
    rgb = bytearray()
    cursor = 0
    for _ in range(height):
        filter_type = decompressed[cursor]
        cursor += 1
        encoded_row = decompressed[cursor:cursor + row_width]
        cursor += row_width
        row = _unfilter_png_row(filter_type, encoded_row, prev_row, bytes_per_pixel)
        prev_row = row
        if color_type == 6:
            for pixel_offset in range(0, len(row), 4):
                rgb.extend(row[pixel_offset:pixel_offset + 3])
        else:
            rgb.extend(row)

    return width, height, zlib.compress(bytes(rgb))


def _unfilter_png_row(filter_type, encoded_row, prev_row, bytes_per_pixel):
    row = bytearray(encoded_row)
    if filter_type == 0:
        return bytes(row)
    if filter_type == 1:
        for i in range(len(row)):
            left = row[i - bytes_per_pixel] if i >= bytes_per_pixel else 0
            row[i] = (row[i] + left) & 0xFF
        return bytes(row)
    if filter_type == 2:
        for i in range(len(row)):
            row[i] = (row[i] + prev_row[i]) & 0xFF
        return bytes(row)
    if filter_type == 3:
        for i in range(len(row)):
            left = row[i - bytes_per_pixel] if i >= bytes_per_pixel else 0
            up = prev_row[i]
            row[i] = (row[i] + ((left + up) // 2)) & 0xFF
        return bytes(row)
    if filter_type == 4:
        for i in range(len(row)):
            left = row[i - bytes_per_pixel] if i >= bytes_per_pixel else 0
            up = prev_row[i]
            up_left = prev_row[i - bytes_per_pixel] if i >= bytes_per_pixel else 0
            row[i] = (row[i] + _paeth_predictor(left, up, up_left)) & 0xFF
        return bytes(row)
    raise ValueError(f"PNG Filtertyp {filter_type} wird nicht unterstuetzt.")


def _paeth_predictor(left, up, up_left):
    prediction = left + up - up_left
    distance_left = abs(prediction - left)
    distance_up = abs(prediction - up)
    distance_up_left = abs(prediction - up_left)
    if distance_left <= distance_up and distance_left <= distance_up_left:
        return left
    if distance_up <= distance_up_left:
        return up
    return up_left


def _build_image_xobject(width, height, compressed_rgb):
    return (
        b"<< /Type /XObject\n"
        b"/Subtype /Image\n"
        + f"/Width {width}\n".encode("ascii")
        + f"/Height {height}\n".encode("ascii")
        + b"/ColorSpace /DeviceRGB\n"
        + b"/BitsPerComponent 8\n"
        + b"/Filter /FlateDecode\n"
        + f"/Length {len(compressed_rgb)} >>\n".encode("ascii")
        + b"stream\n"
        + compressed_rgb
        + b"\nendstream"
    )


def _build_pages_object(page_ids, resources_body):
    kids = " ".join(f"{page_id} 0 R" for page_id in page_ids)
    return (
        b"<< /Type /Pages\n"
        + f"/Kids [{kids}]\n".encode("ascii")
        + f"/Count {len(page_ids)}\n".encode("ascii")
        + resources_body
        + b"\n"
        + f"/MediaBox {MEDIA_BOX}\n".encode("ascii")
        + b">>"
    )


def _build_page_object(content_obj_id):
    return (
        b"<< /Type /Page\n"
        + f"/MediaBox {MEDIA_BOX}\n".encode("ascii")
        + b"/Parent 3 0 R\n"
        + f"/Contents {content_obj_id} 0 R\n".encode("ascii")
        + b">>"
    )


def _build_builtin_font_object(base_font_name):
    return (
        b"<< /Type /Font\n"
        b"/Subtype /Type1\n"
        + f"/BaseFont /{base_font_name}\n".encode("ascii")
        + b"/Encoding /WinAnsiEncoding\n"
        b">>"
    )


def _parse_pdf_objects(pdf_bytes):
    objects = {}
    matches = list(re.finditer(rb"(\d+) 0 obj\b", pdf_bytes))
    for index, match in enumerate(matches):
        obj_id = int(match.group(1))
        body_start = match.end()
        next_start = matches[index + 1].start() if index + 1 < len(matches) else len(pdf_bytes)
        body = pdf_bytes[body_start:next_start]
        endobj_pos = body.rfind(b"endobj")
        if endobj_pos != -1:
            body = body[:endobj_pos]
        objects[obj_id] = body.strip()
    return objects


def _build_info_object():
    timestamp = datetime.datetime.now().strftime("D:%Y%m%d%H%M%S+01'00'")
    return (
        b"<<\n"
        b"/Producer (Lagerverwaltung)\n"
        + f"/CreationDate ({timestamp})\n".encode("ascii")
        + f"/ModDate ({timestamp})\n".encode("ascii")
        + b"/Title (\xfe\xff\x00L\x00i\x00e\x00f\x00e\x00r\x00s\x00c\x00h\x00e\x00i\x00n)\n"
        b">>"
    )


def _build_stream_object(stream_text):
    compressed = zlib.compress(stream_text.encode("cp1252"))
    return (
        b"<< /Filter /FlateDecode\n"
        + f"/Length {len(compressed)} >>\n".encode("ascii")
        + b"stream\n"
        + compressed
        + b"\nendstream"
    )


def _assemble_pdf(objects):
    highest = max(objects)
    output = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = {0: 0}

    for obj_id in range(1, highest + 1):
        body = objects[obj_id]
        offsets[obj_id] = len(output)
        output.extend(f"{obj_id} 0 obj\n".encode("ascii"))
        output.extend(body)
        if not body.endswith(b"\n"):
            output.extend(b"\n")
        output.extend(b"endobj\n")

    startxref = len(output)
    output.extend(f"xref\n0 {highest + 1}\n".encode("ascii"))
    output.extend(b"0000000000 65535 f \n")
    for obj_id in range(1, highest + 1):
        output.extend(f"{offsets[obj_id]:010d} 00000 n \n".encode("ascii"))
    output.extend(
        f"trailer\n<< /Size {highest + 1}\n/Root 1 0 R\n/Info 5 0 R >>\nstartxref\n{startxref}\n%%EOF\n".encode(
            "ascii"
        )
    )
    return bytes(output)
