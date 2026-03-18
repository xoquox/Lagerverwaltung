#!/usr/bin/env python3
import sys
import warnings
from pathlib import Path

from app_logging import PRINT_LOG_PATH, get_logger
from app_settings import load_settings

warnings.filterwarnings("ignore")

LOGGER = get_logger("print")

LABEL_WIDTH = 696
LABEL_HEIGHT = 271

SYSTEM_REGULAR_FONTS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/TTF/DejaVuSans.ttf",
    "/usr/share/fonts/dejavu/DejaVuSans.ttf",
    "/Library/Fonts/Arial.ttf",
    "C:/Windows/Fonts/segoeui.ttf",
]

SYSTEM_CONDENSED_FONTS = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSansCondensed.ttf",
    "/usr/share/fonts/TTF/DejaVuSansCondensed.ttf",
    "/usr/share/fonts/dejavu/DejaVuSansCondensed.ttf",
    "/Library/Fonts/Arial Narrow.ttf",
    "C:/Windows/Fonts/arialn.ttf",
]


def _load_print_dependencies():
    missing = []

    try:
        from PIL import Image, ImageDraw, ImageFont
    except ImportError:
        missing.append("Pillow")
        Image = ImageDraw = ImageFont = None

    try:
        import barcode
        from barcode.writer import ImageWriter
    except ImportError:
        missing.append("python-barcode")
        barcode = ImageWriter = None

    try:
        from brother_ql.backends.helpers import send
        from brother_ql.conversion import convert
        from brother_ql.raster import BrotherQLRaster
    except ImportError:
        missing.append("brother_ql")
        send = convert = BrotherQLRaster = None

    if missing:
        packages = ", ".join(missing)
        raise RuntimeError(
            f"Fehlende Python-Pakete fuer Labeldruck: {packages}. "
            f"Installiere sie z. B. mit: pip install pillow python-barcode brother_ql"
        )

    return {
        "Image": Image,
        "ImageDraw": ImageDraw,
        "ImageFont": ImageFont,
        "barcode": barcode,
        "ImageWriter": ImageWriter,
        "send": send,
        "convert": convert,
        "BrotherQLRaster": BrotherQLRaster,
    }


def _font_candidates(settings, condensed):
    configured = settings.get("label_font_condensed" if condensed else "label_font_regular", "")
    candidates = []
    if configured:
        candidates.append(Path(configured).expanduser())
    candidates.extend(Path(path) for path in (SYSTEM_CONDENSED_FONTS if condensed else SYSTEM_REGULAR_FONTS))
    if condensed and settings.get("label_font_regular"):
        candidates.append(Path(settings["label_font_regular"]).expanduser())
    return candidates


def _load_font(image_font, settings, size, condensed=False):
    last_error = None
    for candidate in _font_candidates(settings, condensed):
        try:
            if not candidate.exists():
                continue
            return image_font.truetype(str(candidate), size)
        except Exception as exc:
            last_error = exc

    try:
        return image_font.load_default()
    except Exception as exc:
        if last_error:
            raise RuntimeError(f"Kein nutzbarer Font gefunden. Letzter Fehler: {last_error}") from last_error
        raise RuntimeError(f"Kein nutzbarer Font gefunden: {exc}") from exc


def fit_font(draw, text, max_width, start_size, image_font, settings):
    size = start_size

    while size > 12:
        font = _load_font(image_font, settings, size, condensed=False)
        bbox = draw.textbbox((0, 0), text, font=font)

        if bbox[2] <= max_width:
            return font

        size -= 2

    size = start_size

    while size > 12:
        font = _load_font(image_font, settings, size, condensed=True)
        bbox = draw.textbbox((0, 0), text, font=font)

        if bbox[2] <= max_width:
            return font

        size -= 2

    return font


def wrap_text(draw, text, font, max_width):
    words = text.split()
    lines = []
    current = ""

    for word in words:
        test = word if current == "" else current + " " + word
        bbox = draw.textbbox((0, 0), test, font=font)

        if bbox[2] <= max_width:
            current = test
        else:
            lines.append(current)
            current = word

    if current:
        lines.append(current)

    return lines[:2]


def draw_centered(draw, text, font, y, width):
    bbox = draw.textbbox((0, 0), text, font=font)
    text_w = bbox[2]
    x = (width - text_w) // 2
    draw.text((x, y), text, font=font, fill="black")


def print_label(sku, name, lagerplatz):
    deps = _load_print_dependencies()
    settings = load_settings()
    LOGGER.debug(
        "Starte Labeldruck sku=%s platz=%s printer_model=%s printer_uri=%s label_size=%s",
        sku,
        lagerplatz,
        settings.get("printer_model"),
        settings.get("printer_uri"),
        settings.get("label_size"),
    )

    image = deps["Image"]
    image_draw = deps["ImageDraw"]
    image_font = deps["ImageFont"]
    barcode_module = deps["barcode"]
    image_writer = deps["ImageWriter"]
    convert = deps["convert"]
    send = deps["send"]
    brother_ql_raster = deps["BrotherQLRaster"]

    img = image.new("RGB", (LABEL_WIDTH, LABEL_HEIGHT), "white")
    draw = image_draw.Draw(img)

    sku_font = fit_font(draw, sku, LABEL_WIDTH - 40, 100, image_font, settings)
    draw_centered(draw, sku, sku_font, 10, LABEL_WIDTH)

    code128 = barcode_module.get("code128", sku, writer=image_writer())
    barcode_img = code128.render(
        writer_options={
            "module_height": 18,
            "quiet_zone": 2,
            "write_text": False,
        }
    )

    barcode_width = 440
    barcode_height = 60
    barcode_img = barcode_img.resize((barcode_width, barcode_height))

    barcode_x = 20
    barcode_y = LABEL_HEIGHT - barcode_height - 10
    img.paste(barcode_img, (barcode_x, barcode_y))

    name_font = fit_font(draw, name, LABEL_WIDTH - 40, 42, image_font, settings)
    lines = wrap_text(draw, name, name_font, LABEL_WIDTH - 40)

    line_height = 42
    total_text_height = len(lines) * line_height
    name_y = barcode_y - total_text_height - 10

    for line in lines:
        draw_centered(draw, line, name_font, name_y, LABEL_WIDTH)
        name_y += line_height

    platz_font = _load_font(image_font, settings, 72, condensed=False)
    bbox = draw.textbbox((0, 0), lagerplatz, font=platz_font)
    text_w = bbox[2] - bbox[0]
    text_h = bbox[3] - bbox[1]
    platz_x = LABEL_WIDTH - text_w - 20
    platz_y = barcode_y + (barcode_height - text_h) // 2
    draw.text((platz_x, platz_y), lagerplatz, font=platz_font, fill="black")

    img.save("label_preview.png")

    qlr = brother_ql_raster(settings["printer_model"])
    instructions = convert(
        qlr=qlr,
        images=[img],
        label=settings["label_size"],
    )
    LOGGER.debug("Labeldaten erzeugt sku=%s bytes=%s", sku, len(instructions) if instructions is not None else 0)
    send(
        instructions=instructions,
        printer_identifier=settings["printer_uri"],
        backend_identifier="network",
    )
    LOGGER.info("Labeldruck erfolgreich sku=%s platz=%s", sku, lagerplatz)


def _usage():
    print("Usage:")
    print("python label_print.py SKU NAME MENGE REGAL FACH PLATZ")


def main():
    if len(sys.argv) < 7:
        _usage()
        return 1

    sku = sys.argv[1]
    name = sys.argv[2]
    platz = sys.argv[6] or ""

    try:
        print_label(sku, name, platz)
    except Exception as exc:
        LOGGER.exception("Labeldruck fehlgeschlagen fuer SKU=%s Platz=%s", sku, platz)
        print(f"Labeldruck fehlgeschlagen: {exc}", file=sys.stderr)
        print(f"Details im Log: {PRINT_LOG_PATH}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
