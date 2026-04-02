#!/usr/bin/env python3
"""Importiert eine Deutsche-Post-PPL-CSV in ein eigenes JSON-Mapping."""

import argparse
import csv
import json
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE_DIR = ROOT_DIR / "local_only" / "docs" / "apis" / "post"
DEFAULT_TARGET = ROOT_DIR / "data" / "post_products.json"


def parse_decimal(value):
    cleaned = str(value or "").strip().replace(".", "").replace(",", ".")
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def parse_int(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def normalize_text(value):
    return " ".join(str(value or "").strip().split())


def slugify(text):
    value = normalize_text(text).lower()
    replacements = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "ß": "ss",
        "&": "und",
        "+": "plus",
    }
    for old, new in replacements.items():
        value = value.replace(old, new)
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def infer_scope(code, variant):
    if str(variant or "").strip().upper() == "I":
        return "international"
    if str(code or "").isdigit() and int(code) >= 10000:
        return "international"
    return "domestic"


def infer_category(name):
    lower = normalize_text(name).lower()
    if "warensendung" in lower:
        return "goods"
    if "postkarte" in lower:
        return "postcard"
    if "einschreiben" in lower:
        return "registered"
    if "brief" in lower:
        return "letter"
    return "other"


def infer_base_product(name):
    lower = normalize_text(name).lower()
    if "postkarte" in lower:
        return "postcard"
    if "standardbrief" in lower:
        return "standardbrief"
    if "kompaktbrief" in lower:
        return "kompaktbrief"
    if "gro" in lower and "brief" in lower:
        return "grossbrief"
    if "maxibrief" in lower:
        return "maxibrief"
    if "warensendung" in lower:
        return "warensendung"
    return "other"


BASE_PRODUCT_LABELS = {
    "postcard": "Postkarte",
    "standardbrief": "Standardbrief",
    "kompaktbrief": "Kompaktbrief",
    "grossbrief": "Großbrief",
    "maxibrief": "Maxibrief",
    "warensendung": "Warensendung",
    "other": "Sonstiges",
}


def clean_base_display_name(name):
    value = normalize_text(name)
    value = value.replace(" Integral", "")
    value = value.replace(" Intern. GK", "")
    value = value.replace(" BZL GK", "")
    value = value.replace(" plus über 1.000g BZL GK", " bis 2000 g")
    value = value.replace(" plus über 1.000g", " bis 2000 g")
    return normalize_text(value)


def split_addon_parts(name):
    value = normalize_text(name)
    if not value:
        return []
    parts = [normalize_text(part) for part in value.split("+")]
    return [part for part in parts if part]


def infer_option_code(name):
    lower = normalize_text(name).lower()
    if "einschreiben einwurf" in lower:
        return "einschreiben_einwurf"
    if "einschreiben" in lower:
        return "einschreiben"
    if "r" in lower and "ckschein" in lower:
        return "rueckschein"
    if "zusatzentgelt mbf" in lower:
        return "mbf"
    if "gewichtszuschlag" in lower:
        return "gewichtszuschlag"
    return slugify(name)


def infer_addons(name):
    return [infer_option_code(part) for part in split_addon_parts(name)]


def parse_bool_flag(value):
    return str(value or "").strip().lower() == "ja"


def build_product(row):
    code = normalize_text(row.get("PROD_ID"))
    if not code:
        return None
    name = normalize_text(row.get("PROD_NAME"))
    if not name:
        return None

    price = parse_decimal(row.get("PROD_BRPREIS"))
    effective_from = normalize_text(row.get("PROD_GUEAB"))
    variant = normalize_text(row.get("PROD_AUSR"))
    tracked = normalize_text(row.get("T&T")) == "1"
    bp_name = normalize_text(row.get("BP_NAME"))
    add_name = normalize_text(row.get("ADD_NAME"))
    note = normalize_text(row.get("INTMA_HINWTEXT"))
    hint = normalize_text(row.get("PROD_ANM"))
    base_product = infer_base_product(name)
    base_display_name = BASE_PRODUCT_LABELS.get(base_product, clean_base_display_name(bp_name or name.split("+", 1)[0]))
    addon_parts = split_addon_parts(add_name)
    addons = infer_addons(add_name)
    product = {
        "product_code": code,
        "name": name,
        "selection_label": name,
        "price_eur": f"{price:.2f}" if price is not None else "",
        "price_cents": int(price * 100) if price is not None else None,
        "effective_from": effective_from,
        "scope": infer_scope(code, variant),
        "variant": variant or "N",
        "category": infer_category(name),
        "base_product": base_product,
        "base_label": base_display_name,
        "base_key": slugify(base_display_name),
        "addons": addons,
        "addon_labels": addon_parts,
        "tracked": tracked,
        "base_price_name": bp_name,
        "base_price_eur": f"{parse_decimal(row.get('BP_BRPREIS')):.2f}" if parse_decimal(row.get("BP_BRPREIS")) is not None else "",
        "addon_name": add_name,
        "addon_price_eur": f"{parse_decimal(row.get('ADD_BRPREIS')):.2f}" if parse_decimal(row.get("ADD_BRPREIS")) is not None else "",
        "min_length_mm": parse_int(row.get("MINL")),
        "min_width_mm": parse_int(row.get("MINB")),
        "min_height_mm": parse_int(row.get("MINH")),
        "max_length_mm": parse_int(row.get("MAXL")),
        "max_width_mm": parse_int(row.get("MAXB")),
        "max_height_mm": parse_int(row.get("MAXH")),
        "min_weight_g": parse_int(row.get("MING")),
        "max_weight_g": parse_int(row.get("MAXG")),
        "info_url": normalize_text(row.get("INTMA_PROD_URL")),
        "contract_required": parse_bool_flag(row.get("INTMA_VERTRAG")),
        "customs_required": parse_bool_flag(row.get("INTMA_ZOLLERKL")),
        "hint": hint,
        "description": note,
    }
    return product


def build_selection_groups(products):
    groups = {}
    options = {}
    for product in products:
        scope = product.get("scope") or "domestic"
        base_key = product.get("base_key") or slugify(product.get("base_label") or product.get("name") or "")
        if not base_key:
            continue
        group_key = f"{scope}:{base_key}"
        entry = groups.setdefault(
            group_key,
            {
                "group_key": group_key,
                "scope": scope,
                "base_key": base_key,
                "base_label": product.get("base_label") or product.get("name"),
                "base_product": product.get("base_product") or "other",
                "category": product.get("category") or "other",
                "tracked_variants": [],
                "untracked_variants": [],
                "option_codes": [],
            },
        )
        variant_info = {
            "product_code": product["product_code"],
            "label": product["selection_label"],
            "price_eur": product["price_eur"],
            "price_cents": product["price_cents"],
            "addons": product["addons"],
            "addon_labels": product["addon_labels"],
            "tracked": bool(product["tracked"]),
            "max_weight_g": product["max_weight_g"],
        }
        target = entry["tracked_variants"] if product.get("tracked") else entry["untracked_variants"]
        target.append(variant_info)
        for code, label in zip(product["addons"], product["addon_labels"]):
            if code not in entry["option_codes"]:
                entry["option_codes"].append(code)
            option_entry = options.setdefault(
                code,
                {
                    "option_code": code,
                    "label": label,
                    "tracked": code.startswith("einschreiben") or code == "rueckschein",
                    "used_by": [],
                },
            )
            if base_key not in option_entry["used_by"]:
                option_entry["used_by"].append(base_key)

    for entry in groups.values():
        entry["tracked_variants"].sort(key=lambda item: (item["price_cents"] or 10**12, item["product_code"]))
        entry["untracked_variants"].sort(key=lambda item: (item["price_cents"] or 10**12, item["product_code"]))
        entry["option_codes"].sort()
    for option in options.values():
        option["used_by"].sort()
    return {
        "base_products": sorted(groups.values(), key=lambda item: (item["scope"], item["category"], item["base_label"])),
        "options": sorted(options.values(), key=lambda item: item["label"]),
    }


def sort_key(product):
    return (
        product.get("scope") != "domestic",
        product.get("category") or "",
        product.get("base_product") or "",
        product.get("price_cents") if product.get("price_cents") is not None else 10**12,
        int(product.get("product_code") or 0),
    )


def import_csv(source_path, target_path):
    source = Path(source_path)
    target = Path(target_path)
    products = []
    with source.open("r", encoding="latin-1", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        for row in reader:
            product = build_product(row)
            if product:
                products.append(product)

    products.sort(key=sort_key)
    selection = build_selection_groups(products)
    payload = {
        "meta": {
            "source_filename": source.name,
            "product_count": len(products),
            "format": "deutsche-post-ppl-v1",
        },
        "selection": selection,
        "products": products,
    }
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2, sort_keys=True)
        handle.write("\n")
    return payload


def find_default_source():
    candidates = sorted(DEFAULT_SOURCE_DIR.glob("*.csv"))
    if not candidates:
        raise FileNotFoundError(f"Keine PPL-CSV unter {DEFAULT_SOURCE_DIR}")
    return candidates[-1]


def main():
    parser = argparse.ArgumentParser(description="Importiert die lokale Deutsche-Post-PPL-CSV.")
    parser.add_argument("--source", default="", help="Pfad zur PPL-CSV (Default: neueste CSV unter local_only/docs/apis/post)")
    parser.add_argument("--target", default=str(DEFAULT_TARGET), help="Zieldatei fuer das abgeleitete JSON-Mapping")
    args = parser.parse_args()

    source = Path(args.source) if args.source else find_default_source()
    payload = import_csv(source, args.target)
    print(f"Importiert: {payload['meta']['product_count']} Produkte")
    print(f"Quelle: {source}")
    print(f"Ziel: {Path(args.target)}")


if __name__ == "__main__":
    main()
