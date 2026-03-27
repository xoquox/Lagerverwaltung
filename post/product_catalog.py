#!/usr/bin/env python3
"""Hilfsfunktionen fuer das lokale Deutsche-Post-Produktmapping."""

import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
POST_PRODUCTS_PATH = ROOT_DIR / "data" / "post_products.json"


def load_post_products(path=None):
    source = Path(path) if path else POST_PRODUCTS_PATH
    if not source.exists():
        return {"meta": {}, "products": []}
    with source.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        return {"meta": {}, "products": []}
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    products = data.get("products") if isinstance(data.get("products"), list) else []
    selection = data.get("selection") if isinstance(data.get("selection"), dict) else {}
    return {"meta": meta, "selection": selection, "products": products}


def find_post_product(product_code, path=None):
    code = str(product_code or "").strip()
    if not code:
        return None
    for product in load_post_products(path=path)["products"]:
        if str(product.get("product_code", "")).strip() == code:
            return product
    return None


def list_post_products(path=None, *, category=None, domestic_only=False, tracked_only=None):
    products = load_post_products(path=path)["products"]
    result = []
    for product in products:
        if category and product.get("category") != category:
            continue
        if domestic_only and product.get("scope") != "domestic":
            continue
        if tracked_only is not None and bool(product.get("tracked")) != bool(tracked_only):
            continue
        result.append(product)
    return result


def list_post_base_products(path=None, *, scope=None):
    selection = load_post_products(path=path).get("selection") or {}
    groups = selection.get("base_products") if isinstance(selection.get("base_products"), list) else []
    if not scope:
        return groups
    return [group for group in groups if group.get("scope") == scope]


def list_post_options(path=None):
    selection = load_post_products(path=path).get("selection") or {}
    options = selection.get("options") if isinstance(selection.get("options"), list) else []
    return options
