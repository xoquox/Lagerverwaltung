#!/usr/bin/env python3
"""Select the shop during installation; never migrate existing shop data implicitly."""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app_settings import load_settings, save_settings
from shipping.shop import SHOP_SYSTEMS


def main():
    parser = argparse.ArgumentParser(description="Shopsystem fuer Lager-MC auswaehlen")
    parser.add_argument("--shop-system", choices=SHOP_SYSTEMS)
    args = parser.parse_args()
    settings = load_settings()
    selected = args.shop_system
    if selected is None and sys.stdin.isatty():
        selected = (
            input(
                f"Shopsystem (shopify/woocommerce) [{settings['shop_system']}]: "
            ).strip()
            or settings["shop_system"]
        )
    settings["shop_system"] = selected or settings["shop_system"]
    save_settings(settings)
    print(f"Shopsystem: {SHOP_SYSTEMS[settings['shop_system']]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
