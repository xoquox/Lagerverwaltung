#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app_settings import load_settings  # noqa: E402
from post.internetmarke_client import InternetmarkeClient  # noqa: E402


def build_client():
    settings = load_settings()
    return InternetmarkeClient(
        api_url=settings.get("post_api_url", ""),
        partner_id=settings.get("post_partner_id", ""),
        api_key=settings.get("post_api_key", ""),
        api_secret=settings.get("post_api_secret", ""),
        user=settings.get("post_user", ""),
        password=settings.get("post_password", ""),
    )


def main():
    parser = argparse.ArgumentParser(description="Probe fuer Deutsche Post INTERNETMARKE.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("version")
    sub.add_parser("auth")
    sub.add_parser("profile")
    sub.add_parser("catalog")
    sub.add_parser("page-formats")

    preview = sub.add_parser("preview-pdf")
    preview.add_argument("--product-code", type=int, required=True)
    preview.add_argument("--page-format-id", type=int, required=True)
    preview.add_argument("--voucher-layout", default="ADDRESS_ZONE")
    preview.add_argument("--image-id", type=int)
    preview.add_argument("--dpi", default="DPI300")
    preview.add_argument("--download", help="Pfad zum Speichern der Preview-PDF")

    args = parser.parse_args()
    client = build_client()

    if args.command == "version":
        result = client.api_version()
    elif args.command == "auth":
        result = client.authorize(force=True)
    elif args.command == "profile":
        result = client.get_profile()
    elif args.command == "catalog":
        result = client.get_catalog()
    elif args.command == "page-formats":
        result = client.get_page_formats()
    elif args.command == "preview-pdf":
        result = client.preview_pdf(
            product_code=args.product_code,
            page_format_id=args.page_format_id,
            voucher_layout=args.voucher_layout,
            image_id=args.image_id,
            dpi=args.dpi,
        )
        if args.download:
            target = Path(args.download).expanduser()
            pdf_bytes = client.download_binary(result.get("link", ""))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(pdf_bytes)
            print(str(target))
            return
    else:
        raise SystemExit(2)

    print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
