#!/usr/bin/env python3
import getpass
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from shopify_sync import run_manual_connect_flow

DEFAULT_SCOPES = (
    "read_customers,read_inventory,read_locations,"
    "read_merchant_managed_fulfillment_orders,read_orders,read_products,"
    "write_inventory,write_merchant_managed_fulfillment_orders"
)

def prompt(label, default="", secret=False):
    suffix = f" [{default}]" if default else ""
    text = f"{label}{suffix}: "
    if secret:
        return getpass.getpass(text).strip()
    value = input(text).strip()
    if value:
        return value
    return default.strip()


def main():
    print("Lager-MC Shopify Manual OAuth")
    print("Lokaler OAuth-Flow ohne serverseitigen Tokenaustausch.")
    print("")

    shop = prompt("Shop")
    client_id = prompt("Client ID")
    client_secret = prompt("Client Secret", secret=True)
    scopes = prompt("Scopes", default=DEFAULT_SCOPES)
    redirect_uri = prompt("Redirect URI")

    if not shop or not client_id or not client_secret or not scopes or not redirect_uri:
        raise SystemExit("Alle Eingaben sind erforderlich.")

    result = run_manual_connect_flow(
        shop=shop,
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes,
        redirect_uri=redirect_uri,
        open_browser=True,
    )
    print("")
    print("OK: Token in .env gespeichert.")
    print(f"Shop: {result['shop']}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
