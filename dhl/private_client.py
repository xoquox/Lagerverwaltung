#!/usr/bin/env python3
"""
Vorbereiteter Client fuer DHL Parcel DE Private Shipping.

Die Schnittstelle ist absichtlich stabil gehalten. Die eigentlichen API-Calls
werden erst aktiviert, wenn Endpunkte, Auth-Flow und Produktmapping final
hinterlegt sind.
"""


class DHLPrivateClient:
    def __init__(self, api_url, test_api_url, api_key, api_secret, use_test_api=False):
        self.api_url = (api_url or "").strip()
        self.test_api_url = (test_api_url or "").strip()
        self.api_key = (api_key or "").strip()
        self.api_secret = (api_secret or "").strip()
        self.use_test_api = bool(use_test_api)

    def active_api_url(self):
        if self.use_test_api and self.test_api_url:
            return self.test_api_url
        return self.api_url

    def validate(self):
        missing = []
        if not self.active_api_url():
            missing.append("api_url" if not self.use_test_api else "api_test_url")
        if not self.api_key:
            missing.append("api_key")
        if not self.api_secret:
            missing.append("api_secret")
        if missing:
            raise RuntimeError(f"DHL Private Shipping: fehlende Felder: {', '.join(missing)}")

    def create_label(self, shipment):
        self.validate()
        raise RuntimeError("DHL Private Shipping API-Call noch nicht aktiviert.")
