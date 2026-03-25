#!/usr/bin/env python3
"""
Vorbereiteter Client fuer Deutsche Post INTERNETMARKE.

Diese Datei enthaelt bewusst nur eine stabile Schnittstelle, damit der
eigentliche API-Call im naechsten Implementierungsschritt sauber angeschlossen
werden kann.
"""


class InternetmarkeClient:
    def __init__(self, api_url, partner_id, api_key="", api_secret="", user="", password=""):
        self.api_url = (api_url or "").strip()
        self.partner_id = (partner_id or "").strip()
        self.api_key = (api_key or "").strip()
        self.api_secret = (api_secret or "").strip()
        self.user = (user or "").strip()
        self.password = password or ""

    def validate(self):
        missing = []
        if not self.api_url:
            missing.append("api_url")
        if not self.partner_id:
            missing.append("partner_id")
        has_oauth = bool(self.api_key and self.api_secret)
        has_legacy = bool(self.user and self.password)
        if not has_oauth and not has_legacy:
            missing.append("api_key/api_secret oder user/password")
        if missing:
            raise RuntimeError(f"INTERNETMARKE: fehlende Felder: {', '.join(missing)}")

    def create_label(self, shipment):
        self.validate()
        raise RuntimeError("INTERNETMARKE API-Call noch nicht aktiviert.")
