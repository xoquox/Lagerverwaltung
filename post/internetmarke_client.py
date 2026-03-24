#!/usr/bin/env python3
"""
Vorbereiteter Client fuer Deutsche Post INTERNETMARKE.

Diese Datei enthaelt bewusst nur eine stabile Schnittstelle, damit der
eigentliche API-Call im naechsten Implementierungsschritt sauber angeschlossen
werden kann.
"""


class InternetmarkeClient:
    def __init__(self, api_url, user, password, partner_id):
        self.api_url = (api_url or "").strip()
        self.user = (user or "").strip()
        self.password = password or ""
        self.partner_id = (partner_id or "").strip()

    def validate(self):
        missing = []
        if not self.api_url:
            missing.append("api_url")
        if not self.user:
            missing.append("user")
        if not self.password:
            missing.append("password")
        if not self.partner_id:
            missing.append("partner_id")
        if missing:
            raise RuntimeError(f"INTERNETMARKE: fehlende Felder: {', '.join(missing)}")

    def create_label(self, shipment):
        self.validate()
        raise RuntimeError("INTERNETMARKE API-Call noch nicht aktiviert.")

