#!/usr/bin/env python3
"""Client fuer Deutsche Post INTERNETMARKE."""

import json
import ssl
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


class InternetmarkeClient:
    def __init__(self, api_url, partner_id, api_key="", api_secret="", user="", password=""):
        self.api_url = (api_url or "").strip().rstrip("/")
        self.partner_id = (partner_id or "").strip()
        self.api_key = (api_key or "").strip()
        self.api_secret = (api_secret or "").strip()
        self.user = (user or "").strip()
        self.password = password or ""
        self._access_token = ""
        self._token_expires_at = 0.0

    def validate(self):
        missing = []
        if not self.api_url:
            missing.append("api_url")
        if not self.partner_id:
            missing.append("partner_id")
        has_oauth = bool(self.api_key and self.api_secret)
        has_legacy = bool(self.user and self.password)
        if not has_oauth:
            missing.append("api_key/api_secret")
        if not has_legacy:
            missing.append("user/password")
        if missing:
            raise RuntimeError(f"INTERNETMARKE: fehlende Felder: {', '.join(missing)}")

    def create_label(self, shipment):
        self.validate()
        raise RuntimeError("INTERNETMARKE API-Call noch nicht aktiviert.")

    def api_version(self):
        return self._json_request("GET", "/", requires_auth=False)

    def authorize(self, force=False):
        self.validate()
        now = time.time()
        if self._access_token and not force and now < self._token_expires_at:
            return {
                "access_token": self._access_token,
                "expires_in": max(0, int(self._token_expires_at - now)),
            }

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.api_key,
            "client_secret": self.api_secret,
            "username": self.user,
            "password": self.password,
        }
        response = self._json_request(
            "POST",
            "/user",
            payload=payload,
            content_type="application/x-www-form-urlencoded",
            requires_auth=False,
        )
        token = (response.get("access_token") or "").strip()
        if not token:
            raise RuntimeError("INTERNETMARKE: access_token fehlt in der Antwort.")
        expires_in = int(response.get("expires_in") or 0)
        self._access_token = token
        self._token_expires_at = now + max(60, expires_in - 30 if expires_in else 300)
        return response

    def get_profile(self):
        self._ensure_token()
        return self._json_request("GET", "/user/profile")

    def get_catalog(self, types=("PUBLIC", "PAGE_FORMATS")):
        self._ensure_token()
        cleaned = []
        for value in types or ():
            item = str(value).strip().upper()
            if item and item not in cleaned:
                cleaned.append(item)
        if not cleaned:
            cleaned = ["PUBLIC", "PAGE_FORMATS"]
        return self._json_request("GET", "/app/catalog", query={"types": cleaned})

    def get_page_formats(self):
        response = self.get_catalog(types=("PAGE_FORMATS",))
        return response.get("pageFormats") or []

    def preview_pdf(self, product_code, page_format_id, voucher_layout="ADDRESS_ZONE", image_id=None, dpi="DPI300"):
        self._ensure_token()
        payload = {
            "type": "AppShoppingCartPreviewPDFRequest",
            "productCode": int(product_code),
            "voucherLayout": str(voucher_layout or "ADDRESS_ZONE").strip().upper(),
            "pageFormatId": int(page_format_id),
            "dpi": str(dpi or "DPI300").strip().upper(),
        }
        if image_id not in (None, ""):
            payload["imageID"] = int(image_id)
        return self._json_request(
            "POST",
            "/app/shoppingcart/pdf",
            payload=payload,
            query={"validate": "true"},
        )

    def download_binary(self, url):
        request = Request(url, method="GET")
        ctx = ssl.create_default_context()
        try:
            with urlopen(request, timeout=60, context=ctx) as response:
                return response.read()
        except HTTPError as exc:
            raw = exc.read() if hasattr(exc, "read") else b""
            raise RuntimeError(self._format_http_error("INTERNETMARKE Download", exc.code, raw)) from exc
        except URLError as exc:
            raise RuntimeError(f"INTERNETMARKE Netzwerkfehler: {exc.reason}") from exc

    def preview_pdf_binary(self, product_code, page_format_id, voucher_layout="ADDRESS_ZONE", image_id=None, dpi="DPI300"):
        response = self.preview_pdf(
            product_code=product_code,
            page_format_id=page_format_id,
            voucher_layout=voucher_layout,
            image_id=image_id,
            dpi=dpi,
        )
        link = (response.get("link") or "").strip()
        if not link:
            raise RuntimeError("INTERNETMARKE Preview: link fehlt in der Antwort.")
        return self.download_binary(link)

    def _ensure_token(self):
        if not self._access_token or time.time() >= self._token_expires_at:
            self.authorize()

    def _json_request(self, method, path, payload=None, query=None, content_type="application/json", requires_auth=True):
        if requires_auth:
            self._ensure_token()
        body = None
        headers = {
            "Accept": "application/json",
            "X-Partner-Id": self.partner_id,
        }
        if requires_auth:
            headers["Authorization"] = f"Bearer {self._access_token}"
        if content_type == "application/x-www-form-urlencoded":
            body = urlencode(payload or {}).encode("utf-8")
            headers["Content-Type"] = f"{content_type}; charset=UTF-8"
        elif payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        url = self._build_url(path, query=query)
        request = Request(url, data=body, headers=headers, method=method.upper())
        ctx = ssl.create_default_context()
        try:
            with urlopen(request, timeout=60, context=ctx) as response:
                raw = response.read()
        except HTTPError as exc:
            raw = exc.read() if hasattr(exc, "read") else b""
            raise RuntimeError(self._format_http_error("INTERNETMARKE", exc.code, raw)) from exc
        except URLError as exc:
            raise RuntimeError(f"INTERNETMARKE Netzwerkfehler: {exc.reason}") from exc

        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8", errors="replace"))
        except json.JSONDecodeError as exc:
            raise RuntimeError("INTERNETMARKE: Antwort ist kein gueltiges JSON.") from exc

    def _build_url(self, path, query=None):
        url = f"{self.api_url}{path}"
        if not query:
            return url
        items = []
        for key, value in query.items():
            if isinstance(value, (list, tuple)):
                for entry in value:
                    items.append((key, str(entry)))
            else:
                items.append((key, str(value)))
        return f"{url}?{urlencode(items)}"

    @staticmethod
    def _format_http_error(prefix, status_code, raw):
        detail = ""
        if raw:
            try:
                parsed = json.loads(raw.decode("utf-8", errors="replace"))
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, dict):
                title = (parsed.get("title") or "").strip()
                info = (parsed.get("detail") or "").strip()
                detail = " - ".join(part for part in (title, info) if part)
            if not detail:
                detail = raw.decode("utf-8", errors="replace").strip()
        if detail:
            return f"{prefix} HTTP {status_code}: {detail[:500]}"
        return f"{prefix} HTTP {status_code}"
