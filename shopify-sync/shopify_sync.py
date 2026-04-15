import os
import time
import json
import hmac
import argparse
import datetime
import hashlib
import logging
import secrets
import sys
import threading
import urllib.parse
import webbrowser
from logging.handlers import RotatingFileHandler
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

try:
    import psycopg2
    import psycopg2.extras
except ModuleNotFoundError:
    psycopg2 = None

try:
    import requests
except ModuleNotFoundError:
    requests = None

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv():
        return None


def resolve_sync_base_dir(script_path=None):
    app_dir = Path(script_path or __file__).resolve().parent
    repo_root = app_dir.parent
    if (repo_root / "shipping").is_dir():
        return repo_root
    return app_dir


BASE_DIR = resolve_sync_base_dir()
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from shipping.history import (
    SHIPPING_LABEL_TABLE,
    claim_shopify_fulfillment_jobs as _claim_shopify_fulfillment_jobs,
    mark_shopify_fulfillment_job_done as _mark_shopify_fulfillment_job_done,
    mark_shopify_fulfillment_job_failed as _mark_shopify_fulfillment_job_failed,
    upsert_shopify_shipment as _upsert_shopify_shipment_record,
)
from shipping.schema import apply_app_schema, collect_schema_issues
from sync_version import SYNC_VERSION

load_dotenv()

LOG_DIR = BASE_DIR / "logs"
SYNC_LOG_PATH = LOG_DIR / "shopify-sync.log"

SHOP = os.getenv("SHOP")
TOKEN = os.getenv("TOKEN")
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")
TOKEN_EXPIRES_AT = os.getenv("TOKEN_EXPIRES_AT")
REFRESH_TOKEN_EXPIRES_AT = os.getenv("REFRESH_TOKEN_EXPIRES_AT")
TOKEN_SCOPE = os.getenv("TOKEN_SCOPE")
SHOPIFY_CONNECT_BASE_URL = os.getenv("SHOPIFY_CONNECT_BASE_URL")

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

API_VERSION = "2026-04"
SHOPIFY_LOCATION_ID = str(os.getenv("SHOPIFY_LOCATION_ID") or "67402989753").strip()
GRAPHQL_URL = f"https://{SHOP}/admin/api/{API_VERSION}/graphql.json"
SYNC_INTERVAL = 60
REQUEST_TIMEOUT_SECONDS = 45
DEFAULT_CONNECT_TIMEOUT_SECONDS = 300
TOKEN_REFRESH_MARGIN_SECONDS = 300
_LOGGER = None
SYNC_ENV_PATH = Path(__file__).resolve().with_name(".env")


def configure_logging():
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    LOG_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger("lagerverwaltung.shopify_sync")
    logger.setLevel(getattr(logging, os.getenv("LAGERVERWALTUNG_LOG_LEVEL", "INFO").strip().upper(), logging.INFO))
    logger.propagate = False

    if not logger.handlers:
        handler = RotatingFileHandler(SYNC_LOG_PATH, maxBytes=1_000_000, backupCount=5, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
        logger.addHandler(handler)

    _LOGGER = logger
    return logger


def log_info(message, *args):
    rendered = message % args if args else message
    print(rendered)
    configure_logging().info(rendered)


def log_warning(message, *args):
    rendered = message % args if args else message
    print(rendered)
    configure_logging().warning(rendered)


def log_error(message, *args):
    rendered = message % args if args else message
    print(rendered)
    configure_logging().error(rendered)


def log_exception(message, *args):
    rendered = message % args if args else message
    print(rendered)
    configure_logging().exception(rendered)


def shorten_text(value, limit=400):
    text = "" if value is None else str(value).replace("\n", "\\n")
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)] + "..."


def _to_int_or_none(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(text)
    except (TypeError, ValueError):
        return None


def _token_bundle_from_payload(payload):
    token = (payload.get("token") or payload.get("access_token") or "").strip()
    refresh_token = (payload.get("refresh_token") or "").strip()
    token_expires_at = _to_int_or_none(payload.get("token_expires_at") or payload.get("expires_at"))
    refresh_token_expires_at = _to_int_or_none(payload.get("refresh_token_expires_at"))
    now = int(time.time())
    if token_expires_at is None:
        expires_in = _to_int_or_none(payload.get("expires_in"))
        if expires_in and expires_in > 0:
            token_expires_at = now + expires_in
    if refresh_token_expires_at is None:
        refresh_expires_in = _to_int_or_none(payload.get("refresh_token_expires_in"))
        if refresh_expires_in and refresh_expires_in > 0:
            refresh_token_expires_at = now + refresh_expires_in
    if not token:
        raise RuntimeError("Kein access token erhalten.")
    return {
        "token": token,
        "refresh_token": refresh_token,
        "token_expires_at": token_expires_at,
        "refresh_token_expires_at": refresh_token_expires_at,
        "scope": (payload.get("scope") or "").strip(),
    }


def _apply_token_bundle(bundle):
    global TOKEN, REFRESH_TOKEN, TOKEN_EXPIRES_AT, REFRESH_TOKEN_EXPIRES_AT, TOKEN_SCOPE
    TOKEN = bundle["token"]
    if bundle.get("refresh_token"):
        REFRESH_TOKEN = bundle["refresh_token"]
    TOKEN_EXPIRES_AT = bundle.get("token_expires_at")
    REFRESH_TOKEN_EXPIRES_AT = bundle.get("refresh_token_expires_at")
    TOKEN_SCOPE = bundle.get("scope") or TOKEN_SCOPE


def _env_updates_from_token_bundle(bundle):
    updates = {"TOKEN": bundle["token"]}
    if bundle.get("refresh_token"):
        updates["REFRESH_TOKEN"] = bundle["refresh_token"]
    if bundle.get("token_expires_at"):
        updates["TOKEN_EXPIRES_AT"] = bundle["token_expires_at"]
    if bundle.get("refresh_token_expires_at"):
        updates["REFRESH_TOKEN_EXPIRES_AT"] = bundle["refresh_token_expires_at"]
    if bundle.get("scope"):
        updates["TOKEN_SCOPE"] = bundle["scope"]
    return updates


def _token_should_refresh(force=False):
    if force:
        return True
    if not REFRESH_TOKEN:
        return False
    if not TOKEN:
        return True
    expires_at = _to_int_or_none(TOKEN_EXPIRES_AT)
    if not expires_at:
        return False
    return int(time.time()) >= expires_at - TOKEN_REFRESH_MARGIN_SECONDS


def _refresh_access_token(force=False):
    ensure_runtime_dependencies()
    if not _token_should_refresh(force=force):
        return False
    if not REFRESH_TOKEN:
        if force:
            raise RuntimeError("REFRESH_TOKEN fehlt. Bitte Shopify-Verbindung neu einrichten.")
        return False
    relay_base_url = (SHOPIFY_CONNECT_BASE_URL or "").strip().rstrip("/")
    if not relay_base_url:
        raise RuntimeError("SHOPIFY_CONNECT_BASE_URL fehlt.")
    refresh_expires_at = _to_int_or_none(REFRESH_TOKEN_EXPIRES_AT)
    if refresh_expires_at and int(time.time()) >= refresh_expires_at:
        raise RuntimeError("Refresh-Token ist abgelaufen. Bitte Shopify-Verbindung neu einrichten.")
    response = _post_refresh_request(
        relay_base_url,
        {"shop": SHOP, "refresh_token": REFRESH_TOKEN},
    )
    if getattr(response, "status_code", 0) >= 400:
        log_error(
            "Token-Refresh fehlgeschlagen status=%s url=%s body=%s",
            getattr(response, "status_code", "-"),
            getattr(response, "url", "-"),
            shorten_text(getattr(response, "text", "")),
        )
    response.raise_for_status()
    bundle = _token_bundle_from_payload(response.json())
    write_sync_env_values(_env_updates_from_token_bundle(bundle))
    _apply_token_bundle(bundle)
    log_info("Shopify Access Token wurde erneuert.")
    return True


def _post_refresh_request(relay_base_url, payload):
    url = f"{relay_base_url.rstrip('/')}/shopify/refresh"
    redirect_limit = 5
    for _ in range(redirect_limit):
        response = requests.post(
            url,
            json=payload,
            timeout=REQUEST_TIMEOUT_SECONDS,
            allow_redirects=False,
        )
        if getattr(response, "status_code", 0) not in {301, 302, 303, 307, 308}:
            return response
        location = (
            getattr(response, "headers", {}).get("Location")
            or getattr(response, "headers", {}).get("location")
            or ""
        ).strip()
        if not location:
            return response
        url = urllib.parse.urljoin(url, location)
    return response


def _normalize_shop_domain(value):
    raw = (value or "").strip().lower()
    if not raw:
        raise RuntimeError("Shop-Domain fehlt.")
    if raw.startswith("https://"):
        raw = raw[8:]
    elif raw.startswith("http://"):
        raw = raw[7:]
    raw = raw.split("/", 1)[0].strip()
    if not raw.endswith(".myshopify.com"):
        raise RuntimeError("Shop muss auf .myshopify.com enden.")
    allowed = set("abcdefghijklmnopqrstuvwxyz0123456789-.")
    if any(ch not in allowed for ch in raw):
        raise RuntimeError("Shop enthaelt ungueltige Zeichen.")
    if ".." in raw or raw.startswith(".") or raw.endswith("."):
        raise RuntimeError("Shop-Domain ist ungueltig.")
    return raw


def _loopback_callback_url(port):
    return f"http://127.0.0.1:{int(port)}/callback"


def _build_connected_page_url(shop, status="success", error_message=None):
    base_url = (SHOPIFY_CONNECT_BASE_URL or "").strip().rstrip("/")
    if not base_url:
        return None
    params = {"shop": shop, "status": status}
    if error_message:
        params["error"] = error_message
    return f"{base_url}/connected/?{urllib.parse.urlencode(params)}"


def _build_connect_url(shop, relay_base_url, state, return_to):
    base_url = (relay_base_url or "").strip().rstrip("/")
    if not base_url:
        raise RuntimeError("SHOPIFY_CONNECT_BASE_URL fehlt.")
    params = urllib.parse.urlencode(
        {
            "shop": _normalize_shop_domain(shop),
            "state": state,
            "return_to": return_to,
        }
    )
    return f"{base_url}/shopify/install?{params}"


def _build_manual_auth_url(shop, client_id, scopes, redirect_uri, state):
    normalized_shop = _normalize_shop_domain(shop)
    query = urllib.parse.urlencode(
        {
            "client_id": (client_id or "").strip(),
            "scope": (scopes or "").strip(),
            "redirect_uri": (redirect_uri or "").strip(),
            "state": state,
        }
    )
    return f"https://{normalized_shop}/admin/oauth/authorize?{query}"


def _location_gid(location_id=None):
    location_id = str(location_id if location_id is not None else SHOPIFY_LOCATION_ID or "").strip()
    if not location_id:
        raise RuntimeError("SHOPIFY_LOCATION_ID fehlt. shopify-sync/.env pruefen.")
    if location_id.startswith("gid://"):
        return location_id
    return f"gid://shopify/Location/{location_id}"


def _canonical_inventory_item_id(value):
    text = (value or "").strip()
    if not text:
        return ""
    if text.startswith("gid://"):
        return text
    return f"gid://shopify/InventoryItem/{text}"


def _display_sku_for_variant(variant, inventory_item):
    return (variant.get("sku") or inventory_item.get("sku") or "").strip()


def _storage_sku_for_variant(variant, inventory_item):
    display_sku = _display_sku_for_variant(variant, inventory_item)
    if display_sku:
        return display_sku
    variant_id = (variant.get("id") or "").strip()
    inventory_item_id = _canonical_inventory_item_id(inventory_item.get("id"))
    fallback_id = variant_id or inventory_item_id
    if not fallback_id:
        raise RuntimeError("Produktvariante ohne SKU und ohne Shopify-ID gefunden.")
    tail = fallback_id.rsplit("/", 1)[-1]
    return f"__shopify_variant__{tail}"


def _upsert_env_lines(lines, updates):
    normalized_updates = {key: str(value) for key, value in updates.items() if value is not None}
    seen = set()
    rendered = []
    for raw_line in lines:
        line = raw_line.rstrip("\n")
        key, sep, _value = line.partition("=")
        if sep and key in normalized_updates:
            rendered.append(f"{key}={normalized_updates[key]}\n")
            seen.add(key)
        else:
            rendered.append(raw_line if raw_line.endswith("\n") else raw_line + "\n")
    for key, value in normalized_updates.items():
        if key not in seen:
            rendered.append(f"{key}={value}\n")
    return rendered


def write_sync_env_values(updates, env_path=SYNC_ENV_PATH):
    path = Path(env_path)
    existing_lines = path.read_text(encoding="utf-8").splitlines(True) if path.exists() else []
    updated_lines = _upsert_env_lines(existing_lines, updates)
    path.write_text("".join(updated_lines), encoding="utf-8")


def _wait_for_local_oauth_callback(port, expected_state, timeout_seconds):
    result = {"shop": None, "token": None, "refresh_token": None, "token_expires_at": None, "refresh_token_expires_at": None, "scope": None, "state": None, "error": None}
    done = threading.Event()

    class CallbackHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            return None

        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            result["state"] = (params.get("state") or [""])[0]
            result["shop"] = (params.get("shop") or [""])[0]
            result["token"] = (params.get("token") or [""])[0]
            result["refresh_token"] = (params.get("refresh_token") or [""])[0]
            result["token_expires_at"] = (params.get("token_expires_at") or [""])[0]
            result["refresh_token_expires_at"] = (params.get("refresh_token_expires_at") or [""])[0]
            result["scope"] = (params.get("scope") or [""])[0]
            result["error"] = (params.get("error") or [""])[0]

            if parsed.path != "/callback":
                self.send_response(404)
                self.end_headers()
                done.set()
                return
            if result["state"] != expected_state:
                self.send_response(400)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"Ungueltiger state.")
                result["error"] = result["error"] or "state_mismatch"
                done.set()
                return

            if result["error"]:
                body = "Shopify-Verbindung fehlgeschlagen. Das Browserfenster kann geschlossen werden."
                redirect_target = _build_connected_page_url(result["shop"], status="error", error_message=result["error"])
            else:
                body = "Shopify-Verbindung gespeichert. Das Browserfenster kann geschlossen werden."
                redirect_target = _build_connected_page_url(result["shop"], status="success")
            quoted_redirect = urllib.parse.quote(redirect_target, safe=":/?&=%") if redirect_target else ""
            refresh_meta = (
                f'<meta http-equiv="refresh" content="2; url={quoted_redirect}">'
                if quoted_redirect
                else ""
            )
            redirect_html = (
                (
                    "<p class='meta'>Weiterleitung zur Statusseite ...<br>"
                    f"<a href='{quoted_redirect}'>{quoted_redirect}</a></p>"
                )
                if quoted_redirect
                else "<p class='meta'>Das Browserfenster kann jetzt geschlossen werden.</p>"
            )
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            html_body = (
                "<!doctype html><html lang='de'><head><meta charset='utf-8'>"
                "<meta name='viewport' content='width=device-width, initial-scale=1'>"
                f"{refresh_meta}"
                "<title>Lager-MC Verbindung</title>"
                "<style>body{margin:0;background:#f3f0e8;color:#1f1a15;font-family:Georgia,'Times New Roman',serif;display:grid;place-items:center;min-height:100vh}"
                ".card{max-width:640px;margin:24px;padding:28px 30px;border:1px solid #d8cfc0;border-radius:24px;background:#fffdf8;box-shadow:0 20px 50px rgba(68,53,35,.12)}"
                "h1{margin:0 0 10px;font-size:34px}.lead{margin:0;color:#6c6258;line-height:1.6}.meta{margin-top:16px;font-size:14px;color:#6c6258}"
                "a{color:#1e6a52}</style></head><body><main class='card'>"
                "<h1>Lager-MC Verbindung</h1>"
                f"<p class='lead'>{body}</p>"
                + redirect_html
                + "</main></body></html>"
            )
            self.wfile.write(html_body.encode("utf-8"))
            done.set()

    server = ThreadingHTTPServer(("127.0.0.1", int(port)), CallbackHandler)
    server.timeout = 0.5
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True)
    thread.start()
    try:
        if not done.wait(timeout=float(timeout_seconds)):
            raise RuntimeError("Timeout beim Warten auf den Shopify-OAuth-Callback.")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    if result["error"]:
        raise RuntimeError(f"Shopify-Verbindung fehlgeschlagen: {result['error']}")
    if not result["shop"] or not result["token"]:
        raise RuntimeError("Shopify-OAuth-Callback war unvollstaendig.")
    bundle = _token_bundle_from_payload(result)
    bundle["shop"] = _normalize_shop_domain(result["shop"])
    return bundle


def _wait_for_local_manual_oauth_callback(port, expected_state, timeout_seconds):
    result = {"shop": None, "code": None, "state": None, "error": None}
    done = threading.Event()

    class CallbackHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            return None

        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
            result["state"] = (params.get("state") or [""])[0]
            result["shop"] = (params.get("shop") or [""])[0]
            result["code"] = (params.get("code") or [""])[0]
            result["error"] = (params.get("error") or [""])[0]

            if parsed.path != "/callback":
                self.send_response(404)
                self.end_headers()
                done.set()
                return
            if result["state"] != expected_state:
                self.send_response(400)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"Ungueltiger state.")
                result["error"] = result["error"] or "state_mismatch"
                done.set()
                return

            if result["error"]:
                body = "Shopify-Verbindung fehlgeschlagen. Das Browserfenster kann geschlossen werden."
                redirect_target = _build_connected_page_url(result["shop"], status="error", error_message=result["error"])
            else:
                body = "Shopify-Verbindung gespeichert. Das Browserfenster kann geschlossen werden."
                redirect_target = _build_connected_page_url(result["shop"], status="success")
            quoted_redirect = urllib.parse.quote(redirect_target, safe=":/?&=%") if redirect_target else ""
            refresh_meta = (
                f'<meta http-equiv="refresh" content="2; url={quoted_redirect}">'
                if quoted_redirect
                else ""
            )
            redirect_html = (
                (
                    "<p class='meta'>Weiterleitung zur Statusseite ...<br>"
                    f"<a href='{quoted_redirect}'>{quoted_redirect}</a></p>"
                )
                if quoted_redirect
                else "<p class='meta'>Das Browserfenster kann jetzt geschlossen werden.</p>"
            )
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            html_body = (
                "<!doctype html><html lang='de'><head><meta charset='utf-8'>"
                "<meta name='viewport' content='width=device-width, initial-scale=1'>"
                f"{refresh_meta}"
                "<title>Lager-MC Verbindung</title>"
                "<style>body{margin:0;background:#f3f0e8;color:#1f1a15;font-family:Georgia,'Times New Roman',serif;display:grid;place-items:center;min-height:100vh}"
                ".card{max-width:640px;margin:24px;padding:28px 30px;border:1px solid #d8cfc0;border-radius:24px;background:#fffdf8;box-shadow:0 20px 50px rgba(68,53,35,.12)}"
                "h1{margin:0 0 10px;font-size:34px}.lead{margin:0;color:#6c6258;line-height:1.6}.meta{margin-top:16px;font-size:14px;color:#6c6258}"
                "a{color:#1e6a52}</style></head><body><main class='card'>"
                "<h1>Lager-MC Verbindung</h1>"
                f"<p class='lead'>{body}</p>"
                + redirect_html
                + "</main></body></html>"
            )
            self.wfile.write(html_body.encode("utf-8"))
            done.set()

    server = ThreadingHTTPServer(("127.0.0.1", int(port)), CallbackHandler)
    server.timeout = 0.5
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.2}, daemon=True)
    thread.start()
    try:
        if not done.wait(timeout=float(timeout_seconds)):
            raise RuntimeError("Timeout beim Warten auf den Shopify-OAuth-Callback.")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    if result["error"]:
        raise RuntimeError(f"Shopify-Verbindung fehlgeschlagen: {result['error']}")
    if not result["shop"] or not result["code"]:
        raise RuntimeError("Shopify-OAuth-Callback war unvollstaendig.")
    if result["state"] != expected_state:
        raise RuntimeError("Shopify-OAuth-Callback hatte einen ungueltigen state.")
    return {"shop": _normalize_shop_domain(result["shop"]), "code": result["code"]}


def run_connect_flow(shop, relay_base_url=None, port=3459, timeout_seconds=DEFAULT_CONNECT_TIMEOUT_SECONDS, open_browser=True):
    normalized_shop = _normalize_shop_domain(shop)
    state = secrets.token_urlsafe(24)
    return_to = _loopback_callback_url(port)
    connect_url = _build_connect_url(
        shop=normalized_shop,
        relay_base_url=relay_base_url or SHOPIFY_CONNECT_BASE_URL,
        state=state,
        return_to=return_to,
    )
    print("Install-Link:")
    print(connect_url)
    if open_browser:
        webbrowser.open(connect_url)
    callback_payload = _wait_for_local_oauth_callback(
        port=port,
        expected_state=state,
        timeout_seconds=timeout_seconds,
    )
    write_sync_env_values({"SHOP": callback_payload["shop"], **_env_updates_from_token_bundle(callback_payload)})
    _apply_token_bundle(callback_payload)
    return callback_payload


def run_manual_connect_flow(
    shop,
    client_id,
    client_secret,
    scopes,
    redirect_uri,
    port=3459,
    timeout_seconds=DEFAULT_CONNECT_TIMEOUT_SECONDS,
    open_browser=True,
):
    normalized_shop = _normalize_shop_domain(shop)
    state = secrets.token_urlsafe(24)
    auth_url = _build_manual_auth_url(
        shop=normalized_shop,
        client_id=client_id,
        scopes=scopes,
        redirect_uri=redirect_uri,
        state=state,
    )
    print("Install-Link:")
    print(auth_url)
    if open_browser:
        webbrowser.open(auth_url)
    callback_payload = _wait_for_local_manual_oauth_callback(
        port=port,
        expected_state=state,
        timeout_seconds=timeout_seconds,
    )
    response = requests.post(
        f"https://{normalized_shop}/admin/oauth/access_token",
        json={
            "client_id": client_id,
            "client_secret": client_secret,
            "code": callback_payload["code"],
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Token-Request fehlgeschlagen status={response.status_code} body={shorten_text(response.text)}")
    payload = response.json()
    bundle = _token_bundle_from_payload(payload)
    bundle["shop"] = callback_payload["shop"]
    write_sync_env_values(
        {
            "SHOP": bundle["shop"],
            "SHOPIFY_APP_CLIENT_ID": client_id,
            "SHOPIFY_APP_CLIENT_SECRET": client_secret,
            "SHOPIFY_APP_SCOPES": scopes,
            "SHOPIFY_APP_REDIRECT_URI": redirect_uri,
            **_env_updates_from_token_bundle(bundle),
        }
    )
    _apply_token_bundle(bundle)
    return bundle


def summarize_orders(orders):
    count = len(orders or [])
    if not orders:
        return {"count": 0, "latest_name": "-", "latest_created_at": "-", "line_items": 0}

    latest_order = None
    latest_key = None
    line_items = 0
    for order in orders:
        line_items += len(((order.get("lineItems") or {}).get("nodes") or []))
        created_at = order.get("createdAt")
        candidate = (created_at or "", order.get("name") or "")
        if latest_key is None or candidate > latest_key:
            latest_key = candidate
            latest_order = order

    return {
        "count": count,
        "latest_name": (latest_order or {}).get("name") or "-",
        "latest_created_at": (latest_order or {}).get("createdAt") or "-",
        "line_items": line_items,
    }


def build_sync_version_payload():
    return {
        "service": "shopify-sync",
        "version": SYNC_VERSION,
        "reported_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }


def update_service_runtime_state(
    *,
    status=None,
    mark_seen=False,
    mark_started=False,
    mark_finished=False,
    mark_pull=False,
    mark_push=False,
    last_error=None,
    clear_error=False,
):
    con = db()
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO service_runtime_state (
            service,
            version,
            status,
            last_seen_at,
            last_started_at,
            last_finished_at,
            last_pull_at,
            last_push_at,
            last_error,
            updated_at
        )
        VALUES (
            'shopify-sync',
            %s,
            %s,
            CASE WHEN %s THEN NOW() ELSE NULL END,
            CASE WHEN %s THEN NOW() ELSE NULL END,
            CASE WHEN %s THEN NOW() ELSE NULL END,
            CASE WHEN %s THEN NOW() ELSE NULL END,
            CASE WHEN %s THEN NOW() ELSE NULL END,
            %s,
            NOW()
        )
        ON CONFLICT (service) DO UPDATE SET
            version = COALESCE(EXCLUDED.version, service_runtime_state.version),
            status = COALESCE(EXCLUDED.status, service_runtime_state.status),
            last_seen_at = COALESCE(EXCLUDED.last_seen_at, service_runtime_state.last_seen_at),
            last_started_at = COALESCE(EXCLUDED.last_started_at, service_runtime_state.last_started_at),
            last_finished_at = COALESCE(EXCLUDED.last_finished_at, service_runtime_state.last_finished_at),
            last_pull_at = COALESCE(EXCLUDED.last_pull_at, service_runtime_state.last_pull_at),
            last_push_at = COALESCE(EXCLUDED.last_push_at, service_runtime_state.last_push_at),
            last_error = CASE
                WHEN %s THEN NULL
                WHEN EXCLUDED.last_error IS NOT NULL THEN EXCLUDED.last_error
                ELSE service_runtime_state.last_error
            END,
            updated_at = NOW()
        """,
        (
            SYNC_VERSION,
            status,
            bool(mark_seen),
            bool(mark_started),
            bool(mark_finished),
            bool(mark_pull),
            bool(mark_push),
            (last_error or "")[:1000] if last_error else None,
            bool(clear_error),
        ),
    )
    con.commit()
    cur.close()
    con.close()


def ensure_runtime_dependencies():
    missing = []
    if psycopg2 is None:
        missing.append("psycopg2")
    if requests is None:
        missing.append("requests")
    if missing:
        raise RuntimeError(f"Fehlende Python-Abhaengigkeiten: {', '.join(missing)}")


def db():
    ensure_runtime_dependencies()
    connect_kwargs = {
        "host": DB_HOST,
        "database": DB_NAME,
        "user": DB_USER,
        "password": DB_PASS,
    }
    if DB_PORT:
        connect_kwargs["port"] = int(DB_PORT)
    return psycopg2.connect(
        **connect_kwargs,
    )


def init_db():
    con = db()
    cur = con.cursor()
    apply_app_schema(cur)
    con.commit()
    cur.close()
    con.close()


def database_schema_issues():
    con = db()
    cur = con.cursor()
    try:
        return collect_schema_issues(cur)
    finally:
        cur.close()
        con.close()


def graphql_request(query, variables=None):
    ensure_runtime_dependencies()
    payload = {"query": query, "variables": variables or {}}
    retried_after_refresh = False
    while True:
        _refresh_access_token()
        headers = {
            "X-Shopify-Access-Token": TOKEN,
            "Content-Type": "application/json",
        }
        try:
            response = requests.post(
                GRAPHQL_URL,
                json=payload,
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            break
        except requests.RequestException as exc:
            response = getattr(exc, "response", None)
            status = getattr(response, "status_code", "-")
            body = shorten_text(getattr(response, "text", ""))
            if not retried_after_refresh and REFRESH_TOKEN and status in {401, 403}:
                log_warning("GraphQL HTTP-Fehler status=%s, versuche Token-Refresh", status)
                _refresh_access_token(force=True)
                retried_after_refresh = True
                continue
            log_error("GraphQL HTTP-Fehler status=%s body=%s", status, body or "-")
            raise

    try:
        data = response.json()
    except ValueError as exc:
        body = shorten_text(response.text)
        log_error("GraphQL JSON-Fehler body=%s", body or "-")
        raise RuntimeError("Shopify GraphQL Antwort war kein gueltiges JSON.") from exc
    errors = data.get("errors")

    if errors:
        log_error("Shopify GraphQL Fehler: %s", shorten_text(json.dumps(errors, ensure_ascii=False)))
        raise RuntimeError(f"Shopify GraphQL Fehler: {errors}")

    return data["data"]


def _inventory_item_gid(value):
    text = (value or "").strip()
    if not text:
        return ""
    if text.startswith("gid://"):
        return text
    return f"gid://shopify/InventoryItem/{text}"


def _weight_grams_from_measurement(measurement):
    weight = (measurement or {}).get("weight") or {}
    value = weight.get("value")
    unit = (weight.get("unit") or "").upper()
    if value in (None, ""):
        return None
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None

    if unit == "GRAMS":
        grams = numeric_value
    elif unit == "KILOGRAMS":
        grams = numeric_value * 1000.0
    elif unit == "OUNCES":
        grams = numeric_value * 28.349523125
    elif unit == "POUNDS":
        grams = numeric_value * 453.59237
    else:
        return None

    return int(round(grams))


def get_all_product_variants():
    query = """
    query ProductVariantsPage($after: String) {
      productVariants(first: 250, after: $after) {
        nodes {
          id
          sku
          barcode
          price
          compareAtPrice
          inventoryQuantity
          product {
            id
            title
            status
            descriptionHtml
          }
          inventoryItem {
            id
            sku
            unitCost {
              amount
              currencyCode
            }
            measurement {
              weight {
                unit
                value
              }
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """

    variants = []
    after = None
    while True:
        data = graphql_request(query, {"after": after})
        page = data["productVariants"]
        variants.extend(page["nodes"])
        log_info("Produktvarianten-Seite geladen: gesamt=%s has_next=%s", len(variants), page["pageInfo"]["hasNextPage"])
        if not page["pageInfo"]["hasNextPage"]:
            return variants
        after = page["pageInfo"]["endCursor"]
        time.sleep(0.5)


def get_shopify_locations():
    query = """
    query LocationsPage($after: String) {
      locations(first: 100, after: $after) {
        nodes {
          id
          name
          fulfillsOnlineOrders
          isActive
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """

    locations = []
    after = None
    while True:
        data = graphql_request(query, {"after": after})
        page = data["locations"]
        for node in page["nodes"]:
            location_id = (node.get("id") or "").strip()
            if not location_id:
                continue
            locations.append(
                {
                    "location_id": location_id,
                    "name": (node.get("name") or "").strip(),
                    "fulfills_online_orders": bool(node.get("fulfillsOnlineOrders")),
                    "is_active": bool(node.get("isActive", True)),
                }
            )
        if not page["pageInfo"]["hasNextPage"]:
            return locations
        after = page["pageInfo"]["endCursor"]
        time.sleep(0.5)


def build_locations_payload():
    rows = []
    for entry in get_shopify_locations():
        location_id = (entry.get("location_id") or "").strip()
        rows.append(
            {
                "location_id": location_id,
                "location_name": (entry.get("name") or "").strip() or location_id,
                "fulfills_online_orders": bool(entry.get("fulfills_online_orders")),
                "is_active": bool(entry.get("is_active", True)),
            }
        )
    return rows


def format_locations_text(locations):
    if not locations:
        return "Keine Shopify-Locations gefunden."
    lines = []
    for index, entry in enumerate(locations, start=1):
        status_parts = []
        if entry.get("is_active"):
            status_parts.append("active")
        else:
            status_parts.append("inactive")
        if entry.get("fulfills_online_orders"):
            status_parts.append("online")
        line = f"{index}. {entry.get('location_name') or '-'}"
        location_id = (entry.get("location_id") or "").strip()
        if location_id:
            line += f" [{location_id}]"
        if status_parts:
            line += f" ({', '.join(status_parts)})"
        lines.append(line)
    return "\n".join(lines)


def _inventory_quantities_from_entries(entries):
    quantities = {entry["name"]: entry["quantity"] for entry in entries or []}
    unavailable = (
        quantities.get("reserved", 0)
        + quantities.get("damaged", 0)
        + quantities.get("safety_stock", 0)
        + quantities.get("quality_control", 0)
    )
    return {
        "available": quantities.get("available", 0),
        "committed": quantities.get("committed", 0),
        "reserved": quantities.get("reserved", 0),
        "unavailable": unavailable,
        "on_hand": quantities.get("on_hand"),
    }


def get_location_inventory_levels(location_id=None):
    query = """
    query LocationInventoryLevels($locationId: ID!, $after: String) {
      location(id: $locationId) {
        inventoryLevels(first: 250, after: $after) {
          nodes {
            item {
              id
              sku
            }
            quantities(
              names: [
                "available",
                "reserved",
                "committed",
                "on_hand",
                "damaged",
                "safety_stock",
                "quality_control"
              ]
            ) {
              name
              quantity
            }
          }
          pageInfo {
            hasNextPage
            endCursor
          }
        }
      }
    }
    """

    target_location_id = _location_gid(location_id)
    after = None
    inventory_rows = []

    while True:
        data = graphql_request(
            query,
            {
                "locationId": target_location_id,
                "after": after,
            },
        )

        location = data.get("location")
        if not location:
            raise RuntimeError(
                f"Shopify-Location nicht gefunden oder nicht lesbar: {target_location_id}. "
                "SHOPIFY_LOCATION_ID in shopify-sync/.env pruefen."
            )

        levels = location["inventoryLevels"]

        for node in levels["nodes"]:
            item = node["item"] or {}
            inventory_rows.append(
                {
                    "inventory_item_id": _canonical_inventory_item_id(item.get("id")),
                    "sku": (item.get("sku") or "").strip(),
                    **_inventory_quantities_from_entries(node.get("quantities") or []),
                }
            )

        page_info = levels["pageInfo"]

        if not page_info["hasNextPage"]:
            return inventory_rows

        after = page_info["endCursor"]
        time.sleep(0.5)


def _refresh_item_totals(cur, skus=None):
    params = []
    sku_filter = ""
    if skus:
        sku_filter = "WHERE sku = ANY(%s)"
        params.append(list(sorted(set(skus))))
    cur.execute(
        f"""
        WITH totals AS (
            SELECT
                sku,
                COALESCE(SUM(menge), 0) AS menge,
                COALESCE(SUM(available), 0) AS available,
                COALESCE(SUM(reserved), 0) AS reserved,
                COALESCE(SUM(committed), 0) AS committed,
                COALESCE(SUM(unavailable), 0) AS unavailable,
                BOOL_OR(dirty) AS dirty
            FROM item_location_inventory
            {sku_filter}
            GROUP BY sku
        )
        UPDATE items
        SET menge = totals.menge,
            available = totals.available,
            reserved = totals.reserved,
            committed = totals.committed,
            unavailable = totals.unavailable,
            dirty = totals.dirty,
            updated_at = NOW()
        FROM totals
        WHERE items.sku = totals.sku
        """,
        tuple(params),
    )


def push_inventory_changes():

    con = db()
    cur = con.cursor()

    cur.execute(
        """
        SELECT
            ili.sku,
            COALESCE(i.display_sku, i.sku) AS display_sku,
            ili.location_id,
            ili.available,
            i.shopify_inventory_item_id
        FROM item_location_inventory ili
        JOIN items i ON i.sku = ili.sku
        WHERE ili.dirty = TRUE
          AND i.shopify_inventory_item_id IS NOT NULL
        """
    )

    rows = cur.fetchall()

    if not rows:
        con.close()
        return 0

    log_info("Push %s Lageraenderungen zu Shopify", len(rows))

    pushed_count = 0
    touched_skus = set()
    for row in rows:
        if len(row) == 5:
            sku, display_sku, location_id, available_qty, inventory_item_id = row
        else:
            sku, available_qty, inventory_item_id = row
            display_sku = sku
            location_id = _location_gid()
        inventory_item_gid = _inventory_item_gid(inventory_item_id)
        if not inventory_item_gid:
            log_error("Shopify Inventory-Sync uebersprungen: fehlende inventory item id fuer sku=%s", display_sku or sku)
            continue
        mutation = """
        mutation InventorySet($input: InventorySetQuantitiesInput!) {
          inventorySetQuantities(input: $input) {
            inventoryAdjustmentGroup {
              createdAt
            }
            userErrors {
              code
              field
              message
            }
          }
        }
        """
        variables = {
            "input": {
                "name": "available",
                "reason": "correction",
                "referenceDocumentUri": f"gid://lagerverwaltung/InventorySync/{sku}",
                "quantities": [
                    {
                        "inventoryItemId": inventory_item_gid,
                        "locationId": location_id,
                        "quantity": int(available_qty),
                        "changeFromQuantity": None,
                    }
                ],
            }
        }
        try:
            data = graphql_request(mutation, variables)
        except Exception as exc:
            log_error("Shopify Fehler sku=%s action=inventorySetQuantities error=%s", display_sku or sku, shorten_text(exc))
            continue

        payload = (data.get("inventorySetQuantities") or {})
        user_errors = payload.get("userErrors") or []
        if user_errors:
            log_error(
                "Shopify Fehler sku=%s action=inventorySetQuantities user_errors=%s",
                display_sku or sku,
                shorten_text(json.dumps(user_errors, ensure_ascii=False)),
            )
            continue

        log_info("Shopify Update sku=%s location=%s available=%s", display_sku or sku, location_id.rsplit('/', 1)[-1], available_qty)

        cur.execute("""
            UPDATE item_location_inventory
            SET dirty = FALSE,
                updated_at = NOW()
            WHERE sku = %s AND location_id = %s
        """,
        (sku, location_id),
        )
        touched_skus.add(sku)
        pushed_count += 1

        time.sleep(0.5)

    if touched_skus:
        _refresh_item_totals(cur, touched_skus)
        cur.execute(
            """
            UPDATE items
            SET sync_status = 'pushed',
                last_sync = NOW(),
                updated_at = NOW()
            WHERE sku = ANY(%s)
            """,
            (list(sorted(touched_skus)),),
        )

    con.commit()
    con.close()
    return pushed_count


def sync_inventory_levels():
    locations = get_shopify_locations()
    if not locations:
        log_warning("Keine Shopify-Locations geladen")
        return 0

    con = db()
    cur = con.cursor()
    synced_rows = 0
    touched_skus = set()

    for location in locations:
        location_id = location["location_id"]
        cur.execute(
            """
            INSERT INTO shopify_locations(location_id, name, fulfills_online_orders, is_active, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (location_id)
            DO UPDATE SET
                name = EXCLUDED.name,
                fulfills_online_orders = EXCLUDED.fulfills_online_orders,
                is_active = EXCLUDED.is_active,
                updated_at = NOW()
            """,
            (
                location_id,
                location.get("name") or "",
                bool(location.get("fulfills_online_orders")),
                bool(location.get("is_active", True)),
            ),
        )
        for entry in get_location_inventory_levels(location_id):
            inventory_item_id = entry["inventory_item_id"]
            if not inventory_item_id:
                continue
            available = entry["available"]
            committed = entry["committed"]
            reserved = entry["reserved"]
            unavailable = entry["unavailable"]
            on_hand = entry["on_hand"]

            if on_hand is None:
                on_hand = available + committed + unavailable

            cur.execute(
                """
                INSERT INTO item_location_inventory (
                    sku,
                    location_id,
                    menge,
                    available,
                    reserved,
                    committed,
                    unavailable,
                    updated_at
                )
                SELECT
                    items.sku,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    NOW()
                FROM items
                WHERE items.shopify_inventory_item_id = %s
                ON CONFLICT (sku, location_id) DO NOTHING
                """,
                (
                    location_id,
                    on_hand,
                    available,
                    reserved,
                    committed,
                    unavailable,
                    inventory_item_id,
                ),
            )

            cur.execute(
                """
                UPDATE item_location_inventory AS ili
                SET menge = CASE
                        WHEN ili.dirty = TRUE THEN ili.menge
                        ELSE %s
                    END,
                    available = CASE
                        WHEN ili.dirty = TRUE THEN GREATEST(ili.menge - %s - %s, 0)
                        ELSE %s
                    END,
                    committed = %s,
                    reserved = %s,
                    unavailable = %s,
                    updated_at = NOW(),
                    dirty = CASE
                        WHEN ili.dirty = TRUE AND GREATEST(ili.menge - %s - %s, 0) = %s THEN FALSE
                        ELSE ili.dirty
                    END
                FROM items
                WHERE items.shopify_inventory_item_id = %s
                  AND ili.sku = items.sku
                  AND ili.location_id = %s
                """,
                (
                    on_hand,
                    unavailable,
                    committed,
                    available,
                    committed,
                    reserved,
                    unavailable,
                    unavailable,
                    committed,
                    available,
                    inventory_item_id,
                    location_id,
                ),
            )

            cur.execute(
                """
                SELECT sku
                FROM items
                WHERE shopify_inventory_item_id = %s
                """,
                (inventory_item_id,),
            )
            touched_skus.update(row[0] for row in cur.fetchall())
            synced_rows += 1

    if not synced_rows:
        con.close()
        log_warning("Keine Inventory-Levels von Shopify geladen")
        return 0

    _refresh_item_totals(cur, touched_skus)
    cur.execute(
        """
        UPDATE items
        SET sync_status = 'ok',
            last_sync = NOW(),
            updated_at = NOW()
        WHERE sku = ANY(%s)
        """,
        (list(sorted(touched_skus)),),
    )

    con.commit()
    con.close()
    log_info("Inventory-Levels synchronisiert: locations=%s levels=%s", len(locations), synced_rows)
    return synced_rows


def sync_products():
    variants = get_all_product_variants()
    product_ids = set()
    imported_variants = 0
    without_sku_variants = 0

    con = db()
    cur = con.cursor()

    for variant in variants:
        product = variant.get("product") or {}
        inventory_item = variant.get("inventoryItem") or {}

        product_id = product.get("id")
        if product_id:
            product_ids.add(product_id)

        display_sku = _display_sku_for_variant(variant, inventory_item)
        if not display_sku:
            without_sku_variants += 1
        sku = _storage_sku_for_variant(variant, inventory_item)

        variant_id = variant.get("id")
        inventory_item_id = _canonical_inventory_item_id(inventory_item.get("id"))
        barcode = variant.get("barcode")
        price = variant.get("price")
        compare_at_price = variant.get("compareAtPrice")
        weight_grams = _weight_grams_from_measurement(inventory_item.get("measurement"))
        unit_cost = inventory_item.get("unitCost") or {}
        qty = int(variant.get("inventoryQuantity") or 0)
        log_info("Import sku=%s qty=%s", display_sku or "-/-", qty)
        imported_variants += 1

        cur.execute("""
            INSERT INTO items(
                sku,
                name,
                display_sku,
                menge,
                available,
                unavailable,
                committed,
                reserved,
                shopify_product_id,
                shopify_variant_id,
                shopify_inventory_item_id,
                barcode,
                shopify_product_status,
                shopify_description,
                shopify_price,
                shopify_compare_at_price,
                shopify_unit_cost,
                shopify_unit_cost_currency,
                shopify_weight_grams,
                sync_status,
                last_sync,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'ok',NOW(),NOW())
            ON CONFLICT (sku)
            DO UPDATE SET
                name = EXCLUDED.name,
                display_sku = EXCLUDED.display_sku,
                menge = CASE
                    WHEN items.dirty = TRUE THEN items.menge
                    ELSE EXCLUDED.menge
                END,
                available = CASE
                    WHEN items.dirty = TRUE THEN items.available
                    ELSE EXCLUDED.available
                END,
                unavailable = COALESCE(items.unavailable, EXCLUDED.unavailable),
                committed = COALESCE(items.committed, EXCLUDED.committed),
                reserved = COALESCE(items.reserved, EXCLUDED.reserved),
                shopify_product_id = EXCLUDED.shopify_product_id,
                shopify_variant_id = EXCLUDED.shopify_variant_id,
                shopify_inventory_item_id = EXCLUDED.shopify_inventory_item_id,
                barcode = EXCLUDED.barcode,
                shopify_product_status = EXCLUDED.shopify_product_status,
                shopify_description = EXCLUDED.shopify_description,
                shopify_price = EXCLUDED.shopify_price,
                shopify_compare_at_price = EXCLUDED.shopify_compare_at_price,
                shopify_unit_cost = EXCLUDED.shopify_unit_cost,
                shopify_unit_cost_currency = EXCLUDED.shopify_unit_cost_currency,
                shopify_weight_grams = EXCLUDED.shopify_weight_grams,
                last_sync = NOW(),
                sync_status = 'ok',
                updated_at = NOW(),
                dirty = CASE
                    WHEN items.dirty = TRUE AND items.available = EXCLUDED.available THEN FALSE
                    ELSE items.dirty
                END
            """,
        (
            sku,
            product.get("title"),
            display_sku,
            qty,
            qty,
            0,
            0,
            0,
            product_id,
            variant_id,
            inventory_item_id,
            barcode,
            product.get("status"),
            product.get("descriptionHtml"),
            price,
            compare_at_price,
            unit_cost.get("amount"),
            unit_cost.get("currencyCode"),
            weight_grams,
        ))

    con.commit()
    con.close()
    log_info(
        "Produkte synchronisiert: varianten=%s importiert=%s ohne_sku=%s produkte=%s",
        len(variants),
        imported_variants,
        without_sku_variants,
        len(product_ids),
    )
    return len(product_ids)


def get_all_orders():
    query = """
    query OrdersPage($after: String) {
      orders(first: 20, after: $after, reverse: true, sortKey: CREATED_AT) {
        nodes {
          id
          name
          createdAt
          email
          displayFulfillmentStatus
          displayFinancialStatus
          shippingAddress {
            name
            address1
            zip
            city
            country
            phone
          }
          lineItems(first: 50) {
            nodes {
              id
              name
              sku
              quantity
              unfulfilledQuantity
            }
          }
          fulfillmentOrders(first: 20) {
            nodes {
              id
              status
              requestStatus
              assignedLocation {
                location {
                  id
                  name
                  fulfillsOnlineOrders
                  isActive
                }
              }
              lineItems(first: 50) {
                nodes {
                  id
                  sku
                  productTitle
                  remainingQuantity
                  totalQuantity
                  lineItem {
                    id
                    name
                    sku
                  }
                }
              }
            }
          }
          fulfillments {
            id
            status
            createdAt
            trackingInfo {
              number
              company
              url
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """

    orders = []
    after = None

    while True:
        data = graphql_request(query, {"after": after})
        page = data["orders"]
        orders.extend(page["nodes"])
        log_info("Orders-Seite geladen: gesamt=%s has_next=%s", len(orders), page["pageInfo"]["hasNextPage"])

        if not page["pageInfo"]["hasNextPage"]:
            return orders

        after = page["pageInfo"]["endCursor"]
        time.sleep(0.5)


def get_all_customers():
    query = """
    query CustomersPage($after: String) {
      customers(first: 50, after: $after, sortKey: UPDATED_AT) {
        nodes {
          id
          firstName
          lastName
          displayName
          email
          phone
          defaultAddress {
            name
            address1
            zip
            city
            country
            phone
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """

    customers = []
    after = None
    while True:
        data = graphql_request(query, {"after": after})
        page = data["customers"]
        customers.extend(page["nodes"])
        log_info("Customers-Seite geladen: gesamt=%s has_next=%s", len(customers), page["pageInfo"]["hasNextPage"])
        if not page["pageInfo"]["hasNextPage"]:
            return customers
        after = page["pageInfo"]["endCursor"]
        time.sleep(0.5)


def sync_customers():
    customers = get_all_customers()
    con = db()
    cur = con.cursor()
    cur.execute("TRUNCATE TABLE shopify_customers")

    for customer in customers:
        default_address = customer.get("defaultAddress") or {}
        cur.execute(
            """
            INSERT INTO shopify_customers (
                customer_id,
                first_name,
                last_name,
                display_name,
                email,
                phone,
                default_name,
                default_address1,
                default_zip,
                default_city,
                default_country,
                default_phone,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            """,
            (
                customer.get("id"),
                customer.get("firstName"),
                customer.get("lastName"),
                customer.get("displayName"),
                customer.get("email"),
                customer.get("phone"),
                default_address.get("name"),
                default_address.get("address1"),
                default_address.get("zip"),
                default_address.get("city"),
                default_address.get("country"),
                default_address.get("phone"),
            ),
        )

    con.commit()
    cur.close()
    con.close()
    log_info("Kunden synchronisiert: %s", len(customers))
    return len(customers)


def sync_orders():
    orders = get_all_orders()
    stats = summarize_orders(orders)
    log_info(
        "Order-Import gestartet: count=%s latest=%s created_at=%s line_items=%s",
        stats["count"],
        stats["latest_name"],
        stats["latest_created_at"],
        stats["line_items"],
    )

    con = db()
    cur = con.cursor()
    cur.execute(
        "TRUNCATE TABLE shopify_fulfillment_order_items, shopify_fulfillment_orders, shopify_order_items, shopify_orders"
    )

    for order in orders:
        shipping = order.get("shippingAddress") or {}

        cur.execute(
            """
            INSERT INTO shopify_orders (
                order_id,
                order_name,
                created_at,
                shipping_name,
                shipping_address1,
                shipping_zip,
                shipping_city,
                shipping_country,
                shipping_email,
                shipping_phone,
                fulfillment_status,
                payment_status,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
            """,
            (
                order["id"],
                order["name"],
                order["createdAt"],
                shipping.get("name"),
                shipping.get("address1"),
                shipping.get("zip"),
                shipping.get("city"),
                shipping.get("country"),
                order.get("email"),
                shipping.get("phone"),
                order.get("displayFulfillmentStatus"),
                order.get("displayFinancialStatus"),
            ),
        )

        for index, line_item in enumerate(order["lineItems"]["nodes"], start=1):
            cur.execute(
                """
                INSERT INTO shopify_order_items (
                    order_id,
                    line_index,
                    order_line_item_id,
                    sku,
                    title,
                    quantity,
                    fulfilled_quantity
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    order["id"],
                    index,
                    line_item.get("id"),
                    line_item.get("sku"),
                    line_item["name"],
                    line_item["quantity"],
                    max(0, int(line_item.get("quantity") or 0) - int(line_item.get("unfulfilledQuantity") or 0)),
                ),
            )
        fulfillment_orders_payload, fulfillment_order_items_payload = _build_fulfillment_order_records(order)
        for fulfillment_order in fulfillment_orders_payload:
            location_id = fulfillment_order.get("assigned_location_id")
            if location_id:
                cur.execute(
                    """
                    INSERT INTO shopify_locations(location_id, name, fulfills_online_orders, is_active, updated_at)
                    VALUES (%s,%s,%s,%s,NOW())
                    ON CONFLICT (location_id)
                    DO UPDATE SET
                        name = EXCLUDED.name,
                        fulfills_online_orders = EXCLUDED.fulfills_online_orders,
                        is_active = EXCLUDED.is_active,
                        updated_at = NOW()
                    """,
                    (
                        location_id,
                        fulfillment_order.get("assigned_location_name") or location_id,
                        bool(fulfillment_order.get("fulfills_online_orders")),
                        True if fulfillment_order.get("is_active") is None else bool(fulfillment_order.get("is_active")),
                    ),
                )
            cur.execute(
                """
                INSERT INTO shopify_fulfillment_orders (
                    fulfillment_order_id,
                    order_id,
                    assigned_location_id,
                    assigned_location_name,
                    status,
                    request_status,
                    updated_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,NOW())
                """,
                (
                    fulfillment_order["fulfillment_order_id"],
                    fulfillment_order["order_id"],
                    fulfillment_order.get("assigned_location_id"),
                    fulfillment_order.get("assigned_location_name"),
                    fulfillment_order.get("status"),
                    fulfillment_order.get("request_status"),
                ),
            )
        for line in fulfillment_order_items_payload:
            cur.execute(
                """
                INSERT INTO shopify_fulfillment_order_items (
                    fulfillment_order_line_item_id,
                    fulfillment_order_id,
                    order_id,
                    order_line_item_id,
                    sku,
                    title,
                    quantity,
                    remaining_quantity,
                    assigned_location_id,
                    assigned_location_name,
                    updated_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                """,
                (
                    line["fulfillment_order_line_item_id"],
                    line["fulfillment_order_id"],
                    line["order_id"],
                    line.get("order_line_item_id"),
                    line.get("sku"),
                    line.get("title"),
                    line.get("quantity"),
                    line.get("remaining_quantity"),
                    line.get("assigned_location_id"),
                    line.get("assigned_location_name"),
                ),
            )
        sync_order_shipments(cur, order)

    con.commit()
    con.close()
    log_info(
        "Bestellungen synchronisiert: count=%s latest=%s created_at=%s",
        stats["count"],
        stats["latest_name"],
        stats["latest_created_at"],
    )
    return len(orders)


def _normalize_carrier_name(value):
    raw = (value or "").strip()
    if not raw:
        return "shopify"
    normalized = raw.lower()
    if "gls" in normalized:
        return "gls"
    if "post" in normalized:
        return "post"
    return normalized[:32]


def _shopify_tracking_company(value):
    normalized = _normalize_carrier_name(value)
    if normalized == "gls":
        return "GLS"
    if normalized == "post":
        return "Deutsche Post"
    return (value or "").strip() or "GLS"


def _iter_fulfillments(order):
    rows = order.get("fulfillments") or []
    if isinstance(rows, dict):
        rows = rows.get("nodes") or []
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _iter_fulfillment_orders(order):
    rows = order.get("fulfillmentOrders") or []
    if isinstance(rows, dict):
        rows = rows.get("nodes") or []
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _iter_tracking_rows(fulfillment):
    rows = fulfillment.get("trackingInfo") or []
    if isinstance(rows, dict):
        rows = rows.get("nodes") or []
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _assigned_location_payload(fulfillment_order):
    assigned = fulfillment_order.get("assignedLocation") or {}
    location = assigned.get("location") or {}
    location_id = (location.get("id") or "").strip() or None
    location_name = (location.get("name") or "").strip() or location_id or ""
    return {
        "location_id": location_id,
        "location_name": location_name,
        "fulfills_online_orders": bool(location.get("fulfillsOnlineOrders")),
        "is_active": True if location.get("isActive") is None else bool(location.get("isActive")),
    }


def _build_fulfillment_order_records(order):
    orders_payload = []
    items_payload = []
    for fulfillment_order in _iter_fulfillment_orders(order):
        fulfillment_order_id = (fulfillment_order.get("id") or "").strip()
        if not fulfillment_order_id:
            continue
        location = _assigned_location_payload(fulfillment_order)
        orders_payload.append(
            {
                "fulfillment_order_id": fulfillment_order_id,
                "order_id": order["id"],
                "assigned_location_id": location["location_id"],
                "assigned_location_name": location["location_name"],
                "status": (fulfillment_order.get("status") or "").strip(),
                "request_status": (fulfillment_order.get("requestStatus") or "").strip(),
                "fulfills_online_orders": location["fulfills_online_orders"],
                "is_active": location["is_active"],
            }
        )
        for line in (fulfillment_order.get("lineItems") or {}).get("nodes") or []:
            if not isinstance(line, dict):
                continue
            fulfillment_order_line_item_id = (line.get("id") or "").strip()
            if not fulfillment_order_line_item_id:
                continue
            line_item = line.get("lineItem") or {}
            items_payload.append(
                {
                    "fulfillment_order_line_item_id": fulfillment_order_line_item_id,
                    "fulfillment_order_id": fulfillment_order_id,
                    "order_id": order["id"],
                    "order_line_item_id": (line_item.get("id") or "").strip() or None,
                    "sku": (line.get("sku") or line_item.get("sku") or "").strip() or None,
                    "title": (line_item.get("name") or line.get("productTitle") or "-").strip() or "-",
                    "quantity": int(line.get("totalQuantity") or 0),
                    "remaining_quantity": int(line.get("remainingQuantity") or 0),
                    "assigned_location_id": location["location_id"],
                    "assigned_location_name": location["location_name"],
                }
            )
    return orders_payload, items_payload


def upsert_shopify_shipment(cur, order, fulfillment, tracking):
    tracking_number = (tracking.get("number") or "").strip()
    if not tracking_number:
        return
    fulfillment_id = (fulfillment.get("id") or "").strip() or None
    tracking_url = (tracking.get("url") or "").strip() or None
    status = (fulfillment.get("status") or "SHOPIFY_SYNCED").strip() or "SHOPIFY_SYNCED"
    carrier = _normalize_carrier_name(tracking.get("company"))
    parcel_number = tracking_number if tracking_number.isdigit() else None
    created_at = fulfillment.get("createdAt") or datetime.datetime.now(datetime.timezone.utc)
    _upsert_shopify_shipment_record(
        cur,
        carrier=carrier,
        order_id=order["id"],
        order_name=order["name"],
        shipment_reference=order["name"],
        tracking_number=tracking_number,
        parcel_number=parcel_number,
        status=status,
        fulfillment_id=fulfillment_id,
        tracking_url=tracking_url,
        created_at=created_at,
    )


def sync_order_shipments(cur, order):
    for fulfillment in _iter_fulfillments(order):
        for tracking in _iter_tracking_rows(fulfillment):
            upsert_shopify_shipment(cur, order, fulfillment, tracking)


def get_open_fulfillment_order_targets(order_id):
    query = """
    query FulfillmentOrdersForOrder($orderId: ID!) {
      order(id: $orderId) {
        id
        name
        fulfillmentOrders(first: 50) {
          nodes {
            id
            status
            requestStatus
            supportedActions {
              action
            }
            lineItems(first: 100) {
              nodes {
                id
                remainingQuantity
                lineItem {
                  id
                  sku
                }
              }
            }
          }
        }
      }
    }
    """
    data = graphql_request(query, {"orderId": order_id})
    order = data.get("order")
    if not order:
        raise RuntimeError(f"Order nicht gefunden: {order_id}")

    open_targets = []
    for node in (order.get("fulfillmentOrders", {}) or {}).get("nodes", []):
        status = (node.get("status") or "").upper()
        if status in {"CANCELLED", "CLOSED", "FULFILLED"}:
            continue
        actions = {entry.get("action") for entry in (node.get("supportedActions") or [])}
        if "CREATE_FULFILLMENT" in actions or not actions:
            line_items = []
            for li in (node.get("lineItems") or {}).get("nodes", []):
                line_item = li.get("lineItem") or {}
                line_items.append(
                    {
                        "fulfillment_order_line_item_id": li.get("id"),
                        "order_line_item_id": line_item.get("id"),
                        "sku": line_item.get("sku"),
                        "remaining_quantity": int(li.get("remainingQuantity") or 0),
                    }
                )
            open_targets.append({"fulfillment_order_id": node["id"], "line_items": line_items})

    if not open_targets:
        raise RuntimeError(f"Keine offenen FulfillmentOrders fuer {order.get('name') or order_id}.")
    return open_targets


def _build_line_items_by_fulfillment_order(open_targets, requested_items):
    if not requested_items:
        return [{"fulfillmentOrderId": target["fulfillment_order_id"]} for target in open_targets]

    requests = {}
    for item in requested_items:
        line_item_id = (item.get("order_line_item_id") or "").strip()
        quantity = int(item.get("quantity") or 0)
        if not line_item_id or quantity <= 0:
            continue
        requests[line_item_id] = requests.get(line_item_id, 0) + quantity

    if not requests:
        raise RuntimeError("Keine gueltigen line items fuer Fulfillment uebergeben.")

    by_fo = {}
    for line_item_id, requested_qty in requests.items():
        remaining_request = requested_qty
        for target in open_targets:
            fulfillment_order_id = target["fulfillment_order_id"]
            for source in target["line_items"]:
                if source.get("order_line_item_id") != line_item_id:
                    continue
                available = int(source.get("remaining_quantity") or 0)
                if available <= 0:
                    continue
                take = min(available, remaining_request)
                if take <= 0:
                    continue
                by_fo.setdefault(fulfillment_order_id, []).append(
                    {
                        "id": source["fulfillment_order_line_item_id"],
                        "quantity": take,
                    }
                )
                remaining_request -= take
                if remaining_request <= 0:
                    break
            if remaining_request <= 0:
                break

        if remaining_request > 0:
            raise RuntimeError(f"Menge fuer LineItem {line_item_id} nicht mehr offen (Rest {remaining_request}).")

    payload = []
    for fulfillment_order_id, rows in by_fo.items():
        payload.append(
            {
                "fulfillmentOrderId": fulfillment_order_id,
                "fulfillmentOrderLineItems": rows,
            }
        )
    if not payload:
        raise RuntimeError("Keine offenen FulfillmentOrder-Positionen gefunden.")
    return payload


def create_fulfillment(order_id, tracking_number, company, tracking_url=None, notify_customer=False, line_items=None):
    open_targets = get_open_fulfillment_order_targets(order_id)
    mutation = """
    mutation CreateFulfillment($fulfillment: FulfillmentInput!, $message: String) {
      fulfillmentCreate(fulfillment: $fulfillment, message: $message) {
        fulfillment {
          id
          status
          trackingInfo(first: 5) {
            number
            company
            url
          }
        }
        userErrors {
          field
          message
        }
      }
    }
    """

    line_items_payload = _build_line_items_by_fulfillment_order(open_targets, line_items)
    tracking_info = {
        "number": tracking_number,
        "company": _shopify_tracking_company(company),
    }
    if (tracking_url or "").strip():
        tracking_info["url"] = tracking_url.strip()

    variables = {
        "fulfillment": {
            "notifyCustomer": bool(notify_customer),
            "lineItemsByFulfillmentOrder": line_items_payload,
            "trackingInfo": tracking_info,
        },
        "message": "Lager-MC Versand abgeschlossen",
    }
    data = graphql_request(mutation, variables)
    payload = (data.get("fulfillmentCreate") or {})
    user_errors = payload.get("userErrors") or []
    if user_errors:
        raise RuntimeError(f"Fulfillment userErrors: {user_errors}")
    fulfillment = payload.get("fulfillment")
    if not fulfillment:
        raise RuntimeError("Shopify hat kein Fulfillment zurueckgegeben.")
    return {
        "fulfillment_id": fulfillment.get("id"),
        "status": fulfillment.get("status"),
        "tracking": fulfillment.get("trackingInfo"),
        "fulfillment_order_ids": [entry["fulfillmentOrderId"] for entry in line_items_payload],
    }


def claim_fulfillment_jobs(limit=20):
    return _claim_shopify_fulfillment_jobs(
        db,
        cursor_factory=psycopg2.extras.RealDictCursor,
        limit=limit,
    )


def mark_fulfillment_job_done(job_id, label_id, fulfillment_id, status):
    return _mark_shopify_fulfillment_job_done(db, job_id, label_id, fulfillment_id, status)


def mark_fulfillment_job_failed(job_id, label_id, message):
    return _mark_shopify_fulfillment_job_failed(db, job_id, label_id, message)


def process_fulfillment_jobs(limit=20):
    jobs = claim_fulfillment_jobs(limit=limit)
    if not jobs:
        return 0, 0

    success_count = 0
    failed_count = 0
    for job in jobs:
        try:
            line_items = None
            if job.get("line_items_json"):
                try:
                    line_items = json.loads(job["line_items_json"])
                except json.JSONDecodeError:
                    raise RuntimeError(f"line_items_json ungueltig fuer Job {job['id']}")
            result = create_fulfillment(
                order_id=job["order_id"],
                tracking_number=job["tracking_number"],
                company=job["carrier"],
                tracking_url=job.get("tracking_url"),
                notify_customer=job["notify_customer"],
                line_items=line_items,
            )
            mark_fulfillment_job_done(
                job_id=job["id"],
                label_id=job.get("label_id"),
                fulfillment_id=result.get("fulfillment_id"),
                status=result.get("status") or "OK",
            )
            success_count += 1
        except Exception as exc:
            error_text = str(exc)
            mark_fulfillment_job_failed(job["id"], job.get("label_id"), error_text)
            log_error("Fulfillment Job %s fehlgeschlagen: %s", job["id"], error_text)
            failed_count += 1
    return success_count, failed_count


def run_sync_loop():
    issues = database_schema_issues()
    if issues:
        raise RuntimeError(
            "DB Migration noetig. run_db_migrations.py ausfuehren. "
            + "; ".join(issues[:5])
        )
    update_service_runtime_state(status="idle", mark_seen=True, clear_error=True)
    while True:
        run_started_at = time.monotonic()
        update_service_runtime_state(status="running", mark_seen=True, mark_started=True, clear_error=True)
        log_info(
            "Starte Shopify Sync version=%s shop=%s db_host=%s interval=%ss log=%s",
            SYNC_VERSION,
            SHOP or "-",
            DB_HOST or "-",
            SYNC_INTERVAL,
            SYNC_LOG_PATH,
        )
        try:
            ok_jobs, failed_jobs = process_fulfillment_jobs(limit=20)
            if ok_jobs or failed_jobs:
                log_info("Fulfillment Jobs verarbeitet: ok=%s failed=%s", ok_jobs, failed_jobs)
            pushed_inventory = push_inventory_changes()
            if ok_jobs > 0 or pushed_inventory > 0:
                update_service_runtime_state(mark_seen=True, mark_push=True)
            sync_products()
            sync_inventory_levels()
            sync_customers()
            sync_orders()
            update_service_runtime_state(status="ok", mark_seen=True, mark_pull=True, mark_finished=True, clear_error=True)
        except Exception as exc:
            update_service_runtime_state(status="error", mark_seen=True, mark_finished=True, last_error=str(exc))
            log_exception("Sync-Fehler: %s", exc)
        else:
            duration = time.monotonic() - run_started_at
            log_info("Sync abgeschlossen in %.2fs", duration)
        log_info("Warte %s Sekunden", SYNC_INTERVAL)
        time.sleep(SYNC_INTERVAL)


def main():
    parser = argparse.ArgumentParser(description="Shopify Sync / Fulfillment Tool")
    parser.add_argument("--version", action="store_true", help="Aktuelle Shopify-Sync-Version ausgeben")
    sub = parser.add_subparsers(dest="command")
    connect_cmd = sub.add_parser("connect", help="Shopify Public-App-Verbindung einrichten")
    connect_cmd.add_argument("--shop", required=True, help="Shop-Domain, z. B. beispiel.myshopify.com")
    connect_cmd.add_argument("--relay-base-url", help="Basis-URL des gehosteten OAuth-Relay-Servers")
    connect_cmd.add_argument("--port", type=int, default=3459, help="Lokaler Callback-Port fuer den Browser-Redirect")
    connect_cmd.add_argument("--timeout", type=int, default=DEFAULT_CONNECT_TIMEOUT_SECONDS, help="Wartezeit fuer den OAuth-Callback in Sekunden")
    connect_cmd.add_argument("--no-browser", action="store_true", help="Browser nicht automatisch oeffnen")
    manual_connect_cmd = sub.add_parser("manual-connect", help="Lokale Shopify-Verbindung ohne Relay einrichten")
    manual_connect_cmd.add_argument("--shop", required=True, help="Shop-Domain, z. B. beispiel.myshopify.com")
    manual_connect_cmd.add_argument("--client-id", required=True, help="Shopify App Client ID")
    manual_connect_cmd.add_argument("--client-secret", required=True, help="Shopify App Client Secret")
    manual_connect_cmd.add_argument("--scopes", required=True, help="Kommagetrennte Shopify-Scopes")
    manual_connect_cmd.add_argument("--redirect-uri", required=True, help="Erlaubte Redirect-URI der Shopify-App")
    manual_connect_cmd.add_argument("--port", type=int, default=3459, help="Lokaler Callback-Port fuer die Browser-Weiterleitung")
    manual_connect_cmd.add_argument("--timeout", type=int, default=DEFAULT_CONNECT_TIMEOUT_SECONDS, help="Wartezeit fuer den OAuth-Callback in Sekunden")
    manual_connect_cmd.add_argument("--no-browser", action="store_true", help="Browser nicht automatisch oeffnen")
    fulfill_cmd = sub.add_parser("fulfill", help="Fulfillment fuer Bestellung erzeugen")
    fulfill_cmd.add_argument("--order-id", required=True, help="Shopify Order GID")
    fulfill_cmd.add_argument("--tracking-number", required=True, help="Trackingnummer")
    fulfill_cmd.add_argument("--company", required=True, help="Versanddienstleister")
    fulfill_cmd.add_argument("--notify-customer", action="store_true", help="Kundenbenachrichtigung aktivieren")
    locations_cmd = sub.add_parser("list-locations", help="Shopify-Locations auflisten")
    locations_cmd.add_argument("--json", action="store_true", help="Locations als JSON ausgeben")
    version_cmd = sub.add_parser("version", help="Shopify-Sync-Version ausgeben")
    version_cmd.add_argument("--json", action="store_true", help="Version als JSON ausgeben")

    args = parser.parse_args()
    if args.version:
        print(SYNC_VERSION)
        return
    if args.command == "version":
        if args.json:
            print(json.dumps(build_sync_version_payload(), ensure_ascii=False))
        else:
            print(SYNC_VERSION)
        return
    if args.command == "connect":
        result = run_connect_flow(
            shop=args.shop,
            relay_base_url=args.relay_base_url,
            port=args.port,
            timeout_seconds=args.timeout,
            open_browser=not args.no_browser,
        )
        print(json.dumps({"shop": result["shop"], "connected": True}, ensure_ascii=False))
        return
    if args.command == "manual-connect":
        result = run_manual_connect_flow(
            shop=args.shop,
            client_id=args.client_id,
            client_secret=args.client_secret,
            scopes=args.scopes,
            redirect_uri=args.redirect_uri,
            port=args.port,
            timeout_seconds=args.timeout,
            open_browser=not args.no_browser,
        )
        print(json.dumps({"shop": result["shop"], "connected": True}, ensure_ascii=False))
        return
    if args.command == "fulfill":
        result = create_fulfillment(
            order_id=args.order_id,
            tracking_number=args.tracking_number,
            company=args.company,
            notify_customer=args.notify_customer,
        )
        print(json.dumps(result, ensure_ascii=False))
        return
    if args.command == "list-locations":
        locations = build_locations_payload()
        if args.json:
            print(json.dumps({"locations": locations}, ensure_ascii=False))
        else:
            print(format_locations_text(locations))
        return

    run_sync_loop()

if __name__ == "__main__":
    main()
