import os
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    pg: dict
    mysql: dict
    prefix: str = "wp_"
    mode: str = "read-only"
    interval: int = 30
    batch: int = 200
    bridge_max_age: int = 300

    @classmethod
    def from_env(cls, env=None, *, require_mysql=True):
        env = os.environ if env is None else env
        prefix = env.get("WOO_TABLE_PREFIX", "wp_")
        if not re.fullmatch(r"[A-Za-z0-9_]+", prefix):
            raise ValueError("WOO_TABLE_PREFIX darf nur Buchstaben, Ziffern und Unterstriche enthalten.")
        mode = env.get("WOO_SYNC_MODE", "read-only")
        if mode not in {"read-only", "read-write"}:
            raise ValueError("WOO_SYNC_MODE: read-only oder read-write erforderlich.")
        interval, batch = int(env.get("SYNC_INTERVAL", 30)), int(env.get("SYNC_BATCH_SIZE", 200))
        bridge_max_age = int(env.get('WOO_BRIDGE_MAX_AGE',300))
        if interval < 1 or not 1 <= batch <= 1000:
            raise ValueError("SYNC_INTERVAL >= 1, SYNC_BATCH_SIZE zwischen 1 und 1000 erforderlich.")
        if bridge_max_age < 60:
            raise ValueError('WOO_BRIDGE_MAX_AGE muss mindestens 60 Sekunden betragen.')
        pg = dict(host=env.get("DB_HOST", "localhost"), port=int(env.get("DB_PORT", 5432)),
                  dbname=env.get("DB_NAME", "lagerdb"), user=env.get("DB_USER", "lager"),
                  password=env.get("DB_PASS", ""), connect_timeout=10)
        required = [key for key in ("WOO_DB_HOST", "WOO_DB_NAME", "WOO_DB_USER") if not env.get(key)]
        if required and require_mysql:
            raise ValueError("Fehlende WooCommerce-Datenbankkonfiguration: " + ", ".join(required))
        mysql = dict(host=env.get("WOO_DB_HOST", ""), port=int(env.get("WOO_DB_PORT", 3306)),
                     database=env.get("WOO_DB_NAME", ""), user=env.get("WOO_DB_USER", ""),
                     password=env.get("WOO_DB_PASS", ""), charset="utf8mb4",
                     connect_timeout=10, read_timeout=30, write_timeout=30, autocommit=True)
        if env.get("WOO_DB_SSL_CA"):
            mysql.update(ssl_ca=env["WOO_DB_SSL_CA"], ssl_verify_cert=True, ssl_verify_identity=True)
        return cls(pg, mysql, prefix, mode, interval, batch, bridge_max_age)
