#!/usr/bin/env python3
"""Run with --help; .env is loaded only from this sync server's directory."""
import argparse
import logging
import signal
import sys
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent))
from woo_sync import VERSION
from woo_sync.config import Config
from woo_sync.worker import acquire_lock, connect_mysql, connect_pg, cycle


def main(argv=None):
    parser = argparse.ArgumentParser(description='Lager-MC / WooCommerce: ausschliesslich Datenbanktransport')
    parser.add_argument('command', choices=['run', 'once', 'migrate', 'doctor', 'version'])
    args = parser.parse_args(argv)
    if args.command == 'version':
        print(VERSION)
        return 0
    from dotenv import load_dotenv
    load_dotenv(ROOT / '.env', override=False, interpolate=False)
    config = Config.from_env(require_mysql=args.command != 'migrate')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    stopped = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopped.set())
    while not stopped.is_set():
        pg = mysql = None
        locked = False
        try:
            pg = connect_pg(config)
            acquire_lock(pg)
            locked = True
            if args.command == 'migrate':
                from shipping.schema import apply_app_schema
                from woo_sync.warehouse import migrate
                with pg.cursor() as cur:
                    apply_app_schema(cur, 'woocommerce')
                    migrate(cur)
                pg.commit()
                print('WooCommerce-Schema eingerichtet.')
                return 0
            mysql = connect_mysql(config)
            if args.command == 'doctor':
                from woo_sync.bridge import Bridge
                from shipping.shop import assert_shop_system
                from woo_sync.protocol import json_text
                with pg.cursor() as cur:
                    assert_shop_system(cur, 'woocommerce')
                info = Bridge(mysql, config.prefix, config.bridge_max_age).info()
                print(json_text({'store_id': info['store_id'], 'mode': config.mode, 'capabilities': info['capabilities']}))
                return 0
            cycle(config, pg, mysql)
            if args.command == 'once':
                return 0
            # Keep the session lock for the lifetime of this connection.
            while not stopped.wait(config.interval):
                cycle(config, pg, mysql)
        except Exception as exc:
            logging.error('WooCommerce-Sync: %s', exc)
            if pg is not None and locked:
                try:
                    pg.rollback()
                    from woo_sync.warehouse import Warehouse
                    Warehouse(pg).heartbeat('error', error=exc)
                except Exception:
                    pass
            if args.command != 'run':
                return 1
        finally:
            if mysql is not None:
                mysql.close()
            if pg is not None:
                pg.close()
        stopped.wait(config.interval)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
