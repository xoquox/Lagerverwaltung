#!/usr/bin/env python3
"""Prepare the worker's PG settings; preserve existing WooCommerce credentials."""
import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from app_settings import load_settings


def prepare(root, settings):
    root = Path(root)
    target = root / 'woocommerce-sync' / '.env'
    if target.exists():
        return target, False
    if settings.get('shop_system') != 'woocommerce':
        raise ValueError('WooCommerce ist fuer diese Installation nicht ausgewaehlt.')
    values = {name: settings[key] for name, key in (
        ('DB_HOST','db_host'),('DB_PORT','db_port'),('DB_NAME','db_name'),
        ('DB_USER','db_user'),('DB_PASS','db_pass'))}
    example = (root / 'woocommerce-sync' / '.env.example').read_text()
    lines = [line.split('=',1)[0]+'='+json.dumps(str(values[line.split('=',1)[0]]),ensure_ascii=False)
             if '=' in line and line.split('=',1)[0] in values else line
             for line in example.splitlines()]
    target.parent.mkdir(parents=True,exist_ok=True)
    # Exclusive creation protects a concurrently prepared configuration.
    fd = os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w') as stream:stream.write('\n'.join(lines)+'\n')
    return target, True


def main():
    target, created = prepare(ROOT, load_settings())
    print('WooCommerce-Sync-Konfiguration vorbereitet.' if created else 'Vorhandene WooCommerce-Sync-Konfiguration beibehalten.')
    return 0


if __name__ == '__main__':raise SystemExit(main())
