#!/usr/bin/env python3

import sys
from pathlib import Path

import psycopg2
from psycopg2 import sql

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from shopify_sync import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER, db, ensure_runtime_dependencies, load_dotenv
from shipping.schema import apply_app_schema, collect_schema_issues


AUTOCOMMIT_LEVEL = getattr(
    getattr(psycopg2, "extensions", object()),
    "ISOLATION_LEVEL_AUTOCOMMIT",
    0,
)


def database_missing(exc):
    return getattr(exc, "pgcode", "") == "3D000" or "does not exist" in str(exc).lower()


def ensure_database_exists():
    try:
        probe = db()
        probe.close()
        return False
    except psycopg2.Error as exc:
        if not database_missing(exc):
            raise

    maintenance_db_names = ["postgres", "template1"]
    last_error = None
    for maintenance_name in maintenance_db_names:
        try:
            con = psycopg2.connect(
                host=DB_HOST,
                port=int(DB_PORT or 5432),
                database=maintenance_name,
                user=DB_USER,
                password=DB_PASS,
            )
            con.set_isolation_level(AUTOCOMMIT_LEVEL)
            cur = con.cursor()
            try:
                cur.execute(
                    "SELECT 1 FROM pg_database WHERE datname = %s",
                    (DB_NAME,),
                )
                exists = cur.fetchone()
                if not exists:
                    cur.execute(
                        f'CREATE DATABASE "{DB_NAME.replace(chr(34), chr(34) * 2)}"'
                    )
                    return True
                return False
            finally:
                cur.close()
                con.close()
        except psycopg2.Error as exc:
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    return False


def _column_type(cur, table_name, column_name):
    cur.execute(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        """,
        (table_name, column_name),
    )
    row = cur.fetchone()
    return (row[0] or "").lower() if row else ""


def apply_legacy_gid_type_fixes(cur):
    """Altbestand: numerische Shopify-ID-Spalten auf text (GID) umstellen."""
    targets = [
        ("items", "shopify_product_id"),
        ("items", "shopify_variant_id"),
        ("items", "shopify_inventory_item_id"),
    ]
    changed = []
    for table_name, column_name in targets:
        data_type = _column_type(cur, table_name, column_name)
        if not data_type or data_type == "text":
            continue
        cur.execute(
            sql.SQL(
                "ALTER TABLE {} ALTER COLUMN {} TYPE text USING {}::text"
            ).format(
                sql.Identifier(table_name),
                sql.Identifier(column_name),
                sql.Identifier(column_name),
            )
        )
        changed.append(f"{table_name}.{column_name}")
    if changed:
        print("Legacy-ID-Typfix angewendet:", ", ".join(changed))


def main():
    load_dotenv()
    ensure_runtime_dependencies()
    created = ensure_database_exists()
    con = db()
    cur = con.cursor()
    try:
        apply_app_schema(cur)
        apply_legacy_gid_type_fixes(cur)
        con.commit()
        issues = collect_schema_issues(cur)
        if issues:
            print("Migration unvollstaendig:", file=sys.stderr)
            for issue in issues:
                print(f"- {issue}", file=sys.stderr)
            return 1
        if created:
            print("Datenbank angelegt und Migration abgeschlossen.")
        else:
            print("DB Migration abgeschlossen.")
        return 0
    finally:
        cur.close()
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
