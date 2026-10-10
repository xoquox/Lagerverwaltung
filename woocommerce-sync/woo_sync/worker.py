"""Database-only orchestration; one PostgreSQL session owns the worker lease."""
import logging
from .warehouse import Warehouse, migrate
from .bridge import Bridge
from shipping.shop import assert_shop_system

LOG = logging.getLogger('woocommerce-sync')
LOCK_ID = 781403925


def acquire_lock(pg):
    with pg.cursor() as cur:
        cur.execute('SELECT pg_try_advisory_lock(%s) AS locked', (LOCK_ID,))
        if not cur.fetchone()['locked']:
            raise RuntimeError('Ein WooCommerce-Sync laeuft bereits fuer diese Lagerdatenbank.')
    pg.commit()


def cycle(config, pg, mysql):
    warehouse, bridge = Warehouse(pg), Bridge(mysql, config.prefix, config.bridge_max_age)
    with pg.cursor() as cur:
        assert_shop_system(cur, 'woocommerce')
    info = bridge.info()
    bridge.bind(warehouse.bind(info['store_id']))
    caps = info['capabilities']
    if config.mode == 'read-write' and not caps.get('writes_enabled'):
        raise RuntimeError('Schreiben ist in WordPress nicht freigegeben (LAGER_MC_ALLOW_WRITES).')
    if caps.get('last_error'):
        raise RuntimeError('WordPress-Datenbankbruecke meldet: ' + str(caps['last_error']))
    if not caps.get('ready'):
        warehouse.heartbeat('initializing', error='WordPress-Bruecke erstellt den ersten Datenbankexport.')
        return
    events = bridge.events(config.batch)
    if events:
        bridge.acknowledge(warehouse.import_events(events))
    # Even in read-only mode acknowledge previously executed outbox commands.
    for command in warehouse.commands():
        result = bridge.result(command['command_id'])
        if result and result['status'] in {'done', 'failed'}:
            warehouse.complete(command, result)
        elif result and result['status'] not in {'pending', 'processing'}:
            raise RuntimeError('Unbekannter Remote-Auftragsstatus: ' + str(result['status']))
    # Drain the existing backlog before calculating outbound stock quantities.
    pushed = False
    if config.mode == 'read-write' and not bridge.backlog():
        warehouse.enqueue_changes(caps)
        for command in warehouse.commands():
            # Reinsert a missing remote inbox row with the same idempotency key
            # after a remote restore; INSERT IGNORE leaves existing rows intact.
            bridge.submit(command)
            warehouse.submitted(command['command_id'])
            pushed = True
    warehouse.heartbeat('read-only' if config.mode == 'read-only' else 'running', pull=bool(events), push=pushed)
    LOG.info('Datenbankabgleich: %d Ereignisse, Modus %s', len(events), config.mode)


def connect_pg(config):
    import psycopg2
    from psycopg2.extras import RealDictCursor
    return psycopg2.connect(**config.pg, cursor_factory=RealDictCursor)


def connect_mysql(config):
    import pymysql
    return pymysql.connect(**config.mysql, cursorclass=pymysql.cursors.DictCursor)
