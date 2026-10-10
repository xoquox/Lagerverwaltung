import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'woocommerce-sync'))
from woo_sync.config import Config
from woo_sync.protocol import command_id, number, parse_id, validate_event, wc_id
from woo_sync.bridge import Bridge
from woo_sync.worker import cycle


def product(seq=1, stock=8, **fields):
    p = dict(id=42,parent_id=0,name='Testprodukt',sku='SKU42',fingerprint='fp',
             inventory_supported=True,stock=stock,held=1,status='ACTIVE',currency='EUR',
             requires_shipping=True,price='12.50',weight_grams=250)
    p.update(fields)
    return dict(seq=seq,kind='product',entity_id=42,payload=p,deleted=0)


def order(seq=2, qty=2, fulfilled=0, **fields):
    p = dict(id=123,number='#123',currency='EUR',fulfillment_status='unfulfilled',
             payment_status='paid',address=dict(name='Gast',address1='Strasse 1'),
             items=[dict(id=7,product_id=42,name='Testprodukt',quantity=qty,fulfilled=fulfilled,stock_reduced=True)])
    p.update(fields)
    return dict(seq=seq,kind='order',entity_id=123,payload=p,deleted=0)


class WooProtocolTests(unittest.TestCase):
    def test_quantities_reject_fractional_negative_nan_and_boolean(self):
        for value in ['1.5',-1,'NaN',True,None,2147483648]:
            with self.subTest(value=value), self.assertRaises(ValueError): number(value)
        self.assertEqual(number('2.00'),2)

    def test_ids_and_sequences_support_bigint_without_confusing_providers(self):
        self.assertEqual(parse_id('order',wc_id('order',5000000000)),5000000000)
        validate_event(product(seq=5000000000))
        for value in ['gid://shopify/Order/1','wc:product:1','wc:order:NaN','wc:order:1.5']:
            with self.subTest(value=value), self.assertRaises(ValueError): parse_id('order',value)

    def test_invalid_snapshots_are_rejected(self):
        for event in [product(currency='USD'),product(stock=-1),product(price='NaN'),
                      product(inventory_supported='false'),order(fulfilled=3),order(payment_status='processing')]:
            with self.subTest(event=event), self.assertRaises(ValueError): validate_event(event)
        event=order();event['payload']['items']*=2
        with self.assertRaises(ValueError):validate_event(event)

    def test_outbox_ids_are_stable_and_depend_on_revision(self):
        a=command_id('inventory','SKU','rev',{'a':1,'b':2})
        self.assertEqual(a,command_id('inventory','SKU','rev',{'b':2,'a':1}))
        self.assertNotEqual(a,command_id('inventory','SKU','new',{'a':1,'b':2}))

    def test_configuration_rejects_sql_prefix_and_unsafe_modes(self):
        env=dict(WOO_DB_HOST='db',WOO_DB_NAME='wp',WOO_DB_USER='bridge')
        self.assertEqual(Config.from_env(env).mode,'read-only')
        for field,value in [('WOO_TABLE_PREFIX','wp_;DROP'),('WOO_SYNC_MODE','write'),('SYNC_INTERVAL','0')]:
            with self.subTest(field=field),self.assertRaises(ValueError):Config.from_env(dict(env,**{field:value}))
        with self.assertRaises(ValueError): Bridge(Mock(),'bad`')
        config=Config.from_env(dict(env,WOO_DB_SSL_CA='/ca.pem'))
        self.assertTrue(config.mysql['ssl_verify_cert']);self.assertTrue(config.mysql['ssl_verify_identity'])

    def test_stopped_wordpress_cron_is_reported(self):
        from unittest.mock import MagicMock
        con=MagicMock();cur=con.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value=dict(protocol_version=1,age_seconds=301,payload='{"currency":"EUR"}')
        with self.assertRaisesRegex(RuntimeError,'Heartbeat'):Bridge(con,'wp_').info()
        cur.fetchone.return_value=dict(protocol_version=1,age_seconds=10,payload='{"currency":"EUR"}')
        self.assertEqual(Bridge(con,'wp_').info()['capabilities']['currency'],'EUR')


class WooWorkerTests(unittest.TestCase):
    def run_cycle(self,mode='read-only',backlog=0,ready=True,events=None):
        self.w=Mock();self.b=Mock();self.w.bind.return_value='client'
        self.b.info.return_value=dict(store_id='store',capabilities=dict(ready=ready,writes_enabled=True))
        self.b.events.return_value=events or []
        self.b.backlog.return_value=backlog
        self.w.commands.return_value=[]
        cfg=Config({}, {}, mode=mode)
        pg=Mock()
        pg.cursor.return_value.__enter__=Mock(return_value=Mock())
        pg.cursor.return_value.__exit__=Mock()
        with patch('woo_sync.worker.Warehouse',return_value=self.w),patch('woo_sync.worker.Bridge',return_value=self.b),patch('woo_sync.worker.assert_shop_system'):
            cycle(cfg,pg,Mock())

    def test_readonly_never_creates_or_submits_warehouse_changes(self):
        self.run_cycle();self.w.enqueue_changes.assert_not_called();self.b.submit.assert_not_called()

    def test_initial_export_blocks_import_and_writes(self):
        self.run_cycle(mode='read-write',ready=False)
        self.b.events.assert_not_called();self.w.enqueue_changes.assert_not_called()
        self.w.heartbeat.assert_called_once()

    def test_write_waits_for_import_backlog(self):
        self.run_cycle(mode='read-write',backlog=1)
        self.w.enqueue_changes.assert_not_called()
        self.run_cycle(mode='read-write',backlog=0)
        self.w.enqueue_changes.assert_called_once()

    def test_remote_ack_happens_only_after_committed_import(self):
        self.run_cycle(events=[product()]);self.w.import_events.assert_called_once()
        self.b.acknowledge.assert_called_once_with(self.w.import_events.return_value)


class WooInstallerConfigurationTests(unittest.TestCase):
    def test_credentials_roundtrip_and_existing_configuration_is_preserved(self):
        import importlib.util
        import tempfile
        import subprocess
        spec=importlib.util.spec_from_file_location('configure_woo',Path(__file__).resolve().parents[1]/'scripts/configure_woocommerce_sync.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'woocommerce-sync').mkdir()
            (root/'woocommerce-sync'/'.env.example').write_text('DB_HOST=\nDB_PORT=5432\nDB_NAME=\nDB_USER=\nDB_PASS=\nWOO_DB_HOST=\n')
            settings=dict(shop_system='woocommerce',db_host='host',db_port=5432,db_name='db',db_user='user',db_pass='päss " ${SECRET} \\ \nline')
            path,created=module.prepare(root,settings)
            self.assertTrue(created)
            # Other legacy tests install process-wide dotenv stubs. Parse in a
            # clean interpreter using the real dependency, as the worker does.
            result=subprocess.run([sys.executable,'-c',
                'import json,sys,importlib.util\nif importlib.util.find_spec("dotenv"):\n from dotenv import dotenv_values\n print(json.dumps(dotenv_values(sys.argv[1],interpolate=False)))\nelse: print("null")',str(path)],
                check=True,capture_output=True,text=True)
            import json
            parsed=json.loads(result.stdout)
            if parsed is not None:
                self.assertEqual(parsed['DB_PASS'],settings['db_pass'])
            self.assertEqual(path.stat().st_mode & 0o777,0o600)
            original=path.read_bytes()
            _,created=module.prepare(root,dict(settings,db_pass='changed'))
            self.assertFalse(created);self.assertEqual(path.read_bytes(),original)

class WooTransportArchitectureTests(unittest.TestCase):
    def test_worker_has_no_http_clients_or_shop_rest_calls(self):
        import ast
        root=Path(__file__).resolve().parents[1]/'woocommerce-sync'
        forbidden={'requests','httpx','aiohttp','urllib','http','socket'}
        for path in root.rglob('*.py'):
            with self.subTest(path=path.name):
                tree=ast.parse(path.read_text())
                for node in ast.walk(tree):
                    if isinstance(node,ast.Import):
                        for name in node.names:self.assertNotIn(name.name.split('.')[0],forbidden)
                    if isinstance(node,ast.ImportFrom) and node.module:
                        self.assertNotIn(node.module.split('.')[0],forbidden)

if __name__=='__main__':unittest.main()
