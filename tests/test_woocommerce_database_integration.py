"""Separate process: tests use a disposable PostgreSQL instance, never production."""
import os
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'woocommerce-sync'))
import test_shop_database_integration as pg_fixture
from test_woocommerce_sync import product, order
from woo_sync.warehouse import Warehouse, migrate, LOCATION_ID


@unittest.skipUnless(os.environ.get('LAGER_MC_TEST_DSN'),'LAGER_MC_TEST_DSN not configured')
class WooDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.base=pg_fixture.ShopDatabaseIntegrationTests()
        self.base.setUp();self.base.migrate('woocommerce')
        from psycopg2.extras import RealDictCursor
        self.con=self.base.con;self.con.cursor_factory=RealDictCursor
        with self.con.cursor() as cur:migrate(cur)
        self.con.commit();self.w=Warehouse(self.con)

    def tearDown(self):self.base.tearDown()

    def row(self,sql,params=()):
        with self.con.cursor() as c:c.execute(sql,params);return c.fetchone()

    def edit(self,sql,params=()):
        with self.con.cursor() as c:c.execute(sql,params)
        self.con.commit()

    def test_snapshot_stock_paid_order_guests_and_redelivery(self):
        self.w.import_events([product(),order()])
        item=self.row('SELECT * FROM items')
        self.assertEqual((item['menge'],item['available'],item['committed'],item['reserved']),(10,7,2,1))
        self.assertEqual(self.row('SELECT * FROM shop_customers')['customer_id'],'wc:guest:123')
        self.w.import_events([product(stock=99),order(qty=8)])
        self.assertEqual(self.row('SELECT menge FROM items')['menge'],10)
        self.assertEqual(self.row('SELECT quantity FROM shop_order_items')['quantity'],2)

    def test_late_commit_does_not_override_newer_entity(self):
        self.w.import_events([product(seq=5,stock=12)])
        self.w.import_events([product(seq=3,stock=4)])
        self.assertEqual(self.row('SELECT menge FROM items')['menge'],12)

    def test_invalid_batch_writes_nothing(self):
        with self.assertRaises(ValueError):self.w.import_events([product(),order(fulfilled=4)])
        self.con.rollback()
        self.assertEqual(self.row('SELECT count(*) n FROM items')['n'],0)

    def test_local_dirty_product_and_inventory_survive_checkout(self):
        self.w.import_events([product(),order()])
        self.edit("UPDATE items SET name='Lokal',shop_product_dirty=true,updated_at=now()")
        self.edit('UPDATE item_location_inventory SET menge=20,dirty=true,updated_at=now()')
        self.w.import_events([product(seq=3,stock=6,name='Remote'),order(seq=4,qty=4)])
        item=self.row('SELECT * FROM items')
        self.assertEqual((item['name'],item['menge'],item['committed'],item['dirty']),('Lokal',20,4,True))
        self.w.enqueue_changes({})
        commands=self.w.commands()
        stock=next(c for c in commands if c['kind']=='inventory')
        self.assertEqual(stock['payload']['stock'],16)

    def test_ack_waits_for_postwrite_snapshot_and_preserves_newer_edit(self):
        self.w.import_events([product()])
        self.edit('UPDATE item_location_inventory SET menge=20,dirty=true,updated_at=now()')
        self.w.enqueue_changes({});command=self.w.commands()[0]
        self.w.submitted(command['command_id'])
        result=dict(status='done',result=dict(event_seq=3))
        self.w.complete(command,result)
        self.assertTrue(self.row('SELECT dirty FROM items')['dirty'])
        self.w.import_events([product(seq=3,stock=20)])
        self.edit('UPDATE item_location_inventory SET menge=25,updated_at=now()')
        self.w.complete(command,result)
        self.assertEqual(self.row('SELECT menge,dirty FROM items'),dict(menge=25,dirty=True))

    def test_successful_stock_ack_recomputes_using_fresh_remote_stock(self):
        self.w.import_events([product()])
        self.edit('UPDATE item_location_inventory SET menge=20,dirty=true,updated_at=now()')
        self.w.enqueue_changes({});c=self.w.commands()[0]
        self.w.import_events([product(seq=3,stock=20)])
        self.w.complete(c,dict(status='done',result=dict(event_seq=3)))
        self.assertEqual(self.row('SELECT menge,dirty FROM items'),dict(menge=20,dirty=False))

    def test_product_ack_and_failed_revision_require_new_edit(self):
        self.w.import_events([product()])
        self.edit("UPDATE items SET name='Neu',shop_product_dirty=true,updated_at=now()")
        self.w.enqueue_changes({});c=self.w.commands()[0]
        self.w.complete(c,dict(status='failed',error='Conflict'))
        self.w.import_events([product(seq=3,fingerprint='new')])
        self.w.enqueue_changes({})
        self.assertEqual(self.w.commands(),[])
        self.assertEqual(self.row('SELECT shop_product_sync_error FROM items')['shop_product_sync_error'],'Conflict')
        self.edit("UPDATE items SET updated_at=now()")
        self.w.enqueue_changes({});self.assertEqual(len(self.w.commands()),1)

    def test_refunds_and_cancellations_release_committed(self):
        self.w.import_events([product(),order()])
        self.w.import_events([order(seq=3,qty=1),product(seq=4,stock=9)])
        self.assertEqual(self.row('SELECT menge,committed FROM items'),dict(menge=10,committed=1))
        self.w.import_events([order(seq=5,fulfillment_status='cancelled'),product(seq=6,stock=10)])
        self.assertEqual(self.row('SELECT menge,committed FROM items'),dict(menge=10,committed=0))

    def test_guest_unpaid_order_and_unsupported_inventory(self):
        p=product(inventory_supported=False,stock=0)
        o=order(payment_status='pending');o['payload']['items'][0]['stock_reduced']=False
        self.w.import_events([p,o])
        self.assertEqual(self.row('SELECT payment_status FROM shop_orders')['payment_status'],'pending')
        self.assertEqual(self.row('SELECT menge FROM items')['menge'],0)

    def test_wrong_store_and_provider_are_rejected(self):
        client=self.w.bind('store-one');self.assertEqual(client,self.w.bind('store-one'))
        with self.assertRaises(RuntimeError):self.w.bind('store-two')
        self.con.rollback()
        self.edit("UPDATE shop_configuration SET shop_system='shopify'")
        with self.assertRaises(RuntimeError):self.w.import_events([product()])
        self.con.rollback()

    def test_shipping_job_rejection_and_outbox_restart(self):
        self.w.import_events([product(),order()])
        self.edit("INSERT INTO shop_fulfillment_jobs(order_id,tracking_number,carrier) VALUES('wc:order:123','TRACK','gls')")
        self.w.enqueue_changes({'native_fulfillments':False})
        self.assertEqual(self.row('SELECT status FROM shop_fulfillment_jobs')['status'],'failed')
        self.edit("UPDATE shop_fulfillment_jobs SET status='pending'")
        self.w.enqueue_changes({'native_fulfillments':True})
        c=self.w.commands()[0]
        self.assertEqual(self.row('SELECT status FROM shop_fulfillment_jobs')['status'],'processing')
        self.w=Warehouse(self.con)
        self.assertEqual(self.w.commands()[0]['command_id'],c['command_id'])
        self.w.import_events([order(seq=3,fulfilled=2,fulfillment_status='fulfilled')])
        self.w.complete(c,dict(status='done',result=dict(event_seq=3,fulfillment_id='wc:shipment:9')))
        self.assertEqual(self.row('SELECT status,shop_fulfillment_id FROM shop_fulfillment_jobs'),dict(status='done',shop_fulfillment_id='wc:shipment:9'))

    def test_product_deletion_preserves_warehouse_and_manual_data(self):
        self.w.import_events([product(),order()])
        self.edit("INSERT INTO shop_orders(order_id,order_name,source) VALUES('custom:1','#M','custom')")
        self.w.import_events([dict(seq=4,kind='order',entity_id=123,deleted=1),dict(seq=5,kind='product',entity_id=42,deleted=1)])
        self.assertEqual(self.row('SELECT shop_product_status FROM items')['shop_product_status'],'ARCHIVED')
        self.assertEqual(self.row('SELECT order_id FROM shop_orders')['order_id'],'custom:1')

if __name__=='__main__':unittest.main()
