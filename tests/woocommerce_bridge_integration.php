<?php
/** Only a disposable WordPress instance with an explicit marker can run this fixture. */
$root = $argv[1] ?? '';
if (!is_file($root.'/.lager-mc-test-instance')) { fwrite(STDERR,"Isolated test marker missing\n");exit(2); }
$_SERVER['HTTP_HOST']='woo-test.invalid';
$_SERVER['REQUEST_URI']='/';
require $root.'/wp-load.php';
add_filter('pre_wp_mail',static fn()=>true);
if (!class_exists('LagerMC\\Bridge')) { throw new Exception('Bridge not activated'); }
$command = $argv[2] ?? 'tick';
if ($command === 'seed') {
    // Test both HPOS and legacy through the same Woo CRUD exporter.
    $ids=[];
    foreach (['no','yes'] as $hpos) {
        $sync=wc_get_container()->get(\Automattic\WooCommerce\Internal\DataStores\Orders\DataSynchronizer::class);
        $sync->create_database_tables();
        for ($i=0;$i<20 && ($batch=$sync->get_next_batch_to_process(100));$i++) { $sync->process_batch($batch); }
        update_option('woocommerce_custom_orders_table_enabled',$hpos);
        $p=new WC_Product_Simple();
        $p->set_name('Test '.$hpos);$p->set_status('publish');$p->set_sku('FIXTURE-'.$hpos.'-'.time());
        $p->set_manage_stock(true);$p->set_stock_quantity(10);$p->set_regular_price('12.50');$p->set_weight('0.25');$p->save();
        $o=wc_create_order();$o->add_product($p,2);
        $o->set_address(['first_name'=>'Test','last_name'=>'Gast','address_1'=>'Strasse 1','city'=>'Berlin','postcode'=>'12345','country'=>'DE'],'shipping');
        $o->set_billing_email('test@invalid.example');$o->set_currency('EUR');
        $o->calculate_totals();$o->set_status('processing');$o->set_date_paid(time());$o->save();
        wc_reduce_stock_levels($o->get_id());
        $snapshot=LagerMC\Bridge::product($p->get_id());
        if ($snapshot['stock']!==8 || !$snapshot['inventory_supported']) { throw new Exception('Invalid product stock snapshot '.json_encode($snapshot)); }
        $order=LagerMC\Bridge::order($o->get_id());
        if ($order['payment_status']!=='paid' || !$order['items'][0]['stock_reduced']) { throw new Exception('Invalid paid order snapshot'); }
        $ids[$hpos]=['product'=>$p->get_id(),'order'=>$o->get_id(),'line'=>$order['items'][0]['id']];
        LagerMC\Bridge::flush();
    }
    $p=new WC_Product_Simple();$p->set_name('Backorders');$p->set_manage_stock(true);$p->set_stock_quantity(-3);$p->set_backorders('yes');$p->save();
    if (LagerMC\Bridge::product($p->get_id())['inventory_supported']) { throw new Exception('Backorders must be rejected'); }
    update_option('lager_mc_test_fixture',$ids,false);
    file_put_contents($root.'/.lager-mc-fixture.json',json_encode($ids,JSON_THROW_ON_ERROR));
    // Finish initial scan without any HTTP requests.
    for($i=0;$i<20 && !get_option('lager_mc_ready');$i++) { LagerMC\Bridge::tick(); }
    echo json_encode(['capabilities'=>LagerMC\Bridge::capabilities(),'fixture'=>$ids],JSON_THROW_ON_ERROR)."\n";
} elseif ($command==='tick') {
    LagerMC\Bridge::tick();
    $error=get_option('lager_mc_error');
    if ($error) { throw new Exception($error); }
    echo "Bridge tick completed\n";
} else { throw new Exception('Unknown fixture command'); }
