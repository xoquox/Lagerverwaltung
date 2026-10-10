<?php
/**
 * Plugin Name: Lager-MC Database Bridge
 * Description: Database inbox/outbox for Lager-MC. No REST endpoint or external connection.
 * Version: 0.1.0
 * Requires PHP: 8.1
 * Requires Plugins: woocommerce
 */
namespace LagerMC;

use Automattic\WooCommerce\Admin\Features\Fulfillments\Fulfillment;
use Automattic\WooCommerce\Admin\Features\Fulfillments\FulfillmentUtils;

defined('ABSPATH') || exit;

final class Bridge {
    private static array $queued = [];
    private static bool $working = false;

    private static function table(string $name): string {
        global $wpdb;
        return $wpdb->prefix . 'lager_mc_' . $name;
    }

    public static function install(): void {
        global $wpdb;
        require_once ABSPATH . 'wp-admin/includes/upgrade.php';
        $collate = $wpdb->get_charset_collate();
        $events = self::table('events');
        $commands = self::table('commands');
        $info = self::table('info');
        dbDelta("CREATE TABLE $events (
            seq bigint unsigned NOT NULL AUTO_INCREMENT,
            kind varchar(16) NOT NULL,
            entity_id bigint unsigned NOT NULL,
            payload longtext NULL,
            deleted tinyint NOT NULL DEFAULT 0,
            acknowledged tinyint NOT NULL DEFAULT 0,
            created_at datetime NOT NULL,
            PRIMARY KEY (seq),
            KEY pending (acknowledged, seq)
        ) ENGINE=InnoDB $collate;
        CREATE TABLE $commands (
            command_id varchar(64) NOT NULL,
            kind varchar(32) NOT NULL,
            payload longtext NOT NULL,
            status varchar(16) NOT NULL DEFAULT 'pending',
            result longtext NULL,
            error text NULL,
            created_at datetime NOT NULL,
            updated_at datetime NULL,
            PRIMARY KEY (command_id),
            KEY pending (status, created_at)
        ) ENGINE=InnoDB $collate;
        CREATE TABLE $info (
            id int NOT NULL,
            protocol_version int NOT NULL,
            store_id varchar(36) NOT NULL,
            client_id varchar(36) NULL,
            payload longtext NOT NULL,
            updated_at datetime NOT NULL,
            PRIMARY KEY (id)
        ) ENGINE=InnoDB $collate;");
        self::check($wpdb->query($wpdb->prepare("INSERT IGNORE INTO $info (id,protocol_version,store_id,payload,updated_at) VALUES(1,1,%s,'{}',UTC_TIMESTAMP())", wp_generate_uuid4())));
        if (!get_option('lager_mc_scan')) {
            update_option('lager_mc_scan', ['phase'=>'product', 'last'=>0, 'page'=>1], false);
        }
        if (!wp_next_scheduled('lager_mc_tick')) {
            wp_schedule_event(time()+5, 'lager_mc_minute', 'lager_mc_tick');
        }
    }

    public static function schedules(array $schedules): array {
        $schedules['lager_mc_minute'] = ['interval'=>60, 'display'=>'Lager-MC database bridge'];
        return $schedules;
    }

    public static function native(): bool {
        return defined('WC_VERSION') && version_compare(WC_VERSION, '10.7', '>=')
            && get_option('woocommerce_feature_fulfillments_enabled', 'no') === 'yes'
            && class_exists(Fulfillment::class) && class_exists(FulfillmentUtils::class);
    }

    private static function cogs(): bool {
        $class = '\Automattic\WooCommerce\Internal\Features\FeaturesController';
        return class_exists($class) && wc_get_container()->get($class)->feature_is_enabled('cost_of_goods_sold');
    }

    public static function capabilities(): array {
        return ['ready'=>(bool)get_option('lager_mc_ready', false),
            'currency'=>get_woocommerce_currency(), 'woocommerce_version'=>WC_VERSION,
            'native_fulfillments'=>self::native(), 'cogs'=>self::cogs(),
            'writes_enabled'=>defined('LAGER_MC_ALLOW_WRITES') && LAGER_MC_ALLOW_WRITES === true,
            'locations'=>1, 'product_creation'=>false, 'categories'=>false,
            'customer_notifications'=>false,
            'last_error'=>get_option('lager_mc_error', null)];
    }

    private static function check($result) {
        global $wpdb;
        if ($result === false) {
            throw new \RuntimeException('Database bridge query failed: ' . $wpdb->last_error);
        }
        return $result;
    }

    private static function lock(): bool {
        global $wpdb;
        return (int)$wpdb->get_var($wpdb->prepare('SELECT GET_LOCK(%s,5)', self::table('lock'))) === 1;
    }

    private static function unlock(): void {
        global $wpdb;
        $wpdb->get_var($wpdb->prepare('SELECT RELEASE_LOCK(%s)', self::table('lock')));
    }

    public static function queue(string $kind, $id, bool $deleted=false): void {
        if (!self::$working && (int)$id > 0) {
            self::$queued[$kind . ':' . (int)$id] = [$kind, (int)$id, $deleted];
        }
    }

    public static function product_changed($id): void { self::queue('product', $id); }
    public static function stock_changed($product): void { self::queue('product', $product->get_id()); }
    public static function customer_changed($id): void { self::queue('customer', $id); }
    public static function order_changed($id): void {
        if (is_object($id)) { $id = $id->get_id(); }
        $order = wc_get_order($id);
        if ($order instanceof \WC_Order_Refund) {
            $order = wc_get_order($order->get_parent_id());
        }
        if (!$order instanceof \WC_Order) { return; }
        self::queue('order', $order->get_id());
        foreach ($order->get_items() as $item) {
            self::queue('product', $item->get_variation_id() ?: $item->get_product_id());
        }
    }
    public static function fulfillment_changed($fulfillment): void {
        self::order_changed((int)$fulfillment->get_entity_id());
    }
    public static function post_deleted($id): void {
        $type = get_post_type($id);
        if (in_array($type, ['product','product_variation'], true)) { self::queue('product', $id, true); }
        if ($type === 'shop_order') { self::queue('order', $id, true); }
    }
    public static function order_deleted($id): void { self::queue('order', $id, true); }
    public static function customer_deleted($id): void { self::queue('customer', $id, true); }

    public static function flush(): void {
        global $wpdb;
        if (!self::$queued || !function_exists('wc_get_product') || !self::lock()) { return; }
        self::$working = true;
        try {
            self::check($wpdb->query('START TRANSACTION'));
            foreach (self::$queued as [$kind,$id,$deleted]) { self::export($kind,$id,$deleted); }
            self::check($wpdb->query('COMMIT'));
            self::$queued = [];
        } catch (\Throwable $e) {
            $wpdb->query('ROLLBACK');
            update_option('lager_mc_error', $e->getMessage(), false);
            wc_get_logger()->error($e->getMessage(), ['source'=>'lager-mc-bridge']);
        } finally {
            self::$working = false;
            self::unlock();
        }
    }

    private static function export(string $kind, int $id, bool $deleted=false): int {
        global $wpdb;
        $payload = null;
        if (!$deleted) {
            $payload = match($kind) {
                'product'=>self::product($id), 'order'=>self::order($id), 'customer'=>self::customer($id),
                default=>throw new \InvalidArgumentException('Unknown entity type')
            };
            if ($payload === null) { $deleted = true; }
        }
        self::check($wpdb->insert(self::table('events'), ['kind'=>$kind,'entity_id'=>$id,
            'payload'=>$deleted ? null : wp_json_encode($payload, JSON_THROW_ON_ERROR),
            'deleted'=>$deleted ? 1 : 0,'created_at'=>gmdate('Y-m-d H:i:s')]));
        return (int)$wpdb->insert_id;
    }

    public static function product(int $id): ?array {
        $p = wc_get_product($id);
        if (!$p || $p->is_type('variable') || $p->get_status() === 'trash') { return null; }
        $stock = $p->get_stock_quantity('edit');
        $supported = $p->is_type(['simple','variation']) && $p->get_manage_stock('edit') === true
            && $p->get_stock_managed_by_id() === $id && !$p->backorders_allowed()
            && is_numeric($stock) && (float)$stock >= 0 && floor((float)$stock) == (float)$stock;
        $sale = $p->get_sale_price('edit');
        $regular = $p->get_regular_price('edit');
        $sale_active = $p->is_on_sale('edit');
        $weight = $p->get_weight('edit');
        $fields = ['id'=>$id,'parent_id'=>$p->get_parent_id(),'name'=>$p->get_name('edit'),
            'sku'=>$p->get_sku('edit'),'description'=>$p->get_description('edit'),
            'status'=>match($p->get_status('edit')) {'publish'=>'ACTIVE','private'=>'ARCHIVED',default=>'DRAFT'},
            'price'=>$sale_active ? $sale : ($regular !== '' ? $regular : null),
            'compare_at_price'=>$sale_active ? $regular : null,
            'sale_from'=>$p->get_date_on_sale_from() ? $p->get_date_on_sale_from()->date('c') : null,
            'sale_to'=>$p->get_date_on_sale_to() ? $p->get_date_on_sale_to()->date('c') : null,
            'unit_cost'=>self::cogs() && method_exists($p,'get_cogs_total_value') ? (string)$p->get_cogs_total_value() : null,
            'weight_grams'=>$weight !== '' ? (int)round(wc_get_weight($weight, 'g')) : null,
            'barcode'=>method_exists($p,'get_global_unique_id') ? ($p->get_global_unique_id('edit') ?: null) : null,
            'requires_shipping'=>$p->needs_shipping(),'currency'=>get_woocommerce_currency()];
        $fields['fingerprint'] = hash('sha256', wp_json_encode($fields, JSON_THROW_ON_ERROR));
        $fields['inventory_supported'] = $supported;
        $fields['stock'] = $supported ? (int)$stock : 0;
        $fields['held'] = $supported ? (int)wc_get_held_stock_quantity($p) : 0;
        $fields['inventory_error'] = $supported ? null : 'Requires own integer stock, no backorders, simple/variation product.';
        return $fields;
    }

    private static function fulfillments($order): array {
        if (!self::native()) { return []; }
        return \WC_Data_Store::load('order-fulfillment')->read_fulfillments(\WC_Order::class, (string)$order->get_id(), false);
    }

    private static function address($object, string $prefix): array {
        $get = static function(string $field) use ($object,$prefix) {
            $method = 'get_' . $prefix . '_' . $field;
            return method_exists($object,$method) ? $object->$method() : '';
        };
        return ['name'=>trim($get('first_name').' '.$get('last_name')),'address1'=>$get('address_1'),
            'address2'=>$get('address_2'),'zip'=>$get('postcode'),'city'=>$get('city'),
            'country'=>$get('country'),'phone'=>$get('phone'),'email'=>$get('email')];
    }

    public static function order(int $id): ?array {
        $o = wc_get_order($id);
        if (!$o instanceof \WC_Order || $o instanceof \WC_Order_Refund || $o->get_status() === 'trash') { return null; }
        $fulfilled = [];
        $shipments = [];
        foreach (self::fulfillments($o) as $f) {
            if (!$f->get_is_fulfilled()) { continue; }
            foreach ($f->get_items() as $line) {
                $fulfilled[$line['item_id']] = ($fulfilled[$line['item_id']] ?? 0) + (int)$line['qty'];
            }
            $shipments[] = ['id'=>'wc:shipment:'.$f->get_id(),'tracking_number'=>$f->get_tracking_number(),
                'tracking_url'=>$f->get_tracking_url(),'carrier'=>$f->get_shipment_provider() ?: 'other'];
        }
        $lines = [];
        $open = $total = 0;
        foreach ($o->get_items() as $item) {
            $qty = max(0, $item->get_quantity() + $o->get_qty_refunded_for_item($item->get_id()));
            $p = $item->get_product();
            // Without native fulfillments Woo's completed status is the only full-shipment signal.
            $done = self::native() ? min($qty, $fulfilled[$item->get_id()] ?? ($p && !$p->needs_shipping() ? $qty : 0))
                : ($o->has_status('completed') || ($p && !$p->needs_shipping()) ? $qty : 0);
            $lines[] = ['id'=>$item->get_id(),'product_id'=>$item->get_variation_id() ?: $item->get_product_id(),
                'name'=>$item->get_name(),'quantity'=>$qty,'fulfilled'=>$done,
                'stock_reduced'=>(float)$item->get_meta('_reduced_stock',true) > 0];
            $open += $qty-$done;
            $total += $qty;
        }
        $status = $o->has_status(['cancelled','failed','refunded']) ? 'cancelled'
            : ($open === 0 ? 'fulfilled' : ($open < $total ? 'partial' : 'unfulfilled'));
        $payment = $o->has_status('refunded') ? 'refunded'
            : ($o->get_date_paid() ? 'paid' : ($o->has_status(['cancelled','failed']) ? 'voided' : 'pending'));
        $a = self::address($o,'shipping');
        if (!$a['address1']) { $a = self::address($o,'billing'); }
        $a['email'] = $o->get_billing_email();
        $a['phone'] = $a['phone'] ?: $o->get_billing_phone();
        return ['id'=>$id,'number'=>'#'.$o->get_order_number(),'currency'=>$o->get_currency(),
            'created_at'=>$o->get_date_created() ? $o->get_date_created()->date('c') : null,
            'address'=>$a,'customer_id'=>$o->get_customer_id(),'items'=>$lines,'shipments'=>$shipments,
            'fulfillment_status'=>$status,'payment_status'=>$payment];
    }

    public static function customer(int $id): ?array {
        $u = get_userdata($id);
        if (!$u || !in_array('customer',$u->roles,true)) { return null; }
        $c = new \WC_Customer($id);
        $a = self::address($c,'shipping');
        if (!$a['address1']) { $a = self::address($c,'billing'); }
        return ['id'=>$id,'first_name'=>$c->get_first_name(),'last_name'=>$c->get_last_name(),
            'name'=>$c->get_first_name().' '.$c->get_last_name(),'email'=>$c->get_email(),
            'phone'=>$c->get_billing_phone(),'address'=>$a];
    }

    private static function scan(): void {
        global $wpdb;
        $scan = get_option('lager_mc_scan');
        if (!$scan) {
            if ((int)get_option('lager_mc_last_scan',0) < time()-86400) {
                $scan = ['phase'=>'product','last'=>0,'page'=>1];
            } else { return; }
        }
        $kind = $scan['phase'];
        if ($kind === 'product') {
            $ids = $wpdb->get_col($wpdb->prepare("SELECT ID FROM {$wpdb->posts} WHERE ID>%d AND post_type IN ('product','product_variation') ORDER BY ID LIMIT 100", $scan['last']));
        } elseif ($kind === 'order') {
            $ids = wc_get_orders(['type'=>'shop_order','limit'=>100,'page'=>$scan['page'],'orderby'=>'ID','order'=>'ASC','return'=>'ids','status'=>array_keys(wc_get_order_statuses())]);
        } else {
            $ids = get_users(['role'=>'customer','number'=>100,'offset'=>($scan['page']-1)*100,'orderby'=>'ID','order'=>'ASC','fields'=>'ID']);
        }
        if ($wpdb->last_error || !is_array($ids)) { throw new \RuntimeException('Initial export query failed: '.$wpdb->last_error); }
        foreach ($ids as $id) { self::export($kind,(int)$id); }
        if ($ids) { $scan['last'] = (int)end($ids); $scan['page']++; }
        if (count($ids) < 100) {
            $scan = match($kind) {
                'product'=>['phase'=>'order','last'=>0,'page'=>1],
                'order'=>['phase'=>'customer','last'=>0,'page'=>1], default=>null
            };
        }
        update_option('lager_mc_scan',$scan,false);
        if ($scan === null) {
            update_option('lager_mc_ready',true,false);
            update_option('lager_mc_last_scan',time(),false);
        }
    }

    public static function tick(): void {
        global $wpdb;
        if (!function_exists('wc_get_product') || !self::lock()) { return; }
        self::$working = true;
        try {
            self::scan();
            $caps = self::capabilities();
            if ($caps['writes_enabled'] && $caps['ready'] && $caps['currency'] === 'EUR') {
                $commands = $wpdb->get_results('SELECT * FROM '.self::table('commands')." WHERE status IN ('pending','processing') ORDER BY created_at LIMIT 20", ARRAY_A);
                if ($wpdb->last_error || !is_array($commands)) { throw new \RuntimeException('Command query failed: '.$wpdb->last_error); }
                foreach ($commands as $command) { self::execute($command); }
            }
            delete_option('lager_mc_error');
            self::check($wpdb->update(self::table('info'), ['payload'=>wp_json_encode(self::capabilities(),JSON_THROW_ON_ERROR),'updated_at'=>gmdate('Y-m-d H:i:s')], ['id'=>1]));
            self::check($wpdb->query('DELETE FROM '.self::table('events')." WHERE acknowledged=1 AND created_at<UTC_TIMESTAMP()-INTERVAL 7 DAY LIMIT 1000"));
        } catch (\Throwable $e) {
            $wpdb->query('ROLLBACK');
            update_option('lager_mc_error',$e->getMessage(),false);
            wc_get_logger()->error($e->getMessage(),['source'=>'lager-mc-bridge']);
        } finally { self::$working=false; self::unlock(); }
    }

    private static function require_innodb(): void {
        global $wpdb;
        // Transactions must cover core, lookup, fulfillment and bridge tables.
        $tables = $wpdb->get_results($wpdb->prepare('SHOW TABLE STATUS WHERE Name LIKE %s', $wpdb->esc_like($wpdb->prefix).'%'), ARRAY_A);
        foreach ($tables as $table) {
            if (isset($table['Engine']) && strcasecmp($table['Engine'],'InnoDB') !== 0) {
                throw new \RuntimeException('Writes require InnoDB tables; incompatible table: '.$table['Name']);
            }
        }
    }

    private static function execute(array $command): void {
        global $wpdb;
        $table = self::table('commands');
        try {
            self::require_innodb();
            self::check($wpdb->query('START TRANSACTION'));
            $current = $wpdb->get_row($wpdb->prepare("SELECT * FROM $table WHERE command_id=%s FOR UPDATE",$command['command_id']),ARRAY_A);
            if (!in_array($current['status'],['pending','processing'],true)) { $wpdb->query('ROLLBACK'); return; }
            $payload = json_decode($current['payload'],true,512,JSON_THROW_ON_ERROR);
            $result = match($current['kind']) {
                'inventory'=>self::inventory($payload),
                'product_update'=>self::update_product($payload),
                'fulfillment'=>self::fulfill($payload,$current['command_id']),
                default=>throw new \InvalidArgumentException('Unknown command type')
            };
            $kind = $current['kind'] === 'fulfillment' ? 'order' : 'product';
            $id = (int)$payload[$kind === 'order' ? 'order_id' : 'product_id'];
            $result['event_seq'] = self::export($kind,$id);
            self::check($wpdb->update($table,['status'=>'done','result'=>wp_json_encode($result,JSON_THROW_ON_ERROR),'error'=>null,'updated_at'=>gmdate('Y-m-d H:i:s')],['command_id'=>$current['command_id']]));
            self::check($wpdb->query('COMMIT'));
        } catch (\Throwable $e) {
            $wpdb->query('ROLLBACK');
            // Do not retry ambiguous side effects automatically. A terminal failure is reviewable.
            self::check($wpdb->update($table,['status'=>'failed','error'=>$e->getMessage(),'updated_at'=>gmdate('Y-m-d H:i:s')],['command_id'=>$command['command_id']]));
        }
    }

    public static function integer($value, int $minimum=0): int {
        if (is_bool($value) || !is_numeric($value) || !is_finite((float)$value)
            || floor((float)$value)!=(float)$value || (float)$value<$minimum || (float)$value>2147483647) {
            throw new \InvalidArgumentException('Whole nonnegative quantities required');
        }
        return (int)$value;
    }

    private static function locked_product(int $id) {
        global $wpdb;
        // Woo stock updates acquire this same InnoDB row lock (postmeta _stock).
        self::check($wpdb->query($wpdb->prepare("SELECT meta_id FROM {$wpdb->postmeta} WHERE post_id=%d FOR UPDATE",$id)));
        clean_post_cache($id);
        \WC_Cache_Helper::invalidate_cache_group('products');
        wc_delete_product_transients($id);
        $p = wc_get_product($id);
        if (!$p || !$p->is_type(['simple','variation'])) { throw new \RuntimeException('Unsupported or missing product'); }
        return $p;
    }

    private static function inventory(array $p): array {
        $id = self::integer($p['product_id'],1);
        $product = self::locked_product($id);
        $snapshot = self::product($id);
        if (!$snapshot['inventory_supported']) { throw new \RuntimeException('Unsupported inventory: '.$snapshot['inventory_error']); }
        if ($snapshot['stock'] !== self::integer($p['expected_stock'])) { throw new \RuntimeException('Stock conflict: WooCommerce stock changed; review warehouse quantity'); }
        wc_update_product_stock($product,self::integer($p['stock']),'set');
        return [];
    }

    private static function money($value): string {
        if ($value === null || $value === '') { return ''; }
        if (!is_numeric($value) || !is_finite((float)$value) || (float)$value<0) { throw new \InvalidArgumentException('Invalid price/cost'); }
        return wc_format_decimal($value);
    }

    private static function update_product(array $p): array {
        $id = self::integer($p['product_id'],1);
        $product = self::locked_product($id);
        $snapshot = self::product($id);
        if ($snapshot['fingerprint'] !== $p['expected_fingerprint']) { throw new \RuntimeException('Product conflict: WooCommerce product changed'); }
        $f = $p['fields'];
        $status = ['ACTIVE'=>'publish','DRAFT'=>'draft','ARCHIVED'=>'private'][$f['status']] ?? null;
        if (!$status) { throw new \InvalidArgumentException('Invalid product status'); }
        // A variation title/status derives from its parent. Editing it must not silently edit all siblings.
        if ($product->is_type('variation') && ($f['name']!==$snapshot['name'] || $f['status']!==$snapshot['status'])) {
            throw new \RuntimeException('Variation title/status belongs to parent; edit in WooCommerce');
        }
        $cost = self::money($f['unit_cost']);
        if ($cost !== '' && !self::cogs()) { throw new \RuntimeException('Cost of Goods Sold feature must be enabled'); }
        $barcode = (string)($f['barcode'] ?? '');
        if ($barcode !== '' && !preg_match('/^(?:[0-9]{8}|[0-9]{12,14})$/D',$barcode)) { throw new \RuntimeException('WooCommerce global unique ID requires valid GTIN/EAN/UPC/ISBN digits'); }
        $price = self::money($f['price']);
        $compare = self::money($f['compare_at_price']);
        if ($compare !== '' && ($price === '' || (float)$price >= (float)$compare)) { throw new \RuntimeException('Sale price must be below regular price'); }
        $price_changed = self::money($snapshot['price']) !== $price || self::money($snapshot['compare_at_price']) !== $compare;
        if ($price_changed && ($snapshot['sale_from'] || $snapshot['sale_to'])) {
            throw new \RuntimeException('Scheduled sale prices must be edited in WooCommerce');
        }
        $product->set_name((string)$f['name']);
        $product->set_status($status);
        $product->set_sku((string)$f['sku']);
        $product->set_description(wp_kses_post((string)$f['description']));
        if ($price_changed) {
            $product->set_regular_price($compare !== '' ? $compare : $price);
            $product->set_sale_price($compare !== '' ? $price : '');
            $product->set_price($price);
        }
        $product->set_weight($f['weight_grams'] === null ? '' : wc_get_weight(self::integer($f['weight_grams']),get_option('woocommerce_weight_unit','kg'),'g'));
        if (method_exists($product,'set_global_unique_id')) { $product->set_global_unique_id($barcode); }
        elseif ($barcode !== '') { throw new \RuntimeException('Global unique IDs unavailable in this WooCommerce version'); }
        if (self::cogs()) {
            if ($product->is_type('variation')) { $product->set_cogs_value_is_additive(false); }
            $product->set_cogs_value($cost === '' ? null : (float)$cost);
        }
        $product->save();
        return [];
    }

    private static function fulfill(array $p,string $key): array {
        global $wpdb;
        if (!self::native()) { throw new \RuntimeException('Native fulfillments >=10.7 must be enabled'); }
        $id = self::integer($p['order_id'],1);
        $o = wc_get_order($id);
        if (!$o || !$o->has_status(['processing','on-hold','completed'])) { throw new \RuntimeException('Order is not fulfillable'); }
        self::check($wpdb->query($wpdb->prepare("SELECT fulfillment_id FROM {$wpdb->prefix}wc_order_fulfillments WHERE entity_type=%s AND entity_id=%d FOR UPDATE",\WC_Order::class,$id)));
        $fulfillments = self::fulfillments($o);
        foreach ($fulfillments as $f) {
            if ($f->get_meta('_lager_mc_command_id',true)===$key) { return ['fulfillment_id'=>'wc:shipment:'.$f->get_id()]; }
        }
        $pending = FulfillmentUtils::get_pending_items($o,$fulfillments);
        $allowed = [];
        foreach ($pending as $line) {
            $product = $line['item']->get_product();
            if (!$product || $product->needs_shipping()) { $allowed[$line['item_id']] = (int)$line['qty']; }
        }
        $lines = $p['items'] ?? array_map(static fn($id,$qty)=>['item_id'=>$id,'qty'=>$qty],array_keys($allowed),array_values($allowed));
        if (!$lines) { throw new \RuntimeException('No unallocated shippable items remain'); }
        $seen = [];
        foreach ($lines as &$line) {
            $line = ['item_id'=>self::integer($line['item_id'],1),'qty'=>self::integer($line['qty'],1)];
            if (isset($seen[$line['item_id']]) || $line['qty']>($allowed[$line['item_id']] ?? 0)) { throw new \RuntimeException('Invalid or already allocated fulfillment quantity'); }
            $seen[$line['item_id']] = true;
        }
        unset($line);
        $f = new Fulfillment();
        $f->set_entity_type(\WC_Order::class);
        $f->set_entity_id((string)$id);
        $f->set_items($lines);
        $f->set_tracking_number((string)$p['tracking_number']);
        $f->set_shipment_provider(strtolower((string)$p['carrier']));
        $f->set_tracking_url((string)($p['tracking_url'] ?? ''));
        $f->update_meta_data('_lager_mc_command_id',$key);
        $f->set_status('fulfilled');
        // The first protocol version explicitly supports no requested email.
        // Core notification dispatch is controlled by REST/admin, not the data store.
        $f->save();
        return ['fulfillment_id'=>'wc:shipment:'.$f->get_id()];
    }
}

add_filter('cron_schedules',[Bridge::class,'schedules']);
register_activation_hook(__FILE__,[Bridge::class,'install']);
register_deactivation_hook(__FILE__,static function(){ wp_clear_scheduled_hook('lager_mc_tick'); });
add_action('before_woocommerce_init',static function(){
    $class = '\Automattic\WooCommerce\Utilities\FeaturesUtil';
    if (class_exists($class)) { $class::declare_compatibility('custom_order_tables',__FILE__,true); }
});
add_action('lager_mc_tick',[Bridge::class,'tick']);
add_action('shutdown',[Bridge::class,'flush'],999);
foreach (['woocommerce_update_product','woocommerce_new_product','woocommerce_update_product_variation','woocommerce_new_product_variation'] as $hook) {
    add_action($hook,[Bridge::class,'product_changed']);
}
foreach (['woocommerce_product_set_stock','woocommerce_variation_set_stock'] as $hook) { add_action($hook,[Bridge::class,'stock_changed']); }
foreach (['woocommerce_new_order','woocommerce_update_order','woocommerce_order_status_changed','woocommerce_order_refunded','woocommerce_reduce_order_stock','woocommerce_restore_order_stock'] as $hook) {
    add_action($hook,[Bridge::class,'order_changed']);
}
foreach (['woocommerce_created_customer','woocommerce_update_customer','profile_update'] as $hook) { add_action($hook,[Bridge::class,'customer_changed']); }
foreach (['woocommerce_fulfillment_after_create','woocommerce_fulfillment_after_update','woocommerce_fulfillment_after_delete'] as $hook) { add_action($hook,[Bridge::class,'fulfillment_changed']); }
add_action('before_delete_post',[Bridge::class,'post_deleted']);
add_action('woocommerce_delete_order',[Bridge::class,'order_deleted']);
add_action('deleted_user',[Bridge::class,'customer_deleted']);
