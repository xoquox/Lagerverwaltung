# Gemeinsame Grundlage fuer Shopify und WooCommerce

Lager-MC waehlt genau ein Shopsystem pro Installation und Datenbank:
`shopify` oder `woocommerce`. Die Oberflaeche, Artikelpflege, Bestellungen,
Kunden, Standorte und Versandjobs verwenden denselben Datenbankvertrag.
Der Shopify-Adapter ist auf diesen Vertrag umgestellt. Der WooCommerce-Sync-Server
und die WordPress-Datenbankbruecke sind implementiert; die Auswahl allein importiert
keine Daten. Einrichtung: [WooCommerce-Sync](woocommerce-sync.md).

## Kommunikationsweg

```mermaid
flowchart LR
    MC[Lager-MC] <--> DB[(Lager-MC PostgreSQL)]
    DB <--> SYNC[Sync-Server des gewaehlten Shopsystems]
    SYNC <--> SHOP[Shopify API oder WooCommerce-Datenbank]
```

Shop-Zugangsdaten gehoeren ausschliesslich zum jeweiligen Sync-Server.
Lager-MC liest Snapshots und schreibt Aenderungsauftraege in PostgreSQL.
Es importiert keinen Shop-API-Client und startet keinen Sync-Prozess.
Direkte Verbindungen zu Versanddienstleistern, Druckern und einem konfigurierten
Logo bleiben Teil der Lager- und Versandfunktionen.

## Installation

Der gefuehrte Installer bietet die Shopauswahl an. Fuer automatisierte Aufrufe:

```bash
bash scripts/install-standalone-de.sh --shop-system woocommerce --mode complete
bash scripts/install-linux.sh --shop-system woocommerce
bash scripts/install-from-github.sh --shop-system woocommerce
```

Ohne explizite Auswahl bleibt beim Update das bisherige Shopsystem erhalten.
Der Linux-Installer fragt bei interaktivem Aufruf nach dem System und uebernimmt
bei nichtinteraktivem Aufruf die bestehende Einstellung (Altbestand: Shopify).
Die Auswahl steht in `settings.local.json` als `shop_system`.

Bei WooCommerce wird kein Shopify-OAuth gestartet und keine Shopify-Konfiguration
angelegt. Ein eigener Sync-Server muss die Lager-MC-PostgreSQL-Datenbank bedienen
und die Daten mit der WooCommerce-Datenbank abgleichen. Die Remote-Anbindung erfolgt
ueber drei eigene WordPress-Brueckentabellen; Bestellspeicherung und Schreibregeln
sind in [WooCommerce-Sync](woocommerce-sync.md) beschrieben.

Die Datenbank speichert ihre Bindung in `shop_configuration` (`id = 1`).
Lager-MC prueft die Bindung vor dem Eintritt in die Arbeitsoberflaeche.
Der Shopify-Sync prueft sie beim Start und vor jedem Synchronisationsdurchlauf;
bei `woocommerce` bricht er vor Shopanfragen und Versandverarbeitung ab.
Die direkte Shopify-Fulfillment-CLI prueft ebenfalls die Datenbankbindung.
Ein laufendes System darf nicht durch manuelles Aendern dieser Bindung umgeschaltet werden.

## Update einer bestehenden Shopify-Installation

1. Datenbank sichern und Lager-MC sowie Sync-Server anhalten.
2. Lager-MC und Shopify-Sync gemeinsam auf diesen Code aktualisieren.
3. `python scripts/run_db_migrations.py` mit der bisherigen Shopify-Konfiguration
   ausfuehren. Beim separat installierten Sync geht auch dessen `run_db_migrations.py`.
4. `SHOPIFY_LOCATION_ID` im Sync konfigurieren, falls bisher nur die alte feste Fallback-ID verwendet wurde. Beide Prozesse wieder starten.

Die Migration laeuft in einer Transaktion und wird durch eine Datenbanksperre
gegen parallele Migrationen geschuetzt. Sie benennt bestehende Tabellen und Spalten um,
haelt IDs, Bestellungen, Artikel, Bestand, Fremdschluessel, Versandhistorie und offene
Jobs bei und kann wiederholt ausgefuehrt werden. Numerische alte Produkt-IDs werden
in Text ueberfuehrt. Die Statuswerte `shopify_pending` und `shopify_error` werden
zu `shop_pending` und `shop_error`.

Vorhandene `shopify_*`-Einstellungen werden beim Einlesen auf `shop_*` abgebildet.
Neue Schluessel haben Vorrang, wenn beide Formen in derselben Datei stehen.
Lokale Einstellungen haben weiterhin Vorrang vor Projekteinstellungen.
Beim Speichern werden die neutralen Schluessel geschrieben.

Die alten Programmversionen koennen die umbenannten Tabellen nicht verwenden.
Die Migration legt keine parallelen, leeren Ersatzdaten fuer Altbestand an.
Wenn alte und neue Tabellen oder Spalten gleichzeitig existieren, wird abgebrochen.
Shopify-Altbestand wird auch bei angeforderter WooCommerce-Auswahl abgelehnt.

Fuer den spaeteren Shopwechsel eine separate WooCommerce-Lagerdatenbank verwenden.
Die Uebernahme von Artikeln und Kategorien aus der Shopify-Datenbank wird als
eigener Schritt umgesetzt. Bestehende Shopify-IDs duerfen dabei nicht als WooCommerce-IDs
interpretiert werden.

## Vertrag fuer den neuen WooCommerce-Sync-Server

| Daten / Auftraege | Gemeinsame Tabellen und Felder |
| --- | --- |
| Artikel | `items`, `shop_product_id`, `shop_variant_id`, `shop_inventory_item_id` |
| Produktdaten | `shop_product_status`, `shop_description`, `shop_price`, `shop_compare_at_price`, `shop_unit_cost`, `shop_unit_cost_currency`, `shop_weight_grams` |
| Produkt-Aenderungsauftraege | `shop_product_dirty`, `shop_product_sync_action` (`create` / `update`), `shop_product_sync_error` |
| Bestand | `item_location_inventory` mit `dirty`, Mengen und `location_id`; Artikel-Gesamtsummen in `items` |
| Standorte | `shop_locations` |
| Kunden-Snapshot | `shop_customers` |
| Bestellungen / Positionen | `shop_orders`, `shop_order_items` |
| Versandzuordnung / offene Mengen | `shop_fulfillment_orders`, `shop_fulfillment_order_items` |
| Versandauftraege | `shop_fulfillment_jobs` |
| Sendungen | `shipping_labels`, `shop_fulfillment_id`, `shop_synced_at` |
| Status und Version | `service_runtime_state`, Schluessel `woocommerce-sync` bzw. `shopify-sync` |

Der Worker muss vor der Arbeit `shipping.shop.assert_shop_system(cur, 'woocommerce')`
pruefen. Das Schema wird mit `shipping.schema.apply_app_schema(cur, 'woocommerce')`
angelegt und anschliessend gemeinsam committed. Diese Migration ist ein Installationsschritt,
kein regelmaessiger Sync-Schritt.

Weitere Regeln fuer den Adapter:

- IDs sind undurchsichtige Textwerte. Beispielsweise `wc:order:123`, `wc:product:42`,
  `wc:line:7`, `wc:location:main`; Lager-MC setzt keine Shopify-GIDs voraus.
- Bestellungen muessen `source = 'woocommerce'` explizit schreiben.
  Manuelle Lager-MC-Auftraege tragen weiterhin `source = 'custom'`.
- Produktzustaende werden in `ACTIVE`, `DRAFT`, `ARCHIVED` normalisiert.
  Fulfillment-Zustaende werden in `unfulfilled`, `partial`, `fulfilled`, `cancelled`
  normalisiert. Bezahlstatus verwendet die vorhandenen gemeinsamen Werte wie `paid`,
  `pending` und `refunded`. Die WooCommerce-Zuordnung ist im Adapter implementiert.
- Auch bei einem Shop ohne Standortmodell einen stabilen Lagerstandort und die
  Versandzuordnungen der offenen Bestellpositionen bereitstellen. Die Standortfilter
  und Teilausfuehrung benoetigen diese Daten.
- Mengen sind Stueckzahlen, Gewicht ist in Gramm. Preise werden als Dezimaltexte
  gespeichert; die bestehende Oberflaeche rechnet und zeigt EUR an.
- Fuer Versandjobs stehen die gemeinsamen Helfer in `shipping.history` bereit:
  `claim_shop_fulfillment_jobs` (`FOR UPDATE SKIP LOCKED`),
  `mark_shop_fulfillment_job_done`, `mark_shop_fulfillment_job_failed` und
  `upsert_shop_shipment(..., source='woocommerce')`.
- `line_items_json` enthaelt fuer Teilversand `order_line_item_id`, `quantity` und
  optional `sku`. Ein leerer Wert steht fuer die gesamte offene Bestellung.
- Snapshot-Importe duerfen lokale Stammdaten, offene Dirty-Aenderungen, manuelle
  Auftraege und Versandhistorie nicht ueberschreiben. Schreibbestaetigungen muessen
  die Version des bearbeiteten Auftrags pruefen, damit zwischenzeitliche Aenderungen
  erhalten bleiben. Remote-Schreibaktionen brauchen Wiederholbarkeit.
- Heartbeats enthalten Version, Zustand, letzte Aktivitaet, Pull-/Push-Zeit und Fehler;
  reine Bereitstellung des Schemas darf keinen erfolgreichen Sync melden.

Kategorien, Shopify-Kategorieexport und die Erstbefuellung des WooCommerce-Shops
sind bewusst noch nicht Bestandteil dieses Schritts.

## Pruefung

```bash
python3 -m unittest discover -s tests -q
# Separat, gegen eine isolierte PostgreSQL-Testinstanz:
LAGER_MC_TEST_DSN='host=/pfad/zum/test-socket dbname=postgres user=testuser' \
  python3 -m unittest discover -s tests -p test_shop_database_integration.py -q
```

Die Integrationstests erstellen und entfernen zufaellig benannte Testdatenbanken.
Die Testrolle braucht dafuer `CREATEDB`. Keinen produktiven DSN verwenden.
