<p align="center">
  <img src="assets/lager-mc.svg" alt="Lager-MC" width="160">
</p>

# Lager-MC

Terminalbasierte Lager- und Versandsoftware mit Lagerplatzpflege, Bestellansicht, Inventur, waehlbarem Shopsystem und Versandabwicklung.

Shopify und WooCommerce sind ueber separate Sync-Server angebunden. Lager-MC
kommuniziert mit dem Shop ausschliesslich ueber PostgreSQL. Der WooCommerce-Worker
verwendet eine Datenbankbruecke in WordPress; Einrichtung und Funktionsgrenzen:
[WooCommerce-Sync](docs/woocommerce-sync.md).

Lizenz: [MIT](LICENSE)

## Kernfunktionen

- Artikel, Lagerplaetze und Bestandsmengen pflegen
- Bestellungen aus der Shop-Datenbank einlesen und im Auftragsfenster bearbeiten
- Picklisten, Lieferscheine und Versandlabels erzeugen
- Versand mit GLS, Deutsche Post INTERNETMARKE und Adresslabels
- Teilausfuehrung, Bulk-Ausfuehrung und Versandhistory
- Inventur mit Snapshot, CSV-Export und Uebernahme
- lokale Bundle-Dateien fuer neue Arbeitsplaetze

## Aktueller Versandstand

Produktive Carrier-Integrationen:

- `GLS`
- `Deutsche Post INTERNETMARKE`

Zusaetzlich vorhanden:

- `Adresslabel` fuer interne Labels ohne Carrier-API

Die Carrier-Struktur verwendet zentrale Carrier-Definitionen mit gemeinsamer Versandhistory und Shop-Anbindung.

## Dokumentation

- Shopauswahl und Migration: [docs/shop-systeme.md](docs/shop-systeme.md)

- Bedienungsanleitung: [docs/bedienungsanleitung.md](docs/bedienungsanleitung.md)
- Versand-Integrationsdoku: [docs/versanddienstleister.md](docs/versanddienstleister.md)
- Shopify-Verbindung mit Lager-MC: [docs/shopify-verbindung-mit-lager-mc.md](docs/shopify-verbindung-mit-lager-mc.md)
- English README: [README.en.md](README.en.md)
- English user manual: [docs/bedienungsanleitung.en.md](docs/bedienungsanleitung.en.md)
- English shipping integration notes: [docs/shipping-providers.en.md](docs/shipping-providers.en.md)
- Release-Historie: [CHANGELOG.md](CHANGELOG.md)
- GitHub Releases: <https://github.com/xoquox/Lagerverwaltung/releases>

## Varianten

- Fork mit den Kernfunktionen von Lager-MC ohne Shopify-Integration und Versand: [simple-storage-core](https://github.com/b4ckspace/simple-storage-core)
