<p align="center">
  <img src="assets/lager-mc.svg" alt="Lager MC" width="160">
</p>

# Lagerverwaltung

Terminalbasierte Lagerverwaltung mit Lagerplatzpflege, Bestellansicht, Inventur, Shopify-Anbindung und Versandabwicklung.

Lizenz: [MIT](/home/chrisi/Lagerverwaltung/LICENSE)

## Kernfunktionen

- Artikel, Lagerplaetze und Bestandsmengen pflegen
- Bestellungen aus Shopify einlesen und im Auftragsfenster bearbeiten
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

Die Carrier-Struktur ist fuer weitere Versanddienstleister vorbereitet.

## Dokumentation

- Bedienungsanleitung: [docs/bedienungsanleitung.md](/home/chrisi/Lagerverwaltung/docs/bedienungsanleitung.md)
- Versand-Integrationsdoku: [docs/versanddienstleister.md](/home/chrisi/Lagerverwaltung/docs/versanddienstleister.md)
- English README: [README.en.md](/home/chrisi/Lagerverwaltung/README.en.md)
- English user manual: [docs/bedienungsanleitung.en.md](/home/chrisi/Lagerverwaltung/docs/bedienungsanleitung.en.md)
- English shipping integration notes: [docs/shipping-providers.en.md](/home/chrisi/Lagerverwaltung/docs/shipping-providers.en.md)
- Release-Historie: [CHANGELOG.md](/home/chrisi/Lagerverwaltung/CHANGELOG.md)
- GitHub Releases: <https://github.com/xoquox/Lagerverwaltung/releases>

## Varianten

- Projektstand mit Shopify- und Versandfunktionen: [Lagerverwaltung](https://github.com/xoquox/Lagerverwaltung)
- Projektstand ohne Shopify- und Versandfunktionen: [simple-storage-core](https://github.com/b4ckspace/simple-storage-core)
