# Shopify-Sync Changelog

Nur Releases des separaten Shopify-Syncs. Entwicklungszwischenstaende werden ueber Git-Branches und Commits nachvollzogen.

## [0.1.0] - 2026-03-25

### Added
- Eigenes rotierendes Logging fuer den Shopify-Sync mit Datei `shopify-sync.log`.
- Eigene Sync-Versionsquelle in `shopify-sync/sync_version.py`.
- CLI-Abfrage der laufenden Sync-Version ueber `python shopify_sync.py --version` und `python shopify_sync.py version --json`.
- JSON-Ausgabe der laufenden Sync-Version mit Service, Version und Zeitstempel.

### Fixed
- Order-Sync an die aktuelle Shopify-GraphQL-Struktur fuer `fulfillments` angepasst.
- Shipment-Import verarbeitet Fulfillment- und Tracking-Daten jetzt robust fuer Listen- und `nodes`-Strukturen.
