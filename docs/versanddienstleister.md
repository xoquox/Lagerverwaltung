# Versanddienstleister und Integrationspunkte

## Zweck

Dieses Dokument beschreibt den aktuellen Stand der Versandintegration und die technischen Punkte fuer weitere Carrier.

## Aktueller Stand

Produktive Carrier-Integrationen:

- `GLS`
- `Deutsche Post INTERNETMARKE`

Zusaetzlich vorhanden:

- `Adresslabel` fuer interne Labels ohne Carrier-API

Nicht umgesetzt sind derzeit weitere Carrier-APIs wie DHL, DPD, UPS oder Hermes.

Die Carrier-Struktur umfasst zentrale Carrier-Definitionen, gemeinsame Versandhistory und Shopify-Fulfillment-Anbindung.

## Carrier-Definition

Die zentrale Carrier-Struktur liegt in [shipping/carriers.py](/home/chrisi/Lagerverwaltung/shipping/carriers.py).

Dort werden pro Carrier unter anderem gepflegt:

- `code`
- `label`
- `short_label`
- `default_format`
- `shopify_allowed`
- `option_mode`
- zugehoerige Settings-Felder fuer Drucker, Format, Tracking-Modus, Tracking-URL und optionale Vorlage
- zusätzliche Settings-Felder fuer carrier-spezifische Zugangsdaten

## Versandhistory und Shopify-Queue

Die gemeinsame Datenhaltung liegt in [shipping/history.py](/home/chrisi/Lagerverwaltung/shipping/history.py).

Abgedeckt sind dort:

- Schema und Migration fuer `shipping_labels`
- Schema und Migration fuer `shopify_fulfillment_jobs`
- Laden, Schreiben und Aktualisieren von Versandlabels
- Anlegen und Bearbeiten von Shopify-Fulfillment-Jobs
- Upsert von aus Shopify eingelesenen Sendungen

Diese Logik wird sowohl von der TUI als auch vom Shopify-Sync genutzt.

## Runtime-Anbindung in der TUI

Die TUI registriert Carrier-Runtimes in [lager_mc.py](/home/chrisi/Lagerverwaltung/lager_mc.py).

Aktuelle Runtime-Hooks:

- `create_label`
- `reprint_label`
- `cancel_label`

Fuer neue Carrier wird mindestens `create_label` benoetigt.

`reprint_label` und `cancel_label` sind optional.

## Option-Auswahl pro Carrier

Die carrier-spezifische Auswahl wird ueber `option_mode` gesteuert.

Aktuelle Modi:

- `gls_services`
- `post_products`
- `None`

Neue Carrier koennen einen eigenen Modus bekommen oder ohne Zusatzdialog arbeiten.

## Shopify-Weitergabe

Carrier mit `shopify_allowed=True` koennen an Shopify uebergeben werden.

Die Shopify-Weitergabe arbeitet mit:

- Tracking-Nummer
- Carrier-Name
- optionaler Tracking-URL

Carrier-spezifische Defaults fuer Tracking-Modus und Tracking-URL werden ueber die Carrier-Definition und die Settings gesteuert.

## Notwendige Schritte fuer einen neuen Carrier

1. Carrier-Definition in [shipping/carriers.py](/home/chrisi/Lagerverwaltung/shipping/carriers.py) anlegen
2. benoetigte Standard-Settings in [app_settings.py](/home/chrisi/Lagerverwaltung/app_settings.py) ergaenzen
3. Carrier-Implementierung in [lager_mc.py](/home/chrisi/Lagerverwaltung/lager_mc.py) schreiben
4. Runtime-Spezifikation in `SHIPPING_CARRIER_RUNTIME_SPECS` registrieren
5. falls Shopify genutzt werden soll:
   - Tracking-Company und Tracking-URL sauber abbilden
6. Tests ergaenzen in:
   - [tests/test_release_suite.py](/home/chrisi/Lagerverwaltung/tests/test_release_suite.py)
   - [tests/test_shopify_sync_logging.py](/home/chrisi/Lagerverwaltung/tests/test_shopify_sync_logging.py)

## Aktuelle Schnittstellen

### GLS

- Label-Erstellung
- Reprint
- Storno
- Shopify-Tracking ueber Carrier-Name

### Deutsche Post INTERNETMARKE

- Produktwahl ueber importierte Produktpreisliste
- Kauf von INTERNETMARKEN
- Shopify-Tracking mit konfigurierbarer Tracking-URL

### Adresslabel

- keine externe API
- keine Shopify-Weitergabe

## Abgrenzung

Dieses Dokument beschreibt die vorhandenen technischen Integrationspunkte.

Betriebsanleitung und Benutzerablauf stehen in [docs/bedienungsanleitung.md](/home/chrisi/Lagerverwaltung/docs/bedienungsanleitung.md).
