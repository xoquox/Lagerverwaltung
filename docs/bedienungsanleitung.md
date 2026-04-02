# Bedienungsanleitung

## Zweck

Lagerverwaltung ist eine terminalbasierte Anwendung fuer Lagerbestand, Bestellungen, Inventur und Versand.

Das Handbuch beschreibt diese Bereiche:

- Lagerbestand mit Lagerplatzlogik `Regal / Fach / Platz`
- Bestellungen aus Shopify
- Picklisten und Lieferscheine
- Versand mit GLS, Deutsche Post INTERNETMARKE und Adresslabels
- Versandhistory mit Reprint, Storno und Shopify-Queue
- Inventur mit Snapshot und CSV-Export

## Aktueller Versandstand

Produktive Carrier-Integrationen:

- `GLS`
- `Deutsche Post INTERNETMARKE`

Zusaetzlich vorhanden:

- `Adresslabel` fuer interne Adresslabels ohne Carrier-API

Die Carrier-Struktur verwendet zentrale Carrier-Definitionen mit gemeinsamer Versandhistory und Shopify-Anbindung.

Details zur Carrier-Struktur und zu den Integrationspunkten stehen in [docs/versanddienstleister.md](/home/chrisi/Lagerverwaltung/docs/versanddienstleister.md).

## Voraussetzungen

- Python 3.11 oder neuer
- PostgreSQL
- `curses`
- ein funktionierendes Drucksystem mit `lp`
- `WeasyPrint` fuer HTML-basierte Vorlagen

## Installation

Standard auf Linux:

```bash
git clone git@github.com:xoquox/Lagerverwaltung.git
cd Lagerverwaltung
./scripts/install-linux.sh
```

Start:

```bash
python3 lager_mc.py
```

## Ersteinrichtung

Vor dem ersten produktiven Einsatz werden diese Punkte gesetzt:

1. Datenbankverbindung
2. Picklisten- und Lieferschein-Drucker
3. Versanddrucker und Formate je Carrier
4. Shopify-Sync
5. Versandzugaenge fuer GLS und POST

Die Einstellungen liegen unter `Shift+F11`.

## Bedienkonzept

Die Anwendung ist fuer Tastaturbedienung ausgelegt.

Grundregeln:

- `↑↓` bewegen die Auswahl
- `Enter` bestaetigt die aktuelle Auswahl oder wechselt ins naechste Feld
- `F9` schliesst den aktuellen Dialog
- `Tab` wechselt in der Lageransicht zwischen Teileliste und Regalliste
- `Tab` und `Shift+Tab` wechseln in den Einstellungen zwischen Tabs
- Suchfilter werden in Listen direkt ueber die Tastatur eingegeben

Die untere Statuszeile zeigt die gueltigen Tasten fuer den aktuellen Dialog.

## Lageransicht

Die Lageransicht ist der Hauptbildschirm fuer Artikel und Lagerplaetze.

Mit `Tab` wird zwischen Teileliste und Regalliste gewechselt.

Wichtige Aktionen:

- `F1` Sortierung wechseln
- `F2` nur lokale Artikel
- `F3` fehlende Lagerorte
- `F4` Artikelinfo
- `F5` neuen Artikel anlegen
- `Shift+F5` Artikel bearbeiten
- `F6` Lagerplatz aendern
- `F7` Menge aendern
- `F8` Versandlabel fuer den gewaehlten Artikel
- `Shift+F1` Inventur
- `Shift+F11` Einstellungen
- `F12` Auftragsansicht

## Auftragsansicht

Die Auftragsansicht ist das Arbeitsfenster fuer Bestellungen.

Wichtige Aktionen:

- `Space` markiert Auftraege fuer Bulk-Aktionen
- `A` markiert alle sichtbaren Auftraege
- `F1` Filter fuer Offen/Alle
- `F2` Fulfillment-Status
- `F3` Zahlungsstatus
- `F4` Sprung zu einer Bestellung
- `F5` Versandlabel erzeugen
- `Shift+F5` manuelles Versandlabel
- `F6` Teilausfuehrung
- `F7` Bulk-Ausfuehrung
- `F8` Versandhistory
- `F10` Pickliste
- `F11` Lieferschein

## Versand

### Carrier-Auswahl

Bei Versandaktionen wird zuerst der Versanddienstleister abgefragt.

Aktuell verfuegbar:

- `GLS`
- `POST`
- `Adresslabel`

Welche Carrier im Auswahlfenster erscheinen, wird in den Einstellungen ueber `Aktive Versanddienste` gesteuert.

### GLS

Nach der Carrier-Auswahl koennen GLS-Services gewaehlt werden, zum Beispiel:

- FlexDelivery
- AddresseeOnly
- Guaranteed24
- PreAdvice
- SMS Service

Die Standardwerte kommen aus den Einstellungen und koennen pro Auftrag geaendert werden.

### POST

Die POST-Auswahl arbeitet mit Grundprodukt und Zusatzoptionen.

Beispiele:

- Maxibrief
- Grossbrief
- Warensendung
- Einschreiben
- Einschreiben Einwurf
- Rueckschein

Die Produktdaten stammen aus der importierten Produktpreisliste.

### Adresslabel

`Adresslabel` erzeugt ein reines Adresslabel mit Absender und Empfaenger.

Eigenschaften:

- keine Sendungsnummer
- keine Carrier-API
- keine Shopify-Uebertragung

Adresslabels koennen fuer Einzelversand, manuelle Labels und Bulk-Ausfuehrung verwendet werden.

### Manuelles Versandlabel

Die manuelle Versandmaske liegt in der Auftragsansicht auf `Shift+F5`.

Wichtige Funktionen:

- `F3` Land
- `F4` dienstleister-spezifische Auswahl
- `F5` Ausgabe `PDF + Drucken` oder `Nur PDF`
- `F6` Kunde aus der lokal synchronisierten Shopify-Kundendatenbank laden

## Versandhistory

Die Versandhistory liegt in der Auftragsansicht auf `F8`.

Wichtige Aktionen:

- `F5` vorhandene PDF erneut ausgeben
- `F6` Storno, falls der Carrier das unterstuetzt
- `F7` Reprint
- `F10` an Shopify uebergeben

Die History zeigt pro Eintrag:

- Carrier
- TrackID oder Sendungsnummer
- Status
- Quelle
- Referenz
- lokaler PDF-Pfad
- Tracking-URL
- letzten Shopify-Job

## Picklisten und Lieferscheine

Picklisten und Lieferscheine werden aus der Auftragsansicht gestartet.

- `F10` Pickliste
- `F11` Lieferschein

Beim Lieferschein stehen diese Ausgabemodi zur Verfuegung:

- `Drucken`
- `Drucken + PDF`
- `Nur PDF`

Vorlage und Logo werden in den Einstellungen gesetzt.

## Bulk-Ausfuehrung

Bulk-Ausfuehrung liegt in der Auftragsansicht auf `F7`.

Ablauf:

1. Auftraege markieren
2. Carrier waehlen
3. carrier-spezifische Optionen festlegen
4. Ausgabemodus waehlen
5. Auftraege gesammelt verarbeiten

Label und Lieferscheine werden fuer den Sammeldruck intern zusammengefuehrt. Die Einzel-PDFs bleiben erhalten.

## Teilausfuehrung

Teilausfuehrung liegt in der Auftragsansicht auf `F6`.

Hier wird pro Position die Menge festgelegt. Danach folgt derselbe Versandablauf wie bei einer normalen Ausfuehrung.

## Einstellungen

Die Einstellungen liegen auf `Shift+F11`.

Tab-Struktur:

- Allgemein
- Lager
- Druck
- Versand

Wichtige Bedienung:

- `Tab` und `Shift+Tab` fuer Tabs
- `Enter` fuer Auswahl oder naechstes Feld
- `F2` Speichern
- `F3` Drucker-Auswahl
- `F4` Format-Auswahl
- `F6` Dateiauswahl oder Ordnerauswahl, je nach Feld

Wichtige Einstellungen im Versand-Tab:

- aktive Versanddienste
- Drucker und Formate je Carrier
- Shopify-Tracking-Modus je Carrier
- Tracking-URL je Carrier
- GLS-Zugangsdaten
- POST-Zugangsdaten
- Vorlage fuer Adresslabels

## Shopify-Sync

Der Shopify-Sync liefert diese Shopify-Daten in die Datenbank:

- Bestellungen
- Bestellpositionen
- Fulfillment-Status
- Zahlungsstatus
- Kunden fuer die manuelle Versandmaske

Mindestens benoetigte Shopify-Scopes:

- `read_products`
- `read_inventory`
- `write_inventory`
- `read_locations`
- `read_orders`

Zusatz fuer aeltere Bestellungen:

- `read_all_orders`

Fuer die Kundensuche in manuellen Versandlabels wird zusaetzlich `read_customers` benoetigt.

## Arbeitsplaetze uebernehmen

Mit den Bundle-Skripten lassen sich lokale Dateien auf einen neuen Arbeitsplatz uebernehmen.

Export:

```bash
python3 scripts/create_local_bundle.py
```

Import:

```bash
python3 scripts/apply_local_bundle.py /pfad/zum/lager_mc_local_bundle_*.zip
```

Ein Bundle enthaelt portable Projektdateien wie:

- API-Zugaenge
- Shopify-Sync `.env`
- Fonts
- Logos
- ausgewaehlte Vorlagen und Themes

Nicht ueberschrieben werden lokale Arbeitsplatz-Einstellungen wie:

- Drucker
- Druckformate
- PDF- und Label-Zielordner

## Logs

Wichtige Logdateien:

- [logs/lagerverwaltung.log](/home/chrisi/Lagerverwaltung/logs/lagerverwaltung.log)
- [logs/druck.log](/home/chrisi/Lagerverwaltung/logs/druck.log)
- [logs/shopify-sync.log](/home/chrisi/Lagerverwaltung/logs/shopify-sync.log)
