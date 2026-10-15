# Bedienungsanleitung

## Zweck

Lager-MC ist eine terminalbasierte Anwendung fuer Lagerbestand, Bestellungen, Inventur und Versand.

Das Handbuch beschreibt diese Bereiche:

- Lagerbestand mit Lagerplatzlogik `Regal / Fach / Platz`
- Bestellungen aus Shopify oder WooCommerce
- Picklisten und Lieferscheine
- Versand mit GLS, Deutsche Post INTERNETMARKE und Adresslabels
- Versandhistory mit Reprint, Storno und Shop-Queue
- Inventur mit Snapshot und CSV-Export

## Aktueller Versandstand

Produktive Carrier-Integrationen:

- `GLS`
- `Deutsche Post INTERNETMARKE`

Zusaetzlich vorhanden:

- `Adresslabel` fuer interne Adresslabels ohne Carrier-API

Die Carrier-Struktur verwendet zentrale Carrier-Definitionen mit gemeinsamer Versandhistory und Shop-Anbindung.

Details zur Carrier-Struktur und zu den Integrationspunkten stehen in [docs/versanddienstleister.md](versanddienstleister.md).

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

Direkt von GitHub:

```bash
curl -fsSL https://raw.githubusercontent.com/xoquox/Lagerverwaltung/main/scripts/install-from-github.sh | bash
```

Die Installation fragt optional nach:

- einem Bundle-Archiv fuer lokale Settings und Dateien
- einer DB-Migration

Das Migrationsskript legt die konfigurierte Datenbank bei Bedarf an und baut danach das Schema auf.

Start:

```bash
python3 lager_mc.py
```

## Ersteinrichtung

Vor dem ersten produktiven Einsatz werden diese Punkte gesetzt:

1. Datenbankverbindung
2. Picklisten- und Lieferschein-Drucker
3. Versanddrucker und Formate je Carrier
4. Sync-Server des ausgewaehlten Shopsystems
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
- `Shift+F10` Pickliste
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
- keine Shop-Uebertragung

Adresslabels koennen fuer Einzelversand, manuelle Labels und Bulk-Ausfuehrung verwendet werden.

### Manuelles Versandlabel

Die manuelle Versandmaske liegt in der Auftragsansicht auf `Shift+F5`.

Wichtige Funktionen:

- `F3` Land
- `F4` dienstleister-spezifische Auswahl
- `F5` Ausgabe `PDF + Drucken` oder `Nur PDF`
- `F6` Kunde aus der lokal synchronisierten Shop-Kundendatenbank laden

## Versandhistory

Die Versandhistory liegt in der Auftragsansicht auf `F8`.

Wichtige Aktionen:

- `F5` vorhandene PDF erneut ausgeben
- `F6` Storno, falls der Carrier das unterstuetzt
- `F7` Reprint
- `F10` an den Shop uebergeben

Die History zeigt pro Eintrag:

- Carrier
- TrackID oder Sendungsnummer
- Status
- Quelle
- Referenz
- lokaler PDF-Pfad
- Tracking-URL
- letzten Shop-Job

## Picklisten und Lieferscheine

Picklisten und Lieferscheine werden aus der Auftragsansicht gestartet.

- `Shift+F10` Pickliste
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

Die Menueleiste am oberen Bildschirmrand bietet `Datei`, `Artikel`, `Ansicht`, `Auftraege`, `Extras` und `Hilfe`. `F10` oder ein Mausklick oeffnet ein Menue. `Links`/`Rechts` wechselt das Menue, `Hoch`/`Runter` die ausfuehrbaren Eintraege, `Enter` fuehrt aus und `Esc` oder `F9` schliesst. Ausgegraute Aktionen benoetigen eine passende Ansicht oder Auswahl. In der Auftragsansicht liegt die Pickliste auf `Shift+F10`.

`Datei → Beenden` beendet Lager-MC; offene Datenbankschreibvorgaenge werden dabei beruecksichtigt. `Extras` enthaelt Inventur, Einstellungen, Druckeinstellungen, Tastaturbelegung und Sync-Status. `Hilfe` zeigt Tastenuebersicht, Bedienungsanleitung und Versionsinformationen.

Die Maussteuerung wird im Tab `Allgemein` mit `Maussteuerung → Ein/Aus` gewaehlt und mit `F2` gespeichert. Die Maus wird verwendet, wenn das Terminal Mausereignisse liefert; die Tastaturbedienung ist unabhaengig davon verfuegbar. Ein Klick waehlt Artikel, Lagerplaetze oder Auftraege aus und wechselt den aktiven Artikelbereich. Ein Doppelklick auf einen Artikel oder Lagerplatzeintrag oeffnet die Artikelinformationen. Das Mausrad navigiert durch die Liste unter dem Mauszeiger; im rechten Auftragsbereich scrollt es die Details. Ein Klick auf `[ ]` oder ein Doppelklick auf die Auftragszeile markiert einen Auftrag fuer die Sammelausfuehrung.

Menues, Auswahllisten, Druckerauswahl, Einstellungstabs und die Tastaturbelegung lassen sich anklicken. Das Mausrad bewegt die Auswahl; lange Einstellungslisten scrollen mit. In Textfeldern setzt ein Klick den Cursor. Sichtbare Tastenfelder in der Fussleiste fuehren die angezeigte Aktion aus.

Die Tastaturbelegung wird unter `Extras → Tastaturbelegung` oder im Tab `Allgemein` der Einstellungen bearbeitet. `Tab` wechselt zwischen Artikelansicht mit einem Standort, Artikelansicht mit mehreren Standorten und Auftragsansicht. `Enter` oder `F6` waehlt eine Taste; `Nur Menue` entfernt die Tastenzuweisung. Doppelte Belegungen werden abgewiesen. `F5` stellt fuer die angezeigte Ansicht die Standardbelegung wieder her. `F2` uebernimmt, `F9` oder `Esc` verwirft die Aenderungen. Beim Aufruf aus den Einstellungen speichert deren `F2` die uebernommene Belegung. `F10` bleibt fuer das Menue reserviert. Unterdialoge behalten ihre Bedien- und Speichertasten.

Menueeintraege, Tastenuebersicht und Fussleiste zeigen die eingestellten Tastenkombinationen. Die Tastenlisten im Handbuch beschreiben die Standardbelegung.

Die Einstellungen liegen auf `Shift+F11`.

### Darstellung

Das Farbschema wird im Tab `Allgemein` mit `Enter` oder `F6` gewaehlt. Die Auswahl erscheint als Vorschau in den Einstellungen. `F2` speichert sie; `F9` oder `Esc` stellt das gespeicherte Farbschema wieder her. Bei einem Pruefungsfehler bleibt der Dialog mit den eingegebenen Werten geoeffnet. Ein Etikettendrucker ist optional; Modell und Labelformat sind bei eingetragener Drucker-URI erforderlich.

| Farbschema | Darstellung |
| --- | --- |
| Commander (Blau) | Blaue Arbeitsflaeche, tuerkiser Kopfbalken und Auswahl, gelbe Ueberschriften |
| Gruen | Gruene Schrift auf Schwarz mit gruener Auswahl und Kopfleiste |
| Monochrom | Schwarz und Weiss mit invertierter Auswahl und Kopfleiste |
| Megatrends (DOS) | Graue Arbeitsflaeche, blaue Schrift und blaue Auswahl nach BIOS-Stil |
| Smoth (DOS) | Blaue Arbeitsflaeche mit hellgrauer Auswahl und Kopfleiste |
| Norton (DOS) | Cyanfarbene Schrift und Rahmen auf Blau, tuerkise Auswahl und Kopfleiste |
| Gold Standard (DOS) | Gelbe Schrift auf Schwarz mit goldener Auswahl und Kopfleiste |
| Subtile (DOS) | Helle Arbeitsflaeche mit dunkler Schrift und blauen Akzenten |
| Monokai | Dunkle Arbeitsflaeche, helle Schrift, gruene Auswahl und cyanfarbene Kopfleiste |

Produktnummer und Name verwenden dieselbe Textfarbe. Die Synchronisationskennzeichen unterscheiden Erfolg, ausstehende Aenderungen, Fehler und lokale Artikel durch themeabhaengige Farben. Die Kennzeichen `S`, `D` und `L` bleiben erhalten. Tastenkuerzel sind in der Fusszeile separat hervorgehoben. Eigene Theme-Dateien mit den bisherigen sechs Farbwerten bleiben verwendbar.

Tab-Struktur:

- Allgemein
- Lagerlabel
- Drucker
- Versand
- Lieferschein

Wichtige Bedienung:

- `Tab` und `Shift+Tab` fuer Tabs
- `Enter` fuer Auswahl oder naechstes Feld
- `F2` Speichern
- `F6` oeffnet die passende Auswahl zum aktuellen Feld
- `F7` startet eine Testseite fuer das aktuelle Drucker-/Format-Feld

Im Tab `Drucker` stehen fuer Lieferscheine sowie Pick-/Inventurlisten die Duplexoptionen `Einseitig`, `Duplex lange Kante` und `Duplex kurze Kante` zur Auswahl. Versandlabels werden einseitig ausgegeben. Bei Duplex werden Lieferscheine im Sammelablauf als getrennte Druckauftraege ausgegeben, damit jede Bestellung auf einem eigenen Blatt beginnt.

Im Tab `Lieferschein` wird die Positionssortierung mit `Enter` oder `F6` gewaehlt: Bestellreihenfolge, Lagerplatz, Name, SKU oder Gewicht. Alle Kriterien ausser Bestellreihenfolge bieten auf- und absteigende Sortierung. Lagerplaetze und SKUs werden mit Zahlenvergleich sortiert; fehlende Werte stehen am Ende. Extern gelagerte Positionen bleiben ausgeschlossen. Die Auswahl gilt fuer PDF-Export, Einzel- und Sammeldruck.

`F7` verwendet die im Dialog ausgewaehlten Druckoptionen auch vor dem Speichern. Ein Duplex-Test erzeugt Vorder- und Rueckseite. Allgemeines Versandformat und allgemeine Skalierung stehen im Tab `Drucker`; das Treiberformat `PCard100x148` bleibt vom PDF-Seitenmass getrennt. Lieferschein-PDFs werden im ausgewaehlten Format erzeugt; PDF-Vorlagen werden proportional eingepasst.

Wichtige Einstellungen im Versand-Tab:

- aktive Versanddienste
- Drucker und Formate je Carrier
- Shop-Tracking-Modus je Carrier
- Tracking-URL je Carrier
- GLS-Zugangsdaten
- POST-Zugangsdaten
- Vorlage fuer Adresslabels

## Shopauswahl

Jede Installation verwendet Shopify oder WooCommerce mit einem eigenen Sync-Server. Die Lagerfunktionen lesen und schreiben in der lokalen PostgreSQL-Datenbank. Einrichtung und Funktionsumfang fuer WooCommerce: [WooCommerce-Sync](woocommerce-sync.md).

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

## Start bei langsamer Datenbankverbindung

Beim Programmstart wird die Datenbankverbindung geprueft.

Wenn die Verbindung noch nicht bereit ist, bleibt die Startansicht sichtbar und zeigt den aktuellen Status.

- `Enter` startet die Pruefung erneut
- `F2` oeffnet die Einstellungen
- `F9` beendet das Programm

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

DB-Migration:

```bash
python3 scripts/run_db_migrations.py
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

- [logs/lagerverwaltung.log](../logs/lagerverwaltung.log)
- [logs/print.log](../logs/print.log)
- [logs/shopify-sync.log](../logs/shopify-sync.log)
