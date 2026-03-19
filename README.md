<p align="center">
  <img src="assets/lager-mc.svg" alt="Lager MC" width="160">
</p>

# Lagerverwaltung

Terminalbasierte Lagerverwaltung fuer kleine bis mittlere Teilelager mit Shopify-Anbindung, Lagerplatzlogik, Picklisten, Lieferscheinen, Inventur und Labeldruck.

Lizenz: [MIT](/home/chrisi/Lagerverwaltung/LICENSE)

## Funktionen

- Artikel suchen, bearbeiten und lokal anlegen
- Lagerplaetze nach `Regal / Fach / Platz` pflegen
- Bestellungen aus Shopify synchronisieren
- Zahlungs- und Fulfillment-Status im Auftragsfenster anzeigen
- Picklisten per Drucker drucken
- Lieferscheine als PDF erzeugen oder per Drucker drucken
- Brother-QL-Etiketten drucken
- Inventur starten, zaehlen, exportieren und uebernehmen
- extern gelagerte Fulfillment-Artikel ausblenden

## Projektstruktur

- [lager_mc.py](/home/chrisi/Lagerverwaltung/lager_mc.py)
  Hauptanwendung fuer Lager, Bestellungen und Inventur.
- [label_print.py](/home/chrisi/Lagerverwaltung/label_print.py)
  Etikettendruck fuer Brother-QL-Drucker.
- [delivery_note.py](/home/chrisi/Lagerverwaltung/delivery_note.py)
  PDF-Erzeugung fuer Lieferscheine auf Basis der Vorlage.
- [app_settings.py](/home/chrisi/Lagerverwaltung/app_settings.py)
  Laedt Projekt-Defaults und lokale Overrides.
- [settings.json](/home/chrisi/Lagerverwaltung/settings.json)
  Versionierte Standardkonfiguration des Projekts.
- [shpoify-sync/shopify_sync.py](/home/chrisi/Lagerverwaltung/shpoify-sync/shopify_sync.py)
  Separater Shopify-Sync fuer Produkte, Bestand und Bestellungen.

## Voraussetzungen

- Python 3.11+
- PostgreSQL
- `curses`
- fuer Labeldruck: `Pillow`, `python-barcode`, `brother_ql`
- fuer Listen-/Lieferschein-Druck: Drucksystem mit `lp`
- fuer HTML/CSS-Lieferscheine: `WeasyPrint` (inkl. Systembibliotheken wie cairo/pango)

Die Anwendung erweitert benoetigte Datenbankspalten und legt die Tabellen fuer Bestellungen und Inventur bei Bedarf selbst an.

## Konfiguration

Es gibt zwei Ebenen:

- [settings.json](/home/chrisi/Lagerverwaltung/settings.json)
  versionierte Projekt-Defaults
- `settings.local.json`
  lokale, nicht versionierte Laufzeitwerte


Wichtige Einstellungen:

- `db_host`, `db_name`, `db_user`, `db_pass`
- `language` (`de` oder `en`)
- `color_theme` (`blue`, `green`, `mono`, `megatrends`, `smoth`, `norton`, `gold-standard`, `subtile`, `monokai`)
- `color_theme_file` (optional, Pfad zu eigener JSON-Datei mit Themes)
- `printer_uri`, `printer_model`, `label_size`
- `label_font_regular`, `label_font_condensed` (optional, lokale TrueType/OpenType Fonts fuer Labeldruck)
- `location_regex_regal`, `location_regex_fach`, `location_regex_platz`
- `picklist_printer`
- `delivery_note_printer`
- `pdf_output_dir`
- `delivery_note_template_path` (optional, lokaler Pfad zur PDF-Vorlage ausserhalb von Git)
- `delivery_note_logo_source` (optional, lokaler Pfad oder `https://`-URL fuer das Logo)
- `delivery_note_sender_name`
- `delivery_note_sender_street`
- `delivery_note_sender_city`
- `delivery_note_sender_email`

## Shopify-Sync

Der Sync laeuft getrennt von der TUI und kann direkt oder im Container gestartet werden.

Mindestens benoetigte Shopify-Scopes:

- `read_products`
- `read_inventory`
- `write_inventory`
- `read_locations`
- `read_orders`

Zusaetzlich:

- `read_all_orders`
  falls Bestellungen aelter als 60 Tage geladen werden sollen

Der Sync schreibt unter anderem:

- Bestellungen und Positionen
- Fulfillment-Status
- Zahlungsstatus

## Installation

Automatische Installation fuer die meisten Linux-Distributionen:

```bash
git clone <repo-url>
cd Lagerverwaltung
./scripts/install-linux.sh
```

Das Script erkennt `dnf`, `apt`, `pacman` oder `zypper` automatisch, installiert Systempakete, erstellt `.venv` und legt einen Starter unter `~/.local/bin/lager-mc` an.

Manueller Start ohne Install-Script:

```bash
git clone <repo-url>
cd Lagerverwaltung
python3 -m py_compile lager_mc.py
python3 lager_mc.py
```

Falls Pakete fehlen:

```bash
pip install psycopg2-binary pillow python-barcode brother_ql requests python-dotenv
```

## Druck

### Labeldruck

Etiketten werden ueber [label_print.py](/home/chrisi/Lagerverwaltung/label_print.py) erzeugt. Unterstuetzt werden Brother-QL-Netzwerkdrucker.

Wenn gewuenscht, koennen benutzerdefinierte lokale Fontdateien in den Einstellungen gesetzt werden:

```json
{
  "label_font_regular": "/pfad/zu/deinem/font-regular.ttf",
  "label_font_condensed": "/pfad/zu/deinem/font-condensed.ttf"
}
```

### Picklisten

Picklisten werden textbasiert ueber den in den Einstellungen hinterlegten Drucker gesendet.

### Lieferscheine

Lieferscheine werden standardmaessig ueber HTML/CSS mit WeasyPrint erzeugt.
`delivery_note_template_path` kann optional auf eine `.html`/`.htm`-Datei zeigen, um das Layout frei anzupassen.
Wenn stattdessen eine `.pdf`-Vorlage hinterlegt wird, nutzt die Anwendung den Legacy-PDF-Renderer.

Beispiel (`settings.local.json`):

```json
{
  "delivery_note_template_path": "/home/<user>/Dokumente/lager/lieferschein_template.html",
  "delivery_note_logo_source": "https://example.com/logo.png"
}
```

Unterstuetzt werden:

- PDF-Export
- getrennten Drucker
- mehrseitige Ausgabe
- Seitennummerierung

## Logging

Rotierende Logdateien liegen unter [logs](/home/chrisi/Lagerverwaltung/logs):

- [logs/lagerverwaltung.log](/home/chrisi/Lagerverwaltung/logs/lagerverwaltung.log)
- [logs/druck.log](/home/chrisi/Lagerverwaltung/logs/druck.log)

Beispiel:

```bash
export LAGERVERWALTUNG_LOG_LEVEL=DEBUG
```

## Tests

```bash
python3 -m unittest discover -s tests -v
```
- In den Einstellungen koennen `language` und `color_theme` ueber eine Auswahl (F4) gewaehlt werden, ohne Werte manuell zu tippen.

Beispiel fuer eigene Themes (`color_theme_file`):

```json
{
  "themes": {
    "my-blue": {
      "pair_1_fg": "white",
      "pair_1_bg": "blue",
      "pair_2_fg": "black",
      "pair_2_bg": "cyan",
      "pair_3_fg": "white",
      "pair_3_bg": "black"
    }
  }
}
```

Erlaubte Farbnamen: `black`, `red`, `green`, `brown`, `yellow`, `blue`, `magenta`, `cyan`, `white`,
`brightblack`, `brightred`, `brightgreen`, `brightyellow`, `brightblue`, `brightmagenta`, `brightcyan`, `brightwhite`.
