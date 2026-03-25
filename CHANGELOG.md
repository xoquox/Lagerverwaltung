# Changelog

Nur Releases auf `main`. Entwicklungszwischenstaende werden ueber Git-Branches und Commits nachvollzogen.
Kuenftige Release-Eintraege sollen zusaetzlich die zugehoerige Shopify-Sync-Version nennen und auf
[shopify-sync/CHANGELOG.md](/home/chrisi/Lagerverwaltung/shopify-sync/CHANGELOG.md) verweisen.

## [1.20.028] - 2026-03-24

### Fixed
- CUPS-Druckerliste setzt bei `lpstat` jetzt `LC_ALL=C` und `LANG=C`, damit Drucker auch auf deutsch lokalisierten Linux-/Fedora-Systemen korrekt erkannt werden.

## [1.20.027] - 2026-03-24

### Added
- Skript `scripts/export_local_bundle.sh` erzeugt jetzt ein ZIP-Archiv mit lokalen/private Dateien fuer einen neuen PC.

## [1.20.026] - 2026-03-24

### Added
- Shopify-Sync liest jetzt Fulfillments und Trackingnummern zurueck und speichert sie zentral in der Versandhistory.

### Changed
- Auftragsdetails zeigen jetzt alle bekannten Sendungen einer Bestellung statt nur der letzten.
- Versandhistory speichert zusaetzlich Quelle, Shopify-Fulfillment-ID und Tracking-URL pro Sendung.

### Fixed
- Bereits aus Shopify rueckgelesene Sendungen koennen nicht mehr erneut ueber `F10 Shopify` eingereiht werden.

## [1.20.025] - 2026-03-24

### Fixed
- `F9` in Bulk-/Versand-Auswahldialogen bricht jetzt wirklich ab, statt still mit Defaultwerten weiterzulaufen.
- Vor der normalen Einzel-Labelerstellung gibt es jetzt eine explizite Abfrage mit `Enter = Nein`, `F9 = Abbruch`, bevor irgendein API-Call gestartet wird.
- Manueller Labeldialog beendet sich ebenfalls sauber, wenn der Ausgabedialog mit `F9` abgebrochen wird.

## [1.20.024] - 2026-03-24

### Added
- Neues Setting `shipping_label_output_dir` fuer frei waehlbaren Speicherort von Versandlabels.
- Test-Versanddienstleister `TEST` mit Fake-Label-PDF zum Durchtesten von Bulk-, Druck- und Lieferschein-Workflow.

### Changed
- Bulk-Abfrage "Dienstleister je Auftrag festlegen?" nutzt jetzt `Enter = Nein` als Default.
- Bulk setzt nach Abschluss die markierten Auftraege wieder zurueck.
- Offene Auftraege sind in der Bestellliste deutlicher markiert (`[!]` statt kleinem `*`).

### Fixed
- Test-Labels koennen nicht an Shopify uebertragen werden.

## [1.20.023] - 2026-03-24

### Added
- Shopify-Sync uebernimmt jetzt Versand-E-Mail und Versand-Telefon in `shopify_orders`.
- Auftragsdetailansicht zeigt E-Mail und Telefon an, falls vorhanden.

### Changed
- GLS uebermittelt jetzt E-Mail und Telefonnummer im Empfaenger-Adressblock.
- Wenn `FlexDelivery` aktiv ist, wird eine fehlende Empfaenger-E-Mail jetzt vor dem API-Call sauber abgefangen.

## [1.20.022] - 2026-03-24

### Fixed
- GLS `Service`-Payload auf das erwartete REST-Format korrigiert (`Service -> ServiceName` als Wrapper-Objekt).
- Bulk-/Einzel-Labelerstellung mit GLS scheiterte dadurch nicht mehr an `Unrecognized field "ServiceName"`.

## [1.20.021] - 2026-03-24

### Changed
- GLS-API-Fehlerlogging verbessert: bei HTTP-Fehlern werden jetzt Fehlermeldungen aus der GLS-Antwort extrahiert und in `lagerverwaltung.log` geschrieben.
- GLS Create/Reprint/Storno geben bei API-Fehlern jetzt aussagekraeftigere Details zurueck.

## [1.20.020] - 2026-03-23

### Fixed
- Bulk-Fehler werden jetzt zusaetzlich in `lagerverwaltung.log` protokolliert, nicht nur im Druck-Log.
- Bulk-Abschluss zeigt bei Fehlern eine Kurzursache plus Hinweis auf `lagerverwaltung.log`.

## [1.20.019] - 2026-03-23

### Fixed
- Bulk-, Einzel- und Teilausfuehrung fallen jetzt sicher auf GLS zurueck, wenn in den Settings ein vorbereiteter, aber noch nicht implementierter Carrier gesetzt ist.
- Carrier-Auswahldialoge fuer ausfuehrbare Versandaktionen zeigen aktuell nur wirklich implementierte Carrier an.

## [1.20.018] - 2026-03-22

### Added
- Eigene Versandlabel-Drucker je Dienstleister:
  `shipping_label_printer_gls`, `shipping_label_printer_dhl`, `shipping_label_printer_post`.
- Eigene Versandlabel-Formate je Dienstleister:
  `shipping_label_format_gls`, `shipping_label_format_dhl`, `shipping_label_format_post`.
- POST-Defaultformat auf `100x62` vorbereitet (z. B. QL-500/CUPS).

### Changed
- Labeldruck verwendet jetzt automatisch den passenden Drucker und das passende Format pro Carrier.
- CUPS-Media-Unterstuetzung erweitert um `100x62` (`media=Custom.100x62mm`).

## [1.20.017] - 2026-03-22

### Added
- Vorbereitung fuer Deutsche Post INTERNETMARKE:
  neue POST-Settings (`post_api_url`, `post_user`, `post_password`, `post_partner_id`).
- Neuer Carrier `post` in Auswahl-Dialogen (Settings, Teilausfuehrung, Bulk pro Auftrag).
- Neue Modulstruktur unter `post/` mit vorbereitetem `InternetmarkeClient`-Stub.

### Changed
- Versand-Routing akzeptiert jetzt zusaetzlich `post` als Carrier (vorbereitet, API-Call folgt).

## [1.20.016] - 2026-03-22

### Changed
- Deutsche Status-/Zahlungsanzeigen auf echte Umlaute umgestellt (z. B. Ausgeführt, Unausgeführt, Zahlung: Bezahlt).
- Teilausführungsdialog schmaler gemacht und Screen-Redraw verbessert (keine Fensterreste nach Abbruch).
- Teilausführungsdialog zeigt jetzt auch bereits ausgeführte Positionen (als nicht mehr auswählbar).

### Added
- Lokaler Schutz gegen Doppelausführung: offene Mengen berücksichtigen zusätzlich bereits eingeplante/abgeschlossene Shopify-Teiljobs aus der Queue.
- Shopify-Sync aktualisiert `fulfilled_quantity` jetzt sofort nach erfolgreichem Fulfillment-Job (ohne auf den nächsten Orders-Refresh warten zu müssen).

## [1.20.015] - 2026-03-22

### Added
- Bulk-Ausfuehrung: optionale direkte Shopify-Queue fuer Tracking/Carrier beim Erstellen der Labels.
- Bulk-Ausfuehrung: Dienstleister je Auftrag waehlbar (pro Auftrag umstellbar, vorbereitet fuer mehrere Carrier).

### Changed
- Bulk-Ergebnis zeigt jetzt Queue-Zaehler (`Q: erfolgreich/fehlerhaft`) zusaetzlich zu OK/Fehler.

## [1.20.014] - 2026-03-22

### Added
- Teilausfuehrung pro Bestellung:
  Positionen auswaehlbar, Menge je Position anpassbar (Standard = volle offene Menge).
- Shopify-Fulfillment-Queue fuer Teilausfuehrung mit Positionsmengen (`line_items_json`) vorbereitet.
- Sendungsnummer in der Bestell-Detailansicht (bevorzugt GLS Paketnummer, sonst TrackID).

### Changed
- Shopify-Tracking fuer Fulfillment nutzt bei GLS bevorzugt die numerische Paketnummer.
- Bestellpositionsanzeige zeigt jetzt `offen/gesamt` statt nur Gesamtmenge.
- Auftragsansicht: neuer Shortcut `Shift+F7` (Fallback `T`) fuer Teilausfuehrung.

## [1.20.013] - 2026-03-22

### Changed
- Deutsche Uebersetzung fuer Laenderanzeige in der Auftrags-Detailansicht.
- Deutsche Uebersetzung fuer Fulfillment-Status in der Auftrags-Detailansicht (z. B. Ausgefuehrt, Unausgefuehrt, In Arbeit).
- Status-Filtertexte in Deutsch angepasst (Unausgefuehrt, Ausgefuehrt, Teilweise).
- Länderauswahl im manuellen Label-Dialog zeigt in deutscher Sprache deutsche Ländernamen.

## [1.20.012] - 2026-03-21

### Added
- Automatische Versandgewichtsberechnung fuer Auftraege:
  Artikelgewichte (`shopify_weight_grams`) werden mit Mengen summiert und um Verpackungsgewicht ergaenzt.
- Neues Setting `shipping_packaging_weight_grams` (Default: `400`).
- Manuelle Label-Erstellung ohne Bestellung mit Eingabemaske fuer Adresse, Referenz, Gewicht und Ausgabeart.
- Länderauswahl fuer manuelle Labels (EU + Schengen + Schweiz + UK inkl. Ministaaten).
- Separate Service-Auswahl fuer manuelle Labels.

### Changed
- GLS-Labelerstellung akzeptiert jetzt uebergebene Werte fuer Gewicht, Referenz und Services.
- Auftragsdetails zeigen berechnetes Versandgewicht in g und kg.
- Versionsnummer auf `1.20.012` erhoeht.

### Fixed
- Ordnername `shpoify-sync` auf `shopify-sync` korrigiert.
- README-Link auf den korrigierten `shopify-sync`-Pfad angepasst.
