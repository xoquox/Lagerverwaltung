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
- Versand fuer GLS, Deutsche Post INTERNETMARKE und Adresslabels
- Teilausfuehrung, Bulk-Ausfuehrung und Versandhistory
- Inventur mit Snapshot, CSV-Export und Uebernahme
- lokale Bundle-Dateien fuer neue Arbeitsplaetze

## Dokumentation

- Bedienungsanleitung: [docs/bedienungsanleitung.md](/home/chrisi/Lagerverwaltung/docs/bedienungsanleitung.md)
- Release-Historie: [CHANGELOG.md](/home/chrisi/Lagerverwaltung/CHANGELOG.md)
- GitHub Releases: <https://github.com/xoquox/Lagerverwaltung/releases>
- Release-Regeln fuer `develop` und `main`: [RELEASE.md](/home/chrisi/Lagerverwaltung/RELEASE.md)

## Varianten

- Vollversion mit Shopify und Versand: dieses Repository
- reduzierte Variante ohne Shopify- und Versandfunktionen: <https://github.com/b4ckspace/simple-storage-core>

## Schnellstart

```bash
git clone git@github.com:xoquox/Lagerverwaltung.git
cd Lagerverwaltung
./scripts/install-linux.sh
```

Manueller Start:

```bash
python3 lager_mc.py
```

## Hinweise

- lokale Laufzeitwerte liegen in `settings.local.json`
- neue Arbeitsplaetze lassen sich mit `scripts/create_local_bundle.py` und `scripts/apply_local_bundle.py` vorbereiten
