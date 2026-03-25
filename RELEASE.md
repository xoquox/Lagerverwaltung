# Release-Prozess

## Branches

- `main`
  - produktiver Branch
  - nur getestete und freigegebene Stande
- `develop`
  - Integrationsbranch fuer laufende Entwicklung
  - Basis fuer funktionale Tests vor der Uebernahme nach `main`
- `feature/*`
  - konkrete Umsetzungen oder groessere Baustellen
  - Merge zuerst nach `develop`
- `hotfix/*`
  - direkte produktive Fehlerbehebungen auf Basis von `main`
  - nach dem Fix Merge nach `main` und Rueckmerge nach `develop`

## Versionierung

Es gilt ein einfaches Release-Schema:

- Produktive Releases auf `main`: `MAJOR.MINOR.PATCH`
  - Beispiel: `1.21.0`
- Entwicklungsstaende auf `develop`: `MAJOR.MINOR.PATCH-dev`
  - Beispiel: `1.21.0-dev`

Regeln:

- `MAJOR` nur bei groben, inkompatiblen Umstellungen erhoehen
- `MINOR` bei groesseren neuen Funktionsbloecken erhoehen
- `PATCH` fuer produktive Hotfixes auf derselben Release-Linie erhoehen
- `VERSION_STAGE` in [app_version.py](/home/chrisi/Lagerverwaltung/app_version.py) ist
  - leer auf `main`
  - `dev` auf `develop`

Aktueller Uebergang:

- letzter produktiver Stand vor der Umstellung: `1.20.028`
- laufender Entwicklungsstand nach der Umstellung: `1.21.0-dev`
- naechster Release auf `main`: `1.21.0`

## Changelog

- [CHANGELOG.md](/home/chrisi/Lagerverwaltung/CHANGELOG.md) wird nur fuer Releases auf `main` gepflegt
- kein Eintrag fuer unfertige Zwischenstaende auf `develop`
- ein Changelog-Eintrag fasst alle relevanten Aenderungen seit dem letzten Release auf `main` zusammen

## Empfohlener Ablauf

1. Von `develop` einen Feature-Branch anlegen
2. Aenderungen im Feature-Branch umsetzen und lokal testen
3. Merge nach `develop`
4. Gesamtstand auf `develop` testen
5. Vor Release:
   - `app_version.py` auf finale Release-Version setzen
   - `VERSION_STAGE = ""`
   - Changelog fuer alle Aenderungen seit dem letzten Release ergaenzen
6. `develop` nach `main` mergen
7. Git-Tag fuer den Release setzen, z. B. `v1.21.0`
8. `develop` danach auf die naechste Zielversion bzw. wieder auf `-dev` stellen

## Mindest-Checkliste vor Merge nach `main`

- relevante Funktionen manuell getestet
- keine ungewollten lokalen Secrets oder Exportdateien im Commit
- Versionsnummer gesetzt
- Changelog aktualisiert
- bei Bedarf `shopify-sync` und TUI gemeinsam getestet
