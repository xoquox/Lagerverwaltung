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
- Entwicklungsstaende auf `develop`: `MAJOR.MINOR.PATCH-dev.BUILD`
  - Beispiel: `1.21.0-dev.1`

Regeln:

- `MAJOR` nur bei groben, inkompatiblen Umstellungen erhoehen
- `MINOR` bei groesseren neuen Funktionsbloecken erhoehen
- `PATCH` fuer produktive Hotfixes auf derselben Release-Linie erhoehen
- `VERSION_STAGE` in [app_version.py](/home/chrisi/Lagerverwaltung/app_version.py) ist
  - leer auf `main`
  - `dev` auf `develop`
- `VERSION_BUILD` wird auf `develop` bei sichtbaren Zwischenstaenden oder
  relevanten Testbuilds weiter hochgezaehlt
- der Shopify-Sync fuehrt seine eigene Version separat in [sync_version.py](/home/chrisi/Lagerverwaltung/shopify-sync/sync_version.py)
  und wird nicht ueber `app_version.py` mitversioniert

Aktueller Uebergang:

- letzter produktiver Stand vor der Umstellung: `1.20.028`
- laufender Entwicklungsstand nach der Umstellung: `1.21.0-dev.1`
- naechster Release auf `main`: `1.21.0`

## Changelog

- [CHANGELOG.md](/home/chrisi/Lagerverwaltung/CHANGELOG.md) wird nur fuer Releases auf `main` gepflegt
- kein Eintrag fuer unfertige Zwischenstaende auf `develop`
- ein Changelog-Eintrag fasst alle relevanten Aenderungen seit dem letzten Release auf `main` zusammen
- der Shopify-Sync pflegt zusaetzlich sein eigenes Changelog in [shopify-sync/CHANGELOG.md](/home/chrisi/Lagerverwaltung/shopify-sync/CHANGELOG.md)
- Eintraege im Haupt-Changelog nennen die zugehoerige Sync-Version und verweisen auf das separate Sync-Changelog

## Empfohlener Ablauf

1. Von `develop` einen Feature-Branch anlegen
2. Aenderungen im Feature-Branch umsetzen und lokal testen
3. Merge nach `develop`
4. Gesamtstand auf `develop` testen
5. Vor Release:
   - `app_version.py` auf finale Release-Version setzen
   - `VERSION_STAGE = ""`
   - `VERSION_BUILD = 0`
   - Changelog fuer alle Aenderungen seit dem letzten Release ergaenzen
   - zugehoerige Shopify-Sync-Version notieren und auf `shopify-sync/CHANGELOG.md` verweisen
6. `develop` nach `main` mergen
7. Git-Tag fuer den Release setzen, z. B. `v1.21.0`
8. `develop` danach auf die naechste Zielversion bzw. wieder auf `-dev` stellen

## Mindest-Checkliste vor Merge nach `main`

- relevante Funktionen manuell getestet
- keine ungewollten lokalen Secrets oder Exportdateien im Commit
- Versionsnummer gesetzt
- Changelog aktualisiert
- bei Bedarf `shopify-sync` und TUI gemeinsam getestet
