# Shopify Custom-App Installation fuer Lager-MC

## Zweck

Diese Anleitung beschreibt den aktuellen Installations- und Authentifizierungsablauf fuer Lager-MC mit einer im Shopify-Admin erzeugten Custom App.

Der Ablauf passt zu dem aktuellen Lager-MC-Installer und zum lokalen `shopify-sync`-Manual-OAuth-Flow.

## Voraussetzungen

- Shopify-Shop mit Zugriff auf `Settings > Apps and sales channels`
- Berechtigung zum Entwickeln und Installieren von Custom Apps
- Lager-MC-Installer oder eine bestehende Installation mit `shopify-sync`
- oeffentlich erreichbare Redirect-URI fuer den aktuellen Lager-MC-Flow, zum Beispiel:
  - `https://install.lagerverwaltung.org/manual-oauth-callback.html`

Fuer den Zugriff auf Kunden- und Bestelldaten koennen zusaetzliche Shopify-Plan- und Berechtigungsanforderungen gelten. Shopify beschreibt Custom Level 2 PII Apps im Help Center.

## Zielbild

Am Ende des Prozesses liegen diese Daten lokal fuer Lager-MC vor:

- Shop-Domain
- Client ID
- Client Secret
- freigegebene Admin-API-Scopes
- lokal erzeugte Shopify-Zugangsdaten fuer `shopify-sync`

## Schritte im Shopify-Admin

### 1. App-Entwicklung aktivieren

Falls Custom-App-Entwicklung im Shop noch nicht aktiviert ist:

1. Im Shopify-Admin `Settings > Apps and sales channels` oeffnen.
2. `Develop apps` waehlen.
3. `Allow custom app development` bestaetigen.

Danach ist der Zugriff auf den merchantseitigen Dev-Dashboard-Flow aktiv.

### 2. Dev Dashboard aus dem Shop-Admin oeffnen

1. Im Shopify-Admin `Settings > Apps and sales channels` oeffnen.
2. `Develop apps` waehlen.
3. `Build apps in Dev Dashboard` oeffnen.

Von dort wird die App direkt fuer den eigenen Shop verwaltet.

### 3. App anlegen

1. Im Dev Dashboard `Create app` waehlen.
2. Einen neutralen App-Namen eintragen, zum Beispiel `Lager-MC`.
3. App anlegen.

### 4. Erste App-Version konfigurieren

In der ersten Version werden mindestens diese Punkte gesetzt:

- `App URL`
- `Allowed redirect URL(s)` bzw. Redirect-URLs
- `Webhooks API version`
- `Admin API scopes`

Fuer den aktuellen Lager-MC-Flow sind diese Werte relevant:

- `App URL`:
  - eine oeffentliche URL fuer die App-Konfiguration
  - neutrales Beispiel: `https://install.lagerverwaltung.org/`
- `Redirect URI`:
  - die im Installer und im Manual-OAuth verwendete Redirect-URI
  - aktuelles Beispiel: `https://install.lagerverwaltung.org/manual-oauth-callback.html`
- `Embedded`:
  - fuer den aktuellen Lager-MC-Flow nicht erforderlich
- `Webhooks API version`:
  - auf die im Projekt verwendete Shopify-API-Version abstimmen

### 5. Admin-API-Scopes setzen

Der aktuelle Lager-MC-Standard fuer Shopify lautet:

```text
read_customers,read_inventory,read_locations,read_merchant_managed_fulfillment_orders,read_orders,read_products,write_inventory,write_merchant_managed_fulfillment_orders
```

Diese Scopes decken den aktuellen Lager-MC- und `shopify-sync`-Stand fuer Artikel, Lagerbestaende, Locations, Bestellungen und Fulfillment ab.

### 6. App-Version freigeben

Nach URLs, API-Version und Scopes:

1. Version freigeben (`Release`)
2. optional Versionsname und Versionsnachricht setzen
3. Freigabe abschliessen

### 7. Client ID und Client Secret abrufen

Die Zugangsdaten liegen im Dev Dashboard in den App-Einstellungen:

1. App im Dev Dashboard oeffnen
2. `Settings` aufrufen
3. `Client ID` und `Client Secret` anzeigen oder kopieren

Diese Werte werden spaeter lokal in den Lager-MC-Installer eingetragen.

## Lokaler Lager-MC-Installations- und Auth-Flow

### 8. Lager-MC installieren

Lager-MC lokal installieren oder aktualisieren.

Der gefuehrte Installer fragt im Shopify-Teil diese Werte ab:

- Shop-Domain, zum Beispiel `beispiel.myshopify.com`
- Client ID
- Client Secret
- Shopify-Scopes
- Redirect-URI

Die Shopify-Scopes sind im Installer bereits mit dem aktuellen Standardwert vorbelegt. Andere Felder bleiben neutral und ohne persoenliche Vorgaben.

### 9. Lokalen Auth-Flow starten

Der Installer startet den lokalen `shopify-sync`-Befehl `manual-connect`.

Dabei passiert technisch:

1. lokal wird ein Callback-Listener auf `127.0.0.1:3459` gestartet
2. der Installer erzeugt den Shopify-Autorisierungslink
3. dieser Link wird im Browser geoeffnet
4. Shopify leitet nach der Freigabe auf die oeffentliche Redirect-URI weiter
5. die statische Callback-Seite leitet den Browser lokal auf `http://127.0.0.1:3459/callback?...` um
6. `shopify-sync` tauscht den Rueckgabecode lokal gegen Token aus
7. die Zugangsdaten werden lokal in `shopify-sync/.env` gespeichert

### 10. Installation abschliessen

Nach erfolgreichem OAuth-Flow:

- `shopify-sync` kann mit dem Shop kommunizieren
- der Installer kann Shopify-Locations laden
- Lager-MC kann mit den lokal gespeicherten Einstellungen weiterarbeiten

## Was im Merchant-Admin sichtbar ist

Waerend der Installation zeigt Shopify beim App-Grant die angeforderten Datenbereiche und Berechtigungen an.

Dazu gehoeren je nach Scopes unter anderem:

- Kundendaten
- Produktdaten
- Lagerbestaende
- Standorte
- Bestellungen
- merchant-managed Fulfillment

Die Berechtigungsanzeige sollte vor dem finalen App-Grant geprueft werden.

## Typische Fehlerpunkte

### Redirect-URI passt nicht exakt

Die Redirect-URI im Shopify-Dev-Dashboard und im Installer muessen exakt uebereinstimmen.

### Scopes wurden geaendert, aber nicht neu freigegeben

Nach Scope-Aenderungen muss die neue App-Version freigegeben und anschliessend neu autorisiert werden.

### Falscher App-Datensatz im Shop

Bei mehreren aehnlich benannten Apps im Dev Dashboard sollte vor der Authentifizierung geprueft werden, dass Client ID und App-Einstellungen zur richtigen App gehoeren.

### Plan- oder Berechtigungsgrenzen

Bei Kunden- und Bestelldaten kann Shopify plan- oder berechtigungsabhaengige Einschraenkungen anwenden.

## Aktueller Lager-MC-Stand

Der aktuelle Lager-MC-Flow verwendet:

- eine im Shopify-Admin angelegte Custom App
- lokale Eingabe von Client ID und Client Secret
- einen gefuehrten lokalen Manual-OAuth-Flow
- keine zentrale serverseitige Secret-Verarbeitung fuer den eigentlichen Token-Austausch

## Quellen

- Shopify Help Center: Apps for your Shopify store  
  <https://help.shopify.com/en/manual/apps>
- Shopify Help Center: Installing and setting up apps  
  <https://help.shopify.com/en/manual/apps/install-setup-apps>
- Shopify Dev Docs: About client credentials  
  <https://shopify.dev/docs/apps/build/authentication-authorization/client-secrets>
