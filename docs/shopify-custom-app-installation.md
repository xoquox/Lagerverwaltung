# Shopify-Custom-App Installation fuer Lager-MC

## Voraussetzungen

- Zugriff auf `Settings > Apps and sales channels` im Shopify-Admin
- Berechtigung fuer `Develop apps`
- Lager-MC-Installer oder eine bestehende Installation mit `shopify-sync`
- Redirect-URI:
  - `https://install.lagerverwaltung.org/manual-oauth-callback.html`
- Webhooks API version:
  - `2026-04`

## Shopify-Admin

### 1. App-Entwicklung aktivieren

1. `Settings > Apps and sales channels` oeffnen.
2. `Develop apps` waehlen.
3. `Allow custom app development` bestaetigen.

### 2. Dev Dashboard oeffnen

1. `Settings > Apps and sales channels` oeffnen.
2. `Develop apps` waehlen.
3. `Build apps in Dev Dashboard` oeffnen.

### 3. App anlegen

1. `Create app` waehlen.
2. App-Namen eintragen, zum Beispiel `Lager-MC`.
3. App anlegen.

### 4. App konfigurieren

Diese Werte setzen:

- `App URL`
  - `https://install.lagerverwaltung.org/`
- `Allowed redirect URL(s)`
  - `https://install.lagerverwaltung.org/manual-oauth-callback.html`
- `Webhooks API version`
  - `2026-04`
- `Embedded`
  - fuer diesen Ablauf aus

### 5. Admin-API-Scopes setzen

```text
read_customers,read_inventory,read_locations,read_merchant_managed_fulfillment_orders,read_orders,read_products,write_inventory,write_merchant_managed_fulfillment_orders
```

### 6. App-Version freigeben

1. `Release` waehlen.
2. Freigabe abschliessen.

### 7. Client ID und Client Secret abrufen

1. App im Dev Dashboard oeffnen.
2. `Settings` oeffnen.
3. `Client ID` und `Client Secret` kopieren.

## Lager-MC-Installer

### 8. Shopify-Daten eintragen

Der Installer fragt diese Werte ab:

- Shop-Domain, zum Beispiel `beispiel.myshopify.com`
- Client ID
- Client Secret
- Shopify-Scopes
- Redirect-URI

Die Scopes sind im Installer vorbelegt:

```text
read_customers,read_inventory,read_locations,read_merchant_managed_fulfillment_orders,read_orders,read_products,write_inventory,write_merchant_managed_fulfillment_orders
```

### 9. Authentifizierung ausfuehren

Der Installer startet `shopify_sync.py manual-connect`.

Ablauf:

1. lokaler Listener auf `127.0.0.1:3459`
2. Shopify-Autorisierungslink erzeugen
3. Link im Browser oeffnen
4. App im Shopify-Admin freigeben
5. Rueckleitung auf `https://install.lagerverwaltung.org/manual-oauth-callback.html`
6. lokale Weiterleitung auf `http://127.0.0.1:3459/callback?...`
7. Token in `shopify-sync/.env` speichern

### 10. Abschluss

Nach erfolgreicher Authentifizierung:

- `shopify-sync` kann mit dem Shop arbeiten
- Shopify-Locations koennen geladen werden
- Lager-MC kann mit der gespeicherten Konfiguration weiterarbeiten

## Berechtigungsanzeige im Shopify-Admin

Beim App-Grant zeigt Shopify die angeforderten Bereiche an, darunter je nach Scope:

- Kundendaten
- Produktdaten
- Lagerbestaende
- Standorte
- Bestellungen
- merchant-managed Fulfillment

## Quellen

- Shopify Help Center: Apps for your Shopify store  
  <https://help.shopify.com/en/manual/apps>
- Shopify Help Center: Installing and setting up apps  
  <https://help.shopify.com/en/manual/apps/install-setup-apps>
- Shopify Dev Docs: About client credentials  
  <https://shopify.dev/docs/apps/build/authentication-authorization/client-secrets>
