# Shopify connection with Lager-MC

## Requirements

- access to `Settings > Apps and sales channels` in the Shopify admin
- permission for `Develop apps`
- Lager-MC installer or an existing installation with `shopify-sync`
- redirect URI:
  - `https://install.lagerverwaltung.org/manual-oauth-callback.html`
- Webhooks API version:
  - `2026-04`

## Shopify admin

### 1. Enable app development

1. Open `Settings > Apps and sales channels`.
2. Select `Develop apps`.
3. Confirm `Allow custom app development`.

### 2. Open the Dev Dashboard

1. Open `Settings > Apps and sales channels`.
2. Select `Develop apps`.
3. Open `Build apps in Dev Dashboard`.

### 3. Create the app

1. Select `Create app`.
2. Enter an app name, for example `Lager-MC`.
3. Create the app.

### 4. Configure the app

Set these values:

- `App URL`
  - `https://install.lagerverwaltung.org/`
- `Allowed redirect URL(s)`
  - `https://install.lagerverwaltung.org/manual-oauth-callback.html`
- `Webhooks API version`
  - `2026-04`
- `Embedded`
  - off for this flow

### 5. Set the Admin API scopes

```text
read_customers,read_inventory,read_locations,read_merchant_managed_fulfillment_orders,read_orders,read_products,write_products,write_inventory,write_merchant_managed_fulfillment_orders
```

### 6. Release the app version

1. Select `Release`.
2. Complete the release.

### 7. Retrieve the client ID and client secret

1. Open the app in the Dev Dashboard.
2. Open `Settings`.
3. Copy the `Client ID` and `Client Secret`.

## Lager-MC installer

### 8. Enter the Shopify values

The installer asks for:

- shop domain, for example `example.myshopify.com`
- client ID
- client secret
- Shopify scopes
- redirect URI

The scopes are prefilled in the installer:

```text
read_customers,read_inventory,read_locations,read_merchant_managed_fulfillment_orders,read_orders,read_products,write_products,write_inventory,write_merchant_managed_fulfillment_orders
```

### 9. Run the authentication

The installer starts `shopify_sync.py manual-connect`.

Flow:

1. local listener on `127.0.0.1:3459`
2. generate the Shopify authorization link
3. open the link in the browser
4. approve the app in the Shopify admin
5. redirect to `https://install.lagerverwaltung.org/manual-oauth-callback.html`
6. local redirect to `http://127.0.0.1:3459/callback?...`
7. store the token in `shopify-sync/.env`

### 10. Finish

After successful authentication:

- `shopify-sync` can work with the shop
- Shopify locations can be loaded
- Lager-MC can continue with the stored configuration

## Permission screen in the Shopify admin

During the app grant, Shopify shows the requested areas, including these depending on the scopes:

- customer data
- product data
- inventory data
- locations
- orders
- merchant-managed fulfillment

## Sources

- Shopify Help Center: Apps for your Shopify store  
  <https://help.shopify.com/en/manual/apps>
- Shopify Help Center: Installing and setting up apps  
  <https://help.shopify.com/en/manual/apps/install-setup-apps>
- Shopify Dev Docs: About client credentials  
  <https://shopify.dev/docs/apps/build/authentication-authorization/client-secrets>
