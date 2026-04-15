# Shopify Custom App Installation for Lager-MC

## Purpose

This guide describes the current installation and authentication flow for Lager-MC with a custom app created in the Shopify admin.

The flow matches the current Lager-MC installer and the local `shopify-sync` manual OAuth flow.

## Requirements

- Shopify store access to `Settings > Apps and sales channels`
- permission to develop and install custom apps
- Lager-MC installer or an existing installation with `shopify-sync`
- a publicly reachable redirect URI for the current Lager-MC flow, for example:
  - `https://install.lagerverwaltung.org/manual-oauth-callback.html`

Additional Shopify plan and permission requirements can apply for customer and order data access. Shopify documents custom Level 2 PII apps in the Help Center.

## Target state

At the end of the process, these values exist locally for Lager-MC:

- shop domain
- client ID
- client secret
- approved Admin API scopes
- locally generated Shopify credentials for `shopify-sync`

## Steps in the Shopify admin

### 1. Enable app development

If custom app development isn't enabled for the shop yet:

1. Open `Settings > Apps and sales channels` in the Shopify admin.
2. Select `Develop apps`.
3. Confirm `Allow custom app development`.

After that, the merchant-side Dev Dashboard flow is available.

### 2. Open the Dev Dashboard from the shop admin

1. Open `Settings > Apps and sales channels` in the Shopify admin.
2. Select `Develop apps`.
3. Open `Build apps in Dev Dashboard`.

From there, the app is managed directly for the merchant's own store.

### 3. Create the app

1. Select `Create app` in the Dev Dashboard.
2. Enter a neutral app name, for example `Lager-MC`.
3. Create the app.

### 4. Configure the first app version

At minimum, set these values in the first version:

- `App URL`
- `Allowed redirect URL(s)`
- `Webhooks API version`
- `Admin API scopes`

For the current Lager-MC flow, these values are relevant:

- `App URL`:
  - a public URL for the app configuration
  - neutral example: `https://install.lagerverwaltung.org/`
- `Redirect URI`:
  - the redirect URI used by the installer and manual OAuth flow
  - current example: `https://install.lagerverwaltung.org/manual-oauth-callback.html`
- `Embedded`:
  - not required for the current Lager-MC flow
- `Webhooks API version`:
  - keep it aligned with the Shopify API version used by the project

### 5. Set the Admin API scopes

The current Lager-MC standard is:

```text
read_customers,read_inventory,read_locations,read_merchant_managed_fulfillment_orders,read_orders,read_products,write_inventory,write_merchant_managed_fulfillment_orders
```

These scopes cover the current Lager-MC and `shopify-sync` state for items, inventory levels, locations, orders, and fulfillment.

### 6. Release the app version

After URLs, API version, and scopes are configured:

1. release the version
2. optionally set a version name and release message
3. complete the release

### 7. Retrieve the client ID and client secret

The credentials are available in the Dev Dashboard app settings:

1. open the app in the Dev Dashboard
2. open `Settings`
3. view or copy the `Client ID` and `Client Secret`

These values are entered later into the local Lager-MC installer.

## Local Lager-MC installation and authentication flow

### 8. Install Lager-MC

Install or update Lager-MC locally.

In the Shopify step, the guided installer asks for:

- shop domain, for example `example.myshopify.com`
- client ID
- client secret
- Shopify scopes
- redirect URI

The Shopify scopes are already prefilled in the installer with the current standard value. Other fields stay neutral and contain no personal defaults.

### 9. Start the local auth flow

The installer starts the local `shopify-sync` command `manual-connect`.

Technically, this does the following:

1. start a local callback listener on `127.0.0.1:3459`
2. generate the Shopify authorization link
3. open this link in the browser
4. Shopify redirects to the public redirect URI after approval
5. the static callback page redirects the browser locally to `http://127.0.0.1:3459/callback?...`
6. `shopify-sync` exchanges the returned code for tokens locally
7. the credentials are stored locally in `shopify-sync/.env`

### 10. Finish the installation

After a successful OAuth flow:

- `shopify-sync` can communicate with the shop
- the installer can load Shopify locations
- Lager-MC can continue with the locally stored configuration

## What is visible in the merchant admin

During the installation, Shopify displays the requested access areas and permissions in the app grant screen.

Depending on the scopes, these include:

- customer data
- product data
- inventory data
- locations
- orders
- merchant-managed fulfillment

The grant screen should be checked before the final approval.

## Typical error points

### Redirect URI does not match exactly

The redirect URI in the Shopify Dev Dashboard and in the installer must match exactly.

### Scopes were changed but not reapproved

After scope changes, the new app version must be released and then authorized again.

### Wrong app selected in the shop

If several similarly named apps exist in the Dev Dashboard, make sure the client ID and app settings belong to the correct app before starting authentication.

### Plan or permission limits

For customer and order data, Shopify can enforce plan- or permission-based restrictions.

## Current Lager-MC state

The current Lager-MC flow uses:

- a custom app created in the Shopify admin
- local entry of the client ID and client secret
- a guided local manual OAuth flow
- no central server-side secret processing for the actual token exchange

## Sources

- Shopify Help Center: Apps for your Shopify store  
  <https://help.shopify.com/en/manual/apps>
- Shopify Help Center: Installing and setting up apps  
  <https://help.shopify.com/en/manual/apps/install-setup-apps>
- Shopify Dev Docs: About client credentials  
  <https://shopify.dev/docs/apps/build/authentication-authorization/client-secrets>
