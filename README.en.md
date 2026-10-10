<p align="center">
  <img src="assets/lager-mc.svg" alt="Lager-MC" width="160">
</p>

# Lager-MC

Terminal-based warehouse and shipping software with storage locations, order handling, stocktaking, selectable shop integration and shipping workflows.

License: [MIT](LICENSE)

## Core features

- maintain items, storage locations and stock quantities
- import and process shop orders
- create pick lists, delivery notes and shipping labels
- shipping with GLS, Deutsche Post INTERNETMARKE and address labels
- partial execution, bulk execution and shipping history
- stocktaking with snapshots, CSV export and apply step
- local bundle files for new workstations

## Current shipping state

Implemented carrier integrations:

- `GLS`
- `Deutsche Post INTERNETMARKE`

Also available:

- `Address label` for internal labels without a carrier API

The carrier structure uses central carrier definitions with shared shipping history and shop handover.

## Documentation

- User manual: [docs/bedienungsanleitung.en.md](docs/bedienungsanleitung.en.md)
- Shipping integration notes: [docs/shipping-providers.en.md](docs/shipping-providers.en.md)
- Shopify connection with Lager-MC: [docs/shopify-connection-with-lager-mc.en.md](docs/shopify-connection-with-lager-mc.en.md)
- Release history: [CHANGELOG.md](CHANGELOG.md)
- GitHub Releases: <https://github.com/xoquox/Lagerverwaltung/releases>

## Repository variants

- Fork with the core Lager-MC features without Shopify integration and shipping: [simple-storage-core](https://github.com/b4ckspace/simple-storage-core)

## Shopify and WooCommerce

Installation supports `--shop-system shopify` or `--shop-system woocommerce`.
Lager-MC uses a shared PostgreSQL contract and never connects directly to a shop.
Both workers use the shared database contract. The WooCommerce worker exchanges data
through MySQL/MariaDB bridge tables and a bundled WordPress plugin, without REST calls.
See [setup and capability differences](docs/woocommerce-sync.md) (German).
Existing Shopify installations require a coordinated app/worker update and database migration.
See [shop selection and migration](docs/shop-systeme.md).
