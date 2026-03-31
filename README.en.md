<p align="center">
  <img src="assets/lager-mc.svg" alt="Lager MC" width="160">
</p>

# Lagerverwaltung

Terminal-based inventory management with storage locations, order handling, stocktaking, Shopify integration and shipping workflows.

License: [MIT](/home/chrisi/Lagerverwaltung/LICENSE)

## Core features

- maintain items, storage locations and stock quantities
- import and process Shopify orders
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

The carrier structure is prepared for additional shipping providers.

## Documentation

- User manual: [docs/bedienungsanleitung.en.md](/home/chrisi/Lagerverwaltung/docs/bedienungsanleitung.en.md)
- Shipping integration notes: [docs/shipping-providers.en.md](/home/chrisi/Lagerverwaltung/docs/shipping-providers.en.md)
- Release history: [CHANGELOG.md](/home/chrisi/Lagerverwaltung/CHANGELOG.md)
- GitHub Releases: <https://github.com/xoquox/Lagerverwaltung/releases>

## Repository variants

- Project variant with Shopify and shipping support: [Lagerverwaltung](https://github.com/xoquox/Lagerverwaltung)
- Project variant without Shopify and shipping support: [simple-storage-core](https://github.com/b4ckspace/simple-storage-core)
