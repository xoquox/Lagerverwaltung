# User Manual

## Purpose

Lagerverwaltung is a terminal-based application for inventory, orders, stocktaking and shipping.

This manual covers:

- stock management with `Shelf / Bin / Slot` location logic
- Shopify orders
- pick lists and delivery notes
- shipping with GLS, Deutsche Post INTERNETMARKE and address labels
- shipping history with reprint, cancellation and Shopify queue
- stocktaking with snapshots and CSV export

## Current shipping state

Implemented carrier integrations:

- `GLS`
- `Deutsche Post INTERNETMARKE`

Also available:

- `Address label` for internal labels without a carrier API

The carrier structure uses central carrier definitions with shared shipping history and Shopify handover.

Details about carrier integration points are available in [docs/shipping-providers.en.md](/home/chrisi/Lagerverwaltung/docs/shipping-providers.en.md).

## Requirements

- Python 3.11 or newer
- PostgreSQL
- `curses`
- a working `lp` printing system
- `WeasyPrint` for HTML-based templates

## Installation

Standard on Linux:

```bash
git clone git@github.com:xoquox/Lagerverwaltung.git
cd Lagerverwaltung
./scripts/install-linux.sh
```

Start:

```bash
python3 lager_mc.py
```

## Initial setup

These points are set before productive use:

1. database connection
2. pick list and delivery note printers
3. shipping printers and media formats per carrier
4. Shopify sync
5. shipping credentials for GLS and POST

The settings dialog is available under `Shift+F11`.

## Interaction model

The application is designed for keyboard use.

Core rules:

- `↑↓` move the selection
- `Enter` confirms the current selection or moves to the next field
- `F9` closes the current dialog
- `Tab` switches between the item list and the storage location list in the inventory view
- `Tab` and `Shift+Tab` switch tabs in settings
- list filters are typed directly from the keyboard

The bottom status line shows the valid keys for the current dialog.

## Inventory view

The inventory view is the main screen for items and storage locations.

`Tab` switches between the item list and the storage location list.

Important actions:

- `F1` change sorting
- `F2` local items only
- `F3` missing locations
- `F4` item details
- `F5` create new item
- `Shift+F5` edit item
- `F6` change location
- `F7` change quantity
- `F8` create shipping label for the selected item
- `Shift+F1` stocktaking
- `Shift+F11` settings
- `F12` orders

## Order view

The order view is the working screen for orders.

Important actions:

- `Space` mark orders for bulk actions
- `A` mark all visible orders
- `F1` filter open/all
- `F2` fulfillment status
- `F3` payment status
- `F4` jump to an order
- `F5` create shipping label
- `Shift+F5` manual shipping label
- `F6` partial execution
- `F7` bulk execution
- `F8` shipping history
- `F10` pick list
- `F11` delivery note

## Shipping

### Carrier selection

Shipping actions start with carrier selection.

Currently available:

- `GLS`
- `POST`
- `Address label`

The carrier list is controlled by the `Active shipping carriers` setting.

### GLS

After carrier selection, GLS service options can be selected, for example:

- FlexDelivery
- AddresseeOnly
- Guaranteed24
- PreAdvice
- SMS Service

The default values come from settings and can be changed per shipment.

### POST

POST uses base products and optional add-ons.

Examples:

- Maxibrief
- Grossbrief
- Warensendung
- Einschreiben
- Einschreiben Einwurf
- Rueckschein

Product data comes from the imported price list.

### Address label

`Address label` creates a plain address label with sender and receiver.

Properties:

- no shipment number
- no carrier API
- no Shopify transfer

Address labels can be used for single shipments, manual labels and bulk execution.

### Manual shipping label

The manual shipping form is available in the order view under `Shift+F5`.

Important functions:

- `F3` country
- `F4` carrier-specific selection
- `F5` output `PDF + Print` or `PDF only`
- `F6` load customer from the locally synchronized Shopify customer table

## Shipping history

Shipping history is available in the order view under `F8`.

Important actions:

- `F5` output the existing PDF again
- `F6` cancel if supported by the carrier
- `F7` reprint
- `F10` send to Shopify

Each entry shows:

- carrier
- tracking ID or shipment number
- status
- source
- reference
- local PDF path
- tracking URL
- latest Shopify job

## Pick lists and delivery notes

Pick lists and delivery notes are started from the order view.

- `F10` pick list
- `F11` delivery note

Available delivery note output modes:

- `Print`
- `Print + PDF`
- `PDF only`

Template and logo are configured in settings.

## Bulk execution

Bulk execution is available in the order view under `F7`.

Process:

1. mark orders
2. select carrier
3. set carrier-specific options
4. choose output mode
5. process the selected orders

Labels and delivery notes are merged for batch printing. Individual PDFs remain available.

## Partial execution

Partial execution is available in the order view under `F6`.

The quantity is set per position. The remaining flow matches the normal shipping workflow.

## Settings

Settings are available under `Shift+F11`.

Tabs:

- General
- Inventory
- Printing
- Shipping

Important keys:

- `Tab` and `Shift+Tab` for tabs
- `Enter` for selection or next field
- `F2` save
- `F3` printer selection
- `F4` format selection
- `F6` file selection or directory selection, depending on the field

Important settings in the shipping tab:

- active shipping carriers
- printer and media format per carrier
- Shopify tracking mode per carrier
- tracking URL per carrier
- GLS credentials
- POST credentials
- template for address labels

## Shopify sync

Shopify sync writes these Shopify data sets into the database:

- orders
- order line items
- fulfillment status
- payment status
- customers for the manual shipping form

Minimum Shopify scopes:

- `read_products`
- `read_inventory`
- `write_inventory`
- `read_locations`
- `read_orders`

Additional scope for older orders:

- `read_all_orders`

`read_customers` is required for customer lookup in manual shipping labels.

## Moving to a new workstation

Bundle scripts can transfer local files to a new workstation.

Export:

```bash
python3 scripts/create_local_bundle.py
```

Import:

```bash
python3 scripts/apply_local_bundle.py /path/to/lager_mc_local_bundle_*.zip
```

A bundle contains portable project files such as:

- API credentials
- Shopify sync `.env`
- fonts
- logos
- selected templates and theme files

These local workstation settings are not overwritten:

- printers
- media formats
- PDF and label output directories

## Logs

Important log files:

- [logs/lagerverwaltung.log](/home/chrisi/Lagerverwaltung/logs/lagerverwaltung.log)
- [logs/druck.log](/home/chrisi/Lagerverwaltung/logs/druck.log)
- [logs/shopify-sync.log](/home/chrisi/Lagerverwaltung/logs/shopify-sync.log)
