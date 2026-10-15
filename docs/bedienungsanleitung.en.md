# User Manual

## Purpose

Lager-MC is a terminal-based application for inventory, orders, stocktaking and shipping.

This manual covers:

- stock management with `Shelf / Bin / Slot` location logic
- Shopify or WooCommerce orders
- pick lists and delivery notes
- shipping with GLS, Deutsche Post INTERNETMARKE and address labels
- shipping history with reprint, cancellation and shop queue
- stocktaking with snapshots and CSV export

## Current shipping state

Implemented carrier integrations:

- `GLS`
- `Deutsche Post INTERNETMARKE`

Also available:

- `Address label` for internal labels without a carrier API

The carrier structure uses central carrier definitions with shared shipping history and shop handover.

Details about carrier integration points are available in [docs/shipping-providers.en.md](shipping-providers.en.md).

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

Direct from GitHub:

```bash
curl -fsSL https://raw.githubusercontent.com/xoquox/Lagerverwaltung/main/scripts/install-from-github.sh | bash
```

The installation can ask for:

- a bundle archive with local settings and files
- a DB migration

The migration script creates the configured database when needed and then builds the schema.

Start:

```bash
python3 lager_mc.py
```

## Initial setup

These points are set before productive use:

1. database connection
2. pick list and delivery note printers
3. shipping printers and media formats per carrier
4. sync server for the selected shop system
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
- `Shift+F10` pick list
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
- no shop transfer

Address labels can be used for single shipments, manual labels and bulk execution.

### Manual shipping label

The manual shipping form is available in the order view under `Shift+F5`.

Important functions:

- `F3` country
- `F4` carrier-specific selection
- `F5` output `PDF + Print` or `PDF only`
- `F6` load customer from the locally synchronized shop customer table

## Shipping history

Shipping history is available in the order view under `F8`.

Important actions:

- `F5` output the existing PDF again
- `F6` cancel if supported by the carrier
- `F7` reprint
- `F10` send to the shop

Each entry shows:

- carrier
- tracking ID or shipment number
- status
- source
- reference
- local PDF path
- tracking URL
- latest shop job

## Pick lists and delivery notes

Pick lists and delivery notes are started from the order view.

- `Shift+F10` pick list
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

The top menu bar offers `File`, `Article`, `View`, `Orders`, `Extras` and `Help`. `F10` or a mouse click opens a menu. `Left`/`Right` switches menus, `Up`/`Down` selects enabled entries, `Enter` executes and `Esc` or `F9` closes. Disabled actions require the appropriate view or selection. The order view uses `Shift+F10` for picklists.

`File → Exit` closes Lager-MC and checks pending database writes. `Extras` contains stocktake, settings, print settings, keyboard shortcuts and sync status. `Help` offers the shortcut overview, user manual and version information.

Mouse control is selected under `General → Mouse control → On/Off` and saved with `F2`. Mouse events are used when provided by the terminal; keyboard controls remain available independently. Clicking selects items, locations or orders and switches the active item panel. Double-clicking an item or location entry opens item details. The wheel navigates the list under the pointer; in the right order panel it scrolls order details. Clicking `[ ]` or double-clicking an order row marks it for bulk execution.

Menus, choice lists, printer selection, settings tabs and keyboard assignments support clicks. The wheel moves selection and scrolls long settings lists. Clicking a text field positions the cursor. Visible footer buttons execute their displayed actions.

Keyboard shortcuts are configured under `Extras → Keyboard shortcuts` or the `General` settings tab. `Tab` switches between items with a single shop location, items with multiple shop locations and orders. `Enter` or `F6` selects a key; `Menu only` removes its assignment. Duplicate assignments are rejected. `F5` restores defaults for the displayed view. `F2` accepts; `F9` or `Esc` discards changes. When opened from settings, the settings dialog's `F2` saves the accepted assignments. `F10` remains reserved for the menu. Subdialogs retain their navigation and save keys.

Menu entries, the shortcut overview and the footer show configured shortcuts. The key lists in this manual describe the default assignments.

Settings are available under `Shift+F11`.

### Appearance

The colour scheme is selected in the `General` tab with `Enter` or `F6`. The selection is previewed in settings. `F2` saves it; `F9` or `Esc` restores the saved scheme. Validation errors keep the dialog open with the entered values. A label printer is optional; its model and label format are required when a printer URI is set.

| Colour scheme | Appearance |
| --- | --- |
| Commander (Blue) | Blue work area, cyan header and selection, yellow headings |
| Green | Green text on black with green selection and header |
| Monochrome | Black and white with inverted selection and header |
| Megatrends (DOS) | Gray work area, blue text and blue selection inspired by BIOS screens |
| Smoth (DOS) | Blue work area with light gray selection and header |
| Norton (DOS) | Cyan text and borders on blue with cyan selection and header |
| Gold Standard (DOS) | Yellow text on black with gold selection and header |
| Subtile (DOS) | Light work area with dark text and blue accents |
| Monokai | Dark work area, bright text, green selection and cyan header |

Product numbers and names share the same text colour. Sync indicators distinguish success, pending changes, errors and local items using theme-specific colours. The `S`, `D` and `L` indicators remain visible. Keyboard shortcuts are highlighted separately in the footer. Custom theme files using the original six colour values remain supported.

Tabs:

- Allgemein
- Lagerlabel
- Drucker
- Versand
- Lieferschein

Important keys:

- `Tab` and `Shift+Tab` for tabs
- `Enter` for selection or next field
- `F2` save
- `F6` opens the matching selection for the current field
- `F7` starts a test page for the current printer/media field

The `Printers` tab offers one-sided, long-edge duplex and short-edge duplex printing for delivery notes and pick/inventory lists. Shipping labels are printed one-sided. Duplex delivery notes are submitted as separate jobs during bulk execution so each order starts on its own sheet.

The `Delivery note` tab selects item sorting with `Enter` or `F6`: shop order sequence, storage location, name, SKU or weight. All criteria except shop order sequence support ascending and descending sorting. Location and SKU numbers use natural ordering; missing values stay at the end. External stock items remain excluded. Sorting applies to PDF export, individual printing and bulk printing.

`F7` uses the selected print options before saving. Duplex tests include a front and reverse page. General shipping media and scaling are available in the `Printers` tab; the driver media code `PCard100x148` is handled separately from PDF dimensions. Delivery note PDFs use the selected page size; PDF templates are fitted proportionally.

Important settings in the shipping tab:

- active shipping carriers
- printer and media format per carrier
- shop tracking mode per carrier
- tracking URL per carrier
- GLS credentials
- POST credentials
- template for address labels

## Shop selection

Each installation uses Shopify or WooCommerce with its own sync server. Warehouse operations read and write the local PostgreSQL database. WooCommerce setup and supported operations: [WooCommerce sync](woocommerce-sync.md).

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

## Startup with slow database connections

The application checks the database connection during startup.

If the connection is not ready yet, the startup view remains visible and shows the current status.

- `Enter` starts the check again
- `F2` opens settings
- `F9` exits the application

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

DB migration:

```bash
python3 scripts/run_db_migrations.py
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

- [logs/lagerverwaltung.log](../logs/lagerverwaltung.log)
- [logs/print.log](../logs/print.log)
- [logs/shopify-sync.log](../logs/shopify-sync.log)
