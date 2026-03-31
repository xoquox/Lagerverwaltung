# Shipping providers and integration points

## Purpose

This document describes the current shipping integration state and the technical extension points for additional carriers.

## Current state

Implemented carrier integrations:

- `GLS`
- `Deutsche Post INTERNETMARKE`

Also available:

- `Address label` for internal labels without a carrier API

Additional carrier APIs such as DHL, DPD, UPS or Hermes are not implemented at this time.

The carrier structure includes central carrier definitions, shared shipping history and Shopify fulfillment handover.

## Carrier definition

The central carrier structure is defined in [shipping/carriers.py](/home/chrisi/Lagerverwaltung/shipping/carriers.py).

Per-carrier fields include:

- `code`
- `label`
- `short_label`
- `default_format`
- `shopify_allowed`
- `option_mode`
- related settings fields for printer, format, tracking mode, tracking URL and optional template
- additional settings fields for carrier-specific credentials

## Shipping history and Shopify queue

Shared data handling is implemented in [shipping/history.py](/home/chrisi/Lagerverwaltung/shipping/history.py).

It covers:

- schema and migration for `shipping_labels`
- schema and migration for `shopify_fulfillment_jobs`
- loading, writing and updating shipping labels
- creating and updating Shopify fulfillment jobs
- upserting shipments imported from Shopify

This logic is used by both the TUI and the Shopify sync process.

## Runtime integration in the TUI

The TUI registers carrier runtimes in [lager_mc.py](/home/chrisi/Lagerverwaltung/lager_mc.py).

Current runtime hooks:

- `create_label`
- `reprint_label`
- `cancel_label`

At minimum, a new carrier needs `create_label`.

`reprint_label` and `cancel_label` are optional.

## Carrier-specific option selection

Carrier-specific selection is controlled through `option_mode`.

Current modes:

- `gls_services`
- `post_products`
- `None`

New carriers can define their own mode or work without an extra selection dialog.

## Shopify handover

Carriers with `shopify_allowed=True` can be handed over to Shopify.

The Shopify handover uses:

- tracking number
- carrier name
- optional tracking URL

Carrier-specific defaults for tracking mode and tracking URL are controlled through the carrier definition and settings.

## Required steps for a new carrier

1. add a carrier definition in [shipping/carriers.py](/home/chrisi/Lagerverwaltung/shipping/carriers.py)
2. add required default settings in [app_settings.py](/home/chrisi/Lagerverwaltung/app_settings.py)
3. implement carrier behavior in [lager_mc.py](/home/chrisi/Lagerverwaltung/lager_mc.py)
4. register the runtime spec in `SHIPPING_CARRIER_RUNTIME_SPECS`
5. if Shopify support is required:
   - map tracking company and tracking URL correctly
6. add tests in:
   - [tests/test_release_suite.py](/home/chrisi/Lagerverwaltung/tests/test_release_suite.py)
   - [tests/test_shopify_sync_logging.py](/home/chrisi/Lagerverwaltung/tests/test_shopify_sync_logging.py)

## Current interfaces

### GLS

- label creation
- reprint
- cancellation
- Shopify tracking via carrier name

### Deutsche Post INTERNETMARKE

- product selection via imported product price list
- INTERNETMARKE purchase
- PDF output
- Shopify tracking with configurable tracking URL

### Address label

- local PDF
- no external API
- no Shopify handover

## Scope boundary

This document describes the available technical integration points.

User workflow and operation are documented in [docs/bedienungsanleitung.en.md](/home/chrisi/Lagerverwaltung/docs/bedienungsanleitung.en.md).
