from dataclasses import dataclass, field


@dataclass
class OrdersDialogState:
    order_filter: str | None = None
    only_pending: bool = False
    fulfillment_filter: str = "all"
    payment_filter: str = "all"
    selected: int = 0
    top_index: int = 0
    selected_order_ids: set = field(default_factory=set)
    reload_orders_snapshot: bool = True
    orders_snapshot_reload_pending: bool = False
    rebuild_orders_view: bool = True

    def reset_view(self):
        self.selected = 0
        self.top_index = 0
        self.rebuild_orders_view = True

    def clamp_selection(self, row_count):
        if self.selected >= row_count:
            self.selected = row_count - 1
        if self.selected < 0:
            self.selected = 0

    def selected_order(self, orders):
        return orders[self.selected] if orders else None

    def selected_order_id(self, orders):
        order = self.selected_order(orders)
        return order["order_id"] if order else None

    def trim_selected_order_ids(self, orders):
        visible_ids = {row["order_id"] for row in orders}
        self.selected_order_ids = {order_id for order_id in self.selected_order_ids if order_id in visible_ids}

    def toggle_selected_order(self, order):
        order_id = order["order_id"]
        if order_id in self.selected_order_ids:
            self.selected_order_ids.remove(order_id)
        else:
            self.selected_order_ids.add(order_id)

    def toggle_all_orders(self, orders):
        if not orders:
            return
        if len(self.selected_order_ids) == len(orders):
            self.selected_order_ids.clear()
        else:
            self.selected_order_ids = {row["order_id"] for row in orders}


def order_open_hint(order):
    location_count = int(order.get("shopify_location_count") or 0)
    if location_count > 0:
        return "[!]" if int(order.get("active_location_remaining_qty") or 0) > 0 else "   "
    status_value = (order.get("fulfillment_status") or "").strip().lower()
    return "[!]" if status_value not in {"fulfilled", "cancelled"} else "   "


def build_order_list_lines(orders, selected_order_ids, left_width, fit_text, format_address):
    lines = []
    for order in orders:
        mark = "[x]" if order["order_id"] in selected_order_ids else "[ ]"
        lines.append(
            f"{mark}{order_open_hint(order)} {fit_text(order['order_name'], 10)} "
            f"{fit_text(format_address(order), left_width - 19)}"
        )
    return lines


def build_orders_footer(
    base_footer,
    order_filter=None,
    only_pending=False,
    fulfillment_filter="all",
    payment_filter="all",
    translate=None,
    fulfillment_filter_label=None,
    payment_filter_label=None,
):
    filter_tags = []
    if order_filter:
        filter_tags.append(translate("orders_filter_text", value=order_filter))
    if only_pending:
        filter_tags.append(translate("orders_filter_only_open"))
    if fulfillment_filter != "all":
        filter_tags.append(fulfillment_filter_label(fulfillment_filter).replace("Status: ", ""))
    if payment_filter != "all":
        filter_tags.append(payment_filter_label(payment_filter).replace("Zahlung: ", ""))
    if filter_tags:
        return f" Filter[{', '.join(filter_tags)}] " + base_footer
    return base_footer


def build_order_detail_lines(
    selected_order,
    selected_order_id,
    order_items,
    order_items_loaded,
    order_shipments,
    order_shipments_loaded,
    right_width,
    active_location_label,
    translate,
    fit_text,
    format_address,
    calculate_order_shipping_weight,
    localized_country_display,
    localized_fulfillment_status,
    localized_payment_status,
    shipment_summary_lines,
    format_order_item_header,
    format_order_item_row,
    format_datetime,
):
    detail_lines = []
    if not selected_order:
        detail_lines.append(translate("order_not_found"))
        return detail_lines

    selected_weight_kg, selected_weight_grams = calculate_order_shipping_weight(selected_order, order_items)
    country = localized_country_display(selected_order.get("shipping_country"))
    created_at = selected_order.get("created_at")
    ordered_at_text = format_datetime(created_at)
    detail_lines.append(fit_text(translate("orders_detail_order", value=selected_order["order_name"]), right_width - 2))
    detail_lines.append(fit_text(format_address(selected_order), right_width - 2))
    detail_lines.append(fit_text(translate("orders_detail_country", value=country), right_width - 2))
    detail_lines.append(fit_text(translate("orders_detail_email", value=selected_order.get("shipping_email") or "-"), right_width - 2))
    detail_lines.append(fit_text(translate("orders_detail_phone", value=selected_order.get("shipping_phone") or "-"), right_width - 2))
    detail_lines.append(fit_text(translate("orders_detail_ordered_at", value=ordered_at_text), right_width - 2))
    status = localized_fulfillment_status(selected_order["fulfillment_status"])
    payment_status = localized_payment_status(selected_order["payment_status"])
    internal_qty = selected_order.get("local_internal_qty") or 0
    active_location_qty = int(selected_order.get("active_location_internal_qty") or 0)
    active_location_remaining_qty = int(selected_order.get("active_location_remaining_qty") or 0)
    shopify_location_count = int(selected_order.get("shopify_location_count") or 0)
    detail_lines.append(fit_text(translate("orders_detail_status", value=status), right_width - 2))
    detail_lines.append(fit_text(translate("orders_detail_payment", value=payment_status), right_width - 2))
    detail_lines.append(fit_text(translate("orders_detail_internal_qty", value=internal_qty), right_width - 2))
    detail_lines.append(fit_text(translate("orders_detail_shopify_location", value=active_location_label), right_width - 2))
    detail_lines.append(fit_text(translate("orders_detail_location_qty", value=active_location_qty), right_width - 2))
    detail_lines.append(fit_text(translate("orders_detail_location_open_qty", value=active_location_remaining_qty), right_width - 2))
    if shopify_location_count > 1:
        detail_lines.append(fit_text(translate("orders_detail_location_split", value=shopify_location_count), right_width - 2))
    detail_lines.append(
        fit_text(
            translate("orders_detail_shipping_weight", grams=selected_weight_grams, kg=selected_weight_kg),
            right_width - 2,
        )
    )
    if selected_order_id and not order_shipments_loaded:
        detail_lines.append(fit_text(translate("orders_detail_shipments_loading"), right_width - 2))
    else:
        detail_lines.extend(shipment_summary_lines(order_shipments, right_width - 13))
    detail_lines.append("")
    qty_width, sku_width, regal_width, fach_width, platz_width, title_width = format_order_item_header(right_width - 2)
    detail_lines.append(
        translate(
            "orders_detail_items_header",
            qty=fit_text("Off/Ges", qty_width),
            sku=fit_text("SKU", sku_width),
            item=fit_text(translate("items_panel"), title_width),
            regal=fit_text(translate("field_regal_short"), regal_width),
            fach=fit_text(translate("field_fach_short"), fach_width),
            platz=fit_text(translate("field_platz_short"), platz_width),
        )
    )
    detail_lines.append("-" * max(1, right_width - 2))

    if selected_order_id and not order_items_loaded:
        detail_lines.append(fit_text(translate("orders_detail_positions_loading"), right_width - 2))
        return detail_lines

    if not order_items and shopify_location_count > 0:
        detail_lines.append(fit_text(translate("orders_detail_no_location_positions"), right_width - 2))
    for row in order_items:
        detail_lines.append(format_order_item_row(row, right_width - 2))
    return detail_lines
