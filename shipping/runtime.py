"""Carrier-Runtime-Registry fuer Versandmodule."""

from shipping.carriers import ShippingCarrierRuntime, normalize_carrier_code

from . import free, gls, post, testcarrier


_RUNTIMES = {
    "gls": ShippingCarrierRuntime(
        create_label=gls.create_label,
        reprint_label=gls.reprint_label,
        cancel_label=gls.cancel_label,
    ),
    "post": ShippingCarrierRuntime(create_label=post.create_label),
    "free": ShippingCarrierRuntime(create_label=free.create_label),
    "test": ShippingCarrierRuntime(create_label=testcarrier.create_label),
}

_MODULES = {
    "gls": gls,
    "post": post,
    "free": free,
    "test": testcarrier,
}


def carrier_runtime(carrier):
    return _RUNTIMES.get(normalize_carrier_code(carrier))


def carrier_module(carrier):
    return _MODULES.get(normalize_carrier_code(carrier))
