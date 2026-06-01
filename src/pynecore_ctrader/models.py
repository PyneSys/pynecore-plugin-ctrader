"""Internal data models for the cTrader broker layer.

Sits at the bottom of the dependency graph: depended on by the execution /
state mix-ins but imports from none of them. Holds the per-symbol order-sizing
and precision rules cache row sourced from ``ProtoOASymbol``.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class _SymbolRules:
    """Cached order-sizing + precision rules for one cTrader symbol.

    Sourced from the full ``ProtoOASymbol`` detail record. The three volume
    fields are INT64 centi-units (protocol ``volume`` of 1000 == 10.00 traded
    units), used by :func:`~pynecore_ctrader.helpers.quantize_volume` and the
    min/max acceptance gate in ``execute_entry``. ``digits`` is the price
    precision the absolute order prices (limit / stop / SL / TP) are rounded
    to before they go on the wire.

    The rules are effectively static during a trading session, so the cache
    is populated lazily on first order for the symbol and not time-expired in
    M2 — a future refresh hook can be added if a venue rotates them intraday.

    :ivar symbol_id: The numeric ``symbolId`` the order messages reference.
    :ivar digits: Price precision (``ProtoOASymbol.digits``).
    :ivar min_volume: Smallest accepted ``volume`` (centi-units).
    :ivar step_volume: ``volume`` granularity (centi-units).
    :ivar max_volume: Largest accepted ``volume`` (centi-units); ``0`` when the
        venue quotes no ceiling.
    """
    symbol_id: int
    digits: int
    min_volume: int
    step_volume: int
    max_volume: int
