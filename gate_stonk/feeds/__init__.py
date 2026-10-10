"""Market data feeds: venue adapters, WebSocket workers and the FeedHub.

``build_hub(cfg)`` wires the configured index sources, the Gate spot traded book
and (when an enabled source quotes USDC) the FX stream into one ``FeedHub``.
Venue adapter modules are imported lazily through ``VENUE_ADAPTERS`` so that
importing ``gate_stonk.feeds.base`` (or the hub) never pulls in every parser.
"""
from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

from ..errors import ConfigError

if TYPE_CHECKING:
    from .hub import FeedHub

# venue (config ``index.sources[].venue``) -> (module in gate_stonk.feeds, class name).
# Every class is constructed as ``cls(source: SourceConfig, *, timeout: float)``.
VENUE_ADAPTERS: dict[str, tuple[str, str]] = {
    'bybit_linear': ('bybit', 'BybitAdapter'),
    'bitget_usdt_futures': ('bitget', 'BitgetAdapter'),
    'bingx_swap': ('bingx', 'BingxAdapter'),
    'lighter': ('lighter', 'LighterAdapter'),
    'gate_futures_usdt': ('gate_futures', 'GateFuturesAdapter'),
}


def adapter_class(venue: str) -> type:
    """Import the adapter class for ``venue`` on demand."""
    try:
        module_name, class_name = VENUE_ADAPTERS[venue]
    except KeyError:
        raise ConfigError(f'index.sources[].venue: unsupported venue {venue!r}') from None
    module = importlib.import_module(f'{__name__}.{module_name}')
    return getattr(module, class_name)


def build_source_adapter(source: Any, *, timeout: float) -> Any:
    return adapter_class(source.venue)(source, timeout=timeout)


def build_adapters(cfg: Any) -> list[Any]:
    """Adapters for every enabled index source, the traded book and live FX."""
    from .fx import build_fx_adapter, fx_needed
    from .gate_spot import GateSpotAdapter

    timeout = float(cfg.app['request_timeout_seconds'])
    settings = cfg.index_settings()
    adapters = [build_source_adapter(source, timeout=timeout)
                for source in settings.sources if getattr(source, 'enabled', True)]
    adapters.append(GateSpotAdapter(cfg, timeout=timeout))
    if fx_needed(settings.sources) and settings.fx is not None:
        market = cfg.market
        fx = build_fx_adapter(settings.fx, timeout=timeout, ws_url=market.get('spot_ws_url'),
                              rest_url=market.get('rest_url'))
        if fx is not None:
            adapters.append(fx)
    return adapters


def build_hub(cfg: Any, *, start_threads: bool = True, connect: Any = None, http: Any = None) -> FeedHub:
    """FeedHub with adapters for the enabled configuration (threads start on ``hub.start()``)."""
    from .hub import FeedHub

    return FeedHub(cfg, adapters=build_adapters(cfg), start_threads=start_threads, connect=connect, http=http)


__all__ = ['VENUE_ADAPTERS', 'adapter_class', 'build_adapters', 'build_hub', 'build_source_adapter']
