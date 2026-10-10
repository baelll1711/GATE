"""Config loading/validation (DESIGN §3; reference validator style, notes/ref_config_cli_ops.md §1).

All loads use ``read_env=False`` except the tests that pin the ``.env`` behaviour, and
every config file is written to ``tmp_path`` so ``.env``/state never touch the repo.
"""
from __future__ import annotations

import copy
import json
import os
import re
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from gate_stonk.config import (
    Config,
    FxSettings,
    IndexSettings,
    LevelConfig,
    Secrets,
    SourceConfig,
    env_file_has_live_confirm,
    load_config,
)
from gate_stonk.errors import ConfigError

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / 'config.example.yaml'


# ----------------------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------------------
@pytest.fixture
def sample() -> dict:
    return yaml.safe_load(EXAMPLE.read_text(encoding='utf-8'))


def write(tmp_path: Path, sample: dict, name: str = 'config.yaml') -> Path:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(sample, sort_keys=False, allow_unicode=True), encoding='utf-8')
    return path


def load_sample(tmp_path: Path, sample: dict) -> Config:
    return load_config(write(tmp_path, sample), read_env=False)


_PART = re.compile(r'([A-Za-z0-9_]+)((?:\[\d+\])*)')


def _walk(data, dotted: str):
    """Return (container, last_key) for a dotted path like ``pair.buy.levels[0].edge_pct``."""
    steps: list = []
    for part in dotted.split('.'):
        m = _PART.fullmatch(part)
        assert m, part
        steps.append(m.group(1))
        steps.extend(int(i) for i in re.findall(r'\[(\d+)\]', m.group(2)))
    node = data
    for step in steps[:-1]:
        node = node[step]
    return node, steps[-1]


def put(data: dict, dotted: str, value) -> None:
    node, key = _walk(data, dotted)
    node[key] = value


def drop(data: dict, dotted: str) -> None:
    node, key = _walk(data, dotted)
    del node[key]


def expect_error(tmp_path: Path, sample: dict, path: str, fragment: str | None = None) -> str:
    """The load fails with a ConfigError whose message starts with the dotted path."""
    with pytest.raises(ConfigError) as info:
        load_sample(tmp_path, sample)
    message = str(info.value)
    assert message.startswith(f'{path}:'), message
    if fragment is not None:
        assert fragment in message, message
    return message


def track_env(monkeypatch, *names: str) -> None:
    """Make monkeypatch restore (delete) variables that the code under test may set."""
    for name in names:
        monkeypatch.setenv(name, 'placeholder')
        monkeypatch.delenv(name)


# ----------------------------------------------------------------------------------------
# the canonical example
# ----------------------------------------------------------------------------------------
def test_example_loads_with_expected_levels(tmp_path, sample):
    cfg = load_sample(tmp_path, sample)
    assert cfg.mode == 'paper' and cfg.live is False
    buy, sell = cfg.levels('buy'), cfg.levels('sell')
    assert [lvl.id for lvl in buy] == ['b1', 'b2'] and [lvl.id for lvl in sell] == ['s1', 's2']
    assert all(isinstance(lvl, LevelConfig) for lvl in buy + sell)
    b1, b2 = buy
    assert (b1.side, b1.mode, b1.edge_pct, b1.min_edge_pct, b1.max_edge_pct) == ('buy', 'exact', Decimal('1.0'),
                                                                                 None, None)
    assert b1.order_notional_usdt == Decimal('20') and b1.order_qty is None
    assert b1.iceberg_display_qty is None and b1.iceberg_display_pct is None and not b1.has_iceberg
    assert b2.edge_pct == Decimal('2.0') and b2.order_notional_usdt == Decimal('30')
    assert b2.iceberg_display_pct == Decimal('25') and b2.has_iceberg
    assert b1.slot == 'buy:b1' and sell[1].slot == 'sell:s2'
    for lvl in buy + sell:   # side defaults inherited
        assert lvl.band_tolerance_pct == Decimal('0.15')
        assert lvl.reprice_threshold_pct == Decimal('0.15')
        assert lvl.min_reprice_seconds == Decimal('3') and isinstance(lvl.min_reprice_seconds, Decimal)
        assert lvl.deeper_tolerance_pct is None
        assert lvl.skip_notional_usdt == 0
    assert [lvl.edge_pct for lvl in sell] == [Decimal('1.0'), Decimal('2.0')]
    assert cfg.all_levels() == buy + sell
    assert cfg.level('sell', 's2') == sell[1]
    with pytest.raises(KeyError):
        cfg.level('sell', 'b1')


def test_example_sources_and_index_settings(tmp_path, sample):
    cfg = load_sample(tmp_path, sample)
    sources = cfg.sources()
    assert all(isinstance(s, SourceConfig) for s in sources)
    assert [s.id for s in sources] == ['bybit_perp', 'bitget_perp', 'bingx_perp', 'gate_perp', 'lighter_perp']
    by_id = {s.id: s for s in sources}
    bybit = by_id['bybit_perp']
    assert (bybit.venue, bybit.symbol, bybit.quote, bybit.price) == ('bybit_linear', 'STONKUSDT', 'USDT', 'mid')
    assert bybit.weight == Decimal('0.4') and bybit.required is True
    assert bybit.max_spread_pct == Decimal('0.5') and bybit.heartbeat_seconds == 3.0
    assert bybit.rest_refresh_after_seconds is None
    assert by_id['bingx_perp'].symbol == 'STONK-USDT'
    assert by_id['bitget_perp'].rest_refresh_after_seconds == 1.5
    assert by_id['lighter_perp'].quote == 'USDC' and by_id['lighter_perp'].market_index is None
    for s in sources:
        assert s.max_age_seconds == 3.0 and isinstance(s.max_age_seconds, float)
        assert s.enabled and s.url is None
        if s.venue != 'bybit_linear':
            assert s.heartbeat_seconds == 0.0 and not s.required
    settings = cfg.index_settings()
    assert isinstance(settings, IndexSettings) and isinstance(settings.fx, FxSettings)
    assert settings.method == 'weighted_median' and settings.min_sources == 2
    assert settings.max_age_seconds == 3.0 and settings.max_timestamp_skew_seconds == 2.0
    assert settings.max_source_deviation_pct == Decimal('1.0') and settings.basis_offset_pct == 0
    assert isinstance(settings.basis_offset_pct, Decimal)
    assert settings.rejoin_snapshots == 3 and settings.observation_interval_seconds == 0.2
    assert settings.fx == FxSettings(mode='fixed', usdc_to_usdt=Decimal('1'), max_depeg_pct=Decimal('0.5'),
                                     max_age_seconds=3.0, max_spread_pct=Decimal('0.2'))
    assert settings.sources == sources


def test_example_sections_identity_paths_and_types(tmp_path, sample):
    cfg = load_sample(tmp_path, sample)
    assert cfg.identity() == {'campaign_id': 'stonk_gate_test', 'currency_pair': 'STONK_USDT', 'mode': 'paper',
                              'client_prefix': 'stk1', 'account': 'spot'}
    assert cfg.path == (tmp_path / 'config.yaml').resolve() and cfg.base_dir == tmp_path.resolve()
    assert cfg.state_dir == tmp_path.resolve() / 'runtime_stonk'
    assert cfg.stop_trading_path() == tmp_path.resolve() / 'STOP_TRADING'
    assert cfg.stop_side_path('buy') == tmp_path.resolve() / 'STOP_BUY'
    assert cfg.stop_side_path('sell') == tmp_path.resolve() / 'STOP_SELL'
    # numbers: Decimal for money/pct/qty, float for seconds, int for counts
    assert cfg.app['loop_seconds'] == 0.2 and isinstance(cfg.app['loop_seconds'], float)
    assert cfg.app['request_expiry_seconds'] == 10.0 and isinstance(cfg.app['poll_seconds'], float)
    assert cfg.safety['dms_timeout_seconds'] == 45 and type(cfg.safety['dms_timeout_seconds']) is int
    assert cfg.safety['dms_refresh_seconds'] == 15.0
    assert cfg.safety['min_new_edge_pct'] == Decimal('1.0') and cfg.safety['hard_min_edge_pct'] == Decimal('0.8')
    assert cfg.safety['rate_limit_utilization'] == Decimal('0.5')
    assert type(cfg.safety['max_open_orders']) is int and cfg.safety['max_signed_actions_per_hour'] == 1000
    assert cfg.market['depth_levels'] == 20 and cfg.market['depth_interval'] == '100ms'
    assert cfg.market['account'] == 'spot' and cfg.market['expected_precision'] is None
    assert cfg.execution == {'time_in_force': 'poc', 'use_batch': True, 'reprice_via_amend': False,
                             'iceberg_poc_verified': False}
    assert cfg.portfolio['fee_estimate_pct'] == Decimal('0.1') and cfg.portfolio['fee_reserve_pct'] == Decimal('0.3')
    assert cfg.pair['max_inventory_usdt'] == Decimal('200') and cfg.pair['max_inventory_qty'] is None
    assert cfg.paper['initial_usdt'] == Decimal('500') and cfg.paper['fee_pct'] is None
    assert cfg.paper['fill_model'] == 'trade_through' and cfg.paper['fill_on_touch'] is False
    assert cfg.telegram['enabled'] is False and cfg.telegram['max_unhealthy_seconds'] == 120.0
    assert cfg.telegram['summary_minutes'] == 60 and cfg.telegram['notify_orders'] is False
    assert cfg.index['fx']['mode'] == 'fixed'


def test_side_settings_include_limits_and_cross_side_keys(tmp_path, sample):
    cfg = load_sample(tmp_path, sample)
    buy, sell = cfg.side('buy'), cfg.side('sell')
    assert buy['max_daily_notional_usdt'] == Decimal('200') and sell['max_daily_notional_usdt'] is None
    assert buy['max_total_qty'] is None and buy['max_total_notional_usdt'] is None
    assert buy['band_tolerance_pct'] == Decimal('0.15') and buy['min_reprice_seconds'] == Decimal('3')
    assert buy['compensate_base_fee'] is False and buy['min_profit_pct'] is None
    assert sell['min_profit_pct'] is None and sell['profit_floor_mode'] == 'hold'
    assert sell['compensate_base_fee'] is False and buy['profit_floor_mode'] == 'hold'
    assert buy['enabled'] and sell['enabled'] and 'levels' not in buy
    with pytest.raises(ValueError):
        cfg.side('BUY')
    with pytest.raises(ValueError):
        cfg.levels('long')
    with pytest.raises(ValueError):
        cfg.stop_side_path('both')


def test_repo_example_file_loads_in_place():
    cfg = load_config(EXAMPLE, read_env=False)
    assert cfg.path == EXAMPLE.resolve()
    raw = yaml.safe_load(EXAMPLE.read_text(encoding='utf-8'))

    def keys_kept(raw_node, data_node, path):
        if isinstance(raw_node, dict):
            for key, value in raw_node.items():
                assert key in data_node, f'{path}.{key} dropped'
                keys_kept(value, data_node[key], f'{path}.{key}')
        elif isinstance(raw_node, list):
            for i, item in enumerate(raw_node):
                keys_kept(item, data_node[i], f'{path}[{i}]')

    keys_kept(raw, cfg.data, 'config')   # every canonical key is accepted and kept


def test_min_sell_profit_and_numeric_strings(tmp_path, sample):
    put(sample, 'pair.sell.min_profit_pct', '0.5')
    put(sample, 'pair.buy.levels[1].edge_pct', '2.75')
    put(sample, 'index.sources[0].weight', '.4')
    cfg = load_sample(tmp_path, sample)
    assert cfg.side('sell')['min_profit_pct'] == Decimal('0.5')
    assert cfg.levels('buy')[1].edge_pct == Decimal('2.75')
    assert cfg.sources()[0].weight == Decimal('0.4')


# ----------------------------------------------------------------------------------------
# defaults and normalization
# ----------------------------------------------------------------------------------------
def _strip_optional(sample: dict) -> dict:
    s = copy.deepcopy(sample)
    for section in ('execution', 'portfolio', 'paper', 'telegram'):
        del s[section]
    s['app'] = {k: s['app'][k] for k in ('mode', 'campaign_id', 'client_prefix')}
    s['market'] = {k: s['market'][k] for k in ('currency_pair', 'base', 'quote', 'account')}
    s['index'] = {'sources': s['index']['sources'], 'fx': {'mode': s['index']['fx']['mode']}}
    for source in s['index']['sources']:
        for key in ('required', 'enabled', 'heartbeat_seconds', 'rest_refresh_after_seconds'):
            default = {'required': False, 'enabled': True, 'rest_refresh_after_seconds': None,
                       'heartbeat_seconds': 3 if source['venue'] == 'bybit_linear' else 0}[key]
            if source.get(key, default) == default:
                source.pop(key, None)
    s['safety'] = {k: s['safety'][k] for k in ('min_new_edge_pct', 'hard_min_edge_pct')}
    for key in ('inventory_reserve_qty', 'initial_avg_price'):
        del s['pair'][key]
    for side in ('buy', 'sell'):
        for key in ('band_tolerance_pct', 'reprice_threshold_pct', 'min_reprice_seconds', 'deeper_tolerance_pct',
                    'compensate_base_fee', 'min_profit_pct', 'profit_floor_mode'):
            s['pair'][side].pop(key, None)
        for level in s['pair'][side]['levels']:
            for key in ('iceberg_display_qty', 'iceberg_display_pct'):
                if level.get(key, 'x') is None:
                    del level[key]
    return s


def test_missing_optional_keys_get_the_example_defaults(tmp_path, sample):
    full = load_sample(tmp_path, sample)
    minimal_raw = _strip_optional(sample)
    assert 'telegram' not in minimal_raw and 'state_dir' not in minimal_raw['app']
    minimal = Config(minimal_raw, tmp_path / 'minimal.yaml')
    assert minimal.data == full.data
    assert minimal.levels('buy') == full.levels('buy') and minimal.index_settings() == full.index_settings()


def test_revalidating_normalized_data_is_idempotent_and_input_untouched(tmp_path, sample):
    cfg = load_sample(tmp_path, sample)
    again = Config(copy.deepcopy(cfg.data), cfg.path)
    assert again.data == cfg.data
    raw = copy.deepcopy(sample)
    Config(raw, tmp_path)
    assert raw == sample                      # validation never mutates its input
    assert cfg.data is not again.data and cfg.data['pair'] is not again.data['pair']


def test_config_accepts_a_directory_as_base_dir(tmp_path, sample):
    cfg = Config(sample, tmp_path)
    assert cfg.base_dir == tmp_path.resolve()
    assert cfg.state_dir == tmp_path.resolve() / 'runtime_stonk'
    assert cfg.stop_trading_path() == tmp_path.resolve() / 'STOP_TRADING'


def test_absolute_state_dir_is_kept(tmp_path, sample):
    put(sample, 'app.state_dir', str(tmp_path / 'elsewhere'))
    assert load_sample(tmp_path, sample).state_dir == tmp_path / 'elsewhere'


def test_null_and_zero_caps_remain_distinct(tmp_path, sample):
    put(sample, 'pair.buy.max_total_qty', 0)
    put(sample, 'pair.max_inventory_usdt', 0)
    cfg = load_sample(tmp_path, sample)
    assert cfg.side('buy')['max_total_qty'] == 0 and isinstance(cfg.side('buy')['max_total_qty'], Decimal)
    assert cfg.side('sell')['max_total_qty'] is None
    assert cfg.pair['max_inventory_usdt'] == 0 and cfg.pair['max_inventory_qty'] is None


def test_live_mode_loads_and_is_reported(tmp_path, sample):
    put(sample, 'app.mode', 'live')
    cfg = load_sample(tmp_path, sample)
    assert cfg.live is True and cfg.identity()['mode'] == 'live'


# ----------------------------------------------------------------------------------------
# unknown / missing keys
# ----------------------------------------------------------------------------------------
@pytest.mark.parametrize('path', [
    'bogus',
    'app.modee',
    'market.pair',
    'index.max_age',
    'index.fx.mod',
    'index.sources[1].wieght',
    'safety.dms_timeout',
    'execution.post_only',
    'portfolio.cash',
    'pair.max_inventory',
    'pair.buy.max_profitt',
    'pair.sell.levels[1].edge',
    'paper.fill_on_touchh',
    'telegram.chat_id',
])
def test_unknown_key_rejected_with_dotted_path(tmp_path, sample, path):
    put(sample, path, 1)
    expect_error(tmp_path, sample, path, 'unknown')


@pytest.mark.parametrize('path', [
    'app.mode', 'app.campaign_id', 'app.client_prefix',
    'market.currency_pair', 'market.account',
    'index.sources', 'index.fx.mode', 'index.sources[0].weight', 'index.sources[2].venue',
    'safety.min_new_edge_pct', 'safety.hard_min_edge_pct',
    'pair.mode', 'pair.max_inventory_usdt', 'pair.max_inventory_qty', 'pair.sell',
    'pair.buy.max_total_qty', 'pair.sell.max_daily_notional_usdt', 'pair.buy.levels',
    'pair.buy.levels[0].id',
])
def test_missing_required_key_rejected(tmp_path, sample, path):
    drop(sample, path)
    expect_error(tmp_path, sample, path, 'required key is missing')


@pytest.mark.parametrize('section', ['app', 'market', 'index', 'safety', 'pair'])
def test_missing_required_section_rejected(tmp_path, sample, section):
    del sample[section]
    expect_error(tmp_path, sample, section, 'required section is missing')


def test_null_section_rejected(tmp_path, sample):
    sample['telegram'] = None
    expect_error(tmp_path, sample, 'telegram', 'mapping')


# ----------------------------------------------------------------------------------------
# wrong types and ranges
# ----------------------------------------------------------------------------------------
NUMERIC_PATHS = ['app.poll_seconds', 'index.sources[1].weight', 'safety.min_new_edge_pct',
                 'pair.max_inventory_usdt', 'pair.buy.levels[0].edge_pct', 'pair.sell.levels[1].order_notional_usdt',
                 'paper.initial_usdt', 'portfolio.fee_estimate_pct']


@pytest.mark.parametrize('path', NUMERIC_PATHS)
@pytest.mark.parametrize('value', [True, False, float('nan'), float('inf'), float('-inf'), 'NaN', 'Infinity',
                                   '-Infinity', 'sNaN', 'abc', [1], {'a': 1}])
def test_non_finite_boolean_and_garbage_numbers_rejected(tmp_path, sample, path, value):
    put(sample, path, value)
    expect_error(tmp_path, sample, path)


@pytest.mark.parametrize('path,value', [
    ('app.mode', 'Live'),
    ('app.campaign_id', 'bad id'), ('app.campaign_id', 'x' * 33), ('app.campaign_id', 'a-b'), ('app.campaign_id', 7),
    ('app.client_prefix', 'STK1'), ('app.client_prefix', 'stonk12'), ('app.client_prefix', 'st-1'),
    ('app.client_prefix', ''),
    ('app.state_dir', ''), ('app.state_dir', None),
    ('app.loop_seconds', '0.201'), ('app.loop_seconds', 0),
    ('app.poll_seconds', '0.99'), ('app.poll_seconds', 61),
    ('app.request_timeout_seconds', 21), ('app.request_timeout_seconds', 0),
    ('app.request_expiry_seconds', 4), ('app.request_expiry_seconds', 61),
    ('app.read_error_grace_seconds', 0), ('app.status_interval_seconds', -1),
    ('app.log_level', 'info'),
    ('market.account', 'margin'), ('market.account', None),
    ('market.depth_levels', 15), ('market.depth_levels', '20'), ('market.depth_levels', 20.0),
    ('market.depth_interval', '500ms'), ('market.depth_interval', 100),
    ('market.quote', 'USDC'), ('market.currency_pair', 'STONK_USDC'), ('market.currency_pair', 'stonk_usdt'),
    ('market.base', 'stonk'),
    ('market.rest_url', 'http://api.gateio.ws/api/v4'), ('market.rest_url', 'https://'),
    ('market.spot_ws_url', 'ws://api.gateio.ws/ws/v4/'),
    ('market.expected_precision', 19), ('market.expected_amount_precision', -1),
    ('market.expected_precision', 2.0),
    ('index.method', 'median'),
    ('index.min_sources', 0), ('index.min_sources', 1.5), ('index.min_sources', True),
    ('index.rejoin_snapshots', 0), ('index.basis_offset_pct', 50), ('index.basis_offset_pct', -50),
    ('index.max_source_deviation_pct', 0), ('index.max_age_seconds', 0), ('index.max_timestamp_skew_seconds', -1),
    ('index.observation_interval_seconds', 0),
    ('index.fx.mode', 'binance'), ('index.fx.usdc_to_usdt', 0), ('index.fx.max_depeg_pct', 0),
    ('index.fx.max_spread_pct', 100), ('index.fx.max_age_seconds', 0),
    ('index.sources[0].id', 'Bybit'), ('index.sources[0].id', 'fx'), ('index.sources[0].id', 'gate_spot'),
    ('index.sources[0].id', '1bybit'),
    ('index.sources[0].venue', 'binance_futures'), ('index.sources[0].price', 'index'),
    ('index.sources[0].weight', 0), ('index.sources[0].weight', '-0.1'), ('index.sources[0].quote', 'BUSD'),
    ('index.sources[0].max_spread_pct', None), ('index.sources[0].max_spread_pct', 0),
    ('index.sources[0].max_spread_pct', 100),
    ('index.sources[0].heartbeat_seconds', -1), ('index.sources[0].heartbeat_seconds', 31),
    ('index.sources[0].max_age_seconds', 0),
    ('index.sources[0].rest_refresh_after_seconds', 0), ('index.sources[0].url', 'https://stream.bybit.com'),
    ('index.sources[0].market_index', 5), ('index.sources[0].required', 'yes'),
    ('index.sources[0].enabled', 1), ('index.sources[0].symbol', ''), ('index.sources[0].symbol', 'STONK USDT'),
    ('index.sources[4].market_index', -1), ('index.sources[4].market_index', '3'),
    ('safety.dedicated_account_ack', 'true'), ('safety.dead_man_switch', 0), ('safety.require_telegram', None),
    ('safety.min_new_edge_pct', 0), ('safety.min_new_edge_pct', 100), ('safety.hard_min_edge_pct', 0),
    ('safety.max_open_orders', 21), ('safety.max_open_orders', 0), ('safety.max_open_orders', '8'),
    ('safety.max_signed_actions_per_hour', 1500.5), ('safety.dms_timeout_seconds', 45.5),
    ('safety.dms_timeout_seconds', '45'), ('safety.dms_refresh_seconds', 0),
    ('safety.rate_limit_utilization', '1.5'), ('safety.rate_limit_utilization', 0),
    ('safety.terminal_settle_seconds', '0.5'), ('safety.max_order_requests_per_10s', 0),
    ('safety.max_order_requests_per_10s', 101), ('safety.max_clock_skew_ms', 0),
    ('safety.unknown_create_margin_seconds', 1), ('safety.clock_check_seconds', 0),
    ('execution.time_in_force', 'gtc'), ('execution.time_in_force', 'POC'), ('execution.use_batch', 1),
    ('execution.reprice_via_amend', 'false'), ('execution.iceberg_poc_verified', None),
    ('portfolio.cash_reserve_usdt', -1), ('portfolio.fee_estimate_pct', 1), ('portfolio.fee_reserve_pct', 100),
    ('portfolio.inventory_cap_tolerance_pct', 11),
    ('pair.mode', 'all'), ('pair.initial_avg_price', 0), ('pair.inventory_reserve_qty', -1),
    ('pair.max_inventory_qty', '-1'),
    ('pair.buy.band_tolerance_pct', 100), ('pair.sell.reprice_threshold_pct', False),
    ('pair.buy.deeper_tolerance_pct', 100), ('pair.buy.min_reprice_seconds', -1),
    ('pair.buy.compensate_base_fee', 'false'), ('pair.sell.min_profit_pct', -100),
    ('pair.sell.min_profit_pct', 1000), ('pair.sell.profit_floor_mode', 'sell'),
    ('pair.buy.max_total_notional_usdt', -5), ('pair.sell.max_daily_notional_usdt', True),
    ('pair.buy.levels', {'b1': 1}),
    ('pair.buy.levels[0]', 'b1'),
    ('pair.buy.levels[0].id', 'B1'), ('pair.buy.levels[0].id', 'abcde'), ('pair.buy.levels[0].id', 'b-1'),
    ('pair.buy.levels[0].id', 1),
    ('pair.buy.levels[0].band_tolerance_pct', None), ('pair.buy.levels[0].reprice_threshold_pct', 100),
    ('pair.buy.levels[0].min_reprice_seconds', -1), ('pair.buy.levels[0].deeper_tolerance_pct', 100),
    ('pair.buy.levels[0].order_qty', -1),
    ('paper.fill_model', 'none'), ('paper.fill_model', None), ('paper.fee_pct', 1), ('paper.initial_base', -1),
    ('paper.fill_on_touch', 1),
    ('telegram.enabled', 'yes'), ('telegram.max_unhealthy_seconds', 60), ('telegram.max_unhealthy_seconds', 0),
    ('telegram.summary_minutes', 0), ('telegram.summary_minutes', 1441), ('telegram.summary_minutes', 1.5),
    ('telegram.notify_orders', 'no'),
])
def test_invalid_values_rejected_with_dotted_path(tmp_path, sample, path, value):
    put(sample, path, value)
    expect_error(tmp_path, sample, path)


def test_optional_nullables_accept_null(tmp_path, sample):
    put(sample, 'telegram.summary_minutes', None)
    put(sample, 'pair.initial_avg_price', '0.25')
    put(sample, 'market.expected_precision', 4)
    put(sample, 'market.expected_amount_precision', 0)
    put(sample, 'paper.fee_pct', '0.2')
    put(sample, 'pair.sell.min_profit_pct', '-0.5')
    cfg = load_sample(tmp_path, sample)
    assert cfg.telegram['summary_minutes'] is None
    assert cfg.pair['initial_avg_price'] == Decimal('0.25')
    assert cfg.market['expected_precision'] == 4 and cfg.market['expected_amount_precision'] == 0
    assert cfg.paper['fee_pct'] == Decimal('0.2') and cfg.side('sell')['min_profit_pct'] == Decimal('-0.5')


# ----------------------------------------------------------------------------------------
# levels
# ----------------------------------------------------------------------------------------
def test_level_edge_must_reach_min_new_edge(tmp_path, sample):
    put(sample, 'pair.buy.levels[0].edge_pct', '0.9')
    message = expect_error(tmp_path, sample, 'pair.buy.levels[0].edge_pct')
    assert 'must be >= safety.min_new_edge_pct (1.0)' in message


def test_min_new_edge_is_checked_after_loading_safety(tmp_path, sample):
    put(sample, 'safety.min_new_edge_pct', '1.5')
    expect_error(tmp_path, sample, 'pair.buy.levels[0].edge_pct', 'safety.min_new_edge_pct (1.5)')
    put(sample, 'pair.buy.levels[0].edge_pct', '1.5')
    expect_error(tmp_path, sample, 'pair.sell.levels[0].edge_pct', 'safety.min_new_edge_pct (1.5)')
    put(sample, 'pair.sell.levels[0].edge_pct', '1.5')
    assert load_sample(tmp_path, sample).levels('sell')[0].edge_pct == Decimal('1.5')


def test_range_level_min_edge_must_reach_min_new_edge(tmp_path, sample):
    sample['pair']['buy']['levels'][0] = {'id': 'b1', 'min_edge_pct': 0.5, 'max_edge_pct': 1.5,
                                          'skip_notional_usdt': 0, 'order_notional_usdt': 20}
    expect_error(tmp_path, sample, 'pair.buy.levels[0].min_edge_pct', 'safety.min_new_edge_pct (1.0)')


@pytest.mark.parametrize('hard', ['1.0', '1.2', 0, '-0.1'])
def test_hard_min_edge_must_be_positive_and_below_min_new_edge(tmp_path, sample, hard):
    put(sample, 'safety.hard_min_edge_pct', hard)
    expect_error(tmp_path, sample, 'safety.hard_min_edge_pct')


def test_hard_min_edge_just_below_min_new_edge_is_valid(tmp_path, sample):
    put(sample, 'safety.hard_min_edge_pct', '0.99')
    assert load_sample(tmp_path, sample).safety['hard_min_edge_pct'] == Decimal('0.99')


@pytest.mark.parametrize('side,edges', [('buy', [1.0, 1.0]), ('buy', [2.0, 1.5]), ('sell', [1.5, 1.2]),
                                        ('sell', [3, 3])])
def test_exact_levels_must_be_strictly_increasing(tmp_path, sample, side, edges):
    for i, edge in enumerate(edges):
        put(sample, f'pair.{side}.levels[{i}].edge_pct', edge)
    expect_error(tmp_path, sample, f'pair.{side}.levels[1]', 'strictly monotone')


def _range(level_id, low, high, skip=0, **extra):
    return {'id': level_id, 'min_edge_pct': low, 'max_edge_pct': high, 'skip_notional_usdt': skip,
            'order_notional_usdt': 20, **extra}


def test_range_levels_are_built_with_defaults_and_options(tmp_path, sample):
    sample['pair']['buy']['levels'] = [
        _range('r1', 1.0, 2.0),
        _range('r2', 1.0, 3.0, '2500.5', out_of_range='clamp', insufficient_depth='wait'),
        _range('r3', 1.5, 3.0, 0, iceberg_display_qty=5, order_qty=50),
    ]
    cfg = load_sample(tmp_path, sample)
    r1, r2, r3 = cfg.levels('buy')
    assert (r1.mode, r1.edge_pct, r1.min_edge_pct, r1.max_edge_pct) == ('range', None, Decimal('1.0'),
                                                                        Decimal('2.0'))
    assert (r1.skip_notional_usdt, r1.out_of_range, r1.insufficient_depth) == (0, 'nearest_index', 'clamp')
    assert (r2.skip_notional_usdt, r2.out_of_range, r2.insufficient_depth) == (Decimal('2500.5'), 'clamp', 'wait')
    assert r3.iceberg_display_qty == Decimal('5') and r3.order_qty == Decimal('50') and r3.has_iceberg
    assert cfg.data['pair']['buy']['levels'][0]['out_of_range'] == 'nearest_index'
    assert 'edge_pct' not in cfg.data['pair']['buy']['levels'][0]


@pytest.mark.parametrize('first,second', [
    ((1.0, 2.0), (1.0, 2.0)),     # identical
    ((1.5, 3.0), (1.2, 3.5)),     # min decreases
    ((1.0, 3.0), (1.5, 2.5)),     # max decreases
])
def test_range_levels_must_be_monotone(tmp_path, sample, first, second):
    sample['pair']['sell']['levels'] = [_range('s1', *first), _range('s2', *second)]
    expect_error(tmp_path, sample, 'pair.sell.levels[1]', 'strictly monotone')


def test_mixed_exact_and_range_levels_follow_interval_monotonicity(tmp_path, sample):
    sample['pair']['buy']['levels'] = [{'id': 'b1', 'edge_pct': 1.0, 'order_qty': 10}, _range('b2', 1.0, 2.0)]
    cfg = load_sample(tmp_path, sample)
    assert [lvl.mode for lvl in cfg.levels('buy')] == ['exact', 'range']
    sample['pair']['buy']['levels'] = [{'id': 'b1', 'edge_pct': 2.0, 'order_qty': 10}, _range('b2', 1.5, 3.0)]
    expect_error(tmp_path, sample, 'pair.buy.levels[1]', 'strictly monotone')


@pytest.mark.parametrize('level,path,fragment', [
    ({'id': 'b1', 'min_edge_pct': 1.0, 'max_edge_pct': 2.0, 'order_qty': 1}, '.skip_notional_usdt',
     'required for a range level'),
    ({'id': 'b1', 'edge_pct': 1.0, 'skip_notional_usdt': 0, 'order_qty': 1}, '.skip_notional_usdt',
     'only for range levels'),
    ({'id': 'b1', 'edge_pct': 1.0, 'out_of_range': 'clamp', 'order_qty': 1}, '.out_of_range',
     'only for range levels'),
    ({'id': 'b1', 'edge_pct': 1.0, 'insufficient_depth': None, 'order_qty': 1}, '.insufficient_depth',
     'only for range levels'),
    ({'id': 'b1', 'min_edge_pct': 1.0, 'order_qty': 1}, '', 'needs both'),
    ({'id': 'b1', 'max_edge_pct': 2.0, 'edge_pct': None, 'order_qty': 1}, '', 'needs both'),
    ({'id': 'b1', 'edge_pct': 1.0, 'min_edge_pct': 1.0, 'max_edge_pct': 2.0, 'skip_notional_usdt': 0,
      'order_qty': 1}, '', 'exclusive'),
    ({'id': 'b1', 'order_qty': 1}, '', 'set edge_pct'),
    ({'id': 'b1', 'edge_pct': None, 'order_qty': 1}, '', 'set edge_pct'),
    ({'id': 'b1', 'min_edge_pct': 2.0, 'max_edge_pct': 2.0, 'skip_notional_usdt': 0, 'order_qty': 1},
     '.min_edge_pct', 'below max_edge_pct'),
    ({'id': 'b1', 'min_edge_pct': 1.0, 'max_edge_pct': 2.0, 'skip_notional_usdt': -1, 'order_qty': 1},
     '.skip_notional_usdt', None),
    ({'id': 'b1', 'min_edge_pct': 1.0, 'max_edge_pct': 2.0, 'skip_notional_usdt': 0, 'out_of_range': 'nearest',
      'order_qty': 1}, '.out_of_range', None),
    ({'id': 'b1', 'min_edge_pct': 1.0, 'max_edge_pct': 2.0, 'skip_notional_usdt': 0, 'insufficient_depth': 'ignore',
      'order_qty': 1}, '.insufficient_depth', None),
    ({'id': 'b1', 'edge_pct': 100, 'order_qty': 1}, '.edge_pct', 'nonpositive price'),
    ({'id': 'b1', 'min_edge_pct': 1.0, 'max_edge_pct': 100, 'skip_notional_usdt': 0, 'order_qty': 1},
     '.max_edge_pct', 'nonpositive price'),
])
def test_level_shape_rules(tmp_path, sample, level, path, fragment):
    sample['pair']['buy']['levels'] = [level]
    expect_error(tmp_path, sample, f'pair.buy.levels[0]{path}', fragment)


def test_sell_edges_above_100_are_valid_but_typo_guarded(tmp_path, sample):
    put(sample, 'pair.sell.levels[1].edge_pct', 150)
    assert load_sample(tmp_path, sample).levels('sell')[1].edge_pct == Decimal('150')
    put(sample, 'pair.sell.levels[1].edge_pct', 1000)
    expect_error(tmp_path, sample, 'pair.sell.levels[1].edge_pct', 'typo')


def test_order_size_requires_at_least_one_finite_limit(tmp_path, sample):
    put(sample, 'pair.buy.levels[0].order_notional_usdt', None)
    expect_error(tmp_path, sample, 'pair.buy.levels[0]', 'finite order size')
    drop(sample, 'pair.buy.levels[0].order_notional_usdt')
    expect_error(tmp_path, sample, 'pair.buy.levels[0]', 'finite order size')


@pytest.mark.parametrize('key', ['order_notional_usdt', 'order_qty'])
def test_zero_order_size_rejected(tmp_path, sample, key):
    put(sample, f'pair.sell.levels[0].{key}', 0)
    expect_error(tmp_path, sample, f'pair.sell.levels[0].{key}', '0 would never trade')


def test_qty_only_and_both_sizes_are_valid(tmp_path, sample):
    drop(sample, 'pair.buy.levels[0].order_notional_usdt')
    put(sample, 'pair.buy.levels[0].order_qty', '12.5')
    put(sample, 'pair.sell.levels[0].order_qty', 40)
    cfg = load_sample(tmp_path, sample)
    b1, s1 = cfg.levels('buy')[0], cfg.levels('sell')[0]
    assert b1.order_notional_usdt is None and b1.order_qty == Decimal('12.5')
    assert s1.order_notional_usdt == Decimal('20') and s1.order_qty == Decimal('40')


def test_iceberg_qty_and_pct_are_mutually_exclusive(tmp_path, sample):
    put(sample, 'pair.buy.levels[1].iceberg_display_qty', 5)
    expect_error(tmp_path, sample, 'pair.buy.levels[1]', 'mutually exclusive')


@pytest.mark.parametrize('key,value', [('iceberg_display_pct', 0), ('iceberg_display_pct', 100),
                                       ('iceberg_display_pct', 150), ('iceberg_display_pct', -1),
                                       ('iceberg_display_pct', True), ('iceberg_display_qty', 0),
                                       ('iceberg_display_qty', '-2'), ('iceberg_display_qty', 'NaN')])
def test_iceberg_values_out_of_range_rejected(tmp_path, sample, key, value):
    sample['pair']['sell']['levels'][1] = {'id': 's2', 'edge_pct': 2.0, 'order_notional_usdt': 30, key: value}
    expect_error(tmp_path, sample, f'pair.sell.levels[1].{key}')


def test_iceberg_display_qty_must_be_below_order_qty(tmp_path, sample):
    sample['pair']['buy']['levels'][1] = {'id': 'b2', 'edge_pct': 2.0, 'order_qty': 10, 'iceberg_display_qty': 10}
    expect_error(tmp_path, sample, 'pair.buy.levels[1].iceberg_display_qty', 'below order_qty')
    sample['pair']['buy']['levels'][1]['iceberg_display_qty'] = '2.5'
    level = load_sample(tmp_path, sample).levels('buy')[1]
    assert level.iceberg_display_qty == Decimal('2.5') and level.iceberg_display_pct is None


def test_level_overrides_and_inheritance(tmp_path, sample):
    put(sample, 'pair.buy.deeper_tolerance_pct', '0.4')
    put(sample, 'pair.buy.levels[0].deeper_tolerance_pct', None)     # explicit null disables
    sample['pair']['buy']['levels'][1].update(band_tolerance_pct='0.3', reprice_threshold_pct='0.2',
                                              min_reprice_seconds=10)
    put(sample, 'pair.sell.min_reprice_seconds', '1.5')
    cfg = load_sample(tmp_path, sample)
    b1, b2 = cfg.levels('buy')
    assert b1.deeper_tolerance_pct is None and b2.deeper_tolerance_pct == Decimal('0.4')
    assert b1.band_tolerance_pct == Decimal('0.15') and b2.band_tolerance_pct == Decimal('0.3')
    assert b2.reprice_threshold_pct == Decimal('0.2') and b2.min_reprice_seconds == Decimal('10')
    assert all(lvl.min_reprice_seconds == Decimal('1.5') for lvl in cfg.levels('sell'))
    # the normalized data keeps the null override, so re-validation gives the same levels
    assert Config(copy.deepcopy(cfg.data), cfg.path).levels('buy') == (b1, b2)


def test_level_ids_unique_across_sides(tmp_path, sample):
    put(sample, 'pair.sell.levels[0].id', 'b1')
    expect_error(tmp_path, sample, 'pair.sell.levels[0].id', 'duplicate level id')


def test_level_ids_unique_within_a_side(tmp_path, sample):
    put(sample, 'pair.buy.levels[1].id', 'b1')
    expect_error(tmp_path, sample, 'pair.buy.levels[1].id', 'duplicate level id')


def test_pair_mode_controls_required_levels(tmp_path, sample):
    put(sample, 'pair.mode', 'sell')
    put(sample, 'pair.buy.levels', [])
    cfg = load_sample(tmp_path, sample)
    assert cfg.levels('buy') == () and cfg.side_enabled('sell') and not cfg.side_enabled('buy')
    assert cfg.side('buy')['enabled'] is False
    put(sample, 'pair.mode', 'both')
    expect_error(tmp_path, sample, 'pair.buy.levels', 'at least one level')


def test_disabled_side_levels_are_still_validated(tmp_path, sample):
    put(sample, 'pair.mode', 'buy')
    put(sample, 'pair.sell.levels[1].edge_pct', '0.5')
    expect_error(tmp_path, sample, 'pair.sell.levels[1].edge_pct', 'min_new_edge_pct')


# ----------------------------------------------------------------------------------------
# DMS, actions and order count rules
# ----------------------------------------------------------------------------------------
def _timing(sample, *, timeout, refresh, poll=5, request_timeout=5, expiry=10):
    put(sample, 'safety.dms_timeout_seconds', timeout)
    put(sample, 'safety.dms_refresh_seconds', refresh)
    put(sample, 'app.poll_seconds', poll)
    put(sample, 'app.request_timeout_seconds', request_timeout)
    put(sample, 'app.request_expiry_seconds', expiry)


def test_prompt_original_dms_timing_fails_the_timing_margin(tmp_path, sample):
    # prompt §9: DMS 30/10, request timeout 8, expiry 10, poll 5 -> 30 <= 10 + 3*8 + 5 = 39
    _timing(sample, timeout=30, refresh=10, request_timeout=8)
    expect_error(tmp_path, sample, 'safety.dms_timeout_seconds', 'timing margin')


def test_dms_timeout_must_cover_three_refreshes(tmp_path, sample):
    _timing(sample, timeout=40, refresh=15)
    expect_error(tmp_path, sample, 'safety.dms_timeout_seconds', '>=3 times refresh interval')


def test_dms_timing_margin_boundary_is_exclusive(tmp_path, sample):
    _timing(sample, timeout=30, refresh=10)          # 30 == 10 + 3*5 + 5
    expect_error(tmp_path, sample, 'safety.dms_timeout_seconds', 'timing margin')


def test_dms_would_cancel_every_order_before_renewal(tmp_path, sample):
    # reference rule 3: left = T - R - P - q must exceed max(q + max(q,e) + 0.5, q + e + 1)
    _timing(sample, timeout=60, refresh=20, request_timeout=8, expiry=20)    # left 27 <= 29
    expect_error(tmp_path, sample, 'safety.dms_timeout_seconds', 'before each renewal')


def test_dms_renewal_rule_boundary(tmp_path, sample):
    _timing(sample, timeout=36, refresh=10)          # left = 36-10-5-5 = 16 == max(15.5, 16)
    expect_error(tmp_path, sample, 'safety.dms_timeout_seconds', 'before each renewal')
    _timing(sample, timeout=37, refresh=10)          # left 17 > 16
    assert load_sample(tmp_path, sample).safety['dms_timeout_seconds'] == 37


def test_reference_dms_example_and_gate_minimum(tmp_path, sample):
    _timing(sample, timeout=60, refresh=20, request_timeout=8, expiry=10)    # reference T60 R20 P5 q8 e10
    assert load_sample(tmp_path, sample).safety['dms_refresh_seconds'] == 20.0
    put(sample, 'safety.dms_timeout_seconds', 4)
    expect_error(tmp_path, sample, 'safety.dms_timeout_seconds', '>= 5')


def test_signed_actions_must_cover_dms_renewals_plus_reserve(tmp_path, sample):
    _timing(sample, timeout=45, refresh=10)          # ceil(3600/10) = 360 renewals + 140
    put(sample, 'safety.max_signed_actions_per_hour', 499)
    expect_error(tmp_path, sample, 'safety.max_signed_actions_per_hour', 'must be >= 500 (360 DMS renewals')
    put(sample, 'safety.max_signed_actions_per_hour', 500)
    assert load_sample(tmp_path, sample).safety['max_signed_actions_per_hour'] == 500


def test_signed_actions_rounds_renewals_up(tmp_path, sample):
    put(sample, 'safety.dms_refresh_seconds', 14)     # ceil(3600/14) = 258 -> 398
    put(sample, 'safety.max_signed_actions_per_hour', 397)
    expect_error(tmp_path, sample, 'safety.max_signed_actions_per_hour', '>= 398')


def test_max_open_orders_must_cover_all_levels(tmp_path, sample):
    put(sample, 'safety.max_open_orders', 3)
    expect_error(tmp_path, sample, 'safety.max_open_orders', 'number of configured levels (4)')
    put(sample, 'safety.max_open_orders', 4)
    assert load_sample(tmp_path, sample).safety['max_open_orders'] == 4


def test_unknown_create_margin_must_exceed_clock_skew(tmp_path, sample):
    put(sample, 'safety.max_clock_skew_ms', 2000)
    put(sample, 'safety.unknown_create_margin_seconds', 2)
    expect_error(tmp_path, sample, 'safety.unknown_create_margin_seconds', 'max_clock_skew_ms')


def test_telegram_unhealthy_window_covers_healthchecks(tmp_path, sample):
    put(sample, 'telegram.healthcheck_seconds', 60)       # 2*60 + 5 = 125 > 120
    expect_error(tmp_path, sample, 'telegram.max_unhealthy_seconds', '2*telegram.healthcheck_seconds')


# ----------------------------------------------------------------------------------------
# sources, FX, paper
# ----------------------------------------------------------------------------------------
def test_duplicate_source_ids_rejected(tmp_path, sample):
    put(sample, 'index.sources[2].id', 'bybit_perp')
    expect_error(tmp_path, sample, 'index.sources[2].id', 'duplicate source id')


def test_duplicate_feed_rejected(tmp_path, sample):
    put(sample, 'index.sources[1].venue', 'bybit_linear')     # same venue/symbol/price as sources[0]
    expect_error(tmp_path, sample, 'index.sources[1]', 'duplicate source')


def test_usdc_source_requires_fx_settings(tmp_path, sample):
    del sample['index']['fx']
    expect_error(tmp_path, sample, 'index.fx', "'lighter_perp' quotes USDC")


def test_fx_defaults_when_no_enabled_source_quotes_usdc(tmp_path, sample):
    del sample['index']['fx']
    put(sample, 'index.sources[4].enabled', False)      # disabled USDC source needs no FX
    cfg = load_sample(tmp_path, sample)
    assert cfg.index_settings().fx.mode == 'fixed' and cfg.index['fx']['usdc_to_usdt'] == 1
    assert [s.id for s in cfg.sources()] == ['bybit_perp', 'bitget_perp', 'bingx_perp', 'gate_perp']
    assert [s.id for s in cfg.all_sources()][-1] == 'lighter_perp' and not cfg.all_sources()[-1].enabled
    assert [s.id for s in cfg.index_settings().sources] == [s.id for s in cfg.sources()]


def test_fx_block_partial_gets_defaults_and_modes(tmp_path, sample):
    sample['index']['fx'] = {'mode': 'binance_spot', 'max_depeg_pct': 1}
    fx = load_sample(tmp_path, sample).index_settings().fx
    assert fx == FxSettings(mode='binance_spot', usdc_to_usdt=Decimal('1'), max_depeg_pct=Decimal('1'),
                            max_age_seconds=3.0, max_spread_pct=Decimal('0.2'))


def test_fixed_fx_rate_must_be_inside_depeg_band(tmp_path, sample):
    put(sample, 'index.fx.usdc_to_usdt', '1.006')
    expect_error(tmp_path, sample, 'index.fx.usdc_to_usdt', 'max_depeg_pct')
    put(sample, 'index.fx.usdc_to_usdt', '0.995')
    assert load_sample(tmp_path, sample).index_settings().fx.usdc_to_usdt == Decimal('0.995')


def test_required_source_must_be_enabled(tmp_path, sample):
    put(sample, 'index.sources[0].enabled', False)
    expect_error(tmp_path, sample, 'index.sources[0].required', 'must be enabled')


def test_min_sources_cannot_exceed_enabled_sources(tmp_path, sample):
    put(sample, 'index.min_sources', 6)
    expect_error(tmp_path, sample, 'index.min_sources', 'number of enabled sources (5)')
    put(sample, 'index.min_sources', 2)
    for i in (1, 2, 3, 4):
        put(sample, f'index.sources[{i}].enabled', False)
    expect_error(tmp_path, sample, 'index.min_sources', 'number of enabled sources (1)')
    put(sample, 'index.min_sources', 1)
    assert [s.id for s in load_sample(tmp_path, sample).sources()] == ['bybit_perp']


def test_at_least_one_enabled_source(tmp_path, sample):
    for i in range(5):
        put(sample, f'index.sources[{i}].enabled', False)
    put(sample, 'index.sources[0].required', False)
    expect_error(tmp_path, sample, 'index.sources', 'at least one enabled source')
    put(sample, 'index.sources', [])
    expect_error(tmp_path, sample, 'index.sources', 'nonempty list')


def test_source_defaults_and_optional_fields(tmp_path, sample):
    del sample['index']['sources'][0]['heartbeat_seconds']          # bybit default 3
    sample['index']['sources'][1].update(price='mark', max_age_seconds=5, heartbeat_seconds='1.5',
                                         url='wss://ws.bitget.com/v2/ws/public')
    del sample['index']['sources'][1]['max_spread_pct']            # mark: may be omitted -> null
    sample['index']['sources'][2].update(price='last', max_spread_pct=None)
    sample['index']['sources'][3].update(price='microprice')
    sample['index']['sources'][4].update(market_index=7)
    sources = load_sample(tmp_path, sample).sources()
    assert sources[0].heartbeat_seconds == 3.0
    assert sources[1].price == 'mark' and sources[1].max_spread_pct is None
    assert sources[1].max_age_seconds == 5.0 and sources[1].heartbeat_seconds == 1.5
    assert sources[1].url == 'wss://ws.bitget.com/v2/ws/public'
    assert sources[2].price == 'last' and sources[2].max_spread_pct is None
    assert sources[3].price == 'microprice' and sources[3].max_spread_pct == Decimal('1.0')
    assert sources[4].market_index == 7


@pytest.mark.parametrize('price', ['mid', 'microprice'])
def test_book_price_kinds_require_a_spread_cap(tmp_path, sample, price):
    sample['index']['sources'][1]['price'] = price
    del sample['index']['sources'][1]['max_spread_pct']
    expect_error(tmp_path, sample, 'index.sources[1].max_spread_pct', 'only for mark/last')
    sample['index']['sources'][1]['max_spread_pct'] = None
    expect_error(tmp_path, sample, 'index.sources[1].max_spread_pct', 'only for price mark/last')


def test_index_min_sources_is_validated_against_enabled_sources_only(tmp_path, sample):
    put(sample, 'index.sources[4].enabled', False)
    put(sample, 'index.min_sources', 5)
    expect_error(tmp_path, sample, 'index.min_sources', '(4)')


def test_paper_touch_simulation_is_rejected_explicitly(tmp_path, sample):
    put(sample, 'paper.fill_on_touch', True)
    expect_error(tmp_path, sample, 'paper.fill_on_touch', 'PAPER_TOUCH_SIMULATION_NOT_SUPPORTED')


def test_paper_fill_model_must_be_trade_through(tmp_path, sample):
    put(sample, 'paper.fill_model', 'touch')
    expect_error(tmp_path, sample, 'paper.fill_model', 'trade_through')


def test_only_post_only_time_in_force(tmp_path, sample):
    put(sample, 'execution.time_in_force', 'ioc')
    expect_error(tmp_path, sample, 'execution.time_in_force', 'only poc')


# ----------------------------------------------------------------------------------------
# YAML file handling
# ----------------------------------------------------------------------------------------
def test_yaml_duplicate_key_rejected(tmp_path, sample):
    text = yaml.safe_dump(sample, sort_keys=False)
    path = tmp_path / 'dup.yaml'
    path.write_text(text.replace('  mode: paper\n', '  mode: paper\n  mode: live\n', 1), encoding='utf-8')
    with pytest.raises(ConfigError, match='Duplicate YAML key: mode'):
        load_config(path, read_env=False)


def test_yaml_nested_duplicate_key_rejected(tmp_path):
    path = tmp_path / 'dup.yaml'
    path.write_text('pair:\n  buy:\n    levels: []\n    levels: []\n', encoding='utf-8')
    with pytest.raises(ConfigError, match='Duplicate YAML key: levels'):
        load_config(path, read_env=False)


def test_utf8_bom_is_tolerated(tmp_path, sample):
    path = tmp_path / 'bom.yaml'
    path.write_bytes(b'\xef\xbb\xbf' + EXAMPLE.read_bytes())
    assert load_config(path, read_env=False).identity()['campaign_id'] == 'stonk_gate_test'


@pytest.mark.parametrize('text,fragment', [
    ('', 'empty'),
    ('- app\n- market\n', 'mapping of sections'),
    ('app: [unclosed\n', 'invalid YAML'),
    ('app:\n  mode: paper\n mode: live\n', 'invalid YAML'),
])
def test_unusable_files_raise_config_error(tmp_path, text, fragment):
    path = tmp_path / 'bad.yaml'
    path.write_text(text, encoding='utf-8')
    with pytest.raises(ConfigError, match=fragment):
        load_config(path, read_env=False)


def test_missing_config_file(tmp_path):
    with pytest.raises(ConfigError, match='not found'):
        load_config(tmp_path / 'nope.yaml', read_env=False)


def test_non_string_keys_rejected(tmp_path, sample):
    sample['app'][1] = 'x'
    with pytest.raises(ConfigError, match='keys must be strings'):
        load_sample(tmp_path, sample)


# ----------------------------------------------------------------------------------------
# secrets, .env and live confirmation
# ----------------------------------------------------------------------------------------
SECRET_ENV = {'GATE_API_KEY': 'key-TOPSECRET-1', 'GATE_API_SECRET': 'sec-TOPSECRET-2',
              'TELEGRAM_BOT_TOKEN': '123:TOPSECRET-3', 'TELEGRAM_CHAT_ID': '-100777'}


def test_secrets_read_from_env_and_masked(tmp_path, sample, monkeypatch):
    for name, value in SECRET_ENV.items():
        monkeypatch.setenv(name, f'  {value}  ')
    cfg = load_sample(tmp_path, sample)
    s = cfg.secrets
    assert (s.gate_key, s.gate_secret, s.telegram_token, s.telegram_chat) == tuple(SECRET_ENV.values())
    assert s.has_gate_keys and s.has_telegram
    for text in (repr(s), str(s), f'{s}', repr(cfg), str(cfg), f'{cfg}', json.dumps(cfg.data, default=str)):
        assert 'TOPSECRET' not in text and '-100777' not in text
    assert 'gate_key=set' in repr(s)


def test_missing_or_blank_secrets_are_none(tmp_path, sample, monkeypatch):
    monkeypatch.setenv('GATE_API_KEY', '   ')
    cfg = load_sample(tmp_path, sample)
    assert cfg.secrets == Secrets() and not cfg.secrets.has_gate_keys and not cfg.secrets.has_telegram
    assert 'gate_key=None' in repr(cfg.secrets)


def test_explicit_secrets_are_used_as_given(tmp_path, sample, monkeypatch):
    monkeypatch.setenv('GATE_API_KEY', 'from-env')
    cfg = Config(sample, tmp_path, Secrets(gate_key='given'))
    assert cfg.secrets.gate_key == 'given' and cfg.secrets.gate_secret is None


def test_env_file_next_to_config_does_not_override_shell(tmp_path, sample, monkeypatch):
    track_env(monkeypatch, 'GATE_API_SECRET', 'TELEGRAM_CHAT_ID', 'GATE_LIVE_CONFIRM', 'GATE_EXTRA_SETTING')
    monkeypatch.setenv('GATE_API_KEY', 'from-shell')
    (tmp_path / '.env').write_text(
        'GATE_API_KEY=from-file\nGATE_API_SECRET=file-secret\nTELEGRAM_CHAT_ID=777\n'
        'GATE_EXTRA_SETTING=1\nGATE_LIVE_CONFIRM=I_UNDERSTAND_SPOT_TRADING\n', encoding='utf-8')
    path = write(tmp_path, sample)
    cfg = load_config(path)                                   # read_env defaults to True
    assert cfg.secrets.gate_key == 'from-shell'               # shell wins
    assert cfg.secrets.gate_secret == 'file-secret' and cfg.secrets.telegram_chat == '777'
    assert os.environ['GATE_API_KEY'] == 'from-shell' and os.environ['GATE_EXTRA_SETTING'] == '1'
    assert 'GATE_LIVE_CONFIRM' not in os.environ              # never imported from the file
    assert env_file_has_live_confirm(cfg) is True


def test_env_file_ignored_without_read_env(tmp_path, sample, monkeypatch):
    track_env(monkeypatch, 'GATE_API_KEY')
    (tmp_path / '.env').write_text('GATE_API_KEY=from-file\n', encoding='utf-8')
    cfg = load_sample(tmp_path, sample)
    assert cfg.secrets.gate_key is None and 'GATE_API_KEY' not in os.environ


def test_env_file_elsewhere_is_not_read(tmp_path, sample, monkeypatch):
    track_env(monkeypatch, 'GATE_API_KEY')
    (tmp_path / '.env').write_text('GATE_API_KEY=wrong-dir\n', encoding='utf-8')
    sub = tmp_path / 'cfgdir'
    sub.mkdir()
    cfg = load_config(write(sub, sample))
    assert cfg.secrets.gate_key is None and cfg.base_dir == sub.resolve()


@pytest.mark.parametrize('content,expected', [
    (None, False),
    ('GATE_API_KEY=x\n', False),
    ('# GATE_LIVE_CONFIRM=I_UNDERSTAND_SPOT_TRADING\n', False),
    ('GATE_LIVE_CONFIRM=I_UNDERSTAND_SPOT_TRADING\n', True),
    ('export GATE_LIVE_CONFIRM=I_UNDERSTAND_SPOT_TRADING\n', True),
    ('GATE_API_KEY=x\nGATE_LIVE_CONFIRM=\n', True),
    ('GATE_LIVE_CONFIRM\n', True),
    ('﻿GATE_LIVE_CONFIRM=wrong\n', True),
])
def test_env_file_has_live_confirm(tmp_path, sample, content, expected):
    if content is not None:
        (tmp_path / '.env').write_text(content, encoding='utf-8')
    path = write(tmp_path, sample)
    cfg = load_config(path, read_env=False)
    assert env_file_has_live_confirm(cfg) is expected
    assert env_file_has_live_confirm(path) is expected
    assert env_file_has_live_confirm(tmp_path) is expected
    assert 'GATE_LIVE_CONFIRM' not in os.environ
