# Тесты gate_stonk 0.1.0

Дата прогона: 2026-10-10. Среда: Linux (контейнер сборки), Python 3.13.16, pytest 9.0.2.

**Результат: 1808 тестов прошли, 0 пропущено, 0 упало, около 45 с** (`pytest -q`). Число тестов взято из
`pytest --collect-only -q`: 1808 элементов, включая параметризованные варианты, из 949 тестовых функций в
34 файлах. На Windows набор в этой среде не запускался.

Все тесты офлайн. Они проверяют логику на синтетических данных и не подтверждают поведение реальных бирж.
Что можно проверить только вживую — раздел 6.

## 1. Как запустить

Сначала установите зависимости с extra `test` (README_RU, раздел 4). Сеть и ключи не нужны.

**Linux:**

```bash
.venv/bin/python -m pytest -q                                   # весь набор
.venv/bin/python -m pytest --collect-only -q | tail -1          # сколько тестов собрано
.venv/bin/python -m pytest -q tests/test_index.py               # один файл
.venv/bin/python -m pytest -q -k iceberg                        # по подстроке имени
.venv/bin/python -m pytest -q tests/test_pricing_property.py tests/test_engine_stress.py   # вероятностные
```

**Windows (PowerShell, из папки проекта):**

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m pytest -q tests\test_engine_recon.py
```

`setup_windows.ps1` запускает `pytest -q` сам и прерывает установку, если тесты не прошли. Тесты пишут только
во временные папки pytest (`tmp_path`). Рабочие `config.yaml`, `.env`, `runtime_stonk` и STOP-файлы проекта
они не трогают. Флаг `-p no:cacheprovider` дополнительно отключает папку `.pytest_cache`.

## 2. Офлайн-гарантии

- **Сеть заблокирована.** Автоматическая фикстура `_offline` в `tests/conftest.py` подменяет
  `socket.socket.connect`, `socket.socket.connect_ex`, `socket.create_connection` и `socket.getaddrinfo`.
  Любая попытка соединения падает с `NetworkBlocked`, и тест не проходит.
- **Секреты удалены.** Перед каждым тестом из окружения удаляются все переменные с префиксами `GATE_` и
  `TELEGRAM_`: ключи Gate, токен Telegram и `GATE_LIVE_CONFIRM`. Конфиги тесты пишут во временные папки,
  пример `config.example.yaml` читают как текст или с `read_env=False`. Поэтому `.env` проекта не читается.
  Исключение — тесты поведения `.env`: у них свой временный `.env`.
- **Время управляемое.** Фикстура `clock` подменяет `time.time` и `time.monotonic`. Паузы, окна
  `x-gate-exptime`, DMS, сутки UTC и задержки проверяются без реального ожидания.
- **Внешние зависимости внедряются.** HTTP заменяют фейковые сессии (`FakeSession`, `FakeHttp`), которые
  записывают каждый запрос: метод, URL, заголовки, тело. Ещё есть фейковые WS-сокеты, `FakeHub` (котировки),
  `FakeVenue` (сценарная биржа Gate для engine: блокировки балансов, post-only, частичные исполнения, задержка
  истории, чужие заявки, заявки и сделки других пар аккаунта, потерянные ответы, DMS) и фейковый Telegram API.
  Вспомогательные модули: `tests/adapter_fakes.py`, `tests/cli_fakes.py`, `tests/engine_fakes.py`.
  Тесты движка берут `config.example.yaml`, но закрепляют `band_tolerance_pct` и `reprice_threshold_pct` на 0.15
  (`engine_fakes.config_data`): их ценовые сценарии рассчитаны на этот допуск, а в примере сейчас 0.5.
- **Синтетика помечена.** Все 144 файла фикстур в `tests/fixtures/` (8 папок) **синтетические**: живых
  ответов бирж при сборке не было. Значения для STONK выдуманы около цены 0.1359. Схемы взяты из официальных SDK
  и документации, ccxt и других источников, перечисленных в `API_NOTES_RU.md`. Пометки такие:
  - в каждой папке есть `README.md` с заголовком «СИНТЕТИКА» или «SYNTHETIC» и таблицей «файл → источник
    схемы»;
  - у файлов `gate/`, `gate_spot/` и `fx/` есть поле `"_comment": "SYNTHETIC ..."`, и загрузчики
    `test_venue_gate.py`, `test_gate_spot_feed.py`, `test_fx_feed.py` проверяют эту пометку;
  - синтетические сделки в `test_store.py` и `test_report.py` несут `_comment` с источником схемы.
- **Live-торговля в тестах не выполняется.** Live-обёртка Gate проверяется на сценарных HTTP-ответах.
  Допуск к live проверяется до любой сети (`test_cli.py::test_run_gates_refuse_without_network_or_orders`).

## 3. Файлы и число тестов

| Файл | Тестов | Что проверяет |
|---|---:|---|
| `test_config.py` | 402 | Валидация конфига: пути ошибок, лимиты, уровни, iceberg, DMS-тайминги, `.env` (`GATE_LIVE_CONFIRM` в любом регистре не импортируется), BOM |
| `test_venue_gate.py` | 183 | Live-обёртка Gate на сценарном HTTP: точное тело post-only, `text`, пакеты, отмены, DMS, iceberg, unified, метаданные, неизвестные ответы, чтение открытых заявок и сделок других пар аккаунта |
| `test_pricing.py` | 176 | Сетка цены Gate, полосы уровней, выбор цены по глубине, `+TOUCH`, вычитание своих объёмов, защита `hard_min_edge` |
| `test_index.py` | 115 | Индекс: порядок проверок, веса, медиана, FX и депег, рассинхрон, выбросы, гистерезис возврата, тихий Bybit |
| `test_store.py` | 92 | SQLite: резервы до отправки, `sent_ms` и миграция старой базы, привязка `order_id`, учёт исполнений, дедупликация, поздние исполнения, улики других пар, прежние цены amend в своих объёмах, сутки UTC, outbox |
| `test_gate_rest.py` | 84 | Подпись APIv4 (векторы), `x-gate-exptime` только на записях, классификация ошибок, лимитер, 429, часы назад: отказ только созданиям и amend |
| `test_feed_hub.py` | 55 | FeedHub: приём кадров, регрессии, лента сделок, REST-подкачка (по ключу, по возрасту события, backoff, зависшая биржа), устаревший кадр не стирает снимок другого транспорта, QuoteHealth |
| `test_adapter_bybit.py` | 55 | Bybit: `orderbook.1`, повторы с тем же `u`, tickers, REST, проверка инструмента |
| `test_sizing.py` | 53 | Размер уровня, компенсация комиссии в базе, видимая часть iceberg, минимумы пары |
| `test_cli.py` | 51 | `validate`, допуск live (`GATE_LIVE_CONFIRM` в `.env` в любом регистре), цикл `run`, SIGTERM внутри вызова биржи и во время остановки, кодировка консоли и `PYTHONUTF8` в скриптах, свежие метаданные пары после сбоя, `cancel-only`, `status`, `report`, блокировки, маскировка секретов |
| `test_gate_spot_feed.py` | 41 | Стакан и лента спота Gate, конверты, REST-снимок, тихая книга с замороженным `t` |
| `test_adapter_lighter.py` | 41 | Lighter: локальная книга, непрерывность nonce, `market_index`, REST |
| `test_adapter_bingx.py` | 40 | BingX: gzip, Ping/Pong, asks по убыванию, mark/last, контракты |
| `test_ladder.py` | 38 | Лестница: буфер после округления, монотонность (и против стоящих заявок других уровней), `LEVEL_COLLAPSED`, `LEVEL_ORDER_WAIT`, самопересечение, свои видимые объёмы |
| `test_venue_paper.py` | 37 | Paper: `trade_through`, общая лента для уровней, комиссии, post-only, ленивый DMS, перезапуск, пустая проверка других пар |
| `test_adapter_bitget.py` | 33 | Bitget: `books1`, ticker, REST, контракты |
| `test_ledger.py` | 29 | Учёт по средней цене: комиссии STONK/USDT/GT/POINT, rebate, порядок исполнения, вывод |
| `test_adapter_gate_futures.py` | 29 | Gate futures: book_ticker, tickers, уровни `{p,s}`, контракт |
| `test_engine_recon.py` | 30 | Сверка: неизвестные создания (окно от отправки, остановка на первом неизвестном, повторная проверка DMS и лимитера перед каждой отправкой), post-only, частичные и поздние исполнения, привязка по `text` в контрольном чтении, вывод токенов, посторонняя активность на паре и других парах, перезапуск |
| `test_notify.py` | 27 | Telegram: outbox, порядок, повторы, 429, разбиение длинных текстов, здоровье, тексты |
| `test_fx_feed.py` | 25 | FX: fixed, Binance depth5, Gate book_ticker, REST-подкачка |
| `test_feeds_ws.py` | 24 | WS-воркер: переподключение, backoff, ping, простои; устаревшие WS-кадры не стирают REST-снимок |
| `test_engine_guard.py` | 23 | Engine: быстрая защита (и для неподтверждённых плановых отмен), STOP, блокировка индекса, запись индекса без заявок, потерянные отмены, DMS, часы |
| `test_engine_create.py` | 20 | Engine: первый шаг, пакет, общие деньги, лимиты, минимумы, iceberg в live, отказы |
| `test_status.py` | 17 | Атомарный `status.json`, строгий JSON, повторы при `PermissionError` (Windows) |
| `test_report.py` | 17 | Отчёт по слотам: значения, посчитанные вручную, окна, два эталона краёв (индекс и центр), rebate по заявкам, ограничения paper, базис, блокировка индекса, только чтение |
| `test_doctor.py` | 17 | doctor: обязательные проверки, сетевые сбои, режим аккаунта, `--probe-write` (только под блокировками бота), самоссылка Gate futures |
| `test_simulate.py` | 13 | Офлайн-симуляция полного цикла на синтетическом рынке (раздел 5a) |
| `test_engine_reprice.py` | 13 | `REPRICE` пакетом, пороги, паузы по лимитам, amend, прежняя цена amend в своих объёмах, монотонность против стоящих заявок при пересоздании |
| `test_fill_wakeup.py` | 8 | Приватный WS Gate только будит сверку: подпись, свои события |
| `test_marks.py` | 8 | Правило ±1 с для индекса в момент исполнения и markout |
| `test_watch.py` | 5 | `index-watch`: SQLite, CSV, итоговые цифры, Ctrl+C |
| `test_pricing_property.py` | 4 | Вероятностные проверки цен (раздел 5) |
| `test_engine_stress.py` | 3 | Вероятностный стресс engine (раздел 5) |
| **Всего** | **1808** | |

## 4. Соответствие требованиям §11 задания

Числа в скобках — тесты в файле целиком. Ключевые тесты перечислены по имени.

| Требование §11 | Где проверяется | Ключевые тесты |
|---|---|---|
| **Индекс:** веса, взвешенная медиана, исключение по возрасту, спреду и отклонению, `required`/`optional`, `min_sources`, USDC→USDT и депег, тихий Bybit, широкий стакан Gate futures | `test_index.py` (115), `test_fx_feed.py` (25); разбор источников — `test_adapter_*.py` (198), `test_feed_hub.py` (55) | `test_weighted_median_function_exact_rules`, `test_weighted_median_exact_half_tie_takes_mean_of_neighbours`, `test_weights_are_renormalised_over_included_sources`, `test_stale_receipt_excludes_source_with_stale_quote`, `test_wide_spread_excludes_source_and_keeps_diagnostics`, `test_outlier_source_is_excluded_and_others_form_index`, `test_required_source_failure_blocks_with_its_reason`, `test_optional_source_failure_does_not_block`, `test_min_sources_blocks_with_insufficient_sources`, `test_fixed_fx_multiplies_usdc_price`, `test_live_fx_depeg_excludes_usdc_sources`, `test_quiet_bybit_book_3_5_seconds_old_is_valid_with_heartbeat`, `test_quiet_bybit_book_4_1_seconds_old_is_stale_and_blocks_as_required`, `test_gate_futures_with_fifty_percent_spread_drops_out_while_others_form_index` |
| **Цены:** округление вниз/вверх по `precision`, буфер 1% после округления, `hard_min_edge_pct` в preflight, монотонность и `LEVEL_COLLAPSED`, `+TOUCH`, самопересечение между всеми уровнями, вычитание своих видимых объёмов, вероятностная проверка; монотонность и `LEVEL_ORDER_WAIT` против стоящих заявок других уровней | `test_pricing.py` (176), `test_ladder.py` (38), `test_pricing_property.py` (4), `test_engine_guard.py` (23), `test_engine_reprice.py` (13) | `test_floor_price_never_rounds_up`, `test_ceil_price_never_rounds_down`, `test_rounding_direction_is_away_from_center_on_both_sides`, `test_buffer_is_measured_after_rounding_against_the_center`, `test_hard_min_edge_cancels_from_preflight_without_account_reads`, `test_band_tolerance_never_relaxes_the_hard_guard`, `test_hard_guard_escalates_unconfirmed_routine_cancel_at_once`, `test_exact_levels_that_round_to_the_same_tick_shift_deeper`, `test_range_levels_whose_bands_coincide_at_coarse_precision_collapse`, `test_deeper_level_shifts_below_a_shallower_resting_order`, `test_shallower_level_waits_for_a_deeper_resting_order`, `test_recreated_deeper_level_never_rests_above_a_shallower_resting_order`, `test_range_levels_touch_inside_their_band_and_keep_the_ladder_strict`, `test_buy_levels_never_cross_any_own_sell_level`, `test_sell_levels_never_cross_any_own_buy_level`, `test_own_visible_volume_of_all_same_side_levels_is_subtracted`, `test_own_volume_is_capped_per_level_and_not_carried_to_the_next`, `test_random_ladders_hold_the_pricing_invariants` |
| **Iceberg:** `0 < display < amount`, минимумы, округление, конфликт с `poc` | 37 тестов с iceberg или видимой частью в имени: `test_sizing.py` 18, `test_config.py` 11, `test_engine_create.py` 3, по 1 в `test_cli.py`, `test_pricing.py`, `test_ladder.py`, `test_venue_gate.py`, `test_venue_paper.py` | `test_invalid_iceberg_display_refused` (12 вариантов), `test_visible_part_is_floored_to_the_lot`, `test_visible_part_must_meet_the_pair_minimums`, `test_iceberg_display_qty_must_be_below_order_qty`, `test_iceberg_levels_wait_in_live_until_verified`, `test_iceberg_display_below_minimum_waits_with_its_code`, `test_run_gates_refuse_without_network_or_orders[iceberg_unverified]`, `test_own_iceberg_subtracts_only_the_visible_part_supplied_by_the_caller` |
| **Сверка:** частичные исполнения нескольких уровней, неопределённый результат до и после окна `x-gate-exptime`, поиск по `text` пока заявка открыта и по `order_id` после, потерянная отмена, позднее исполнение, посторонняя заявка (в том числе на других парах аккаунта) → STOP | `test_engine_recon.py` (28), `test_engine_guard.py` (23), `test_store.py` (92), `test_venue_gate.py` (183), `test_engine_stress.py` (3) | `test_partial_fills_on_several_levels_in_one_pass`, `test_unknown_create_absent_cancels_healthy_slots_too`, `test_unknown_create_never_seen_closes_after_expiry_window`, `test_unknown_create_lost_after_acceptance_binds_by_text`, `test_unknown_create_expiry_counts_from_send_not_reservation`, `test_unknown_create_expiry_ignores_forward_wall_clock_jump`, `test_sequential_create_stops_at_first_unknown_and_releases_unsent`, `test_batch_create_stops_after_an_unknown_chunk`, `test_control_read_binds_unbound_terminal_row_by_text`, `test_get_order_by_id_and_text`, `test_order_trades_fallback_when_window_misses_trades`, `test_lost_cancel_retry_schedule_blocks_reads_dms_and_creates`, `test_restart_lost_cancel_is_resent_immediately`, `test_late_visible_trade_blocks_release_and_watermark`, `test_late_fill_triggers_replay_and_correction_message`, `test_foreign_open_order_account_review_settles_own_and_keeps_dms`, `test_foreign_trade_requires_review_and_is_quarantined`, `test_other_pair_open_order_stops_for_review_on_the_account_scan_cadence`, `test_other_pair_trade_requires_review_with_quarantined_evidence`, `test_closed_order_reappearing_is_unsafe` |
| **Учёт:** комиссия в STONK/USDT/GT/POINT, rebate, средняя цена, прибыль | `test_ledger.py` (29), `test_store.py` (92), `test_report.py` (17), `test_engine_recon.py` (28) | `test_buy_fee_in_base_reduces_received_quantity_not_cost`, `test_buy_fee_in_quote_adds_to_cost`, `test_sell_fee_in_quote_reduces_proceeds_and_realizes_against_average_cost`, `test_gt_fee_is_recorded_separately_and_never_converted`, `test_point_fee_is_recorded_separately_and_never_converted`, `test_negative_fee_is_a_rebate_in_both_currencies`, `test_average_price_and_realized_over_multi_fill_sequence`, `test_replay_uses_execution_order_not_arrival_order`, `test_fees_in_stonk_usdt_gt_point_and_rebate_through_the_store`, `test_per_slot_metrics_match_hand_computation`, `test_order_rebates_are_reported_per_currency_and_slot`, `test_clean_withdrawal_is_booked_at_average_cost`, `test_withdrawal_with_fills_since_last_clean_sync_requires_review` |
| **Лимиты:** общий USDT на уровни, суточные лимиты UTC, лимит запаса с допуском | `test_engine_create.py` (20), `test_sizing.py` (53), `test_store.py` (92), `test_config.py` (402) | `test_shared_cash_never_over_reserved`, `test_scarce_cash_later_level_waits_balance_or_limit`, `test_sell_limited_by_base_minus_reserve_and_other_sells`, `test_daily_notional_cap_utc_day`, `test_daily_totals_by_utc_midnight`, `test_inventory_cap_tolerance_for_resting_buy`, `test_order_size_below_exchange_minimum`, `test_max_open_orders_counts_slots` |
| **Фикстуры:** записанные ответы Gate/Bybit/Lighter без ключей | 144 файла в `tests/fixtures/` (gate 18, gate_spot 8, gate_futures 17, bybit 28, bitget 20, bingx 24, lighter 26, fx 3). Используются в `test_adapter_*.py`, `test_gate_spot_feed.py`, `test_fx_feed.py`, `test_venue_gate.py`, `test_gate_rest.py`, `test_doctor.py`, `test_cli.py` | **Требование выполнено частично.** Все фикстуры синтетические по документированным схемам, потому что живых ответов не было. Записанные ответы добавляются на этапах 1 и 5 (раздел 7) |

Сверх §11 есть отдельные тесты для подписи и лимитера Gate (`test_gate_rest.py`), paper (`test_venue_paper.py`),
DMS (`test_engine_guard.py`), CLI и допуска к live (`test_cli.py`), doctor, Telegram, `status.json`,
`index-watch` и отчёта.

## 5. Вероятностные проверки

- **`test_pricing_property.py::test_random_ladders_hold_the_pricing_invariants`.** Seed 20261010, 5000
  случайных случаев. Точность цены 2–6 знаков, центр от 200 до 200 000 тиков, стакан 1–8 уровней на сторону с
  базисом до ±6%. От 1 до 4 уровней на сторону, точных и диапазонных. Свои заявки, порог себестоимости и
  потолок. Несколько процентов намеренно испорченных входов. Для каждого решения проверяется:
  1. цена и границы на сетке, `lower ≤ price ≤ upper`;
  2. направление округления (BUY вниз, SELL вверх, границы внутрь);
  3. выставляемая BUY строго ниже ask и всех своих SELL, SELL зеркально; `+TOUCH` не выходит из полосы;
     точный уровень не сдвигается;
  4. учёт объёма: пропущенный объём равен сумме чужого строго лучше цены; свой объём вычитается не больше
     уровня; больший свой объём никогда не увеличивает видимый чужой;
  5. `insufficient_depth: wait` возвращает решение только при достаточном объёме впереди;
  6. монотонность clamp: требование большего объёма не делает цену агрессивнее, clamp не агрессивнее
     `nearest_index`;
  7. лестница: у выставляемых уровней отступ после округления не меньше буфера; уровни строго монотонны;
     `+LADDER_SHIFT` только вглубь; `LEVEL_COLLAPSED` только когда сдвиг невозможен;
  8. из данных поднимаются только документированные коды `Unsafe`, никаких `ValueError` или
     `ZeroDivisionError`.

  Тест также проверяет, что генератор действительно прошёл интересные ветви: `FIXED_EDGE`, `ZERO_SKIP`,
  `BOOK_VOLUME`, `MIN_NEW_EDGE_BUFFER`, `LEVEL_COLLAPSED`, `POST_ONLY_OR_SELF_CROSS_WAIT`, `+TOUCH`,
  `+LADDER_SHIFT`, `DEPTH_EXHAUSTED`, clamp, ошибки стакана и другие. Ограничение времени — 20 с.
- **`test_grid_helpers_on_random_prices[1|2|3]`.** 3 × 2000 случайных цен с точностью 0–8 знаков:
  `floor_price`, `ceil_price`, `price_before`, `price_after` дают значения на сетке и не дальше одного тика.
- **`test_engine_stress.py::test_stress_invariants[11|23|37]`.** 3 seed × 600 тиков цикла, как в CLI.
  Случайно движется индекс, приходят исполнения с задержкой истории, сбоят чтения, теряются ответы,
  переключаются STOP-файлы, отключаются фиды. На каждом тике проверяется:
  - в слоте не больше одной активной строки и одной своей открытой заявки на бирже;
  - каждая открытая своя заявка есть в базе;
  - резервы BUY с запасом на комиссию не больше USDT на бирже;
  - каждое исполнение принадлежит своей заявке;
  - ни одна заявка не отправлена ближе `min_new_edge_pct`;
  - освобождённая строка завершена на бирже, и её исполнения совпадают с учётом;
  - строка `rejected` на бирже никогда не существовала.

## 5a. Офлайн-симуляция полного цикла

`tests/test_simulate.py` (13 тестов) запускает настоящие `Runner`/`Engine`/`PaperVenue`/`FeedHub` на синтетическом
рынке с виртуальными часами и проверяет инварианты (не больше одной активной заявки на слот, резервы BUY не больше
USDT, заявки не ближе 1% к центру, монотонность лестницы, JSON-безопасный статус). Ручной прогон на час:
`python -m gate_stonk simulate --config config.example.yaml --minutes 60 --seed 1` (около минуты). Итоги
прогонов `--seed 1` и `--seed 2` (по 60 мин, без нарушений инвариантов) — README_RU, раздел 7, шаг 4. Рынок
синтетический: прогоны доказывают только, что цикл работает, а не доходность.

## 6. Что не покрыто офлайн и проверяется вживую

Чек-листы с командами — `API_NOTES_RU.md` §9. Пока эти пункты не выполнены, соответствующие факты имеют
статус «не проверено».

### 6.1 Этап 1: публичные данные, без ключей (API_NOTES §9.1)

- Все пять инструментов существуют и активны под ожидаемыми символами (`STONKUSDT`, `STONK-USDT`,
  `STONK_USDT`, `STONK`); шаг цены и статус каждого; `market_index` Lighter и актуальный хост.
- Реальные `precision`, `amount_precision`, `min_base_amount`, `min_quote_amount` и `trade_status` пары
  `STONK_USDT` на Gate.
- **Bybit: обновляется ли `ts`/`cts` у повтора снимка `orderbook.1` с тем же `u` (Р19).** От этого зависит,
  будет ли обязательный Bybit блокировать индекс в тихие периоды. Офлайн это правило проверено только на
  синтетических кадрах.
- Форматы и частота потоков: паузы Bitget `books1` при неизменной книге; BingX gzip-Ping, формат ack,
  приходит ли `depth5@500ms` без изменений, порядок asks; глубина снимка Lighter и цепочка
  `begin_nonce`/`nonce`; интервал `spot.order_book` Gate в тихий период и формат `spot.trades`; типы полей
  `futures.book_ticker` при `X-Gate-Size-Decimal`.
- Входит ли спот Gate в `index_constituents` фьючерса STONK (петля, Р18).
- Есть ли FX-пары `USDCUSDT` на Binance (и доступ к ней с сервера) и `USDC_USDT` на Gate.
- Есть ли заголовки `X-Gate-RateLimit-*` в REST.
- Реальные спреды источников, доля исключений, базис спот/индекс, доля блокировки индекса (`index-watch`
  30–60 мин). По ним утверждаются веса (Р24).

### 6.2 Этап 5: минимальная live-проверка, с ключами и только после явного «да» (API_NOTES §9.2)

- Режим аккаунта (classic или unified), белый список пар ключа, что отдаёт `/spot/accounts` в этом режиме,
  комиссии и списание в GT, ответ `REQUEST_EXPIRED` при сдвиге часов.
- Право записи (DMS off) и `label` просроченного `x-gate-exptime`.
- Минимальная `poc` BUY далеко от рынка: HTTP 201, `status=open`, `finish_as=open`. Чтение по `order_id` и по
  `text`, пока открыта; после отмены поиск по `text` не находит; список `finished` по окну.
- Путь отказа post-only: ошибка `POC_FILL_IMMEDIATELY` или 201 с `status=cancelled, finish_as=poc` (Р3).
  Отдельное «да»: если `poc` не сработает, минимальная заявка исполнится как taker.
- Формат ответов пакетного создания и пакетной отмены (порядок элементов, `succeeded`, `label`) и
  cancel-all.
- Чтения всего аккаунта для проверки других пар: `GET /spot/open_orders` (группы по парам, страницы внутри
  пары) и `GET /spot/my_trades` без `currency_pair` в режиме аккаунта бота.
- Принимается ли `iceberg` вместе с `poc` и с каким `label` (Р4). Только после этого можно ставить
  `execution.iceberg_poc_verified: true`.
- Поведение amend: цена вместе с количеством; проверяет ли Gate `poc` при смене цены. Только после этого
  можно включать `reprice_via_amend`.
- DMS: `triggerTime`, какой `finish_as` ставит срабатывание, действует ли DMS на заявки `account=unified`.
- Приватный WS: события `put`/`update`/`finish`, `finish_as` у отказа post-only.
- Бот целиком на минимальном live-конфиге (README_RU, раздел 7, шаг 5).

### 6.3 Прочее, чего тесты не доказывают

- Работа на Windows: `setup_windows.ps1`, `run_stonk.ps1`, `monitor_stonk.ps1`, файловые блокировки и
  замена `status.json` под антивирусом. Повтор при `PermissionError` проверен только эмуляцией. Вывод в
  кодовой странице ANSI проверен эмуляцией (`PYTHONIOENCODING=cp1252` на Linux), `PYTHONUTF8=1` в скриптах —
  только чтением их текста.
- Реальная доставка Telegram.
- Paper `trade_through` на настоящей ленте Gate: логика проверена на синтетической ленте.
- Поведение сети: обрывы, прокси, задержки, реальные лимиты и пороги штрафа Gate за низкий fill ratio.
- Синхронизация часов на компьютере пользователя.
- Экономика: прибыль, доля исполнений, markout. Ни тесты, ни paper их не прогнозируют.

## 7. Как записать реальные фикстуры на этапах 1 и 5

Цель: заменить предположения о форме ответов записанными ответами и закрепить их тестами. Существующие
синтетические файлы не перезаписывайте: их загрузчики требуют пометку `SYNTHETIC`. Живые записи кладите
рядом.

1. **Папка.** `tests/fixtures/<venue>/live_<YYYYMMDD>/`, где `<venue>` — `gate`, `gate_spot`, `gate_futures`,
   `bybit`, `bitget`, `bingx`, `lighter` или `fx`.
2. **Этап 1, без ключей.** Выполните REST-запросы из API_NOTES §9.1 с выводом в файлы, например:

   ```bash
   D=tests/fixtures/gate/live_20261011; mkdir -p $D
   curl -s https://api.gateio.ws/api/v4/spot/currency_pairs/STONK_USDT -o $D/currency_pair_stonk_usdt.json
   ```

   WS-потоки записывайте скриптом `ws_rec.py` (API_NOTES §9.3) в JSONL: по строке на принятый кадр с
   `recv_ms`. Вспомогательные скрипты храните вне репозитория и запускайте через `.venv/bin/python -I`. Отчёт
   `doctor.json` и CSV/SQLite `index-watch` остаются в `runtime_stonk` и прикладываются к отчёту этапа, а не к
   фикстурам.
3. **Этап 5, с ключами и только после «да».** Ответы `gcall.py` (API_NOTES §9.3) сохраняйте телом JSON, а
   код и заголовки `x-gate-*` записывайте в README папки. `ws_rec.py` с `auth_gate` пишет только принятые
   кадры: кадры с `KEY` и `SIGN` в файл не попадают.
4. **Очистка перед коммитом.** Удалите `user_id` и адреса из `ip_whitelist` (`/account/detail`). Убедитесь,
   что нет ключей и подписей:

   ```bash
   grep -rniE '"(key|sign|user_id)"|secret|ip_whitelist' tests/fixtures/*/live_* && echo "ПРОВЕРЬТЕ НАХОДКИ"
   ```

   Балансы реального аккаунта при желании замените круглыми значениями и отметьте это в README.
5. **Пометка.** В каждой папке `live_<дата>` положите `README.md` с заголовком «ЖИВЫЕ ДАННЫЕ». В нём:
   дата и время UTC, хост, точная команда или подписка, что проверялось, и какие поля обезличены. Для файлов
   `gate`, `gate_spot` и `fx` сохраните формат обёртки синтетики (`{"_comment": "LIVE <дата> <источник>",
   "data" | "frame" | "response": ...}`). Тогда тест сможет различать синтетику и живые данные по пометке.
6. **Сверка формы.** Сравните каждую запись с синтетическим аналогом: набор ключей, строки или числа,
   секунды или миллисекунды, порядок asks, вложенность. Расхождения запишите в `API_NOTES_RU.md` и смените
   статус факта на подтверждённый живым ответом с датой. Если разбор падает на живом кадре, это ошибка
   кода: её исправляют отдельной задачей с тестом на этом файле.
7. **Тесты на живых файлах.** Подайте записанные кадры в те же адаптеры, что и синтетику. В
   `test_adapter_*.py` это `adapter.on_text(fixture_text(<venue>, 'live_<дата>/<файл>'), NOW, WALL_MS)`. Для
   `gate`, `gate_spot` и `fx` нужен свой загрузчик, который проверяет пометку `LIVE` вместо `SYNTHETIC`. Затем
   прогоните весь набор (`pytest -q`) и обновите числа в этом файле.

Синтетику, для которой появился живой аналог, можно оставить как пограничные случаи (пустые стороны,
испорченные кадры, ошибки). Тогда в README папки отметьте, какой живой файл подтверждает основную схему.
