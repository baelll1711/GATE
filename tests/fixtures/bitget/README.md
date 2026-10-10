# Фикстуры Bitget V2 (classic, USDT-FUTURES, STONKUSDT) — СИНТЕТИКА

Все файлы **синтетические**: значения для STONK выдуманы (порядок цены — из таблицы заказчика, шаг 0.00001 —
из объявления о листинге), живых ответов нет. Структура взята из источников, разобранных в `notes/api_bitget.md`:

| Файлы | Схема (источник) |
|---|---|
| `ws_books1_*.json` | WS `books1`: ccxt `pro/bitget.py:832-857`, JKorf `BitgetOrderBookUpdate.cs` (notes §2.2, §6) |
| `ws_ticker_push.json` | WS `ticker`: ccxt `pro/bitget.py:316-350` (notes §2.4) |
| `ws_sub_ack*.json`, `ws_sub_error.json` | ccxt `pro/bitget.py:91, 2484-2493, 2539-2542` (notes §2.1); текст `msg` выдуман |
| `ws_pong.txt` | текстовый кадр `pong` (notes §2.5) |
| `rest_merge_depth.json` | `GET /api/v2/mix/market/merge-depth`, элементы строками: ccxt `bitget.py:3481-3490` (notes §1.2) |
| `rest_merge_depth_numbers.json` | тот же ответ, элементы числами: siebly `types/response/v2/futures.ts:44-46` |
| `rest_ticker.json` | `GET /api/v2/mix/market/ticker` (`data` — массив): ccxt `bitget.py:3718-3746` (notes §1.1) |
| `rest_contracts_*.json` | `GET /api/v2/mix/market/contracts`: siebly `FuturesContractConfigV2`, ccxt `bitget.py:2228-2262` (notes §1.4) |
| `rest_error_removed.json`, `rest_rate_limited.json` | конверт ошибки, коды 40309 / 429: ccxt `bitget.py:1459, 1512-1513` |
| `ws_not_json.txt` | произвольный не-JSON кадр |
