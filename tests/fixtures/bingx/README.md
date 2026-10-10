# Фикстуры BingX swap V2 (STONK-USDT) — СИНТЕТИКА

Все файлы **синтетические**: значения для STONK-USDT выдуманы вокруг 0.1359 и не являются рыночными данными.
Схема — официальная документация BingX docs-v3 (бандл `docs-v3`, коммит `0cc3b7c`), ccxt 4.5.85; разбор в
`notes/api_bingx.md`. Порядок `asks` воспроизводит документированное поведение BingX: **по убыванию**.

| Файлы | Схема (источник) |
|---|---|
| `ws_depth5*.json` | WS `<SYM>@depth5@500ms`: docs-v3 `v44e` (notes §2.5, §6.2); `ws_depth5_data_T.json` — время в `data.T` вместо `ts` |
| `ws_mark_price.json`, `ws_last_price.json` | WS `@markPrice` / `@lastPrice`: docs-v3 `dataUpdateDemo` (`markPriceUpdate`, `lastPriceUpdate`) |
| `ws_ack.json`, `ws_ack_alt.json`, `ws_sub_error.json`, `ws_error_no_id.json` | docs-v3 `v44e`/`K4xa`, ccxt `pro/bingx.py:1327-1336, 1758-1765` (notes §2.4) |
| `ws_ping.txt`, `ws_ping_json.json` | heartbeat `Ping` (docs-v3 «WebSocket Rules») и JSON-ping спота (ccxt `pro/bingx.py:1418-1435`) |
| `rest_depth.json` | `GET /openApi/swap/v2/quote/depth`: docs-v3 «Order Book» (notes §1.2) |
| `rest_premium_index.json` | `GET /openApi/swap/v2/quote/premiumIndex` (notes §1.4) |
| `rest_ticker.json` | `GET /openApi/swap/v2/quote/ticker` (notes §1.6) |
| `rest_contracts_*.json` | `GET /openApi/swap/v2/quote/contracts` (notes §1.5); `pricePrecision: 5` выдуман |
| `rest_error_symbol.json`, `rest_rate_limited.json` | коды 109425 / 100410 (notes §1.7); тексты `msg` выдуманы |

В тестах WS-кадры сжимаются `gzip.compress(...)` и подаются как `bytes` (сервер BingX шлёт gzip).
