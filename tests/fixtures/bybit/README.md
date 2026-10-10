# Фикстуры Bybit V5 (linear, STONKUSDT) — СИНТЕТИКА

Все файлы этой папки **синтетические**: живых ответов Bybit в момент написания не было (сеть закрыта).
Значения STONK (цены около 0.1359, объёмы, `u`, `ts`) выдуманы и не являются рыночными данными.
Структура повторяет документацию Bybit (репозиторий `bybit-exchange/docs`, ветка `master`), разобранную в
`notes/api_bybit.md`:

| Файлы | Схема (источник) |
|---|---|
| `ws_ob1_*.json` | WS `orderbook.1.<SYM>`: `docs/v5/websocket/public/orderbook.mdx:61-77` (notes §4.4, §8). `ws_ob1_ts_string.json` — `ts` строкой (расхождение примеров документации, notes §7.6) |
| `ws_tickers_*.json` | WS `tickers.<SYM>` snapshot/delta: `docs/v5/websocket/public/ticker.mdx:9-136, 361-395` (notes §4.5) |
| `ws_sub_ok.json`, `ws_pong.json` | дословно `wss-authentication.mdx:177-185, 297-305` (notes §4.2) |
| `ws_sub_fail.json` | конверт отказа по pybit `_websocket_stream.py:525-529`, текст `ret_msg` выдуман |
| `rest_orderbook.json` | `GET /v5/market/orderbook`: `orderbook.mdx:10-42, 123-151` (notes §3.2) |
| `rest_tickers.json` | `GET /v5/market/tickers`: `tickers.mdx:14-133` (notes §3.1) |
| `rest_rate_limited.json` | `retCode 10006` «Too many visits!»: `error.mdx:39` (notes §6) |
| `instruments_info_*.json` | `GET /v5/market/instruments-info`: `instrument.mdx:40-200, 622-680` (notes §2.2, §8) |
| `ws_not_json.txt` | произвольный не-JSON кадр для проверки устойчивости парсера |
