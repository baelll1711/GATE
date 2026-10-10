# Фикстуры Gate USDT-фьючерсов (STONK_USDT) — СИНТЕТИКА

Все файлы **синтетические**: значения для STONK_USDT выдуманы, живых ответов нет. Схема — официальные SDK Gate
(`gate-ws` 0.5.6, Go WS SDK, `gate-api` 7.2.144, `openapi.yaml` v4.106.144), ccxt 4.5.85, gocryptotrader; разбор в
`notes/api_gate_ws_futures.md`.

| Файлы | Схема (источник) |
|---|---|
| `ws_book_ticker*.json` | `futures.book_ticker`: Go SDK `response_future.go:76-84`, GCT `gateio_types.go:2295-2303` (notes §4.1); `_int_sizes` — размеры целыми (без `X-Gate-Size-Decimal`) |
| `ws_tickers.json` | `futures.tickers` (`result` — массив): Go SDK `response_future.go:3-34` (notes §4.5) |
| `ws_sub_ack.json`, `ws_sub_error.json` | конверты подписки: ccxt `pro/gate.py:1781-1800, 1864-1879` (notes §1.3) |
| `ws_pong.json` | `futures.pong` (notes §1.4) |
| `ws_channel_error.json` | конверт `error {code, message}` на канале данных (notes §1.3) |
| `rest_order_book.json` | `GET /futures/usdt/order_book`: OA схема `FuturesOrderBook` (`current` — секунды, уровни `{p, s}`) (notes §5.2) |
| `rest_tickers.json` | `GET /futures/usdt/tickers` (массив): OA / SDK `models/futures_ticker.py` (notes §5.3) |
| `rest_contract_*.json` | `GET /futures/usdt/contracts/{contract}`: OA схема `Contract` (notes §5.1) |
| `rest_contract_not_found.json`, `rest_error_429.json` | тело ошибки `{label, message}` (`api_gate_rest.md` §11); label из карты ccxt `gate.py:1206` |
