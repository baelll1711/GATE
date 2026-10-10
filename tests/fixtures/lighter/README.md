# Фикстуры Lighter (perp STONK, котировка USDC) — СИНТЕТИКА

Все файлы **синтетические**: `market_id = 4100`, `price_decimals = 5`, цены и размеры выдуманы и в коде как истина
не используются. Формы кадров — Lighter SDK 1.1.6, ccxt 4.5.85, NautilusTrader (`test_data/*.json`, снятые
с mainnet/testnet); разбор в `notes/api_lighter_fx.md`, часть A.

| Файлы | Схема (источник) |
|---|---|
| `ws_ob_snapshot*.json`, `ws_ob_update_*.json` | `subscribed/order_book` / `update/order_book`: Nautilus `ws_order_book_subscribed.json`, `ws_order_book_update.json`, `messages.rs:462-477, 640-648` (notes A3.2) |
| `ws_ob_update_no_begin_nonce.json` | форма ccxt `pro/lighter.py:136-159` без `begin_nonce` |
| `ws_ob_snapshot_slash_channel.json` | входящий `channel` с `/` вместо `:` (проверка терпимости разбора) |
| `ws_market_stats.json` | `update/market_stats`: ccxt `pro/lighter.py:224-273`, Nautilus `messages.rs:666-700` (notes A3.4) |
| `ws_connected.json`, `ws_ping*.json`, `ws_shutdown.json` | SDK `ws_client.py:53-79, 165-170` (notes A3.1, A3.5) |
| `ws_error_*.json` | оба формата ошибок: ccxt `pro/lighter.py:1268-1290`, Nautilus `ws_order_book_subscribe_failed.json` (notes A3.6) |
| `rest_order_book_details*.json` | `GET /api/v1/orderBookDetails`: SDK `models/perps_order_book_detail.py`, Nautilus `http_order_book_details.json` (notes A2.2); `_minimal` — без полей, отсутствующих в живом ответе 2026-05-05 |
| `rest_order_book_orders*.json` | `GET /api/v1/orderBookOrders` (отдельные ордера, L3): SDK `models/simple_order.py`, Nautilus `http_order_book_orders.json` (notes A2.3) |
| `rest_error_400.json` | `ResultCode {code, message}` (notes A2.3) |
