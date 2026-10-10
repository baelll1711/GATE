# FX fixtures (SYNTHETIC)

Synthetic frames for the USDC->USDT rate sources; no live capture. Schemas:
- `binance_depth5.json`: Binance spot partial book depth stream `usdcusdt@depth5@100ms`
  (`{lastUpdateId, bids, asks}`, no event time) — vendor/lighter_fx/binance/web-socket-streams.md
  "Partial Book Depth Streams"; reference conspectus notes/ref_feeds.md §3 (fx).
- `binance_book_ticker.json`: REST `GET /api/v3/ticker/bookTicker?symbol=USDCUSDT` — notes/api_lighter_fx.md B1.
- `gate_book_ticker.json`: Gate `spot.book_ticker` USDC_USDT `{t, u, s, b, B, a, A}` — notes/api_lighter_fx.md B2,
  notes/api_gate_ws_futures.md 2.1.
Each file is `{"_comment": ..., "frame" | "response": ...}`.
