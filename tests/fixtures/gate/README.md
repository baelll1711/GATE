# Gate spot REST fixtures (SYNTHETIC)

Every file in this folder is **synthetic**: no file is a recorded reply from Gate. The container
that produced them had no network access to `api.gateio.ws` (see the header of
`notes/api_gate_rest.md`). Shapes follow the official Python SDK models (gate-api 7.2.144,
"Version of the API: v4.106.144") and the real reply examples quoted by ccxt, as summarized in
`notes/api_gate_rest.md`; values (ids, prices, balances, timestamps) are invented for `STONK_USDT`.

File layout: `{"_comment": ..., "_schema_source": "<notes section>", "data": <reply body>}`.
Tests read `data` and build variants (other ids, sides, statuses) from these bases in code.

| File | Endpoint | Schema source (notes/api_gate_rest.md) |
|---|---|---|
| `currency_pair_stonk_usdt.json` | `GET /spot/currency_pairs/STONK_USDT` | §9 |
| `currency_pair_null_minimums.json` | same, `null` minimums/maximums, `trade_status: buyable` | §9 |
| `order_open.json` | `Order` (create reply / `GET /spot/orders/{id}` / list row) | §3.2, §3.4 |
| `order_poc_cancelled.json` | `Order` with `status=cancelled, finish_as=poc` (post-only refusal) | §3.2, §13.2 |
| `batch_orders_reply.json` | `POST /spot/batch_orders` (`BatchOrder` items, shuffled order) | §5.1 |
| `cancel_batch_reply.json` | `POST /spot/cancel_batch_orders` (`CancelOrderResult` items) | §5.2 |
| `cancel_all_reply.json` | `DELETE /spot/orders?currency_pair=` (`OrderCancel` items) | §5.3 |
| `my_trades_row.json` | one `GET /spot/my_trades` row (`create_time_ms` decimal string) | §8 |
| `spot_accounts.json` | `GET /spot/accounts` | §10.2 |
| `unified_accounts.json` | `GET /unified/accounts` (`UnifiedAccount` / `UnifiedBalance`) | §10.5 |
| `account_detail_classic.json` | `GET /account/detail` (`key.mode = 1`) | §10.4 |
| `unified_mode_classic.json` | `GET /unified/unified_mode` | §10.5 |
| `wallet_fee.json` | `GET /wallet/fee?currency_pair=` | §10.3 |
| `debit_fee.json` | `GET /account/debit_fee` | §10.3 |
| `countdown_reply.json` | `POST /spot/countdown_cancel_all` (`triggerTime`, camelCase) | §7 |
| `server_time.json` | `GET /spot/time` | §10.1 |
| `error_post_only.json` | error body `POC_FILL_IMMEDIATELY` | §11.1 |
| `error_order_not_found.json` | error body `ORDER_NOT_FOUND` | §11.2 |

Signature test vectors are not stored here: `tests/test_gate_rest.py` recomputes them with an
independent snippet and compares with notes §1.3.

Re-check every shape against real replies at stage 5 (live read-only checks) and replace values
that turn out different; keep the `_comment` marker until a file is a real recording.

Real recordings go to `recorded/` (created on the first promotion), never into this folder: the
venue tests assert the `SYNTHETIC` marker of every file they read from here. `python -m gate_stonk smoke-test` saves each
sanitized Gate reply as a candidate fixture under `<state_dir>/smoke/fixtures/<UTC>/` in this same
layout, with `_comment` starting `RECORDED` and an extra `_request` block (method, endpoint, query,
body, HTTP status, label, latency); the `README.md` written next to them explains the promotion.
