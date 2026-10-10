# Gate spot WS/REST fixtures (SYNTHETIC)

All files here are synthetic: no live Gate frames were captured (network to Gate was
unavailable during research). Each JSON file is `{"_comment": ..., "frame" | "response": ...}`;
tests strip `_comment` and feed `frame` (as JSON text) to the adapter or `response` to a fake HTTP
client. Schemas follow `notes/api_gate_ws_futures.md` §1.3 (envelopes), §2.2 (`spot.order_book`),
§2.5 (`spot.trades`) and the Gate SDK model `gate_api/models/order_book.py`
(`GET /spot/order_book` -> `{id, current, update, asks, bids}`), `notes/api_gate_rest.md`
(`GET /spot/currency_pairs/{pair}`). Replace with captured frames once `doctor` runs with network.
