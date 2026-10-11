# Исследование API (этап 0): Gate spot STONK_USDT и индекс по перпетуалам STONK

Дата: **2026-10-10**. Проект: `gate_stonk`. Документ сводит шесть рабочих заметок этапа 0: Gate REST, Gate WS
и futures, Bybit, Bitget, BingX, Lighter + FX. Обязательные для кода решения записаны в контракте реализации
(далее DESIGN, §15–16). Здесь собраны факты, их статус и то, что из них следует для бота.

## Главное

1. **Живых ответов бирж нет.** Из контейнера сеть к биржам закрыта, всё собрано по SDK, репозиториям
   документации, сторонним библиотекам и выдаче поиска. Ни одно значение для STONK (tick, минимумы,
   market_index, точное имя символа) **не подтверждено**: это проверяется на этапе 1 (§9).
2. **Gate: отказ post-only приходит двумя путями.** Либо ошибкой `POC_FILL_IMMEDIATELY` (заявка не создана),
   либо созданной заявкой со `status=cancelled, finish_as=poc`. Бот считает оба пути отказом post-only.
3. **Gate: поиск по `text` работает только для открытых заявок**, пакетная отмена по `text` — только 30 минут
   после создания. Поэтому бот сразу запоминает `order_id` и отменяет по нему.
4. **Gate: лимиты.** Создание и amend делят счётчик 10 r/s на UID+рынок. При **низком fill ratio** лимит
   падает до 10 запросов за 10 с. Бот закладывает 8 запросов за 10 с и создаёт заявки пакетами.
5. **Gate: скрытая часть iceberg исполняется по taker-ставке.** Совместимость `iceberg` с `poc` не
   документирована, поэтому в live iceberg выключен до проверки на этапе 5.
6. **Gate: `x-gate-exptime` (мс, не подписывается)** есть у создания, отмены и amend. У DMS
   (`countdown_cancel_all`) его нет.
7. **Bybit: правило «тихого L1» подтверждено.** Без изменений `orderbook.1` повторяет снимок через 3 с с тем же
   `u`. Не подтверждено, обновляется ли при этом `ts`, а от этого зависит, будет ли блокироваться обязательный
   источник Bybit. Проверка №1 этапа 1.
8. **Bitget `books1` молчит, пока книга не меняется.** Поэтому нужен REST-опрос. **У BingX `asks` идут по
   убыванию**, а REST bookTicker вложен в объект и отдаёт цены JSON-числами.
9. **Lighter.** Хост `mainnet.zklighter.elliot.ai`. `market_index` ищется через REST. Непрерывность книги
   проверяется по `begin_nonce` (не по `offset`). Котировка в USDC.
10. **FX USDC→USDT.** Существование `USDCUSDT` на Binance и `USDC_USDT` на Gate официально не подтверждено.
    Режим по умолчанию `fixed` защищён только фильтром выбросов (§7).
11. **Возможная петля.** Неизвестно, входит ли Gate spot в индекс Gate futures STONK. Поэтому `gate_perp`
    берёт `mid` из стакана (не mark), а doctor проверяет `index_constituents`.

---

## 0. Как проверялись факты

### 0.1 Условия

- Из контейнера закрыт доступ к биржам и их документации: DNS `ENOTFOUND` или прокси 403. Это касается
  api.gateio.ws, gate.com, api.bybit.com, api.bitget.com, open-api.bingx.com, mainnet.zklighter.elliot.ai и
  api.binance.com. Работают PyPI, GitHub (raw), proxy.golang.org и WebSearch.
- Источники четырёх видов:
  1. официальные SDK с PyPI и Go proxy: исходники прочитаны, против бирж не запускались;
  2. официальные репозитории документации на GitHub;
  3. сторонние библиотеки (ccxt, gocryptotrader, NautilusTrader и др.): в них есть реальные примеры ответов в
     комментариях и фикстуры, снятые с бирж;
  4. фрагменты официальных страниц в выдаче WebSearch: сами страницы не открывались.
- Ни один пример в документе мы не снимали с биржи сами. Примеры помечены «дословно из <источник>» или
  «СИНТЕТИКА (схема по …)». Все значения для STONK синтетические.
- Требования промта взяты из рабочих заметок, которые с ним сверялись. В этой сессии файл промта не читался.

### 0.2 Легенда статусов

| Статус | Значение |
|---|---|
| **ПОДТВЕРЖДЕНО** | Факт прямо следует из указанного источника: код или модель официального SDK, официальный репозиторий документации, реальный пример ответа в библиотеке. Сторонний источник виден по коду ссылки (CCXT, GCT, NT, SIEBLY, JKORF). |
| **ПОДТВЕРЖДЕНО по поиску** | Есть только фрагмент официальной страницы в выдаче WebSearch, сама страница не открыта. Перепроверить при открытой сети. |
| **НЕ ПОДТВЕРЖДЕНО** | Источника нет, источники противоречат друг другу, или это наш вывод либо предположение. |

Правила записи. Ссылка на источник без пометки значит «ПОДТВЕРЖДЕНО этим источником». «Вывод» и «Рекомендация»
— наши решения, а не факты биржи.

### 0.3 Коды источников (полный список и URL — §10)

| Код | Источник | Версия |
|---|---|---|
| GSDK | официальный Python SDK `gate-api`; ссылки вида `spot_api.py:2093` указывают файл и строку внутри `gate_api/` | 7.2.144 (API v4.106.144) |
| GDOC | `gateio/gateapi-python` docs/*.md | master (7.1.8, старее GSDK) |
| GOA | `openapi.yaml` из официального `gate/gateapi-go/v7` | v7.2.144 |
| GWSPY, GWSGO, GWSEX | официальные WS SDK Gate: Python `gate-ws`, Go `gate/gatews`, пример `local_order_book.py` | 0.5.6; Go от 2026-09-01; пример 2021 г. |
| CCXT | ccxt (`gate.py`, `pro/gate.py`, `bitget.py`, `bingx.py`, `lighter.py`) | 4.5.85 |
| GCT | thrasher-corp/gocryptotrader `exchanges/gateio`, проверен на живом рынке (PR #2366) | master 2026-09 |
| GOGATE, HB, RESTV4 | go-gate v2.8.0 (третьестепенный); hummingbot; README `gateio/rest-v4` v4.22.0 (устарел) | — |
| BYD, BYP | документация Bybit (`bybit-exchange/docs`, ветка master; changelog до 2026-09-30); `pybit` | master; 5.17.0 |
| BGSDK, SIEBLY, JKORF | официальный SDK Bitget (`v3-bitget-api-sdk`); npm `bitget-api`; `JKorf/Bitget.Net` | master; 3.2.5; main |
| BXD, BXSKILL, BXV2 | документация BingX docs-v3 (смещения `@N` в JS-бандле); `BingX-API/api-ai-skills`; старая V2 | коммит 0cc3b7c (2026-10-09); 38e99a1; 2023-08 |
| LSDK, LOA, NT | `lighter-sdk` (PyPI); `openapi.json` lighter-python; NautilusTrader (дока + Rust-адаптер + фикстуры с mainnet от 2026-05-05) | 1.1.6; main; develop |
| BND | `binance/binance-spot-api-docs` | CHANGELOG до 2026-09-18 |
| REF | референсный бот `/home/user/12-chagpt` (`spot_portfolio/feeds.py`, `zec_spot/feeds.py`) | — |
| W | фрагмент официальной страницы в выдаче поиска (URL у факта) | — |

---

## 1. Gate spot REST (`https://api.gateio.ws/api/v4`)

### 1.1 Доступ

- Базовый хост `https://api.gateio.ws/api/v4` (GSDK `configuration.py:65`).
- Публичные эндпоинты (без подписи): `/spot/currencies*`, `/spot/currency_pairs*`, `/spot/tickers`,
  `/spot/order_book`, `/spot/trades`, `/spot/candlesticks`, `/spot/time`. Остальные эндпоинты спота подписываются
  (GSDK `auth_settings` в `spot_api.py`).

### 1.2 Подпись APIv4 — ПОДТВЕРЖДЕНО (GSDK `api_client.py:544-594`; CCXT `gate.py:6782-6804`)

- Строка подписи: `METHOD\n/api/v4<path>\n<query>\n<hex(sha512(body))>\n<timestamp>`. Это пять полей через `\n`,
  в конце `\n` нет. Подпись = `hex(HMAC-SHA512(secret, строка))`. Заголовки: `KEY`, `Timestamp`, `SIGN`,
  плюс `Content-Type: application/json`.
- Путь подписывается **с префиксом `/api/v4`**.
- Query подписывается раскодированной строкой без `?`, в том же порядке параметров, что уходит в URL. Без
  параметров query — пустая строка.
- Хэшируются ровно те байты тела, которые отправляются. Для пустого тела берётся sha512 пустой строки
  (`cf83e135…927da3e`).
- `Timestamp` — Unix-секунды. SDK шлёт дробные, ccxt — целые; бот шлёт целые. Допуск ±60 с, иначе label
  `REQUEST_EXPIRED` — ПОДТВЕРЖДЕНО по поиску (`gate.com/docs/developers/apiv4/en/`).
- Тестовые векторы: `secret="SECRET_TEST"`, `Timestamp="1760000000"`. Посчитаны по алгоритму GSDK и
  перепроверены скриптом из §9.3.
  - `POST /spot/orders`, тело `{"text": "t-stonk1", "currency_pair": "STONK_USDT", "type": "limit", "account": "spot", "side": "buy", "amount": "100", "price": "0.1355", "time_in_force": "poc"}`
    (это `json.dumps` с настройками по умолчанию) → `d744128d089b6333ccf6136e235b55934c81f5f3b7e01fb80b98c468d29ad7f13ab5c5ce7f9d00115d79ce070635b43b76941752f5321a3804923d1edebf1352`.
  - `GET /spot/orders`, query `currency_pair=STONK_USDT&status=open` → `1a50511f4d72070e2a8613a4ad4afc332def58d7192cb36834a8ba5dde6a7d862e48fa62c495bdb97a66cd53f5de4b6a34ff7384f807cee7bee92f4e459bfe4d`.
  - `DELETE /spot/orders/12345`, query `currency_pair=STONK_USDT` → `6c68029245937e6807f63583b8cd730c94c193f770d16050f5fbb33aa4dc37e87c42a8a83f2cf09f29ab74382ff7b3814387aa40889f6398ccaa17ed22cca795`.

### 1.3 Заголовок `x-gate-exptime` — ПОДТВЕРЖДЕНО (GSDK `spot_api.py:2016`, `header_params['x-gate-exptime']`)

Значение — строка с **мс Unix epoch**: «если GATE получает запрос позже этого времени, запрос отклоняется».
Заголовок **не входит в подпись**.

| Метод | HTTP | GSDK |
|---|---|---|
| create_order | `POST /spot/orders` | `spot_api.py:2093` |
| create_batch_orders | `POST /spot/batch_orders` | `:1567` |
| cancel_orders (все по паре) | `DELETE /spot/orders` | `:2234` |
| cancel_batch_orders | `POST /spot/cancel_batch_orders` | `:2351` |
| cancel_order | `DELETE /spot/orders/{order_id}` | `:2623` |
| amend_order | `PATCH /spot/orders/{order_id}` | `:2759` |
| amend_batch_orders | `POST /spot/amend_batch_orders` | `:3249` |

- У `countdown_cancel_all` и всех GET заголовка нет (grep по GSDK).
- Какой label и HTTP-код приходят при просрочке — **НЕ ПОДТВЕРЖДЕНО**. Проверка на этапе 5 (§9.2, шаг 5.2).
- Вывод: гарантия «не исполнено после exptime» действует в момент **получения** запроса биржей. Если запрос
  принят, а ответ потерян, заявка может существовать. Отсюда в боте поиск «неизвестной» заявки по `text` (§1.9).

### 1.4 Создание: `POST /spot/orders` → HTTP **201**, тело `Order` — ПОДТВЕРЖДЕНО (GSDK `spot_api.py:2005-2120`, `models/order.py`)

| Поле | Обяз. | Правило |
|---|---|---|
| `currency_pair` | да | `STONK_USDT` |
| `side` | да | `buy` \| `sell` |
| `amount` | да | у `limit` в **базовой** валюте (STONK) |
| `price` | да (limit) | строка на сетке `precision` |
| `type` | нет, по умолч. `limit` | `limit` \| `market` |
| `account` | нет, по умолч. `spot` | `spot`, `margin`, `unified` (`order.py:527`). SDK **всегда** шлёт `account:"spot"`. Для unified-аккаунта «по умолчанию unified» (`spot_api.py:2008`) |
| `time_in_force` | нет, по умолч. `gtc` | `gtc`, `ioc`, `poc` («post-only, always enjoys a maker fee»), `fok` (`order.py:621`) |
| `iceberg` | нет | видимое количество; `null`/`0` — обычная заявка; скрыть всё нельзя (`order.py:650`) |
| `text` | нет | префикс `t-`, **≤ 28 байт без префикса**, символы `0-9 A-Z a-z _ - .` (`order.py:275`) |
| `stp_act` | нет | `cn`/`co`/`cb`. Если аккаунт не в STP-группе, передача поля — ошибка (`order.py:1072`). **Бот не шлёт** |
| `action_mode` | нет | `ACK` \| `RESULT` \| `FULL` (по умолч.). **Бот не шлёт** (FULL) |

- В ответе эхо полей запроса и ещё: `id` (строка), `create_time`/`update_time` (с, строкой),
  `create_time_ms`/`update_time_ms` (мс; в примерах CCXT бывают строкой — парсить и число, и строку),
  `status` (`open` \| `closed` \| `cancelled`; `cancelled` и при частичном исполнении), `left`, `filled_amount`,
  `filled_total`, `avg_deal_price`, `fee`, `fee_currency`, `point_fee`, `gt_fee`, `gt_maker_fee`,
  `gt_taker_fee`, `gt_discount`, `rebated_fee`, `rebated_fee_currency`, `stp_id`, `amend_text`, `finish_as`.
- Комиссия BUY списывается в **базовой** валюте, SELL — в котируемой (реальные примеры CCXT `gate.py:4755-4782`,
  `:3846-3858`). При оплате GT или поинтами `fee` бывает 0, а списание идёт в `gt_fee`/`point_fee`.
- Какие поля остаются в режимах `ACK`/`RESULT` — **НЕ ПОДТВЕРЖДЕНО**. Клиент считает необязательными все поля,
  кроме `id`/`text`/`status`.
- Пример ответа (СИНТЕТИКА по схеме GSDK и реальному примеру CCXT `gate.py:4755-4782`):
  `{"id":"912345678901","text":"t-stk1-b1-000001","status":"open","currency_pair":"STONK_USDT","type":"limit","account":"spot","side":"buy","amount":"150","price":"0.1345","time_in_force":"poc","iceberg":"0","left":"150","filled_amount":"0","fee":"0","fee_currency":"STONK","create_time_ms":1760090000123,"update_time_ms":1760090000123,"finish_as":"open"}`.

### 1.5 Пакетное создание `POST /spot/batch_orders` — ПОДТВЕРЖДЕНО (GSDK `spot_api.py:1479-1592`, `batch_order.py`)

- Тело — JSON-массив `Order`. `text` **обязателен** у каждой заявки. До 4 пар, **до 10 заявок на пару**, все
  `account` одинаковые.
- Ответ HTTP 200 даже при частичном отказе. Каждый элемент содержит `succeeded`, `label`, `message`, `text`, `id`
  и поля `Order`. Реальный отказ (CCXT `gate.py:4886-4893`):
  `{"text":"t-d18baf9ac44d82e2","succeeded":false,"label":"BALANCE_NOT_ENOUGH","message":"Not enough balance"}`.
- Совпадает ли порядок ответа с порядком запроса — **НЕ ПОДТВЕРЖДЕНО**. Сопоставлять элементы по `text`.

### 1.6 Отмены — ПОДТВЕРЖДЕНО (GSDK)

- `DELETE /spot/orders/{order_id}?currency_pair=…[&account=…]` (`spot_api.py:2514-2644`): `currency_pair`
  обязателен, ответ — `Order`. В пути можно передать `text`, но только пока заявка открыта.
- `POST /spot/cancel_batch_orders` (`:2263-2378`): **до 20** элементов `{currency_pair, id, account}`. `id` —
  order id или `text`, причём **`text` принимается только 30 минут после создания** (`cancel_batch_order.py:98`).
  Для unified-ключа `account` обязателен и равен `"unified"`. Ответ: элементы
  `{currency_pair, id, text, succeeded, label, message}`.
- `DELETE /spot/orders?currency_pair=…&account=…` (`:2128-2255`): **без `currency_pair` отменяются заявки по всем
  парам**. Без `account` в классическом режиме отменяются spot, unified и isolated margin. Ответ — список
  `OrderCancel` (поля `Order` плюс `succeeded/label/message`); `finish_as` в нём ограничен
  `open|filled|cancelled|ioc|stp` (`order_cancel.py:1142`).
- Отмена уже закрытой или несуществующей заявки возвращает label из семейства `ORDER_NOT_FOUND` /
  `ORDER_CLOSED` / `ORDER_CANCELLED` (CCXT `gate.py:1182-1185`). Какой именно label в каком случае —
  **НЕ ПОДТВЕРЖДЕНО**.

### 1.7 Amend — ПОДТВЕРЖДЕНО (GSDK `spot_api.py:2652-2786`, `order_patch.py`, `batch_amend_item.py`)

- `PATCH /spot/orders/{order_id}`, тело `{currency_pair, account, price | amount, amend_text (≤ 31 символа)}`.
  Должно быть указано хотя бы одно из `amount`/`price`. Ответ — `Order`. Если `currency_pair` есть и в теле, и
  в query, приоритет у тела.
- «Order modification and order creation share the same rate limit rules» (`:2655`).
- Приоритет в очереди: сохраняется только при **уменьшении количества**. Изменение цены или увеличение
  количества переносит заявку в конец нового уровня. Количество меньше уже исполненного отменяет заявку.
- `POST /spot/amend_batch_orders`: до 5 элементов, «Only one of amount or price», порядок ответа = порядку
  запроса.
- Amend без изменений, по CCXT, отвечает `{"label":"NO_CHANGE","message":"No change is made"}`.
- **НЕ ПОДТВЕРЖДЕНО:** можно ли в одном PATCH передать сразу `price` и `amount`; проверяется ли `poc` при
  изменении цены (может ли заявка стать taker).

### 1.8 DMS: `POST /spot/countdown_cancel_all` — ПОДТВЕРЖДЕНО (GSDK `spot_api.py:3043-3153`, `countdown_cancel_all_spot_task.py:64`, `trigger_time.py:40`)

- Тело `{"timeout": int (с; минимум 5; 0 выключает), "currency_pair": "STONK_USDT"}`. Без `currency_pair`
  действует на **все рынки**.
- Ответ `{"triggerTime": <мс>}`; ключ в camelCase. `x-gate-exptime` у метода нет.
- Если до `timeout` не пришёл новый вызов, отменяются открытые спот-заявки рынка. Пример в доке: вызывать
  каждые 30 с с `timeout=30`; такой интервал не даёт запаса на сбой.
- Действует ли DMS на заявки `account=unified` — **НЕ ПОДТВЕРЖДЕНО** (в тексте «spot pending orders»).

### 1.9 Чтение заявок — ПОДТВЕРЖДЕНО (GSDK)

- `GET /spot/orders/{order_id}?currency_pair=…&account=…` (`spot_api.py:2386-2506`). В пути — id или `text`,
  дословно: «Operations based on custom IDs can only be checked in pending orders. Only order ID can be used after
  the order is finished» (`:2396`). `currency_pair` SDK требует всегда. Без `account` поиск идёт по spot,
  unified и isolated margin. Не найдено — `ORDER_NOT_FOUND` или `CLIENT_ID_NOT_FOUND` (CCXT).
- `GET /spot/orders?currency_pair=&status=open|finished&page=&limit=&account=&from=&to=&side=` (`:1846-1997`):
  - для `open`: `currency_pair` обязателен, `limit` ≤ 100, только `page`/`limit`;
  - для `finished`: `from`/`to` фильтруют по **времени завершения**, в секундах (так у CCXT; в SDK единицы не
    указаны). Максимальный `limit` для `finished` **НЕ ПОДТВЕРЖДЕН** (SDK пропускает до 1000).
- `GET /spot/open_orders` (`:1602-1720`) пагинирует **внутри каждой пары** (до 100). Ответ:
  `[{currency_pair, total, orders:[…]}]`.
- Фраза «можно найти в течение 1 часа после завершения» есть только у параметра `last_id` **публичного**
  `/spot/trades` (`:729`). Для поиска заявки по `text` она не подтверждена.

### 1.10 Свои сделки: `GET /spot/my_trades` — ПОДТВЕРЖДЕНО (GSDK `spot_api.py:2794-2932`, `models/trade.py`)

- Параметры: `currency_pair`, `limit` (1..1000), `page`, `order_id` (вместе с `currency_pair`), `from`/`to` (с),
  `account` (deprecated: без него возвращаются все счета).
- Без времени отдаются **последние 7 дней**. Диапазон **≤ 30 дней**. Ограничение `limit*(page-1) ≤ 100000`.
- Поля: `id`, `create_time` (с), **`create_time_ms` — строка с дробной частью** (`"1645464610777.399200"`, CCXT
  `gate.py:3848`), `currency_pair`, `side`, `role` (`maker`/`taker`), `amount`, `price`, `order_id`, `fee`,
  `fee_currency`, `point_fee`, `gt_fee`, `amend_text`, `sequence_id`, **`text` (custom text заявки)**
  (`trade.py:510`), `deal`.
- Реальный пример (CCXT `gate.py:3845-3859`):
  `{"id":"2876130500","create_time":"1645464610","create_time_ms":"1645464610777.399200","currency_pair":"DOGE_USDT","side":"sell","role":"taker","amount":"10.97","price":"0.137384","order_id":"125924049993","fee":"0.00301420496","fee_currency":"USDT","point_fee":"1.1","gt_fee":"2.2"}`.

### 1.11 Метаданные пары: `GET /spot/currency_pairs/STONK_USDT` — ПОДТВЕРЖДЕНО (GSDK `spot_api.py:359-463`, `currency_pair.py`)

- Поля: `id`, `base`, `quote`, `fee` (deprecated), `min_base_amount`, `min_quote_amount`, `max_base_amount`,
  `max_quote_amount` (`null` означает «без лимита»), `amount_precision`, `precision` (знаки цены),
  `trade_status`, `sell_start`, `buy_start`, `delisting_time`, `type` (`normal`/`premarket`), `st_tag`,
  `up_rate`/`down_rate`, `trade_quotes`.
- `trade_status`: `tradable` (обе стороны), `buyable` (только BUY), `sellable` (только SELL), `untradable`
  (никакие).
- Реальные значения для STONK_USDT — **НЕ ПОДТВЕРЖДЕНО**. Пример из CCXT: у некоторых пар минимум 3 USDT.
- Связь `up_rate`/`down_rate` с отказом `PRICE_THRESHOLD_EXCEEDED` — **НЕ ПОДТВЕРЖДЕНО**.

### 1.12 Время, балансы, комиссии, аккаунт — ПОДТВЕРЖДЕНО (GSDK)

- `GET /spot/time` → `{"server_time": <мс>}` (публичный).
- `GET /spot/accounts` → `[{currency, available, locked, update_id}]`. Что он отдаёт при unified-аккаунте —
  **НЕ ПОДТВЕРЖДЕНО**: ccxt в этом случае читает `GET /unified/accounts`. `GET /wallet/total_balance`
  кэшируется до 1 минуты и для расчётов не годится (`wallet_api.py:2080`).
- `GET /wallet/fee?currency_pair=` → `maker_fee`/`taker_fee` (**доли**: `"0.002"` = 0.2%), `gt_discount`,
  `gt_maker_fee`, `debit_fee` (1 GT, 2 point card, 3 VIP) и др. (`wallet_api.py:1964-2069`). `GET /spot/fee`
  устарел (deprecated).
- `GET /account/debit_fee` → `{"enabled": bool}`, то есть включено ли списание комиссии в GT.
- `GET /account/detail` → `user_id`, `ip_whitelist`, **`currency_pairs`** (белый список пар ключа: если он не
  пуст, в нём должна быть `STONK_USDT`), `key.mode` (**1 — classic, 2 — legacy unified**), `tier`.
  Прав ключа в ответе **нет** (`account_api.py:39-134`; реальный пример CCXT `gate.py:1268-1276`).
- `GET /unified/unified_mode` → `mode` ∈ `classic | multi_currency | portfolio | single_currency`
  (`unified_api.py:1229-1326`). `PUT` по тому же пути меняет режим: боту его не трогать.
- Как узнать права текущего ключа — **НЕ ПОДТВЕРЖДЕНО**. Такого эндпоинта не найдено. `GET /account/main_keys`
  есть, но неясно, отдаёт ли он текущий ключ; `GET /account/rate_limit` «not yet open for use».

### 1.13 Ошибки и label

Формат — ПОДТВЕРЖДЕНО (GSDK `rest.py:223`, `exceptions.py:112-127`). Любой код вне 2xx — ошибка. Тело —
`{"label": "...", "message": "..."}`, иногда с `detail`. Какие HTTP-коды соответствуют каким label —
**НЕ ПОДТВЕРЖДЕНО**, поэтому классифицировать ответы надо по `label`.

| Label | Смысл | Статус |
|---|---|---|
| `POC_FILL_IMMEDIATELY` | spot: post-only исполнился бы как taker, заявка не принята. Реальное тело: `{"label":"POC_FILL_IMMEDIATELY","message":"Order would match and take immediately so its cancelled"}` | ПОДТВЕРЖДЕНО по поиску (таблица label) + CCXT `gate.py:1181` |
| `ORDER_POC_IMMEDIATE` | то же для **фьючерсов** | CCXT `gate.py:1227`; на споте не ожидается |
| `POC_REJECT` | не найден ни в одном источнике | **НЕ ПОДТВЕРЖДЕНО** |
| `BALANCE_NOT_ENOUGH`, `ORDER_NOT_FOUND`, `ORDER_CLOSED`, `ORDER_CANCELLED`, `INVALID_PRECISION`, `AMOUNT_TOO_LITTLE`, `AMOUNT_TOO_MUCH`, `TOO_MANY_ORDERS` | отказы торговли | ПОДТВЕРЖДЕНО по поиску + CCXT |
| `CLIENT_ID_NOT_FOUND`, `QUANTITY_NOT_ENOUGH`, `REPEATED_CREATION`, `TOO_MANY_CURRENCY_PAIRS`, `MIXED_ACCOUNT_TYPE`, `PRICE_THRESHOLD_EXCEEDED`, `TRADE_RESTRICTED`, `NO_CHANGE` | отказы торговли | CCXT (смысл `QUANTITY_NOT_ENOUGH`, `REPEATED_CREATION` **НЕ ПОДТВЕРЖДЕН**) |
| `INVALID_PARAM_VALUE`, `INVALID_ARGUMENT`, `INVALID_REQUEST_BODY`, `MISSING_REQUIRED_PARAM`, `BAD_REQUEST`, `INVALID_CURRENCY_PAIR` | параметры | CCXT |
| `REQUEST_EXPIRED` | `Timestamp` отличается от серверного времени больше чем на 60 с | ПОДТВЕРЖДЕНО по поиску + CCXT `gate.py:1161` |
| `INVALID_SIGNATURE`, `INVALID_KEY`, `INVALID_CREDENTIALS`, `AUTHENTICATION_FAILED`, `MISSING_REQUIRED_HEADER`, `IP_FORBIDDEN`, `READ_ONLY`, `FORBIDDEN`, `ACCOUNT_LOCKED` | аутентификация и права | CCXT `gate.py:1153-1163` |
| `TOO_MANY_REQUESTS`, `TOO_FAST` | превышение частоты | CCXT `gate.py:1200` |
| `INTERNAL`, `SERVER_ERROR`, `TOO_BUSY` | сбой биржи: **результат запроса неизвестен** | CCXT |
| label при просроченном `x-gate-exptime` | — | **НЕ ПОДТВЕРЖДЕНО** |

**Два пути отказа post-only на споте.** Какой из них используется на практике — **НЕ ПОДТВЕРЖДЕНО**:

- (а) ошибка `POC_FILL_IMMEDIATELY`: заявка **не создана**. Источники: таблица label Gate и CCXT.
- (б) успешный ответ, в котором заявка уже в состоянии `status=cancelled, finish_as=poc`: «rejected after being
  detected as taker». Источник: перечень `finish_as` в GSDK `order.py:1095-1110`.

Бот обрабатывает оба пути одинаково (DESIGN §7, §11). При (а) создание возвращает `Rejected(label)`, при (б) —
`VenueOrder`. В обоих случаях слот получает паузу отказа post-only: 0, 0, 5, 10, 20, 40, 60 с.

**Классификация в боте (DESIGN §7, §15):**
- labelled 4xx → `Rejected(label)`;
- `INTERNAL`/`SERVER_ERROR`/`TOO_BUSY`, таймауты, 5xx, не-JSON → `UnknownResult` для записи и `ReadError` для
  чтения;
- `TOO_MANY_REQUESTS`/`TOO_FAST`: при записи — `Rejected`, плюс блокировка лимитера на 15 с; при чтении —
  `RateLimited`. Основание: «запрос сверх burst отклоняется» (RESTV4; help 17521 — ПОДТВЕРЖДЕНО по поиску);
- auth-ошибки → `Rejected`, после чего engine останавливается.

### 1.14 Лимиты запросов (спот)

| Группа | Лимит | Основа | Статус |
|---|---|---|---|
| Создание (single + batch) **и amend**, общий счётчик | **10 r/s** | UID + рынок | ПОДТВЕРЖДЕНО по поиску (объявление 40657, с 2024-11-14; help 17521). Общий счётчик с amend ПОДТВЕРЖДЕН (GSDK `spot_api.py:2655`) |
| **Штраф за низкий fill ratio** | place/amend ограничиваются до **10 r / 10 s** | UID | ПОДТВЕРЖДЕНО по поиску (40657): окно **24 ч**, пересчёт **ежечасно**, учитываются **все** запросы на создание, отмену и изменение, включая неуспешные; рекомендуется ratio > 0.1. Точные пороги для спота **НЕ ПОДТВЕРЖДЕНЫ** (у перпетуалов было 86 400 запросов/24 ч и 1% — не переносить) |
| Отмена (single + batch) | 200 r/s | UID | ПОДТВЕРЖДЕНО по поиску (объявление 33995; help 17521) |
| Прочие приватные | 200 r / 10 s на эндпоинт | UID | ПОДТВЕРЖДЕНО по поиску (33995) |
| Публичные | 200 r / 10 s на эндпоинт | IP | ПОДТВЕРЖДЕНО по поиску (33995). Старая таблица (900 r/s, RESTV4) противоречит; берём консервативное значение |

- Заголовки `X-Gate-RateLimit-Requests-Remain/-Limit/-Reset-Timestamp` в REST — **НЕ ПОДТВЕРЖДЕНО**. Их аналоги
  найдены только в WS-API. Если заголовки придут, бот их читает, но не полагается на них.
- HTTP 429 с `Retry-After` — **НЕ ПОДТВЕРЖДЕНО**.
- Вывод для бота. При тесте далеко от рынка исполнений почти нет, поэтому штраф вероятен: в среднем ~1
  запрос/с на создание и amend. Отсюда:
  - корзина `order` = `safety.max_order_requests_per_10s: 8` на 10 с (ниже штрафного уровня);
  - пакетное создание считается **одним** запросом;
  - отмены и DMS идут как «аварийные»: учитываются, но никогда не блокируются;
  - прочие группы: документированный лимит × `rate_limit_utilization: 0.5` (DESIGN §10).

### 1.15 Iceberg

- `iceberg` — видимое количество. Полностью скрыть заявку нельзя (GSDK `order.py:650`).
- **Исполнения скрытой части тарифицируются по taker-ставке.** Дословно: «fills against the hidden portion are
  charged at the taker fee rate» (GSDK `spot_api.py:2008`; help `gate.com/help/trade/entrusted_transaction/40463`
  — ПОДТВЕРЖДЕНО по поиску).
- **НЕ ПОДТВЕРЖДЕНО:**
  - совместимость `iceberg` + `time_in_force=poc` (логическое противоречие: `poc` «always enjoys a maker
    fee»);
  - приоритет скрытой части в очереди и как пополняется видимая часть;
  - минимум для видимой части;
  - содержание объявления «Gate Will Adjust the Spot Iceberg Orders of API V4» (article/29076).
- Формула «-1 скрывает всё» из CCXT относится, видимо, к фьючерсам. Не использовать.
- Бот: `execution.iceberg_poc_verified: false`. Пока флаг не выставлен, live `run` с iceberg-уровнем
  отказывается стартовать с `ICEBERG_POC_NOT_VERIFIED`; paper такие уровни допускает (DESIGN §11).
  **Внимание:** в `config.example.yaml` у уровней `b2`/`s2` стоит `iceberg_display_pct: 25`. Перед live их нужно
  выключить или пройти проверку этапа 5.

### 1.16 `finish_as` — ПОДТВЕРЖДЕНО (GSDK `order.py:1101-1110`, allowed values)

| Значение | Смысл | Значение | Смысл |
|---|---|---|---|
| `open` | в работе | `ioc` | не исполнена сразу при IOC |
| `filled` | исполнена полностью | **`poc`** | **post-only не выполнен: заявка признана taker и отклонена** |
| `cancelled` | отменена пользователем | `fok` | FOK не исполнен целиком |
| `liquidate_cancelled` | отменена ликвидацией | `stp` | self-trade prevention |
| `small` | остаток слишком мал | `price_protect_cancelled` | ценовая защита |
| `depth_not_enough` | не хватило глубины | `unknown` | неизвестно |
| `trader_not_enough` | не хватило контрагентов | | |

- Какой `finish_as` ставит отмена по DMS (`countdown_cancel_all`) — **НЕ ПОДТВЕРЖДЕНО** (этап 5, шаг 5.10).
- Вне enum SDK: сторонний трекер видел `unified_check_failed` (github.com/jholze/xagent-trading-bot/issues/549).
  SDK с `client_side_validation=True` бросит `ValueError`. Поэтому у бота свой клиент с терпимым парсером.
- С 2026-01-28 частично исполненные **market**-заявки получают `cancelled` + `ioc`/`price_protect_cancelled`
  (объявление 49555 — ПОДТВЕРЖДЕНО по поиску). К нашим limit/poc это не относится, но показывает, что семантика
  меняется.

---

## 2. Gate WebSocket (spot и USDT futures) и публичный REST USDT futures

### 2.1 Общее — ПОДТВЕРЖДЕНО (GWSPY `client.py`, GWSGO `model.go`/`constant.go`, CCXT `pro/gate.py`, GCT)

- URL: spot `wss://api.gateio.ws/ws/v4/`, USDT futures `wss://fx-ws.gateio.ws/v4/ws/usdt`. Testnet-адреса в
  источниках расходятся, есть ли spot testnet — **НЕ ПОДТВЕРЖДЕНО**. Бот testnet не использует.
- Запрос: `{"time": <с>, "id": <int, опц.>, "channel": "...", "event": "subscribe"|"unsubscribe", "payload": [...], "auth": {...}}`.
- Ack (реальный, CCXT `pro/gate.py:1781`): `{"time":…,"time_ms":…,"id":2,"channel":"spot.trades","event":"subscribe","payload":["LTC_USDT"],"result":{"status":"success"}}`.
- Ошибка: `{"error":{"code":2,"message":"unknown currency pair: LUNARLENS_USDT"},"result":{"status":"fail"}}`.
  Официальной таблицы кодов (1/2/3/4…) нет — **НЕ ПОДТВЕРЖДЕНО**.
- Push: `{"time", "time_ms", "channel", "event": "update"|"all", "result"}`.
- Ping: `{"time": <с>, "channel": "spot.ping"}` (для фьючерсов `futures.ping`), ответ в канале `spot.pong` /
  `futures.pong`. В SDK интервал 5 с (Python) или 10 с (Go).
- Сервер ведёт таймер клиента, ping его сбрасывает (W, `gate.com/docs/developers/apiv4/ws/en/`). Сколько длится
  таймаут — **НЕ ПОДТВЕРЖДЕНО**. Сервер закрывает соединение, если клиент не успевает читать (ISSUE25).
- SDK отключает сжатие WS (`compression=None`, GWSPY `client.py:258-261`). После переподключения подписки
  отправляются заново.

### 2.2 Публичные spot-каналы

| Канал | Payload | Ключевые поля `result` | Статус | Бот |
|---|---|---|---|---|
| `spot.order_book` | `[pair, level, interval]`, напр. `["STONK_USDT","20","100ms"]`; interval `100ms`\|`1000ms` | `t` (мс), `lastUpdateId`, `s`, `l` (с 2025), `bids`/`asks` `[[p,q],…]` строки, **полный снимок** | ПОДТВЕРЖДЕНО (GWSGO `response_spot.go:40-47`; GCT; W). Допустимые level `1,5,10,20,50,100` есть только у GCT, официально **НЕ ПОДТВЕРЖДЕНЫ** | книга торгуемого рынка (`market.depth_levels`, `depth_interval`) |
| `spot.trades` | `[pair]` | `id` (int), `create_time` (с), **`create_time_ms` строка с дробью** (`"1648725035923.0"`), `side` (сторона тейкера), `amount`, `price`; `result` бывает массивом и объектом | ПОДТВЕРЖДЕНО (GWSGO `response_spot.go:123-131`; CCXT `pro/gate.py:939-955`) | лента для paper `trade_through` |
| `spot.book_ticker` | `[pair]` | `t`, `u`, `s`, `b`, `B`, `a`, `A` (регистр важен) | ПОДТВЕРЖДЕНО (GWSGO `response_spot.go:72-80`; CCXT) | FX в режиме `gate_spot` |
| `spot.order_book_update` | `[pair, "100ms"\|"20ms"]` (2 элемента; payload из 3 элементов отвергается — живой замер GOGATE) | `t,e,E,s,U,u,b,a`; с 2025 есть `full=true` («перезаписать книгу») | ПОДТВЕРЖДЕНО (GWSEX; CCXT; объявления 44678, 44815). `1000ms` для этого канала **НЕ ПОДТВЕРЖДЕНО** | не используется |
| `spot.obu` | `["ob.STONK_USDT.50"]` (50\|400) | `t, full, s:"ob.PAIR.50", U, u, b, a` | ПОДТВЕРЖДЕНО (сторонние: CCXT, GCT) | не используется |

- Фикстура `spot.order_book` (СИНТЕТИКА по схеме GWSGO):
  `{"time":1760000000,"time_ms":1760000000123,"channel":"spot.order_book","event":"update","result":{"t":1760000000100,"lastUpdateId":48791820,"s":"STONK_USDT","l":"5","bids":[["0.13580","1520.5"],["0.13570","800"]],"asks":[["0.13600","300"],["0.13610","1200"]]}}`.
- Поля `range` у `spot.trades` нет ни в одном источнике. В changelog упоминается новое `id_market`; его формат
  **НЕ ПОДТВЕРЖДЕН**.
- Приходит ли `spot.order_book` с интервалом 100 мс, **даже когда книга не меняется**, — **НЕ ПОДТВЕРЖДЕНО**.
  От этого зависит свежесть книги торгуемого рынка в тихие периоды (§9.1). Бот покрывает оба варианта: когда
  книга старше 1.5 с по времени получения **или** по своему времени `t` (пуши без изменений с прежним `t`), он
  подкачивает REST-снимок `/spot/order_book` (LOGIC §1.3).

### 2.3 Приватные spot-каналы — ПОДТВЕРЖДЕНО (GWSPY `client.py:101-117`, GWSGO `channel.go:69-81`, CCXT)

- Подпись подписки: `SIGN = hex(HMAC-SHA512(secret, "channel=<channel>&event=<event>&time=<time>"))`. Объект:
  `"auth": {"method": "api_key", "KEY": <key>, "SIGN": <sign>}`. `user_id` в payload для спота не нужен. У
  WS-торгового API (`event:"api"`) подпись другая; бот его не использует.
- `spot.orders` (`[pair]` или `["!all"]`): поля `Order` плюс `user`, `event` (`put`/`update`/`finish`), `biz_info`.
  Реальный пример: CCXT `pro/gate.py:1499-1540`. Использует ли WS тот же набор `finish_as`, что REST, —
  **НЕ ПОДТВЕРЖДЕНО** (GWSGO перечисляет только `open/filled/cancelled/ioc/stp`).
- `spot.usertrades`: `id, user_id, order_id, currency_pair, create_time, create_time_ms (строка), side, amount,
  role, price, fee, fee_currency, point_fee, gt_fee, text, amend_text`. `spot.balances`: `timestamp`,
  `timestamp_ms`, `currency`, `change`, `total`, `available`, `freeze`, `change_type`.
- Бот (DESIGN `fill_wakeup.py`): приватный WS только **будит** REST-сверку. Количества и статусы берутся из REST.

### 2.4 Futures WS

- **Размеры (`size`) считаются в контрактах.** Gate переводит их из целых в строки (дробные лоты). Без заголовка
  `X-Gate-Size-Decimal: 1` size приходит целым, дробная часть отбрасывается. Для WS заголовок передаётся при
  handshake. ПОДТВЕРЖДЕНО по поиску (объявление 48788; дока futures WS с примером Go). Официальный Python WS SDK
  этот заголовок **не** ставит.
- `futures.book_ticker` `[contract]`: `t` (мс), `u`, `s`, `b`, `B` (size), `a`, `A` (size) (GWSGO
  `response_future.go:76-84`; GCT). Частота 10 мс — ПОДТВЕРЖДЕНО по поиску (старое объявление). **Используется
  ботом** для `mid`/`microprice`.
- `futures.tickers` `[contract]`: `result` — **массив** объектов со строковыми полями `contract, last,
  mark_price, index_price, funding_rate, …` (GWSGO `response_future.go:3-34`; GCT). Частота **НЕ
  ПОДТВЕРЖДЕНА**. Бот использует его только при `price: mark|last`.
- `futures.order_book` (legacy), `futures.order_book_update` (`[contract, "20ms"|"100ms", level]`; уровни как
  `{p,s}`), `futures.obu` (`ob.X.50|400`): существуют, бот их не использует. Подписаться на depth-поток одного
  контракта на одном соединении можно только один раз.
- Фикстура (СИНТЕТИКА по схеме GWSGO):
  `{"time":1760000000,"time_ms":1760000000050,"channel":"futures.book_ticker","event":"update","result":{"t":1760000000049,"u":5213871234,"s":"STONK_USDT","b":"0.1354","B":"1200","a":"0.1356","A":"800"}}`.

### 2.5 Публичный REST USDT futures — ПОДТВЕРЖДЕНО (GSDK `futures_api.py`, GOA)

- Все запросы с `X-Gate-Size-Decimal: 1`. Официальный SDK шлёт этот заголовок всегда (`api_client.py:87-88`).
  Без него size урезаются до целых: внешний отчёт приводит `s: 2459` вместо `"2459.3"`
  (github.com/Lqz13Th/extrema_infra/pull/148).
- `GET /futures/usdt/contracts/STONK_USDT`: `name`, `quanto_multiplier` (сколько base в 1 контракте),
  `order_price_round` (tick), `mark_price`, `index_price`, `last_price`, `order_size_min/max`, `enable_decimal`,
  `in_delisting`, **`status`** (`prelaunch|trading|delisting|delisted|circuit_breaker`), `funding_*`.
- `GET /futures/usdt/order_book?contract=&limit=1&with_id=true`:
  - `limit` **1..50** (`futures_api.py:623-626`);
  - ответ: `id`, `current`/`update` — **float, секунды** (у spot REST — int, мс), `asks`/`bids` как
    `[{"p":str,"s":str}]` (s в контрактах).
- `GET /futures/usdt/tickers?contract=STONK_USDT`: всегда **массив**.
- `GET /futures/usdt/index_constituents/STONK_USDT` → `{"index", "constituents":[{"exchange", "symbols":[…]}]}`.
  Весов в ответе нет.

### 2.6 Mark и index Gate; петля обратной связи

- Index: взвешенное среднее спотовых цен нескольких бирж, «including … Gate». Пересчёт раз в секунду. Отклонение
  от медианы на 2–8 % ограничивается 98 %/102 % (W, help 22069). Mark — медиана трёх величин: двух вариантов
  «index + базис» и last price (W, help 22067).
- **Входит ли Gate spot STONK_USDT в индекс Gate futures STONK_USDT — НЕ ПОДТВЕРЖДЕНО.** Если входит, а наш индекс
  берёт mark или index Gate futures, наши же котировки влияют на вход индекса.
- Бот: `gate_perp` берёт `price: mid` из `futures.book_ticker` (собственный стакан фьючерса) с весом 0.1. Doctor
  читает `index_constituents` и предупреждает, если Gate spot входит в индекс при `price: mark` (DESIGN §15).

### 2.7 Лимиты WS

- Число подписок на соединение и сообщений к серверу не ограничено; рекомендуется не больше 100 соединений на
  хост (ISSUE25, 2022).
- Не больше 300 соединений с IP (RESTV4, старый источник).
- Боту хватает двух соединений (spot и futures) плюс приватного spot-соединения в live.

---

## 3. Bybit V5: `STONKUSDT`, category `linear`

- **Листинг — НЕ ПОДТВЕРЖДЕНО первоисточником.** Ссылка заказчика `bybit.com/…/trade/usdt/STONKUSDT` имеет формат
  страницы USDT-перпетуала. По поиску (сниппеты, страницы не открылись): Coinalyze `stonkusdt`, запись Bybit
  «New listing: STONKUSDT Perpetual Contract in Innovation Zone, up to 5x» около 2026-09-18. Вывод: символ
  `STONKUSDT`, `contractType=LinearPerpetual`, `symbolType` скорее всего `innovation`.
- **Проверка инструмента** (`GET /v5/market/instruments-info?category=linear&symbol=STONKUSDT`; BYD
  `instrument.mdx`; правило из REF `spot_portfolio/feeds.py:703-754`):
  - `retCode == 0`, ровно одна строка с точным `symbol`;
  - `contractType == "LinearPerpetual"`, `status == "Trading"`, `isPreListing == false`;
  - `quoteCoin == settleCoin == "USDT"`;
  - `deliveryTime != "0"` означает назначенный делистинг → предупреждение.
  - Все числа приходят строками. Без `symbol` ответ ограничен 500 записями, а linear-символов больше.
  - Как выглядит ответ для несуществующего символа (пустой `list` или `10001`) — **НЕ ПОДТВЕРЖДЕНО**. Оба
    случая обрабатываются как `BYBIT_REFERENCE_NOT_LISTED`.
- **REST** (BYD):
  - `GET /v5/market/orderbook?category=linear&symbol=&limit=1` → `result.s, b, a, ts, u, seq, cts`; RPI-заявки
    исключены;
  - `GET /v5/market/tickers` → `result.list[]`: `markPrice`, `lastPrice`, `indexPrice`, `bid1Price`, …
    **У строки нет времени**: `time` есть только в конверте ответа;
  - `GET /v5/market/time` → `timeSecond`, `timeNano`.
- **WS** `wss://stream.bybit.com/v5/public/linear` (BYD `wss-authentication.mdx`):
  - подписка `{"op":"subscribe","args":["orderbook.1.STONKUSDT"]}`, ack
    `{"success":true,"ret_msg":"","op":"subscribe",…}`;
  - ping `{"op":"ping"}` раз в **20 с** (рекомендация доки); **pong приходит с `"op":"ping"` и
    `"ret_msg":"pong"`**;
  - отказ подписки — `success:false` (текст `ret_msg` **НЕ ПОДТВЕРЖДЕН**);
  - без ping-pong и данных соединение рвётся через 10 мин (сказано в разделе о private; для public формально
    **НЕ ПОДТВЕРЖДЕНО**).
- **`orderbook.1`, правило «тихого L1» — ПОДТВЕРЖДЕНО** (BYD `orderbook.mdx:50,52,75`, CL 2025-03-20):
  - частота L1 — 10 мс;
  - сообщения **только `snapshot`**;
  - дословно: «if 3 seconds have elapsed without a change in the orderbook, a `snapshot` message will be pushed
    again, and the field `u` will be the same as that in the previous message»;
  - `u == 1` означает перезапуск сервиса: книгу перезаписать;
  - у L50 повтора при тишине нет.
- **`tickers.STONKUSDT`**: для linear приходят `snapshot` + `delta`; отсутствующее поле значит «не изменилось».
  Частота 100 мс. Время — верхний `ts`. Heartbeat при неизменных данных **НЕ ПОДТВЕРЖДЁН**.
- **Решение бота** (DESIGN §16; перенос из REF):
  - основной источник `orderbook.1`: только `type=snapshot` и `data.s == symbol`, иначе
    `UNEXPECTED_BYBIT_L1_SCHEMA`;
  - `event_ms = ts`, `update_id = data.u` (повтор того же `u` разрешён, уменьшение — нет, кроме `u==1`);
  - `heartbeat_seconds: 3` → допустимый возраст `max(max_age, 3+1) = 4 с`; для рассинхронизации берётся
    `min(ts+3000, newest)`;
  - `tickers` подписывается только при `price: mark|last`.
- **Не сказано в доке (проверка этапа 1):** обновляются ли `ts`/`cts` у повторного снимка; бывает ли пустая
  сторона L1; насколько точно выдерживаются 3 с. **Риск:** если у повтора `ts` старый, Bybit (`required: true`)
  будет получать `STALE_EVENT_OR_CLOCK_SKEW` в каждом тихом периоде дольше ~4 с, и пара заблокируется
  (fail-closed, но бот станет непригоден).
- **Лимиты:**
  - REST: 600 запросов / 5 с на IP. Превышение даёт **HTTP 403** и бан не меньше 10 минут. 403 приходит также для
    IP из США и материкового Китая;
  - 429 — системная защита частоты;
  - WS: не больше 500 подключений за 5 мин на домен.
  - Doctor при 403 подсказывает про гео-ограничение.
- Расхождения внутри BYD: `data` у tickers в таблице назван массивом, а в примере это объект; `ts` в одном
  примере строкой; опечатка `api.bybick.com`. Парсер терпимый.

---

## 4. Bitget V2 (classic): `STONKUSDT`, `productType=USDT-FUTURES`

- **Листинг — ПОДТВЕРЖДЕНО по поиску:** объявление bitget.com/support/articles/12560603895628. USDT-M perpetual
  с 2026-09-18 (UTC+8), **tick 0.00001**, funding каждые 4 ч, плечо до 10x. Что отдаст `contracts` для STONKUSDT
  (`pricePlace` 5, `priceEndStep` 1) — **вывод, НЕ ПОДТВЕРЖДЕНО**.
- **REST** `https://api.bitget.com`, без подписи:
  - конверт `{"code":"00000","msg":"success","requestTime":<мс>,"data":…}`; `code` — строка;
  - все числа приходят строками (CCXT `bitget.py:3481-3490`; SIEBLY `futures.ts`).
- Эндпоинты:
  - `GET /api/v2/mix/market/ticker?symbol=STONKUSDT&productType=USDT-FUTURES`: **`data` — массив из одного
    объекта**; поля `lastPr`, `bidPr`, `askPr`, `bidSz`, `askSz`, `markPrice`, `indexPrice`, `ts` (строка, мс),
    `usdtVolume`; `deliveryTime` бывает `null`. Лимит 20 req/s/IP — ПОДТВЕРЖДЕНО по поиску.
  - `GET /api/v2/mix/market/merge-depth?…&limit=1|5|15|50|max&precision=scale0`: `data` — объект, `asks`/`bids`
    `[[p,s]]`. Тип элементов (строки или числа) **НЕ ПОДТВЕРЖДЁН**: CCXT показывает строки, SIEBLY — числа.
    Парсить через `Decimal(str(x))`.
  - `GET /api/v2/mix/market/orderbook` есть только в BGSDK `market_api.py:13-14`. Работает ли он —
    **НЕ ПОДТВЕРЖДЕНО**, используем `merge-depth`.
  - `GET /api/v2/mix/market/contracts?productType=USDT-FUTURES&symbol=STONKUSDT`: **tick =
    `priceEndStep × 10^(−pricePlace)`** (логика CCXT `bitget.py:2332-2340`). Источник здоров только при
    **`symbolStatus == "normal"`** (CCXT `bitget.py:2350-2353`; значения `normal|maintain|limit_open|restrictedAPI`
    — JKORF). Статусы `listed`/`off` **НЕ ПОДТВЕРЖДЕНЫ**.
  - Коды ошибок `40308` (обслуживание) и `40309` (контракт удалён) известны только по CCXT.
- **WS** `wss://ws.bitget.com/v2/ws/public`:
  - подписка
    `{"op":"subscribe","args":[{"instType":"USDT-FUTURES","channel":"books1","instId":"STONKUSDT"},{"instType":"USDT-FUTURES","channel":"ticker","instId":"STONKUSDT"}]}`
    (CCXT, SIEBLY). Официальный SDK шлёт `inst_type`/`inst_id` — **не повторять**;
  - ack `{"event":"subscribe","arg":{…}}`, ошибка `{"event":"error","code":30001,"msg":"… doesn't exist"}`;
  - `books1`/`books5`/`books15` — **каждый пуш полный снимок**, `books` — инкременты с checksum. **Пока книга не
    меняется, новых снимков нет** (W, classic best-practices). Частота `books1` для фьючерсов classic 10 мс (W);
  - пуш: `{"action":"snapshot","arg":{…},"data":[{"asks":[["p","s"]],"bids":[…],"checksum":…,"ts":"<мс>"}],"ts":<мс>}`;
  - checksum (crc32 первых 25 уровней, чередование bid/ask, signed int32) нужен только для `books`, бот его не
    использует;
  - ping — **текстовая строка `ping`**, ответ `pong`. Дока советует слать раз в 30 с, без ping сервер рвёт
    соединение через **2 мин** (W);
  - `ticker`: `data[0]` с `lastPr`, `bidPr`, `askPr`, `markPrice`, `indexPrice`, `ts`. Частота **НЕ
    ПОДТВЕРЖДЕНА**.
- **Лимиты:**
  - REST market — 20 req/s/IP;
  - WS (W, JKORF): 300 запросов на подключение за 5 мин на IP, максимум 100 подключений на IP; 240 subscribe в
    час на подключение; **10 сообщений/с от клиента, включая ping**.
- **Бот** (DESIGN §16): `books1` для BBO; `ticker` только при `mark|last`; `rest_refresh_after_seconds: 1.5`
  (если WS молчит или время события снимка не обновляется дольше 1.5 с, опрос REST не чаще 1 раза в секунду;
  после сбоя опроса пауза растёт до 10 с); ping `ping` каждые 30 с. Подписка через v2 «classic»:
  Bitget продвигает UTA v3 (`wss://ws.bitget.com/v3/ws/public`), но даты отключения v2 не найдено.

---

## 5. BingX USDT-M perpetual (swap V2): `STONK-USDT`

- **Листинг — ПОДТВЕРЖДЕНО по поиску:** STONKUSDT perpetual с **2026-09-07 03:00 UTC**
  (bingx.com/en-us/support/articles/17517228543887). Ловушки: `STONKBROKERUSDT` — другой контракт,
  `STONKS` — другой токен. Символ сверяется **строгим равенством** `STONK-USDT`, не по подстроке.
- **REST** `https://open-api.bingx.com` (BXD `@387448`):
  - публичные запросы без подписи («Signature Verification: No»); в query всегда `timestamp=<мс>`, допуск
    **5 с**, иначе код `100421`;
  - конверт `{"code":0,"msg":"","data":…}`;
  - в `bookTicker` цены приходят **JSON-числами**: JSON разбирать с `parse_float=Decimal`.
- Эндпоинты (BXD):
  - `GET /openApi/swap/v2/quote/depth?symbol=STONK-USDT&limit=5`: `data.T`, `bids`, `asks`, `bidsCoin`,
    `asksCoin`. **`asks` отсортированы по УБЫВАНИЮ**, лучший ask стоит последним (BXD и CCXT; BXSKILL
    ошибочно пишет обратное). Лучшие цены: `max(bids)`, `min(asks)`. `qty` у `bids` и `bidsCoin` может
    отличаться в 1000 раз (единицы — **НЕ ПОДТВЕРЖДЕНО**).
  - `GET /openApi/swap/v2/quote/bookTicker?symbol=&timestamp=` → **`data.book_ticker`**
    `{symbol, bid_price, bid_qty, ask_price, ask_qty}` (числа). Поля `time`/`lastUpdateId` есть в описании, но
    не в примере. BXSKILL описывает camelCase без обёртки. Парсер должен понимать обе формы.
  - `GET /openApi/swap/v2/quote/premiumIndex?symbol=` → `markPrice`, `indexPrice` (строки), `lastFundingRate`,
    `nextFundingTime` (абсолютные мс), `fundingIntervalHours`. С `symbol` приходит объект, без него — массив.
  - `GET /openApi/swap/v2/quote/contracts?symbol=` → массив: `pricePrecision`, `quantityPrecision`, `status`
    (`1` online, `0` offline, `5` pre-online, `25` открытие позиций запрещено), `asset`, `currency`. Типы бывают
    int и строкой. Котировка годна только при `status == 1`.
  - Ошибки:
    - HTTP 429 → 418 (бан IP), 403 (файрвол, 5 мин);
    - коды `100410` (лимит), `100421` (время), `109425` (нет пары), `100500` (занято).
- **WS** `wss://open-api-swap.bingx.com/swap-market` (BXD «WebSocket Rules»):
  - все кадры **gzip** (бинарные);
  - сервер шлёт `Ping` примерно каждые 5 с, клиент отвечает текстом `Pong`;
  - подписка `{"id":"<uuid>","reqType":"sub","dataType":"STONK-USDT@depth5@500ms"}`; ack
    `{"id":…,"code":0,"msg":"","dataType":"","data":null}`.
- Каналы:
  - `@depth<5|10|20|50|100>@500ms`: **200ms только для BTC-USDT и ETH-USDT**, для остальных 500ms (с 2025-06-17).
    Каждое сообщение — полный снимок. Время в **верхнем `ts`** (поля `data.T` в WS нет). `asks` по убыванию.
    Пример: `{"code":0,"dataType":"BTC-USDT@depth5@500ms","ts":1759996742928,"data":{"bids":[…],"asks":[…]}}`;
  - `@bookTicker` (200 мс): `{e,u,E,T,s,b,B,a,A}` строками.
- **Лимиты:**
  - REST market — IP-группа 1 = **500 / 10 с** на IP (BXD). Общий ли это лимит на группу или на каждый эндпоинт —
    **НЕ ПОДТВЕРЖДЕНО**;
  - резервный домен `open-api.bingx.io` — 60/мин;
  - WS: ≤ 200 topics на соединение, ≤ 60 соединений на IP, ≤ 10 подписок/с.
- **Бот** (DESIGN §16): `STONK-USDT@depth5@500ms`, лучший ask = min, `event_ms` = верхний `ts`, `code: 0` в данных
  не считается ошибкой; REST-refresh через 1.5 с тишины. Подписи и заголовка `X-SOURCE-KEY` (из BXSKILL) нет.

---

## 6. Lighter (zkLighter): perp STONK, котировка USDC

- **Хост:** `https://mainnet.zklighter.elliot.ai` и WS `wss://mainnet.zklighter.elliot.ai/stream`
  (LSDK `endpoint_profiles.py:26-31`, LOA, CCXT, NT). `zk-perps-api.elliot.ai` встречается только в сниппете
  старой доки — **НЕ ПОДТВЕРЖДЕНО**. Котировка и settlement — **USDC**.
- **REST** (без авторизации):
  - `GET /api/v1/orderBookDetails?filter=perp` → **`order_book_details[]`** (перпы) и
    `spot_order_book_details[]`. Поля: `symbol` (у перпа это просто base, напр. `"ETH"`), `market_id`,
    `market_type`, `status` (`active|inactive`), `price_decimals`, `size_decimals`, `supported_*_decimals`,
    `last_trade_price` (**JSON-число**), `mark_price`/`index_price` (строки), `multiplier`, `quote_multiplier`.
    В живой фикстуре NT от 2026-05-05 нет `mark_price`, `index_price`, `multiplier`, `created_at`, хотя LOA
    называет их обязательными. Парсер мягкий.
  - `GET /api/v1/orderBooks` **не содержит** `price_decimals`/`size_decimals`/`last_trade_price`.
  - `GET /api/v1/orderBookOrders?market_id=&limit=` (оба параметра обязательны, `limit` 1..250): **отдельные
    заявки (L3)** `{price, remaining_base_amount, …}`. Уровни нужно собирать самим; сортировка
    **НЕ ПОДТВЕРЖДЕНА**.
- **WS:**
  - первый кадр `{"type":"connected","session_id":…}`; подписка `{"type":"subscribe","channel":"order_book/<id>"}`;
  - **во входящих кадрах `channel` пишется через двоеточие** (`"order_book:<id>"`), в подписке — через `/`
    (NT `messages.rs:418-420`);
  - типы кадров `subscribed/order_book` (снимок) и `update/order_book` (уровни с **абсолютным** размером, 0
    удаляет уровень);
  - пример update (NT, живой):
    `{"channel":"order_book:0","offset":1558300,"order_book":{"code":0,"asks":[{"price":"2064.54","size":"0.3285"}],"bids":[],"offset":1558300,"nonce":9182390020,"last_updated_at":1774884082309144,"begin_nonce":9182389998},"timestamp":1774884082326,"type":"update/order_book"}`.
- **Непрерывность:** у каждого update `begin_nonce` должен совпасть с `nonce` предыдущего кадра, иначе ресинк.
  **`offset` для этого не годится:** он пропускает значения и меняется между серверами (NT, раздел про
  синхронизацию). Официальный SDK непрерывность не проверяет. В примере CCXT `begin_nonce` отсутствует.
- **Прочее:**
  - сервер шлёт `{"type":"ping"}`, клиент обязан ответить `{"type":"pong"}`; keepalive около 2 мин; бывает
    `{"type":"shutdown","close_in_ms":N}`;
  - есть каналы `ticker/<id>` (только по NT) и `market_stats/<id>` (mark/index строками, объёмы числами);
  - ошибки двух форматов: `{"error":{"code":30005,"message":…}}` и `{"type":"error","code":30012,…}`.
- **market_index STONK — НЕ ПОДТВЕРЖДЕНО.** STONK на Lighter не найден ни в одном источнике. После сентябрьского
  апгрейда 2026 новые рынки получают id **≥ 4095** (NT со ссылкой на t.me/lighter_api_updates/174). В LOA у
  `market_id` всё ещё `int16`. Тип рынка определять только по `market_type`.
- **Лимиты:**
  - REST: стандартный и анонимный доступ — **60 запросов в скользящую минуту** (W, NT); вес «прочих»
    эндпоинтов 300 (точные веса двух нужных **НЕ ПОДТВЕРЖДЕНЫ**);
  - WS: 255 соединений на IP, 200 клиентских сообщений в минуту, 500 подписок на соединение.
- **Бот** (DESIGN §16):
  - `market_index` ищется через `orderBookDetails` по точному `symbol` при `market_type=perp` и
    `status=active`, кэшируется; конфиг может его закрепить;
  - `begin_nonce == prev nonce`, иначе источник невалиден и переподключение;
  - `event_ms` = время получения последнего кадра данных;
  - REST-refresh в конфиге выключен (`null`); если его включить, опрос не чаще раза в 2 с. REST нужен для поиска
    `market_index` и в doctor.

---

## 7. FX USDC→USDT: варианты и рекомендация

Lighter котируется в USDC, индекс строится в USDT. Курс `rate` = сколько USDT стоит 1 USDC, формула
`price_usdt = price_usdc × rate` (DESIGN §4).

| Режим (`index.fx.mode`) | Источник | Факты | Статус пары |
|---|---|---|---|
| `binance_spot` | WS `wss://stream.binance.com:9443/ws/usdcusdt@depth5@100ms` + REST `GET /api/v3/ticker/bookTicker?symbol=USDCUSDT` (вес 2) | Partial depth: «Top <levels> bids and asks», 100 мс, `{lastUpdateId,bids,asks}` (BND `web-socket-streams.md:573-578`). Ping-кадр сервера каждые 20 с, соединение живёт ≤ 24 ч, ≤ 5 входящих сообщений/с (`:44-63`). Хост только для маркет-данных — `data-stream.binance.vision` / `data-api.binance.vision` | **Существование `USDCUSDT` официально НЕ ПОДТВЕРЖДЕНО** (только сторонние зеркала). Доступ с IP сервера (гео) **НЕ ПОДТВЕРЖДЁН** |
| `gate_spot` | WS `spot.book_ticker` `["USDC_USDT"]`, REST `/spot/order_book?currency_pair=USDC_USDT&limit=1` | Схема как в §2.2. С 2026-09-01 у Gate «Unified USD Order Book»: USD, USDC, USD1, RLUSD сведены в книги `XXX/USD` (W, объявление 101473) | **Существование `USDC_USDT` НЕ ПОДТВЕРЖДЕНО**; есть риск переименования или слияния |
| `fixed` | `index.fx.usdc_to_usdt` (по умолчанию 1) | Сети не нужно | — |

Почему бот выбирает `depth5@100ms`, а не `bookTicker` (DESIGN §16, вывод). В payload `bookTicker` нет времени
события, и он приходит только при изменении лучших цен или объёмов (BND `web-socket-streams.md:532-550`). Спокойная книга USDCUSDT выглядела бы устаревшей. Partial depth
приходит периодически, поэтому кадры подтверждают живость. `event_ms` = время получения.

**Оценка влияния.** У Lighter в конфиге вес 0.1, в объёме по снимку заказчика доля 1.98 %. Используется
`weighted_median`, поэтому одиночный Lighter сдвигает индекс только тогда, когда сам оказывается в точке медианы.
Ошибка FX в 10 б.п. пренебрежимо мала. Существенен только депег USDC.

**Защиты:**
- `abs(rate−1)×100 > max_depeg_pct (0.5)` → компоненты USDC исключаются с `FX_DEPEG`;
- нет курса → `FX_UNAVAILABLE`;
- индекс продолжает работать на USDT-источниках, если выполняется `min_sources` (fail-closed для Lighter).

В режиме `fixed` проверка депега сравнивает **константу**, поэтому реальный депег она не видит. Его ловит только
фильтр выбросов `SOURCE_OUTLIER` (`max_source_deviation_pct: 1.0` от медианы источников): депег меньше ~1 %
проходит.

**Рекомендация:**
1. Этап 1: doctor проверяет `USDCUSDT` на Binance (`status=TRADING`, доступ с сервера) и `USDC_USDT` на Gate
   (`trade_status=tradable`).
2. Live: `binance_spot`, если Binance доступен; иначе `gate_spot`, если пара торгуется. Если рыночного курса нет,
   есть два варианта: `fixed` с принятым риском депега меньше 1 % (вес 0.1, медиана) **или** отключить Lighter
   (`lighter_perp.enabled: false`).
3. Paper и офлайн-тесты: `fixed`.
4. Автоматического перехода между режимами в DESIGN нет: режим один на конфиг.

---

## 8. Расхождения с промтом (сводный список)

Формат каждого пункта: **Промт** (по заметкам; «не оговорено» — промт тему не затрагивал) · **Факт** · **Влияние**
· **Бот** (что делает код по DESIGN и конфигу).

### Gate spot REST

**Р1. Поиск заявки по `text` после завершения.**
- Промт: поиск по `text` «в списках — до 1 часа после завершения».
- Факт: `GET/DELETE/PATCH /spot/orders/{text}` работают **только для открытых** заявок (GSDK `spot_api.py:2396`).
  Фраза про «1 час» относится к `last_id` публичного `/spot/trades`.
- Влияние: потерянную заявку нельзя найти по `text` напрямую.
- Бот: `order_id` запоминается из ответа сразу, отмена идёт по id. Поиск неизвестной заявки:
  `get_order(text)` → открытые заявки по `text` → `status=finished` в окне `from/to` → `my_trades` по `text`.
  Доказательство отсутствия — только через `request_expiry + 2×timeout + unknown_create_margin` (DESIGN §11).

**Р2. Пакетная отмена по `text`.**
- Промт: не оговорено.
- Факт: `text` принимается только **30 мин** после создания (`cancel_batch_order.py:98`).
- Влияние: старые заявки по `text` не отменить.
- Бот: отмена всегда по `order_id`. Ответы `ORDER_NOT_FOUND`/`CLOSED`/`CANCELLED` по элементу →
  `'rejected'`, статус уточняется сверкой (DESIGN §15).

**Р3. Отказ post-only.**
- Промт: отказ распознаётся по `finish_as=poc`.
- Факт: возможен и путь ошибкой `POC_FILL_IMMEDIATELY` (заявка не создана). Какой путь реальный — НЕ ПОДТВЕРЖДЕНО.
- Влияние: при учёте только одного пути другой даст ложные `UnknownResult` или «пропавшие» заявки.
- Бот: оба пути — отказ post-only, пауза слота 0/0/5/10/20/40/60 с (§1.13). Проверка — этап 5, шаг 5.7.

**Р4. Iceberg.**
- Промт: «проверь по документации».
- Факт: скрытая часть исполняется по **taker-ставке**; совместимость с `poc` не документирована.
- Влияние: iceberg не гарантирует maker-комиссию.
- Бот: `iceberg_poc_verified: false` → live с iceberg отказывает `ICEBERG_POC_NOT_VERIFIED`; paper разрешает.
  Отчёт должен учитывать taker-комиссию по факту (`role`/`fee` в `my_trades`).

**Р5. Лимиты.**
- Промт: не оговорён штраф за низкий fill ratio.
- Факт: при низком fill ratio лимит place+amend падает до 10 запросов за 10 с, окно 24 ч, учитываются и отмены, и
  неуспешные запросы.
- Влияние: при частых перестановках далеко от рынка лимит быстро исчерпывается.
- Бот: `max_order_requests_per_10s: 8`, пакетное создание, `reprice_threshold_pct`/`min_reprice_seconds`, отмены
  вне бюджета (§1.14).

**Р6. Поле `account`.**
- Промт: поле перечислено без правил.
- Факт: SDK по умолчанию шлёт `spot`; для unified-аккаунта отмены требуют `"unified"`. Режим определяется по
  `/account/detail` (`key.mode`) и `/unified/unified_mode`.
- Влияние: при неверном `account` отмены не сработают.
- Бот: `market.account` передаётся явно во всех create/cancel/list. Несовпадение с режимом →
  `ACCOUNT_MODE_MISMATCH:<detected>`, live отказывает. Пустой белый список пар ключа — нормально; непустой должен
  содержать пару.

**Р7. Cancel-all по паре.**
- Промт: `DELETE /spot/orders?currency_pair=STONK_USDT`.
- Факт: без `currency_pair` отменяются **все пары**; без `account` в classic снимаются и margin-заявки пары.
- Влияние: ошибка в запросе снимает чужие заявки по всем парам или счетам аккаунта.
- Бот: в клиенте `currency_pair` и `account` обязательны. Cancel-all — только запасной путь `cancel-only`
  (DESIGN §14).

**Р8. PATCH.**
- Промт: «`price` и/или `amount`».
- Факт: для batch-amend явно «только одно из двух»; для одиночного PATCH одновременная передача
  **НЕ ПОДТВЕРЖДЕНА**. Изменение цены теряет приоритет. Проверка `poc` при amend не описана.
- Влияние: одновременная передача может быть отклонена или повести себя неожиданно; amend цены не сохраняет очередь,
  выигрыш только в одном запросе вместо двух.
- Бот: amend выключен (`reprice_via_amend: false`). Если включить — только `price` (и `amount` только на
  уменьшение), никогда оба. Неизвестный результат amend → отмена заявки, повтора amend нет.

**Р9. `x-gate-exptime`.**
- Промт: использовать для торговых запросов.
- Факт: подтверждено (мс, не подписывается). Есть у create/batch/cancel/batch cancel/cancel-all/amend, **нет** у
  `countdown_cancel_all`. Label просрочки **НЕ ПОДТВЕРЖДЁН**.
- Влияние: заявка, пришедшая после exptime, не исполнится; если ответ потерян до exptime, она может существовать.
- Бот: `now + request_expiry_seconds (10 с)` ровно на этих методах.

**Р10. DMS-тайминги.**
- Промт (§9): `dms_timeout 30, refresh 10, request_timeout 8`.
- Факт: Gate отменяет всё, если вызов не пришёл до `timeout` (минимум 5 с). Правило референса
  `timeout > request_expiry + 3×request_timeout + poll` даёт `30 > 10 + 24 + 5 = 39` — не выполняется.
- Влияние: при сбое одного обновления DMS снимает все заявки раньше, чем бот успевает повторить: ложные отмены и
  лишние запросы.
- Бот: `dms_timeout_seconds: 45`, `dms_refresh_seconds: 15`, `request_timeout_seconds: 5` (45 > 30), валидатор
  DESIGN §3.

**Р11. Права ключа в doctor.**
- Промт: проверить права ключа.
- Факт: эндпоинта прав текущего ключа нет; `/account/detail` прав не содержит.
- Влияние: ключ без права торговли обнаружится только на первой заявке.
- Бот: live doctor проверяет право записи безвредным `countdown_cancel_all {"timeout":0}`; в выводе label
  `READ_ONLY`/`FORBIDDEN`/`INVALID_KEY`/`IP_FORBIDDEN`. Чтение проверяется любым приватным GET.

**Р12. Комиссии.**
- Промт: рекомендовать отключить списание комиссии в GT.
- Факт: `GET /spot/fee` устарел, нужен `GET /wallet/fee` (доли). Вычет GT — `GET /account/debit_fee`.
- Влияние: при вычете в GT `fee` в сделках бывает 0, а комиссия уходит в `gt_fee`: себестоимость в USDT занижается.
- Бот: doctor читает оба эндпоинта. При включённом вычете GT — предупреждение: учёт GT-комиссий в `fees_other`
  приблизителен.

### Gate WS и futures

**Р13. Канал книги спота.**
- Промт: упоминает инкрементальный `spot.order_book_update` и его интервалы.
- Факт: payload из 2 элементов (`100ms`/`20ms`; `1000ms` не подтверждён). С 2025 приходят `full=true`. Появился
  `spot.obu`.
- Влияние: инкрементальный канал требует ресинка по `U/u` и обработки `full`; ошибка даёт неверную книгу без признака
  сбоя.
- Бот: `spot.order_book` `[pair, "20", "100ms"]` — полные снимки без отслеживания последовательности (DESIGN §16).

**Р14. `spot.trades`.**
- Промт: поле `range`.
- Факт: такого поля нет. `create_time_ms` — строка с дробью; `result` бывает массивом. Упоминается `id_market`
  (формат не подтверждён).
- Влияние: парсер, ожидающий `range` или числовое время, отбросит сделки ленты, и paper перестанет исполнять.
- Бот: лента из `id`, `price`, `amount`, `side`, `int(Decimal(create_time_ms))`; принимаются массив и объект.

**Р15. Размеры фьючерсов.**
- Промт: единицы и тип размера `s` не оговорены.
- Факт: размер в **контрактах**; без `X-Gate-Size-Decimal: 1` дробная часть отбрасывается.
- Влияние: при дробных лотах размер < 1 контракта превратится в 0, и уровень станет невалидным.
- Бот: заголовок на futures REST (DESIGN §15); `mid` и `microprice` не зависят от единиц размера.
  **Рекомендация к реализации:** слать заголовок и при WS-handshake — DESIGN это не оговаривает (открытый вопрос).

**Р16. Форматы futures REST.**
- Промт: не оговорено.
- Факт: `order_book` `limit` ≤ 50; `current` — float в **секундах**; уровни `{p,s}`; `tickers` всегда массив.
- Влияние: при `limit > 50` запрос отклоняется; секунды, прочитанные как мс, сделают книгу «устаревшей».
- Бот: `rest_snapshot` с `limit=1`; время переводится в мс.

**Р17. Серверный таймаут WS.**
- Промт: не оговорено.
- Факт: числом нигде не задан, известно только, что ping сбрасывает таймер.
- Влияние: без ping сервер молча закрывает соединение в неизвестный момент.
- Бот: прикладной ping (рекомендация 5–10 с, как в SDK), `idle_timeout` 60 с → переподключение.

**Р18. Состав индекса Gate futures.**
- Промт: Gate futures как вход индекса.
- Факт: входит ли Gate spot STONK — **НЕ ПОДТВЕРЖДЕНО**.
- Влияние: при `price: mark` возможна петля.
- Бот: `price: mid` у `gate_perp`, вес 0.1; doctor проверяет `index_constituents`.

### Bybit

**Р19. «Тихий» Bybit.**
- Промт: допуск `max(max_age, 3+1)`.
- Факт: правило совпадает с докой, но действует только для L1. Обновляется ли `ts` у повтора —
  **НЕ ПОДТВЕРЖДЕНО**.
- Влияние: при старом `ts` обязательный Bybit будет блокировать пару в тихие периоды.
- Бот: `heartbeat_seconds: 3`, сверка по `min(ts+3000, newest)`. **Проверка №1 этапа 1.** Если `ts` не
  обновляется, нужна правка: брать `event_ms` повтора от времени получения или ослабить проверку для повторов с
  тем же `u`. Это решение архитектора.

**Р20. mark/last у Bybit.**
- Промт: `price: mark|last` для любого источника.
- Факт: у Bybit `markPrice`/`lastPrice` есть только в `tickers` (snapshot+delta).
- Влияние: без `tickers` источник с `price: mark` всегда невалиден (`MISSING_PRICE:mark`).
- Бот: `tickers.<SYM>` с delta-слиянием подписывается только для этих режимов.

**Р21. «Bybit Futures».**
- Промт: Bybit назван «Futures».
- Факт: это USDT-перпетуал (`linear`, `LinearPerpetual`); `LinearFutures` — экспирационные.
- Влияние: экспирационный контракт дал бы цену с базисом к перпетуалу.
- Бот: doctor проверяет `contractType`, `status`, `isPreListing`, `deliveryTime`.

**Р22. Поле `time` у tickers.**
- Задание исследования: упоминает поле `time` у tickers.
- Факт: это время конверта REST; у WS-сообщения время — верхний `ts`.
- Влияние: поиск `time` в строке тикера даст пустое время.
- Бот: WS — `ts` верхнего уровня; REST — время конверта или время получения.

**Р23. HTTP 403 у Bybit.**
- Промт: не оговорено.
- Факт: 403 означает IP-лимит (бан ≥ 10 мин) **или** гео-блок (США, материковый Китай).
- Влияние: частые повторы после 403 продлевают бан. С гео-блокированного сервера Bybit (`required: true`) недоступен
  совсем, и индекс заблокирован.
- Бот: 403 — временная ошибка с длинной паузой; doctor подсказывает про гео.

### Bitget

**Р24. Веса Bitget и BingX.**
- Промт: в таблице заказчика есть только Bybit, Lighter, Gate.
- Факт: цен и объёмов Bitget и BingX нет.
- Влияние: веса 0.25/0.15 в `config.example.yaml` — стартовые, из данных не выведены.
- Бот: утвердить веса после `index-watch` (этап 1).

**Р25. `/api/v2/mix/market/orderbook`.**
- Промт: REST-стакан через `/api/v2/mix/market/orderbook`.
- Факт: существует только в официальном SDK, в документации не найден.
- Влияние: REST-фолбэк на неподтверждённый путь мог бы молча не работать.
- Бот: REST-снимок через `merge-depth` и `ticker`.

**Р26. REST ticker.**
- Промт: `data` как объект.
- Факт: `data` — **массив**.
- Влияние: парсер объекта не разберёт массив, и REST-фолбэк не заработает.
- Бот: берётся элемент с точным `symbol`.

**Р27. Tick Bitget.**
- Промт: точность цены из `pricePlace`; у заказчика цены с 4 знаками.
- Факт: tick = `priceEndStep × 10^(−pricePlace)`; одного `pricePlace` мало. Tick по объявлению 0.00001
  (5 знаков).
- Влияние: в проверке инструмента получился бы неверный tick. На индекс не влияет.
- Бот: tick только из `contracts` (информационно; цены источников не округляются).

**Р28. `books1` «снимок на каждый пуш».**
- Промт: `books1` — снимок на каждый пуш; упоминает checksum.
- Факт: при неизменной книге пушей нет; checksum нужен только для `books`.
- Влияние: тишина `books1` ложно делает Bitget устаревшим; лишняя проверка checksum отбрасывала бы корректные снимки.
- Бот: `rest_refresh_after_seconds: 1.5`; checksum не считается.

**Р29. Ping Bitget.**
- Промт: каждые 30 с.
- Факт: совпадает с докой; без ping разрыв через 2 мин. SDK шлют каждые 25 с и 10 с.
- Влияние: при интервале 30 с пропуск одного ping даёт паузу 60 с, это меньше 2 мин: запас есть.
- Бот: 30 с (DESIGN). Рекомендация: 20–25 с, как в SDK.

**Р30. Версия API Bitget.**
- Промт: v2.
- Факт: v2 переименован в «classic», продвигается UTA v3; даты отключения v2 не найдено.
- Влияние: если v2 отключат, пропадёт фид Bitget (источник необязательный, вес 0.25).
- Бот: v2; адаптер изолирован, переход на v3 затронет только `feeds/bitget.py`.

### BingX

**Р31. REST bookTicker.**
- Промт: поля `bid_price`/`ask_price` (`time?`).
- Факт: поля лежат **внутри `data.book_ticker`**, цены — JSON-числа, `time` необязателен; есть альтернативная
  camelCase-форма.
- Влияние: ожидание `data.bid_price` или строк сломает разбор; float потеряет точность.
- Бот: основной источник WS depth; REST-парсер терпим к обеим формам, `parse_float=Decimal`.

**Р32. depth BingX.**
- Промт: `bids`/`asks` строками, поле `T`.
- Факт: подтверждено, но **`asks` по убыванию**; в WS время в верхнем `ts`.
- Влияние: если взять `asks[0]` как лучший ask, получится худшая из пяти цен и завышенные mid и спред.
- Бот: `best_ask = min`, `best_bid = max`; `event_ms` = `ts`.

**Р33. `contracts` и `premiumIndex`.**
- Промт: `/quote/contracts` (`pricePrecision`, `status`).
- Факт: есть ещё `5` и `25`; типы int и строкой. `premiumIndex` с `symbol` — объект, без него — массив.
- Влияние: источник со `status` 5/25 признавался бы годным, а строковые типы роняли бы парсер.
- Бот: годен только `status == 1`; `int(str(x))`.

**Р34. Интервал depth.**
- Промт: `STONK-USDT@depth5@500ms`.
- Факт: подтверждено. 200ms доступен только BTC и ETH.
- Влияние: `@200ms` для STONK может быть отвергнут или проигнорирован (**НЕ ПОДТВЕРЖДЕНО**).
- Бот: `STONK-USDT@depth5@500ms`; при `max_age_seconds: 3` интервал 500 мс оставляет запас.

**Р35. Источник документации BingX.**
- Промт: `BingX-swap-api-doc/.../Perpetual_Swap_API_Documentation.md`.
- Факт: файл удалён (404). Актуальная дока — `BingX-API/docs-v3`. BXSKILL расходится с ней (подпись, лимит
  1/s, резервный домен).
- Влияние: по старой доке получились бы устаревшие лимиты и форматы.
- Бот: следует docs-v3.

### Lighter и FX

**Р36. Хост Lighter.**
- Промт: `mainnet.zklighter.elliot.ai` или `zk-perps-api.elliot.ai`.
- Факт: первый подтверждён SDK, LOA, CCXT и NT; второй встречается только в старом сниппете.
- Влияние: с неверным хостом Lighter не подключится никогда.
- Бот: первый по умолчанию; URL переопределяется в конфиге.

**Р37. Метаданные Lighter.**
- Промт: decimals и `last_trade_price` в `orderBooks`.
- Факт: они есть только в `orderBookDetails` (два списка); `orderBookOrders` — L3 с обязательными `market_id` и
  `limit`.
- Влияние: без `price_decimals` нельзя проверить сетку; без агрегации L3 REST-книга неверна.
- Бот: метаданные из `orderBookDetails.order_book_details[]`; мягкий парсер.

**Р38. Синхронизация книги Lighter.**
- Промт: по `offset`.
- Факт: только `begin_nonce == prev nonce`; `offset` не непрерывен.
- Влияние: проверка по `offset` давала бы ложные ресинки или пропускала реальные разрывы.
- Бот: проверка `begin_nonce`. Нужно решить, что делать без `begin_nonce` (§9, открытые вопросы).

**Р39. Ping и каналы Lighter.**
- Промт: свой ping.
- Факт: сервер сам шлёт `{"type":"ping"}`, ответ `{"type":"pong"}` обязателен; есть `ticker/<id>`.
- Влияние: без ответа на `{"type":"ping"}` сервер закрывает соединение.
- Бот: отвечает pong; использует `order_book/<id>`.

**Р40. market_index Lighter.**
- Промт: можно найти заранее.
- Факт: STONK не найден; новые id ≥ 4095.
- Влияние: жёстко заданный id может указывать на другой рынок; новые 64-битные id могут не поместиться в `int16` из
  OpenAPI.
- Бот: поиск через REST при старте, кэш; конфиг может закрепить `market_index`.

**Р41. FX-пары.**
- Промт: USDC→USDT через Binance или Gate.
- Факт: обе пары официально не подтверждены. У Gate с 2026-09-01 Unified USD Order Book.
- Влияние: если пары нет, режим FX не работает, и USDC-источник исключается (`FX_UNAVAILABLE`).
- Бот: режим `fixed` по умолчанию; выбор режима — после doctor (§7).

---

## 9. Чек-лист проверок: этап 1 (публичные данные) и этап 5 (минимальная live-заявка)

Сырые ответы этапа 1 сохраняются как фикстуры `tests/fixtures/<venue>/live_<дата>/` с пометкой источника.
Синтетика заменяется после сверки. Вспомогательные скрипты `gcall.py` и `ws_rec.py` приведены в §9.3. Они
проверены офлайн: подпись совпала с векторами §1.2. Против бирж не запускались.

### 9.1 Этап 1: живые публичные данные (без ключей)

**Команды бота:**
```bash
python -m gate_stonk doctor --config config.yaml --seconds 20
python -m gate_stonk index-watch --config config.yaml --minutes 60 --csv runtime_stonk/watch.csv
```

**REST (сохранить ответы в файлы):**
```bash
G=https://api.gateio.ws/api/v4
curl -s $G/spot/currency_pairs/STONK_USDT          # precision, amount_precision, min_*, trade_status, up/down_rate
curl -s $G/spot/time                               # смещение часов
curl -s "$G/spot/order_book?currency_pair=STONK_USDT&limit=5&with_id=true"
curl -s -D - -o /dev/null $G/spot/time             # есть ли X-Gate-RateLimit-* в REST
curl -s -H 'X-Gate-Size-Decimal: 1' $G/futures/usdt/contracts/STONK_USDT        # status, quanto_multiplier, enable_decimal
curl -s -H 'X-Gate-Size-Decimal: 1' "$G/futures/usdt/order_book?contract=STONK_USDT&limit=1&with_id=true"
curl -s -H 'X-Gate-Size-Decimal: 1' "$G/futures/usdt/tickers?contract=STONK_USDT"
curl -s $G/futures/usdt/index_constituents/STONK_USDT   # входит ли "Gate" (петля, Р18)
curl -s $G/spot/currency_pairs/USDC_USDT                # FX gate_spot (Р41)
B=https://api.bybit.com/v5/market
curl -s "$B/instruments-info?category=linear&symbol=STONKUSDT"   # baseCoin, tickSize, status, deliveryTime, symbolType
curl -s "$B/orderbook?category=linear&symbol=STONKUSDT&limit=1"; curl -s "$B/tickers?category=linear&symbol=STONKUSDT"; curl -s $B/time
BG=https://api.bitget.com/api/v2/mix/market
curl -s "$BG/contracts?productType=USDT-FUTURES&symbol=STONKUSDT"   # symbolStatus, pricePlace, priceEndStep
curl -s "$BG/ticker?symbol=STONKUSDT&productType=USDT-FUTURES"
curl -s "$BG/merge-depth?symbol=STONKUSDT&productType=USDT-FUTURES&limit=5"   # строки или числа
curl -s "$BG/orderbook?symbol=STONKUSDT&productType=USDT-FUTURES&limit=5"     # существует ли (Р25)
X=https://open-api.bingx.com/openApi/swap/v2; T=$(date +%s%3N)
curl -s "$X/quote/contracts?symbol=STONK-USDT&timestamp=$T"     # status, pricePrecision, типы полей
curl -s "$X/quote/bookTicker?symbol=STONK-USDT&timestamp=$T"    # форма: book_ticker или camelCase, есть ли time
curl -s -D - "$X/quote/depth?symbol=STONK-USDT&limit=5&timestamp=$T"   # порядок asks; X-RateLimit-Requests-*
curl -s "$X/quote/premiumIndex?symbol=STONK-USDT&timestamp=$T"; curl -s "$X/server/time"
L=https://mainnet.zklighter.elliot.ai/api/v1
curl -s "$L/orderBookDetails?filter=perp" > lighter_details.json
python -I -c "import json;d=json.load(open('lighter_details.json'));print([{k:x.get(k) for k in ('symbol','market_id','status','price_decimals','size_decimals','multiplier','quote_multiplier')} for x in d['order_book_details'] if 'STONK' in x['symbol'].upper()])"
curl -s "$L/orderBookOrders?market_id=<ID>&limit=50"            # сортировка, агрегация L3
curl -s "https://data-api.binance.vision/api/v3/exchangeInfo?symbol=USDCUSDT"   # status TRADING
curl -s "https://api.binance.com/api/v3/ticker/bookTicker?symbol=USDCUSDT"     # гео-доступ основного хоста
```

**WS-записи** (`python -I ws_rec.py URL FRAMES SECONDS OUT [PING EVERY]`). Что измерить в каждой:
```bash
# Gate spot: интервал spot.order_book в тихий период (приходит ли без изменений), формат spot.trades
python -I ws_rec.py wss://api.gateio.ws/ws/v4/ '[{"time":0,"channel":"spot.order_book","event":"subscribe","payload":["STONK_USDT","20","100ms"]},{"time":0,"channel":"spot.trades","event":"subscribe","payload":["STONK_USDT"]}]' 600 gate_spot.jsonl '{"time":0,"channel":"spot.ping"}' 10
# Gate futures: book_ticker (тип B/A при X-Gate-Size-Decimal), частота tickers
python -I ws_rec.py wss://fx-ws.gateio.ws/v4/ws/usdt '[{"time":0,"channel":"futures.book_ticker","event":"subscribe","payload":["STONK_USDT"]},{"time":0,"channel":"futures.tickers","event":"subscribe","payload":["STONK_USDT"]}]' 600 gate_fut.jsonl '{"time":0,"channel":"futures.ping"}' 10
# Bybit: КРИТИЧНО (Р19) — у повтора с тем же u меняется ли ts/cts; интервалы повторов; пустые стороны; symbol в delta tickers
python -I ws_rec.py wss://stream.bybit.com/v5/public/linear '[{"op":"subscribe","args":["orderbook.1.STONKUSDT","tickers.STONKUSDT"]}]' 900 bybit.jsonl '{"op":"ping"}' 20
# Bitget: паузы books1 при неизменной книге, частота ticker, ack/pong
python -I ws_rec.py wss://ws.bitget.com/v2/ws/public '[{"op":"subscribe","args":[{"instType":"USDT-FUTURES","channel":"books1","instId":"STONKUSDT"},{"instType":"USDT-FUTURES","channel":"ticker","instId":"STONKUSDT"}]}]' 600 bitget.jsonl ping 25
# BingX: gzip Ping, формат ack, приходит ли depth5@500ms без изменений, порядок asks
python -I ws_rec.py wss://open-api-swap.bingx.com/swap-market '[{"id":"s1","reqType":"sub","dataType":"STONK-USDT@depth5@500ms"},{"id":"s2","reqType":"sub","dataType":"STONK-USDT@bookTicker"}]' 600 bingx.jsonl
# Lighter: глубина snapshot, частота update, цепочка begin_nonce/nonce, есть ли begin_nonce всегда
python -I ws_rec.py wss://mainnet.zklighter.elliot.ai/stream '[{"type":"subscribe","channel":"order_book/<ID>"},{"type":"subscribe","channel":"ticker/<ID>"}]' 600 lighter.jsonl
# FX: Binance partial depth (оба хоста), Gate USDC_USDT
python -I ws_rec.py wss://stream.binance.com:9443/ws/usdcusdt@depth5@100ms '[]' 120 binance.jsonl
python -I ws_rec.py wss://api.gateio.ws/ws/v4/ '[{"time":0,"channel":"spot.book_ticker","event":"subscribe","payload":["USDC_USDT"]}]' 120 gate_usdc.jsonl '{"time":0,"channel":"spot.ping"}' 10
```

**Критерии этапа 1:**
- (1) Все пять символов найдены и активны. Записаны tick/precision/статусы. `market_index` Lighter найден.
- (2) Bybit `ts` у повторов задокументирован (Р19).
- (3) За 60 мин `index-watch` доля «индекс заблокирован» и причины исключений приемлемы. Базис spot/индекс
  измерен. Веса утверждены (Р24).
- (4) Выбран режим FX (§7).
- (5) Записано, есть ли Gate spot в `index_constituents`.

### 9.2 Этап 5: минимальная live-проверка

**Команда бота.** Шаги 5.1 (чтения, кроме `REQUEST_EXPIRED`), 5.3, 5.4 и 5.6 одной командой выполняет
`python -m gate_stonk smoke-test --config config.live.yaml --live` (README_RU, раздел 7, шаг 5): одна дальняя
post-only BUY под DMS, проверка чтением, отмена, проверка, выключение DMS. С `--probe-exptime` добавляется шаг 5.2
(просроченный `x-gate-exptime` на отмене несуществующей заявки и контроль), с `--dms-fire` — шаг 5.10 (вторая
заявка, DMS 10 с без продления). Сырые ответы без `user_id` и IP и ответы на вопросы ниже («Что записать») она
сохраняет в `<state_dir>/smoke/smoke_<UTC>.json`, кандидаты фикстур — в `<state_dir>/smoke/fixtures/`. Шаги
5.5, 5.7, 5.8, 5.9, 5.11 и 5.12 команда не выполняет: для них остаётся ручной путь ниже.

**Предусловия.**
- Каждый шаг с заявкой выполнять **только после явного «да» заказчика** (требование промта).
- Отдельный субаккаунт; ключ: чтение + спот-торговля, **без вывода**, с привязкой к IP; на счёте несколько десятков
  USDT.
- Ключи вводить в оболочке (`read -rs GATE_API_SECRET; export GATE_API_SECRET`), не в `.env` истории.
- Обозначения:
  - `Q` — минимальный объём: `max(min_base_amount, min_quote_amount/PFAR)` вверх до `amount_precision`;
  - `PFAR` — цена BUY **≤ индекс −10 %** на сетке `precision`;
  - `EXP` — exptime: `EXP=$(( $(date +%s%3N) + 10000 ))`.
- Все ответы сохранять: `user_id` перед коммитом вычистить.

```bash
# 5.1 Только чтение: режим, белый список пар, балансы, комиссии, формат списка, REQUEST_EXPIRED
python -I gcall.py GET /account/detail                 # key.mode, currency_pairs, ip_whitelist
python -I gcall.py GET /unified/unified_mode            # classic | multi_currency | ...
python -I gcall.py GET /spot/accounts                   # (unified: GET /unified/accounts)
python -I gcall.py GET /wallet/fee 'currency_pair=STONK_USDT'
python -I gcall.py GET /account/debit_fee              # GT-вычет: рекомендация выключить
python -I gcall.py GET /spot/orders 'currency_pair=STONK_USDT&status=open&page=1&limit=100&account=spot'
TS_SHIFT=-120 python -I gcall.py GET /spot/accounts     # ожидание: REQUEST_EXPIRED
# 5.2 Запись без заявок: право записи (DMS off) и label просроченного exptime (сравнить с контролем)
python -I gcall.py POST /spot/countdown_cancel_all '' '{"timeout":0,"currency_pair":"STONK_USDT"}'
EXPTIME_MS=1 python -I gcall.py DELETE /spot/orders/1 'currency_pair=STONK_USDT&account=spot'
EXPTIME_MS=$EXP python -I gcall.py DELETE /spot/orders/1 'currency_pair=STONK_USDT&account=spot'   # контроль: ORDER_NOT_FOUND?
# 5.3 Минимальная poc BUY далеко от рынка (ожидание: 201, status=open, finish_as=open)
BODY=$(printf '{"text":"t-s5-a1","currency_pair":"STONK_USDT","type":"limit","account":"spot","side":"buy","amount":"%s","price":"%s","time_in_force":"poc"}' "$Q" "$PFAR")
EXPTIME_MS=$EXP python -I gcall.py POST /spot/orders '' "$BODY"          # записать id -> ID
# 5.4 Чтение по id и по text (пока открыта)
python -I gcall.py GET /spot/orders/$ID 'currency_pair=STONK_USDT&account=spot'
python -I gcall.py GET /spot/orders/t-s5-a1 'currency_pair=STONK_USDT&account=spot'
# 5.5 (только если планируется reprice_via_amend) amend цены; затем price+amount вместе — принимается ли
EXPTIME_MS=$EXP python -I gcall.py PATCH /spot/orders/$ID 'currency_pair=STONK_USDT' "{\"currency_pair\":\"STONK_USDT\",\"account\":\"spot\",\"price\":\"$PFAR2\",\"amend_text\":\"t-am1\"}"
EXPTIME_MS=$EXP python -I gcall.py PATCH /spot/orders/$ID 'currency_pair=STONK_USDT' "{\"currency_pair\":\"STONK_USDT\",\"account\":\"spot\",\"price\":\"$PFAR\",\"amount\":\"$QPLUS\",\"amend_text\":\"t-am2\"}"
#     (опц., отдельное «да»: риск как в 5.7) проверка poc при amend — цена = текущий лучший ask:
EXPTIME_MS=$EXP python -I gcall.py PATCH /spot/orders/$ID 'currency_pair=STONK_USDT' "{\"currency_pair\":\"STONK_USDT\",\"account\":\"spot\",\"price\":\"$ASK\",\"amend_text\":\"t-am3\"}"
# 5.6 Отмена по id; после отмены — поиск по text (ожидание: not found) и по id; список finished по окну
EXPTIME_MS=$EXP python -I gcall.py DELETE /spot/orders/$ID 'currency_pair=STONK_USDT&account=spot'
python -I gcall.py GET /spot/orders/t-s5-a1 'currency_pair=STONK_USDT&account=spot'
python -I gcall.py GET /spot/orders "currency_pair=STONK_USDT&status=finished&from=$(( $(date +%s) - 600 ))&to=$(date +%s)&limit=100&account=spot"
# 5.7 Путь отказа post-only (Р3): BUY poc по текущему лучшему ask, объём Q.
#     Ожидание: ошибка POC_FILL_IMMEDIATELY ИЛИ 201 со status=cancelled, finish_as=poc. Риск: если poc не сработает,
#     исполнится Q по taker (минимальная сумма). Цену ASK взять непосредственно перед запросом:
ASK=$(curl -s 'https://api.gateio.ws/api/v4/spot/order_book?currency_pair=STONK_USDT&limit=1' | python -I -c "import json,sys;print(json.load(sys.stdin)['asks'][0][0])")
EXPTIME_MS=$EXP python -I gcall.py POST /spot/orders '' "$(printf '{"text":"t-s5-p1","currency_pair":"STONK_USDT","type":"limit","account":"spot","side":"buy","amount":"%s","price":"%s","time_in_force":"poc"}' "$Q" "$ASK")"
# 5.8 Пакет: 2 заявки далеко от рынка, затем пакетная отмена по id (порядок элементов, succeeded/label)
EXPTIME_MS=$EXP python -I gcall.py POST /spot/batch_orders '' "[$(printf '{"text":"t-s5-b1","currency_pair":"STONK_USDT","type":"limit","account":"spot","side":"buy","amount":"%s","price":"%s","time_in_force":"poc"}' "$Q" "$PFAR"),$(printf '{"text":"t-s5-b2","currency_pair":"STONK_USDT","type":"limit","account":"spot","side":"buy","amount":"%s","price":"%s","time_in_force":"poc"}' "$Q" "$PFAR")]"
EXPTIME_MS=$EXP python -I gcall.py POST /spot/cancel_batch_orders '' "[{\"currency_pair\":\"STONK_USDT\",\"id\":\"$ID1\",\"account\":\"spot\"},{\"currency_pair\":\"STONK_USDT\",\"id\":\"$ID2\",\"account\":\"spot\"}]"
# 5.9 iceberg + poc (Р4): объём Q3 (~3 минимума), видимая часть V >= min_base_amount; принят ли, эхо iceberg; затем отмена по id
EXPTIME_MS=$EXP python -I gcall.py POST /spot/orders '' "$(printf '{"text":"t-s5-i1","currency_pair":"STONK_USDT","type":"limit","account":"spot","side":"buy","amount":"%s","price":"%s","time_in_force":"poc","iceberg":"%s"}' "$Q3" "$PFAR" "$V")"
# 5.10 DMS: одна дальняя заявка, countdown 10 с, ждать 15 с, проверить отмену (status/finish_as), затем выключить
EXPTIME_MS=$EXP python -I gcall.py POST /spot/orders '' "${BODY/t-s5-a1/t-s5-d1}"     # записать id -> ID_DMS
python -I gcall.py POST /spot/countdown_cancel_all '' '{"timeout":10,"currency_pair":"STONK_USDT"}'   # triggerTime
sleep 15; python -I gcall.py GET /spot/orders/$ID_DMS 'currency_pair=STONK_USDT&account=spot'
python -I gcall.py POST /spot/countdown_cancel_all '' '{"timeout":0,"currency_pair":"STONK_USDT"}'
# 5.11 Cancel-all запасного пути (одна дальняя заявка открыта): формат ответа OrderCancel
EXPTIME_MS=$EXP python -I gcall.py POST /spot/orders '' "${BODY/t-s5-a1/t-s5-c1}"
EXPTIME_MS=$EXP python -I gcall.py DELETE /spot/orders 'currency_pair=STONK_USDT&account=spot'
# 5.12 Приватный WS на время шагов 5.3–5.11: события put/update/finish, finish_as у отказа poc
python -I ws_rec.py wss://api.gateio.ws/ws/v4/ '[{"time":0,"channel":"spot.orders","event":"subscribe","payload":["STONK_USDT"],"auth_gate":true},{"time":0,"channel":"spot.usertrades","event":"subscribe","payload":["STONK_USDT"],"auth_gate":true}]' 1800 gate_private.jsonl '{"time":0,"channel":"spot.ping"}' 10
# 5.13 Бот целиком: config.live.yaml с pair.mode: buy, один уровень edge_pct: 10, без iceberg, минимальный объём
GATE_LIVE_CONFIRM=I_UNDERSTAND_SPOT_TRADING python -m gate_stonk run --config config.live.yaml
python -m gate_stonk status --config config.live.yaml
python -m gate_stonk cancel-only --config config.live.yaml     # затем проверить пустой список open (5.1)
```

`EXP` пересчитывать перед каждым запросом: значение живёт 10 с. Переменные `ID`, `ID1`, `ID2`, `ID_DMS`,
`PFAR2` (другая дальняя цена), `QPLUS` (`Q` + 1 лот), `Q3`, `V`, `ASK` подставлять из предыдущих ответов и
метаданных пары. Если шаг 5.1 показал unified-режим, везде заменить `account=spot` на `account=unified`.

**Что записать по итогам этапа 5:**
- путь отказа post-only;
- label просроченного exptime;
- label поиска по `text` после отмены;
- поведение PATCH (price+amount; проверка poc при amend);
- iceberg+poc принят или нет, и с каким label;
- `triggerTime` и `finish_as` после DMS;
- есть ли `X-Gate-RateLimit-*`;
- что возвращает `/spot/accounts` в нашем режиме аккаунта;
- набор `finish_as` в WS.

После этого можно выставлять `execution.iceberg_poc_verified` и решать про `reprice_via_amend`.

**Открытые вопросы к реализации** (не факты биржи):
- (а) передавать ли `X-Gate-Size-Decimal: 1` при WS-handshake futures (Р15) — рекомендуется;
- (б) Lighter update без `begin_nonce`: принимать без проверки (как предлагает заметка по CCXT-примеру) или
  считать разрывом (fail-closed);
- (в) реакция на Bybit `ts` у повторов (Р19) — после замера.

### 9.3 Вспомогательные скрипты (кладутся вне репозитория, запуск через `.venv/bin/python -I`)

`gcall.py`: один подписанный запрос. Ключи только из окружения, в вывод не попадают.
```python
# python -I gcall.py METHOD PATH [QUERY] [BODY]; env: GATE_API_KEY, GATE_API_SECRET, EXPTIME_MS, TS_SHIFT
import hashlib, hmac, os, sys, time
import requests

method, path = sys.argv[1].upper(), sys.argv[2]
query = sys.argv[3] if len(sys.argv) > 3 else ''
body = sys.argv[4] if len(sys.argv) > 4 else ''
ts = str(int(time.time()) + int(os.environ.get('TS_SHIFT', '0')))
payload = '\n'.join([method, '/api/v4' + path, query, hashlib.sha512(body.encode()).hexdigest(), ts])
sign = hmac.new(os.environ['GATE_API_SECRET'].encode(), payload.encode(), hashlib.sha512).hexdigest()
headers = {'KEY': os.environ['GATE_API_KEY'], 'Timestamp': ts, 'SIGN': sign,
           'Accept': 'application/json', 'Content-Type': 'application/json'}
if os.environ.get('EXPTIME_MS'):
    headers['x-gate-exptime'] = os.environ['EXPTIME_MS']
url = 'https://api.gateio.ws/api/v4' + path + ('?' + query if query else '')
r = requests.request(method, url, headers=headers, data=body.encode() if body else None, timeout=10)
print(r.status_code, {k: v for k, v in r.headers.items() if k.lower().startswith(('x-gate', 'retry-after'))})
print(r.text)
```

`ws_rec.py`: запись принятых WS-кадров в JSONL. Отправленные кадры (с `KEY`) в файл не пишутся.
- gzip распаковывается.
- Отвечает на `Ping` (BingX) и `{"type":"ping"}` (Lighter).
- `"time": 0` в кадре заменяется текущим временем.
- `"auth_gate": true` добавляет подпись Gate из окружения.
```python
# python -I ws_rec.py URL FRAMES_JSON SECONDS OUT.jsonl [PING_FRAME PING_EVERY_S]
import gzip, hashlib, hmac, json, os, sys, time
import websocket

url, frames, secs, out = sys.argv[1], json.loads(sys.argv[2]), float(sys.argv[3]), sys.argv[4]
ping = sys.argv[5] if len(sys.argv) > 5 else None
ping_every = float(sys.argv[6]) if len(sys.argv) > 6 else 20.0


def prep(fr):
    if isinstance(fr, str):
        return fr
    fr = dict(fr)
    if fr.get('time') == 0:
        fr['time'] = int(time.time())
    if fr.pop('auth_gate', False):
        msg = 'channel=%s&event=%s&time=%d' % (fr['channel'], fr['event'], fr['time'])
        sign = hmac.new(os.environ['GATE_API_SECRET'].encode(), msg.encode(), hashlib.sha512).hexdigest()
        fr['auth'] = {'method': 'api_key', 'KEY': os.environ['GATE_API_KEY'], 'SIGN': sign}
    return json.dumps(fr)


def read(ws):
    try:
        data = ws.recv()
    except websocket.WebSocketTimeoutException:
        return None
    if isinstance(data, bytes):
        try:
            data = gzip.decompress(data)
        except OSError:
            pass
        data = data.decode('utf-8', 'replace')
    return data.strip()


def is_json_ping(text):
    try:
        return json.loads(text).get('type') == 'ping'
    except (ValueError, AttributeError):
        return False


hdr = ['X-Gate-Size-Decimal: 1'] if 'fx-ws.gateio.ws' in url else []
ws = websocket.create_connection(url, timeout=2, header=hdr)
end = time.monotonic() + secs
next_ping = time.monotonic() + ping_every
with open(out, 'w', encoding='utf-8') as f:
    first = read(ws)  # приветствие (Lighter шлёт {"type":"connected"} до подписки)
    if first is not None:
        f.write(json.dumps({'recv_ms': int(time.time() * 1000), 'frame': first}) + '\n')
    for fr in frames:
        ws.send(prep(fr))
    while time.monotonic() < end:
        if ping and time.monotonic() >= next_ping:
            ws.send(prep(json.loads(ping)) if ping.startswith('{') else ping)
            next_ping = time.monotonic() + ping_every
        text = read(ws)
        if text is None:
            continue
        if text == 'Ping':
            ws.send('Pong')                      # BingX
        elif is_json_ping(text):
            ws.send('{"type":"pong"}')           # Lighter
        f.write(json.dumps({'recv_ms': int(time.time() * 1000), 'frame': text}) + '\n')
ws.close()
```

---

## 10. Источники

Локальные копии лежали в каталоге сессии `scratchpad/vendor/` и в репозиторий не входят. Ниже — откуда их
получить заново.

**Gate**
- PyPI `gate-api` 7.2.144 (API v4.106.144): https://pypi.org/project/gate-api/
- Docs SDK: https://github.com/gateio/gateapi-python (docs/*.md)
- OpenAPI: `github.com/gate/gateapi-go/v7@v7.2.144/openapi.yaml` (proxy.golang.org)
- WS SDK: PyPI `gate-ws` 0.5.6; https://github.com/gate/gatews (Go SDK, `python/examples/local_order_book.py`)
- https://github.com/gateio/gatews/issues/25
- https://github.com/gateio/rest-v4 (README v4.22.0, устарел)
- Страницы (ПОДТВЕРЖДЕНО по поиску):
  - документация: https://www.gate.com/docs/developers/apiv4/en/ ,
    https://www.gate.com/docs/developers/apiv4/ws/en/ , https://www.gate.com/docs/developers/futures/ws/en/
  - объявления `https://www.gate.com/announcements/article/<N>`, N = 40657 (лимиты, fill ratio), 33995 (лимиты
    2024-01-22), 49555 (finish_as market 2026-01-28), 44678 и 44722 (поля `l`/`full`), 44815 (20ms), 44946 и
    45073 (obu), 48788 (size decimal), 29076 (iceberg), 101473 (Unified USD Order Book);
  - help `https://www.gate.com/help/...`: guide/faq/17521 (лимиты), trade/entrusted_transaction/40463
    (iceberg), futures/logical/22069 (index), futures/Logical/22067 (mark),
    unified-account/unified_account_overview/48328.
- Сторонние:
  - ccxt 4.5.85 (PyPI; `gate.py`, `pro/gate.py`);
  - https://github.com/thrasher-corp/gocryptotrader (exchanges/gateio, PR #2366);
  - `github.com/tonymontanov/go-gate/v2` v2.8.0;
  - hummingbot `gate_io_constants.py`;
  - https://github.com/Lqz13Th/extrema_infra/pull/148 ;
  - https://github.com/jholze/xagent-trading-bot/issues/549 ;
  - https://docs.tardis.dev/historical-data-details/gate-io .

**Bybit**
- https://github.com/bybit-exchange/docs (ветка **master**): `docs/v5/market/{instrument,tickers,orderbook,time}.mdx`,
  `docs/v5/websocket/public/{orderbook,ticker}.mdx`, `docs/v5/websocket/wss-authentication.mdx`,
  `docs/v5/rate-limit/rate-limit.mdx`, `docs/v5/{guide,enum,error}.mdx`, `docs/changelog/v5.mdx`. Ветка `main`
  устарела.
- PyPI `pybit` 5.17.0.
- REF `spot_portfolio/feeds.py`, `zec_spot/feeds.py`.
- Поиск: https://coinalyze.net/stonk-fun/usdt/bybit/open-interest-chart/stonkusdt_oi/ , https://announcements.bybit.com/en/?page=3 ,
  https://blofin.com/en/support/Announcement/Futures-Listing/16118428649589-BloFin-Futures-to-List-STONKUSDT-0-Maker-Fees-50-off-Taker-Fees .

**Bitget**
- https://github.com/BitgetLimited/v3-bitget-api-sdk (`bitget-python-sdk-api`)
- ccxt `bitget.py`, `pro/bitget.py`
- https://github.com/tiagosiebler/bitget-api (npm 3.2.5)
- https://github.com/JKorf/Bitget.Net
- Поиск:
  - https://www.bitget.com/api-doc/classic/contract/market/Get-Ticker
  - https://www.bitget.com/api-doc/classic/contract/market/Get-Merge-Depth
  - https://www.bitget.com/api-doc/classic/quickStart/websocket-intro
  - https://www.bitget.com/api-doc/classic/best-practices
  - https://www.bitget.com/api-doc/common/signature-samaple/interaction
  - https://www.bitget.com/api-doc/classic/uta-api-upgrade-guide
  - https://www.bitget.com/support/articles/12560603895628 (листинг STONKUSDT), …/12560603886018, …/12560603890581
  - https://onekey.so/blog/ecosystem/bitget-lists-usdt-margined-stonk-perpetual-futures-with-up-to-10x-leverage-20260919175840/

**BingX**
- https://github.com/BingX-API/docs-v3 (коммит 0cc3b7c, 2026-10-09) = https://bingx-api.github.io/docs-v3/
- https://github.com/BingX-API/api-ai-skills (38e99a1)
- https://github.com/BingX-API/BingX-swap-api-v2-doc (30fb6e8, 2023-08, устарел)
- ccxt `bingx.py`, `pro/bingx.py`; PyPI `bingx-py` 1.0.2
- Поиск: https://bingx.com/en-us/support/articles/17517228543887 , https://bingx.com/en/support/articles/17516494138767 .

**Lighter и FX**
- PyPI `lighter-sdk` 1.1.6
- https://github.com/elliottech/lighter-python (README, docs/*.md, openapi.json)
- ccxt `lighter.py`, `pro/lighter.py`
- https://github.com/nautechsystems/nautilus_trader (develop: `docs/integrations/lighter.md`,
  `crates/adapters/lighter/`)
- Поиск: https://apidocs.lighter.xyz/docs , https://apidocs.lighter.xyz/docs/rate-limits
- https://github.com/binance/binance-spot-api-docs (`rest-api.md`, `web-socket-streams.md`, `CHANGELOG.md`)
- https://www.tradingviewapi.com/markets/asset/BINANCE-USDCUSDT (сторонний)
