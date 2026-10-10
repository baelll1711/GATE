# gate_stonk 0.1.0: тестовый бот на споте Gate STONK_USDT

Бот ставит лимитные **post-only** заявки лестницей уровней на споте Gate по паре `STONK_USDT`. Цены уровней
считаются от **собственного индекса** бота, который он строит по перпетуалам STONK на Bybit, Bitget, BingX,
Lighter и Gate futures. По умолчанию режим `paper`: реальные заявки не отправляются.

**Это торговля без хеджирования.** Купленный STONK может подешеветь, а индекс может ошибаться. Бот не
открывает фьючерсных позиций и ничем не компенсирует риск запаса.

Документы проекта:

- [LOGIC_RU.md](LOGIC_RU.md): формулы индекса, цен, лимитов, сверки, учёта и отчёта, рекомендации по настройке;
- [API_NOTES_RU.md](API_NOTES_RU.md): факты API бирж с их статусом и чек-листы живых проверок (§9);
- [TESTING_RU.md](TESTING_RU.md): офлайн-тесты, что они покрывают и что проверяется только вживую;
- `config.example.yaml`: полный пример конфигурации с комментариями.

**Состояние на 2026-10-10.** Из среды сборки биржи недоступны: `doctor` здесь завершается `ProxyError`.
Ни одно значение для STONK (шаг цены, минимумы, точные символы) пока не подтверждено. Цифр этапа 1 (спреды,
базис, доля блокировки индекса) ещё нет. Live-действия не выполнялись. Windows и реальная доставка Telegram
не проверялись. Офлайн-тесты: 1724 прошли, пропусков нет (Linux, Python 3.13).

## 1. Что делает бот

- Каждые 0.2 с пересчитывает индекс STONK в USDT: проверяет каждый источник (возраст, спред, рассинхрон
  времени, отклонение от медианы), исключает плохие и агрегирует остальные взвешенной медианой. Спот Gate
  STONK_USDT в индекс не входит. Его стакан нужен только для post-only проверки и статистики базиса.
- На каждой стороне держит лестницу уровней (в примере `b1`/`b2` для BUY и `s1`/`s2` для SELL). В каждом
  слоте `side:level` стоит не больше одной заявки. В примере BUY стоит на 1% и 2% ниже центра индекса, SELL на
  1% и 2% выше. Цены округляются от центра.
- Новая заявка не ставится ближе `safety.min_new_edge_pct` (1%) к центру **после округления**. Стоящая заявка,
  которая оказалась ближе `safety.hard_min_edge_pct` (0.8%), снимается сразу в локальной проверке, не дожидаясь
  сверки.
- Все заявки лимитные, `time_in_force: poc`. Перед отправкой бот проверяет, что BUY строго ниже лучшего ask и
  всех своих SELL, а SELL строго выше лучшего bid и всех своих BUY.
- Переставляет уровни при движении индекса: по порогу и интервалу или сразу, если заявка вышла за допуск.
  Отмены и создания отправляются пакетами.
- Держит резервы USDT и STONK, лимиты на заявку, сутки (UTC), кампанию и запас. Одна сумма USDT не
  резервируется дважды. Токены под SELL не продаются повторно.
- Признаёт заявки, исполнения и балансы только по REST. Резерв освобождается только после двух согласованных
  наблюдений и контрольного чтения. Неизвестный результат создания держит резерв, пока заявка не найдена или не
  истекло окно `x-gate-exptime`.
- В live продлевает DMS (`countdown_cancel_all`), чтобы Gate снял заявки пары, если процесс перестанет отвечать.
- Ведёт себестоимость и прибыль методом средней цены, пишет всё в SQLite, `status.json`, журнал и Telegram.
- В paper использует те же правила (`Engine`), но заявки живут в симуляторе. Исполнение по реальной ленте
  сделок Gate: только если сделка прошла **строго через** цену заявки.

## 2. Чего бот не делает

- Не хеджирует и не ставит фьючерсных заявок. Фьючерсы нужны только как источники цены.
- Не ставит рыночных заявок и заявок без post-only.
- Не выводит средства, не делает переводов между счетами, не берёт заём и не торгует с плечом.
- Не меняет режим аккаунта Gate и не использует `stp_act`.
- Не торгует другими парами и не «усыновляет» чужие или старые заявки. Посторонняя заявка или сделка на паре
  останавливает торговлю (`STOP_ACCOUNT_REVIEW`).
- Не продаёт STONK, которого нет на балансе (без шорта), и не продаёт `inventory_reserve_qty`.
- Не подстраивает базис спот/индекс автоматически: сдвиг центра задаётся вручную (`index.basis_offset_pct`).
- Не распродаёт запас при превышении `max_inventory_usdt`. Лимит только запрещает новые покупки.
- Не обещает прибыли. Ни paper, ни отчёт её не прогнозируют.

## 3. Риски

Прочитайте до live.

- **Нет хеджа, риск запаса.** Убыток ограничен только размером запаса. Держите `pair.max_inventory_usdt`,
  дневные и накопительные лимиты маленькими. `sell.min_profit_pct` защищает от продажи ниже себестоимости, но
  это не стоп-лосс: при долгом падении запас просто ждёт.
- **Индекс может быть неверным.** Все перпетуалы могут одновременно отставать или уходить от спота Gate.
  Взвешенная медиана вырождается в цену одного рынка, если у него больше 50% веса включённых источников
  (LOGIC §1.8). Bybit в примере обязательный: его сбой блокирует всю торговлю. Gate futures со спредом около 50%
  выпадает всегда. В режиме FX `fixed` реальный депег USDC меньше 1% не виден. Если Gate spot входит в индекс
  Gate futures, через `price: mark` возникает петля (поэтому в примере `mid`). Устойчивый базис спот/индекс
  делает одну сторону лестницы почти неисполняемой.
- **Ограничения модели paper.** Очередь на уровне цены не моделируется (это завышает исполнения). Сделка ровно
  по цене заявки её не исполняет (это занижает). Влияние своей заявки на рынок, сетевые задержки, приоритет и
  taker-комиссия скрытой части iceberg тоже не моделируются. Paper не даёт ни верхней, ни нижней оценки live.
- **Штраф Gate за низкий fill ratio.** Создание и amend делят лимит 10 запросов/с. При низкой доле исполнений
  Gate снижает его до 10 запросов за 10 с. Окно 24 ч, пересчёт ежечасно, учитываются все запросы на создание,
  изменение и отмену, включая неуспешные. Рекомендуется ratio > 0.1, точная формула для спота не подтверждена.
  Лестница в 1–2% от индекса исполняется редко, поэтому штраф вероятен. Бот держит 8 запросов за 10 с и
  создаёт заявки пакетом (пакет считается одним запросом). Главный рычаг здесь — реже переставлять (раздел 5,
  LOGIC §9 п. 3–4). `fill_ratio_pct` в отчёте бота — другая метрика, это не ratio Gate.
- **Iceberg.** По документации Gate исполнения скрытой части оплачиваются **по taker-ставке**. Совместимость
  `iceberg` с `poc` не документирована. Пока `execution.iceberg_poc_verified: false`, live с iceberg-уровнем не
  запускается (`ICEBERG_POC_NOT_VERIFIED`). В `config.example.yaml` у `b2`/`s2` iceberg включён: для live
  уберите его или пройдите проверку (API_NOTES §9.2, шаг 5.9).
- **Ограничения DMS.** `countdown_cancel_all` снимает **все** спот-заявки пары на аккаунте, в том числе ручные.
  Минимальный таймаут Gate 5 с, у бота 45 с: после аварии процесса заявки могут простоять до ~45 с. При выходе
  бот снимает заявки, но таймер не выключает. Таймер выключается только после сверки пустого аккаунта.
  `doctor --probe-write` выключает DMS пары, `cancel-only` его не трогает. Действует ли DMS на заявки
  `account: unified` и какой `finish_as` он ставит, не подтверждено (этап 5). Пока есть неподтверждённые
  отмены или новые заявки без сверки, DMS не продлевается. Если до его срока остаётся мало, бот сам снимает
  заявки (`REST_READ_DMS_DEADLINE`, `DMS_TOO_CLOSE_FOR_CREATE`).
- **Синхронизация часов обязательна.** Если часы отличаются от `GET /spot/time` больше чем на 1000 мс, бот
  снимает все заявки (`CLOCK_SKEW`). При расхождении больше 60 с Gate отвечает `REQUEST_EXPIRED`, и бот
  останавливается. Окно `x-gate-exptime`, свежесть котировок и метки «индекс в момент исполнения» и markout
  тоже зависят от часов. Включите синхронизацию времени: Windows `w32tm /query /status` (принудительно
  `w32tm /resync` от администратора), Linux `timedatectl status` (NTP должен быть `active`).
- **Post-only может быть отклонён** биржей, если рынок изменился между проверкой и отправкой. Уровень получает
  паузу 0, 0, 5, 10, 20, 40, 60 с. Проверяет ли Gate post-only при amend цены, не подтверждено: amend мог бы
  исполниться как taker. Поэтому `reprice_via_amend` выключен до проверки этапа 5. Очередь amend цены не
  сохраняет: заявка уходит в конец, как новая.
- **Непроверенное.** Реальные ответы Gate для STONK, живые потоки всех пяти источников, Windows и доставка
  Telegram не проверялись (API_NOTES §0, TESTING_RU).

## 4. Установка

Нужен Python 3.11–3.13 (на Windows установщик берёт 3.12). Зависимости: `requests`, `websocket-client`,
`PyYAML`, `python-dotenv`, `filelock` и `pytest` для тестов; версии закреплены в `pyproject.toml`.

### Windows (PowerShell)

1. Установите Python 3.12 x64 с python.org вместе с Python Launcher (`py`).
2. Распакуйте проект в отдельную папку, например `C:\Bots\gate_stonk`, и откройте в ней PowerShell:

```powershell
Set-Location "C:\Bots\gate_stonk"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\setup_windows.ps1
```

`setup_windows.ps1` создаёт `.venv`, ставит зависимости (`pip install -e ".[test]"`), копирует
`config.example.yaml` в `config.yaml` и `.env.example` в `.env` (если их ещё нет), выполняет `validate` и
офлайн-тесты. Торговлю он не запускает. `Bypass` действует только на этот запуск PowerShell.

Дальше команды можно вызывать напрямую (`.\.venv\Scripts\python.exe -m gate_stonk ...`) или через обёртку
`run_stonk.ps1` (раздел 9). Сохранённый статус удобно смотреть в отдельном окне:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\monitor_stonk.ps1 -StateDir runtime_stonk -Seconds 2
```

### Linux

```bash
cd /opt/gate_stonk
python3.12 -m venv .venv
.venv/bin/python -m pip install -e ".[test]"
cp -n config.example.yaml config.yaml
cp -n .env.example .env && chmod 600 .env
.venv/bin/python -m gate_stonk validate --config config.yaml
.venv/bin/python -m pytest -q
```

Во всех примерах ниже для Linux замените `.\.venv\Scripts\python.exe` на `.venv/bin/python`, а `\` в путях
на `/`.

### Файлы и папки

| Путь | Что это |
|---|---|
| `config.yaml` | Рабочий конфиг paper. Для live заведите отдельный, например `config.live.yaml` |
| `.env` | Ключи; читается из папки конфига |
| `STOP_TRADING`, `STOP_BUY`, `STOP_SELL` | STOP-файлы; создаются в папке конфига (раздел 10) |
| `runtime_stonk/` (`app.state_dir`) | `paper.sqlite3` или `live.sqlite3`, `status.json`, `doctor.json`, `index_watch.sqlite3`, CSV, `logs/gate_stonk_<дата UTC>.log`, `process.lock` |

`.gitignore` исключает из коммитов `.env`, `config.live*.yaml`, `config.my.yaml`, `runtime*/`, базы и
STOP-файлы. Не удаляйте базу ради «сброса»: в ней резервы, счётчики лимитов, учёт и очередь Telegram. Для
нового сценария заведите другой `state_dir` и `campaign_id`. База помнит кампанию, пару, режим, префикс и тип
аккаунта, и при несовпадении `run` откажется стартовать.

## 5. Конфиг: что настроить в первую очередь

Полный список с комментариями — в `config.example.yaml`. YAML чувствителен к отступам: используйте пробелы.
Неизвестные ключи и дубликаты отклоняются с указанием пути (`pair.buy.levels[1].edge_pct: ...`).

- `app.mode`: `paper` или `live`. `app.campaign_id` и `app.state_dir` задают отдельную базу.
- `index.sources`: веса, `required`, `price` (`mid`/`microprice`/`mark`/`last`), `max_spread_pct`.
  `index.fx.mode`: `fixed`, `binance_spot` или `gate_spot`. `index.basis_offset_pct`: ручной сдвиг центра.
- `safety.min_new_edge_pct` (1.0) и `safety.hard_min_edge_pct` (0.8). Валидатор требует
  `0 < hard < min_new` и отступ каждого уровня не меньше `min_new_edge_pct`.
- `pair.mode`: `buy`, `sell` или `both`. Уровни задаются в `pair.buy.levels` и `pair.sell.levels`: точный
  `edge_pct` или диапазон `min_edge_pct`/`max_edge_pct` + `skip_notional_usdt`. Размер задаёт
  `order_notional_usdt` и/или `order_qty` (берётся меньший). Iceberg: `iceberg_display_qty` или
  `iceberg_display_pct`.
- Лимиты: `pair.max_inventory_usdt`/`_qty`, `pair.<side>.max_total_*` и `max_daily_notional_usdt` (сутки UTC),
  `portfolio.cash_reserve_usdt`, `fee_reserve_pct`, `fee_estimate_pct`. `null` отключает лимит, `0` запрещает
  дальнейший объём.
- `band_tolerance_pct` и `reprice_threshold_pct`. При шаге цены около 0.074% (0.0001 при цене около 0.135)
  допуск 0.15 при пороге 0.15 приводит к немедленным снятиям `PREMIUM_RANGE_CHANGED`. Чтобы движение
  обрабатывал медленный `REPRICE`, держите допуск заметно выше порога, например 0.3 и 0.15 (LOGIC §3.2).
- `paper.initial_usdt`/`initial_base`: стартовые бумажные балансы. Они задаются **один раз на кампанию**,
  правка после первого запуска не добавляет денег. В примере `initial_base: 0`, поэтому SELL-уровни ждут с
  `BALANCE_OR_LIMIT`, пока бумажные BUY не исполнятся. Чтобы проверить обе стороны, задайте `initial_base` до
  первого запуска.
- `telegram.enabled`: для paper можно `false`, для live нужно `true`.

Проверить, какие цены даст лестница при заданном центре и точности, можно без сети:

```powershell
.\.venv\Scripts\python.exe -m gate_stonk validate --config config.yaml --center 0.1359 --precision 4 --amount-precision 2
```

## 6. `.env`, ключи и аккаунт

`.env` лежит рядом с конфигом (шаблон `.env.example`):

```dotenv
GATE_API_KEY=
GATE_API_SECRET=
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

- Для `validate`, `doctor` без ключей, `index-watch` и paper ключи не нужны.
- Переменные окружения имеют приоритет над `.env`: файл их не перезаписывает. Ключи можно задать только в
  оболочке текущего сеанса.
- **`GATE_LIVE_CONFIRM` в `.env` не пишите.** Бот его оттуда не читает, а если найдёт, откажется запускать
  live (`LIVE_CONFIRM_IN_ENV_FILE`).
- Секреты в журналах маскируются. Не присылайте `.env`, скриншоты с ключами и журналы команд, где ключи
  набирались.

**Аккаунт и ключ Gate** (до любого live):

1. Отдельный субаккаунт только для бота: без ручных заявок, других ботов и других программ на паре
   `STONK_USDT`. DMS и сверка работают по всей паре: чужая заявка будет снята DMS или остановит бота.
2. API-ключ с правами **только чтение и спот-торговля**. **Без вывода и без переводов.** Включите привязку к
   IP (белый список). Если у ключа есть белый список пар, в нём должна быть `STONK_USDT`.
3. Тип аккаунта: классический спот или unified. В конфиге `market.account: spot` или `unified`. `doctor` и
   `run` сверяют это с режимом ключа и при несовпадении отказывают.
4. **Выключите списание комиссии в GT** (GT deduction) и не используйте point-карты на этом субаккаунте.
   Тогда все комиссии будут в STONK или USDT, и прибыль будет точной. Комиссии в GT и POINT бот хранит
   отдельно и в USDT не пересчитывает: прибыль с ними **завышена**. `doctor` читает `GET /account/debit_fee`
   и предупреждает. Уточните `portfolio.fee_estimate_pct` по maker-комиссии из `doctor`.
5. Права ключа через API Gate не узнать. Проверьте их в веб-интерфейсе. `doctor --probe-write` проверяет право
   записи безвредным запросом `countdown_cancel_all timeout=0`, который **выключает DMS пары**. Проба
   выполняется только при отсутствии открытых заявок.

Telegram: создайте бота, нажмите у него Start в нужном чате, впишите токен и chat id. Бот присылает:

- сообщения о старте и остановке, размеры уровней при старте;
- каждое исполнение с позицией и прибылью;
- позицию после сверки;
- сводку раз в `telegram.summary_minutes`;
- остановки, отказы и неизвестные результаты;
- по желанию (`notify_orders`) — новые заявки.

В paper сообщения помечены `[PAPER]`. Очередь хранится в SQLite и переживает перезапуск. Доставка «минимум
один раз»: после потерянного ответа сообщение может прийти повторно с тем же `[event ...]`.

## 7. Рекомендуемый порядок

Каждый следующий шаг — только после того, как предыдущий прошёл. Всё до шага 5 не отправляет ни одной заявки.

### Шаг 1. `validate`

```powershell
.\.venv\Scripts\python.exe -m gate_stonk validate --config config.yaml
```

Команда проверяет конфиг локально и печатает сводку: источники, уровни, лимиты, DMS, режим. Сеть и ключи не
используются.

### Шаг 2. `doctor`

```powershell
.\.venv\Scripts\python.exe -m gate_stonk doctor --config config.yaml --seconds 30
```

Что проверяется только чтением:

- метаданные пары: точность цены и количества, минимумы и максимумы, `trade_status`;
- часы (Gate, Bybit);
- точное имя и статус каждого инструмента, `market_index` Lighter, масштабированные контракты;
- курс USDC→USDT;
- состав индекса Gate futures (входит ли туда спот Gate);
- котировки и индекс за `--seconds`, базис;
- размеры уровней против минимумов пары и предупреждение про iceberg в live;
- с ключами: режим аккаунта, белый список пар и IP, открытые заявки, балансы, комиссии, списание в GT.

Результат: строки `[ OK ]`/`[СБОЙ]`/`[ВНИМ]`/`[ПРОП]`, итог и `runtime_stonk\doctor.json`. Код выхода 0,
если обязательные проверки пройдены, иначе 1. Заявки, отмены, DMS (без `--probe-write`) и Telegram не
затрагиваются. `live_ready` в отчёте всегда `false`: чтение не подтверждает торговлю.

Чтобы закрепить точность пары, впишите значения из `doctor` в `market.expected_precision` и
`market.expected_amount_precision`. Если Gate их изменит, бот заблокируется, а не будет торговать на чужой
сетке.

### Шаг 3. `index-watch` не меньше 30 минут: цифры этапа 1

```powershell
.\.venv\Scripts\python.exe -m gate_stonk index-watch --config config.yaml --minutes 30 --csv runtime_stonk\watch.csv
```

Команда только читает данные. Раз в 0.2 с она пишет индекс, компоненты, bid/ask спота Gate и базис в
`runtime_stonk\index_watch.sqlite3` (таблица `watch_obs`) и CSV. Каждые 10 с печатается строка состояния, в
конце итог. Ctrl+C тоже печатает итог. Лучше записать 60 минут и в разное время суток.

Цифры этапа 1, которые нужно передать заказчику:

- индекс: валиден / заблокирован, % времени; эпизодов блокировки в час; причины блокировки;
- по каждому источнику: валиден %, исключён %, причины исключения, медиана и p95 спреда, медиана возраста
  события. Gate futures со спредом около 50% будет исключаться всегда;
- базис спота Gate к индексу: среднее, медиана, p5, p95, мин/макс;
- спред спота Gate: медиана и p95;
- из `doctor`: параметры пары, статусы инструментов, `market_index` Lighter, состав индекса Gate futures,
  расхождение часов, доступность FX-пары;
- поведение «тихого» Bybit: обновляется ли `ts` у повторов снимка (API_NOTES §9.1, Р19). Если не обновляется,
  в `index-watch` будут исключения Bybit с `STALE_EVENT_OR_CLOCK_SKEW` в спокойные периоды, а с ним
  блокировка всего индекса.

По этим цифрам решаются веса источников, обязательность Bybit, режим FX и `basis_offset_pct` (LOGIC §9).

### Шаг 4. Paper не меньше часа, затем отчёт

Без доступа к биржам (как в контейнере разработки) проверить весь цикл можно офлайн:
`python -m gate_stonk simulate --config config.yaml --minutes 60 --seed 1`. Это настоящие движок, paper-биржа,
индекс и отчёт, но рынок **синтетический** (случайное блуждание, всплески, выпадение источников, лента сделок с
редкими «проколами»). Цифры симуляции показывают, что цикл работает и где возникают перестановки и ожидания, но не
доказывают доходность. Решение о live принимается только по paper на реальных данных.

```powershell
.\.venv\Scripts\python.exe -m gate_stonk run --config config.yaml --seconds 3600
```

Во втором окне:

```powershell
.\.venv\Scripts\python.exe -m gate_stonk status --config config.yaml
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\monitor_stonk.ps1
```

После прогона:

```powershell
.\.venv\Scripts\python.exe -m gate_stonk report --config config.yaml --hours 1 --out runtime_stonk\report_paper_1h.json
```

Paper читает реальные публичные данные (индекс, стакан и ленту сделок Gate), поэтому ему нужен интернет.
Ключи ему не нужны. Без `--seconds` бот работает до Ctrl+C или `STOP_TRADING`. Смотрите в `status.json`
`slots.*.reason` (раздел 13), `index.ok`, балансы и лимиты. Как читать отчёт — раздел 12. Paper
сопоставляйте с live только за один и тот же период.

### Шаг 5. Live smoke-тест: только после явного «да» заказчика

Каждый шаг с реальной заявкой выполняется **только после явного «да» заказчика в чате**. Цель: одна
минимальная post-only BUY далеко от рынка (не выше индекс −10%), проверка в `status`, отмена, проверка
чтением, DMS. Подробный чек-лист с ручными запросами (`gcall.py`) — API_NOTES §9.2. Ниже тот же сценарий
силами бота.

1. Скопируйте `config.example.yaml` в `config.live.yaml` и измените (это фрагменты, остальные поля оставьте):

   ```yaml
   app:
     mode: live
     campaign_id: stonk_live_smoke
     state_dir: runtime_live_smoke     # отдельная база для live
   market:
     account: spot                     # или unified, как покажет doctor
   safety:
     dedicated_account_ack: true
     dead_man_switch: true
     require_telegram: true
   telegram:
     enabled: true
   pair:
     mode: buy
     max_inventory_usdt: 10
     buy:
       max_total_notional_usdt: 10
       max_daily_notional_usdt: 10
       levels:
         - {id: b1, edge_pct: 10, order_notional_usdt: 3.5, iceberg_display_qty: null}   # без iceberg
     sell:
       levels: []
   ```

   `order_notional_usdt` подберите так, чтобы `doctor` в проверке «Размеры уровней и минимумы пары» показал
   OK: чуть выше `min_quote_amount` пары с запасом на округление количества. Число 3.5 здесь только пример.

2. Проверки только чтением, с ключами в `.env`:

   ```powershell
   .\.venv\Scripts\python.exe -m gate_stonk validate --config config.live.yaml
   .\.venv\Scripts\python.exe -m gate_stonk doctor --config config.live.yaml --seconds 30
   ```

   Нужно, чтобы режим аккаунта совпал, на паре было 0 открытых заявок, баланс USDT покрывал заявку, списание
   в GT было выключено, а уровень `b1` проходил минимумы. По желанию добавьте `--probe-write` (выключает DMS
   пары; разумно, только пока заявок нет).

3. Запуск. Подтверждение задаётся только в текущем окне PowerShell:

   ```powershell
   $env:GATE_LIVE_CONFIRM = "I_UNDERSTAND_SPOT_TRADING"
   .\.venv\Scripts\python.exe -m gate_stonk run --config config.live.yaml --live
   ```

   В Linux подтверждение задаётся только для одной команды:
   `GATE_LIVE_CONFIRM=I_UNDERSTAND_SPOT_TRADING .venv/bin/python -m gate_stonk run --config config.live.yaml --live`.

4. Во втором окне выполните `status --config config.live.yaml` и проверьте:
   - `slots."buy:b1".state = OPEN`, есть `order.order_id`;
   - `order.price` не выше `index.center × 0.9` (центр равен индексу при `basis_offset_pct: 0`);
   - `dms_remaining_seconds` положительный и не больше 45;
   - пришло стартовое сообщение Telegram, заявка видна в веб-интерфейсе Gate.
5. Отмена. Создайте `STOP_TRADING` в папке конфига. В `status` должны быть `state: STOP_TRADING`, слот
   `ABSENT`, `own_orders_settled: true`, `exchange_orders_empty: true` и `dms_deadline_ms: 0` (DMS выключен
   после сверки пустого аккаунта). Затем остановите процесс Ctrl+C.
6. Проверка чтением:
   `.\.venv\Scripts\python.exe -m gate_stonk cancel-only --config config.live.yaml`. Ожидается строка
   «Проверено чтением: открытых заявок бота по паре нет.» Эта команда не требует `GATE_LIVE_CONFIRM` и не
   трогает DMS.
7. DMS, с отдельным «да». Удобнее всего по API_NOTES §9.2, шаг 5.10: одна дальняя заявка, countdown 10 с,
   ожидание 15 с, проверка статуса, выключение. Вариант силами бота:
   - удалите `STOP_TRADING`, снова запустите `run --live` и дождитесь `OPEN`;
   - аварийно завершите процесс (`Stop-Process -Id <process.pid из status.json> -Force`; в Linux
     `kill -9 <pid>`);
   - через 60 с выполните `doctor --config config.live.yaml --seconds 5`: в «Открытые заявки пары» должно быть
     «открыто 0»;
   - создайте `STOP_TRADING` и ещё раз запустите `run --live`, чтобы бот сверил и закрыл сохранённую строку
     (`RECOVERY`); затем Ctrl+C.
8. Запишите результаты по списку «Что записать по итогам этапа 5» (API_NOTES §9.2) и передайте заказчику.
   Флаги `execution.iceberg_poc_verified` и `execution.reprice_via_amend` включаются только после
   соответствующих проверок (шаги 5.5 и 5.9 там же).

### Шаг 6. Небольшой live

Переходите к нему после успешного smoke-теста и отдельного решения заказчика. Начните с одного уровня на
сторону, без iceberg, с размерами у минимума пары и маленькими лимитами (`max_inventory_usdt`,
`max_daily_notional_usdt`, `max_total_notional_usdt`). Сначала одна сторона, если так решит заказчик.
Параллельно гоняйте paper с той же лестницей в другой папке `state_dir`. Раз в сутки смотрите
`report --config config.live.yaml --hours 24`. Расширяйте лестницу только после того, как live совпал с paper
за тот же период.

## 8. Допуск к live

`run` в live проверяет условия локально, до сетевых запросов и записи файлов, в таком порядке. Первое
невыполненное даёт отказ с кодом и выход 2:

| Условие | Код отказа |
|---|---|
| `app.mode: live` требует флага `--live` | `LIVE_CONFIG_REQUIRES_EXPLICIT_RUN_LIVE_FLAG` |
| `--live` при `app.mode: paper` | `LIVE_FLAG_REQUIRES_APP_MODE_LIVE` |
| `--seconds` в live запрещён | `SECONDS_IS_PAPER_ONLY` |
| `GATE_LIVE_CONFIRM` не должен быть записан в `.env` | `LIVE_CONFIRM_IN_ENV_FILE` |
| `GATE_LIVE_CONFIRM=I_UNDERSTAND_SPOT_TRADING` в оболочке | `LIVE_CONFIRM_MISSING` |
| `safety.dedicated_account_ack: true` | `DEDICATED_ACCOUNT_ACK_REQUIRED` |
| `safety.dead_man_switch: true` | `DEAD_MAN_SWITCH_REQUIRED` |
| `GATE_API_KEY` и `GATE_API_SECRET` | `GATE_API_KEYS_MISSING` |
| при `safety.require_telegram: true`: `telegram.enabled: true` | `TELEGRAM_REQUIRED_BUT_DISABLED` |
| ...и `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` | `TELEGRAM_CREDENTIALS_MISSING` |
| iceberg-уровни только при `execution.iceberg_poc_verified: true` | `ICEBERG_POC_NOT_VERIFIED` |

Дальше идут блокировки: `state_dir\process.lock` и, в live, блокировка по ключу и паре во временной папке.
Второй экземпляр не запустится. После них проверяется идентичность базы. При старте бот сначала снимает
сохранённые заявки (`RECOVERY`). Затем идут медленные проверки: режим аккаунта против `market.account`, белый
список пар, `trade_status` для включённых сторон, часы. Временный сбой останавливает торговлю и повторяется
через 30, 60, 120, 240 с, затем каждые 300 с. Окончательный отказ (например, несовпадение режима аккаунта) снимает
сохранённые заявки, после чего процесс завершается.

В live новые заявки ждут первой подтверждённой доставки Telegram (стартового сообщения). Если Telegram
недоступен дольше `telegram.max_unhealthy_seconds` (120 с), заявки снимаются (`TELEGRAM_UNAVAILABLE`).

## 9. Команды

Формат: `python -m gate_stonk <команда> --config <файл>`. `--config` можно указать до или после команды, по
умолчанию `config.yaml`. `--version` печатает версию. Коды выхода: 0 — успех; 1 — ошибка конфига, проваленный
`doctor`, нет статуса или базы; 2 — отказ (допуск live, занятая блокировка, неподтверждённый `cancel-only`,
ошибка); 130 — прервано.

| Команда | Что делает | Сеть | Ключи | Заявки |
|---|---|---|---|---|
| `validate [--center C --precision P --amount-precision A]` | Проверка конфига; с `--center` — цены и размеры уровней на синтетическом широком стакане | нет | нет | нет |
| `doctor [--seconds 1..600] [--probe-write]` | Диагностика только чтением, `doctor.json`. По умолчанию 20 с | да | по желанию | нет; `--probe-write` выключает DMS пары |
| `index-watch [--minutes M] [--csv PATH]` | Запись индекса и спота в `index_watch.sqlite3` и CSV, итог в конце. Без `--minutes` — до Ctrl+C | да | нет | нет |
| `run [--live] [--seconds N]` | Бот: paper или live по `app.mode`. `--seconds` только для paper | да | в live | да |
| `cancel-only [--all-pair-orders --yes]` | Снять заявки бота (`t-<client_prefix>-...` и из базы) и проверить чтением до 60 с. DMS не трогает | в live | в live | только отмены |
| `status` | Показать сохранённый `status.json` и его возраст | нет | нет | нет |
| `report [--hours H] [--out FILE]` | Отчёт по уровням из базы режима `app.mode`, по умолчанию 24 ч | нет | нет | нет |
| `simulate [--minutes M] [--seed S] [--volatility V] [--state-dir DIR]` | Офлайн-прогон настоящего движка и paper на **синтетическом** рынке (виртуальные часы), итог — `report.json`, `status.json` в `<state_dir>/simulation_<seed>/`. Не реальные данные | нет | нет | только бумажные |

Примеры:

```powershell
# проверка и предпросмотр лестницы
.\.venv\Scripts\python.exe -m gate_stonk validate --config config.yaml --center 0.1359 --precision 4 --amount-precision 2
# диагностика 30 с; с ключами в .env — ещё и аккаунт
.\.venv\Scripts\python.exe -m gate_stonk doctor --config config.yaml --seconds 30
.\.venv\Scripts\python.exe -m gate_stonk doctor --config config.live.yaml --seconds 30 --probe-write
# запись индекса 60 минут
.\.venv\Scripts\python.exe -m gate_stonk index-watch --config config.yaml --minutes 60 --csv runtime_stonk\watch.csv
# paper на час; paper до Ctrl+C
.\.venv\Scripts\python.exe -m gate_stonk run --config config.yaml --seconds 3600
.\.venv\Scripts\python.exe -m gate_stonk run --config config.yaml
# live (подтверждение только в этом окне)
$env:GATE_LIVE_CONFIRM = "I_UNDERSTAND_SPOT_TRADING"
.\.venv\Scripts\python.exe -m gate_stonk run --config config.live.yaml --live
# снять заявки бота и проверить чтением (бот должен быть остановлен)
.\.venv\Scripts\python.exe -m gate_stonk cancel-only --config config.live.yaml
# снять ВСЕ заявки пары, включая чужие
.\.venv\Scripts\python.exe -m gate_stonk cancel-only --config config.live.yaml --all-pair-orders --yes
# офлайн-симуляция на синтетическом рынке (60 виртуальных минут примерно за минуту)
.\.venv\Scripts\python.exe -m gate_stonk simulate --config config.yaml --minutes 60 --seed 1
# статус и отчёт
.\.venv\Scripts\python.exe -m gate_stonk status --config config.yaml
.\.venv\Scripts\python.exe -m gate_stonk report --config config.yaml --hours 24 --out runtime_stonk\report_24h.json
```

`cancel-only` работает только при остановленном боте: блокировка `process.lock` не даст запустить его рядом
с `run`. В paper он снимает бумажные заявки в базе. Записи в базе сверяются при следующем `run`.

**Обёртка `run_stonk.ps1`** принимает параметры `-Command` (`doctor`, `index-watch`, `run`, `cancel-only`,
`status`, `report`, `validate`, `simulate`; по умолчанию `run`), `-Config`, `-Live`, `-Minutes` (для `index-watch`), `-Csv`,
`-Hours` (для `report`, по умолчанию 24), `-Seconds` (для paper `run`), `-Seed` (для `simulate`). Флаги `--probe-write`, `--out`, `--center` и
`--all-pair-orders` через обёртку не передаются: для них вызывайте Python напрямую.

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_stonk.ps1 -Command doctor
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_stonk.ps1 -Command index-watch -Minutes 30
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_stonk.ps1 -Command report -Hours 1
$env:GATE_LIVE_CONFIRM = "I_UNDERSTAND_SPOT_TRADING"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\run_stonk.ps1 -Command run -Config config.live.yaml -Live
```

`monitor_stonk.ps1 -StateDir <папка> -Seconds <период>` раз в несколько секунд печатает сохранённый
`status.json` и его возраст. Это локальный снимок, а не свежий запрос к бирже.

## 10. STOP-файлы и остановка

STOP-файлы создаются **в папке конфига**. Бот проверяет их на каждом тике (не реже 0.2 с) и перед каждым
REST-чтением. Сам он их не удаляет: торговля возобновляется, когда файл удалят.

| Файл | Действие |
|---|---|
| `STOP_TRADING` | Снять все заявки, новые не ставить. DMS не продлевается и выключается после сверки пустого аккаунта |
| `STOP_BUY` | То же только для BUY-уровней |
| `STOP_SELL` | То же только для SELL-уровней |

```powershell
New-Item -ItemType File -Path .\STOP_TRADING -Force
New-Item -ItemType File -Path .\STOP_SELL -Force
Remove-Item .\STOP_SELL
Remove-Item .\STOP_TRADING
```

В Linux: `touch STOP_TRADING`, `rm STOP_TRADING`. Если файл не читается, это тоже остановка
(`STOP_TRADING_FILE_UNREADABLE` и т. п.). STOP-файлы общие для всех конфигов в одной папке: `STOP_TRADING`
остановит и paper, и live, запущенные оттуда.

- **Ctrl+C** (и SIGTERM): бот отправляет отмены сохранённых заявок (`PROCESS_SHUTDOWN`), пишет финальный
  статус и до 15 с досылает очередь Telegram. DMS не выключается, а неподтверждённые резервы остаются до
  следующего `run`. Самый аккуратный выход: создать `STOP_TRADING`, дождаться `own_orders_settled: true`, затем
  нажать Ctrl+C.
- **`STOP_ACCOUNT_REVIEW`** — это не файл, а постоянная отметка в базе. Она появляется при посторонней
  открытой заявке на паре или посторонней сделке. Бот доводит свои заявки до конца, чужие не трогает и не
  смешивает с учётом. Перезапуск отметку не снимает. Разберитесь, откуда активность, и начните новую кампанию
  в отдельной папке `state_dir`. Старую базу не удаляйте.

## 11. `status.json`

Файл записывается атомарно раз в `app.status_interval_seconds` (1 с) и после каждого шага. Это локальный
снимок: смотрите на `updated_ms` и возраст файла. Главные поля:

| Поле | Содержание |
|---|---|
| `mode`, `currency_pair`, `version` | Режим, пара, версия |
| `state`, `state_changed_ms`, `stop_reason`, `global_stop` | Состояние шага: `RUNNING`, `RECOVERY`, `STOP_TRADING`, `CANCEL_RECONCILIATION`, `WAIT_ACCOUNT_RECONCILIATION`, `TELEGRAM_UNAVAILABLE`... и причина остановки |
| `last_sync_ms`, `account_complete`, `recovering` | Последняя согласованная сверка; полнота прохода; восстановление после старта |
| `own_orders_settled`, `exchange_orders_empty`, `open_snapshot` | Своих строк нет; на бирже по паре пусто; id из последнего снимка открытых заявок |
| `account_review_required`, `external_activity` | Посторонняя активность (`STOP_ACCOUNT_REVIEW`) |
| `dms_deadline_ms`, `dms_remaining_seconds` | Срок DMS (0 — выключен) и сколько до него осталось |
| `clock_skew_ms` | Расхождение часов с Gate |
| `index` | `ok`, `index`, `center`, `reason`, `n_included`, `fx_rate`; `components` по каждому источнику: `included`, `reason`, `usdt_price`, `spread_pct`, `weight`, `required` |
| `spot` | bid/ask спота Gate, `basis_pct`, `error` |
| `pricing.buy` / `pricing.sell` | Индекс и центр расчёта, `maker_limit`, порог себестоимости и потолок, bid/ask |
| `slots."side:level"` | `state` (`ABSENT`, `OPEN`, `CANCEL_PENDING`, `UNCONFIRMED`, `SETTLING`), `reason` (причина ожидания или отмены, раздел 13), `note`, `target` (цена, причина, `placeable`, полоса), `order` (`text`, `order_id`, цена, количество, исполнено, остаток, iceberg, `cancel_reason`...), `backoff` (попытки, сколько ждать) |
| `balances` | USDT и STONK: `total`, `hold`, `free` |
| `reservations` | Резервы BUY (USDT) и SELL (STONK), доступно для новых заявок |
| `limits` | По сторонам: дневной и накопительный оборот против лимитов, в заявках; `inventory`: запас, BUY в заявках, оценка в USDT против лимита |
| `position` | Позиция бота: количество, средняя цена, себестоимость, реализованная прибыль, комиссии по валютам, продажи из прежнего запаса |
| `startup_sizes` | Размер каждого уровня из конфига при старте: количество, цена, сумма, ниже ли минимума пары |
| `rate_limiter`, `signed_actions_60min` | Корзины лимитера; подписанные действия за час |
| `telegram`, `outbox_pending` | Здоровье Telegram, недоставленные сообщения |
| `feeds`, `index_builder` | Соединения источников, ошибки, состояние возврата исключённых источников |
| `counters`, `errors` | Счётчики: шаги, отправлено, принято, отказы post-only, неизвестные результаты, отмены, продления и выключения DMS |
| `process` | `pid`, `started_ms`, `steps`, `startup_checks` (повторы проверок старта), `refusal`, `fill_wakeup`; в финальном снимке `exit_reason` |

## 12. Как читать `report`

`report` открывает базу только на чтение (можно рядом с работающим ботом) и строит одну таблицу по слотам,
сторонам и итогу за окно `--hours`:

| Колонка | Смысл |
|---|---|
| `Заявок`, `С исп.`, `Fill,%` | Заявки, дошедшие до стакана; из них с хотя бы одним исполнением; доля таких заявок. Отклонённые и отказы post-only не учитываются (они в строке «Отклонено биржей») |
| `Исп.`, `Объём STONK`, `Сумма USDT`, `Ср. цена` | Число исполнений, объём, сумма до комиссий, средняя цена |
| `Выст.,%` | Средний отступ цены заявки от центра при выставлении. Из-за округления он чуть больше отступа уровня: для `b1` 1% при цене 0.1359 и шаге 0.0001 это 1.03% |
| `Исп./выст.,%` | Отступ цены исполнения от того же центра |
| `Исп./индекс,% (n)` | Отступ от индекса **в момент исполнения** (наблюдения не дальше ±1 с), `n` — сколько исполнений имеют такую отметку |
| `5 с,%`, `30 с,%`, `60 с,%` | Markout: отступ цены исполнения от индекса через 5/30/60 с. Плюс — индекс остался на выгодной стороне; устойчивый минус — неблагоприятный отбор |
| `Реал. за окно, USDT` | Реализованная прибыль после комиссий (кроме GT/POINT), приписанная SELL-уровню, который продал |

Знак везде одинаковый: плюс в нашу пользу. Под таблицей идут:

- отклонения биржей и отказы post-only;
- прибыль за окно и за кампанию; «средняя продажа − средняя покупка» до комиссий; оборот;
- позиция бота;
- комиссии по валютам (GT/POINT не пересчитываются);
- доля времени заблокированного индекса и её причины с `покрытием`. В торговом режиме индекс пишется в
  основном, пока есть заявки, поэтому доля занижена. Меряйте её в `index-watch`;
- базис (среднее, медиана, p5, p95);
- правила выставления (`FIXED_EDGE`, `BOOK_VOLUME`, `+TOUCH`...) и причины отмен (`REPRICE`,
  `PREMIUM_RANGE_CHANGED`, `HARD_MIN_EDGE`...);
- сколько отметок индекса взято из журнала и сколько рассчитано.

`--out` сохраняет полный JSON. Отчёт строится по базе режима `app.mode`: для live укажите live-конфиг. Если в
отчёте есть строка «ВНИМАНИЕ: обнаружена посторонняя активность», учтены только собственные исполнения.

На что смотреть: какие уровни вообще исполняются и с каким markout. Много ли `PREMIUM_RANGE_CHANGED` и
`HARD_MIN_EDGE` среди отмен: это много запросов и риск штрафа за fill ratio. Сколько отказов post-only.
Сдвинут ли базис. Подробнее о подборе параметров — LOGIC §7 и §9.

## 13. FAQ: почему уровень не выставляется

Причина видна в `status.json` → `slots."side:level".reason` (слот в состоянии `ABSENT`) и в общем `state`.
Проверки слота идут по порядку, срабатывает первая. При глобальной остановке (`STOP_TRADING`,
`STOP_ACCOUNT_REVIEW`, внутренняя причина) смотрите `state` и `global_stop`. Шаг создания заявок тогда не
выполняется, и `reason` слота может показывать последнее значение до остановки, например `SLOT_ACTIVE`.

| Код | Что значит | Что делать |
|---|---|---|
| `POST_ONLY_OR_SELF_CROSS_WAIT` | Цена уровня коснулась бы лучшей встречной цены спота или своей встречной заявки. Точный уровень (`edge_pct`) не сдвигается, диапазонный сдвигается на тик (`+TOUCH`), только если остаётся в полосе | Обычно это значит, что спот Gate ушёл от индекса больше чем на отступ уровня. Посмотрите `spot.basis_pct` и базис в `index-watch`; при устойчивом базисе рассмотрите `basis_offset_pct` |
| `BALANCE_OR_LIMIT` | После всех ограничений (баланс, резервы, запас, дневной и накопительный лимит, `cash_reserve_usdt`, `inventory_reserve_qty`) количество 0 или ниже минимума пары | Смотрите `balances`, `reservations`, `limits`. В paper с `initial_base: 0` SELL ждёт так, пока нет купленного STONK. Дневной лимит обнуляется в 00:00 UTC |
| `ORDER_SIZE_BELOW_MINIMUM` | Сам размер из конфига после округления до лота меньше `min_base_amount` или `min_quote_amount` пары | Увеличьте `order_notional_usdt`/`order_qty`; проверка минимумов есть в `doctor` и `startup_sizes` |
| `INDEX_BLOCKED:<код>` | Индекс заблокирован. Стоящие заявки при этом снимаются с `BAD_MARKET:<код>` | Код после двоеточия: `REQUIRED_SOURCE_INVALID:bybit_perp:STALE_QUOTE`, `INSUFFICIENT_SOURCES:1/2`, `REJOIN_PENDING`... Детали в `index.components`. Проверьте сеть, источники и часы |
| `SPOT_BOOK:<код>` | Стакан спота Gate устарел или неисправен | Проверьте соединение с Gate (`feeds`) |
| `REJECT_BACKOFF` | Пауза слота после отказа: post-only 0, 0, 5, 10, 20, 40, 60 с; прочие отказы 15–300 с; `NOT_SENT` 5 с | `slots.*.backoff` показывает попытки и остаток паузы. Частые отказы post-only — признак слишком близкой цены или быстрого рынка |
| `ORDER_RATE_WAIT` | Корзина лимитера create + amend заполнена (8 за 10 с), или Gate ответил `TOO_MANY_REQUESTS`/`TOO_FAST` (блок на 15 с) | Кратко — норма. Если держится долго, слишком часто переставляете: расширьте `band_tolerance_pct`, увеличьте `min_reprice_seconds` |
| `ICEBERG_POC_NOT_VERIFIED` | Iceberg-уровень в live при `execution.iceberg_poc_verified: false` (с таким конфигом live даже не стартует) | Уберите iceberg из уровня или пройдите проверку этапа 5 |
| `ICEBERG_DISPLAY_BELOW_MINIMUM`, `ICEBERG_DISPLAY_INVALID` | Видимая часть меньше минимумов пары или не в интервале `0 < видимая < размер` | Увеличьте видимую часть или размер |
| `SLOT_ACTIVE` | Предыдущая заявка слота ещё не освобождена сверкой (обычно 2–3 с после отмены) | Ждать |
| `MAX_OPEN_ORDERS` | Достигнут `safety.max_open_orders` | Увеличьте лимит или уменьшите число уровней |
| `ACTION_CAP_WAIT` | До часового предела подписанных действий осталось 20 или меньше | Слишком много действий: переставляйте реже |
| `MIN_NEW_EDGE_BUFFER`, `LEVEL_COLLAPSED` | Отступ после округления меньше буфера; уровень нельзя сделать строго глубже предыдущего | Разнесите уровни сильнее (шаг цены около 0.07%) |
| `PROFIT_FLOOR_ABOVE_RANGE`, `PROFIT_CEILING_BELOW_RANGE` | При `sell.min_profit_pct` порог себестоимости SELL выше полосы уровня, или потолок BUY от своей SELL ниже полосы | Так и задумано: продажа в убыток запрещена (LOGIC §2.8) |
| `POSITION_RECONCILIATION_WAIT` | Позиция бота больше баланса STONK (возможен вывод), а `min_profit_pct` задан | Ждать двух сверок; не выводите STONK с субаккаунта бота |
| `PAIR_NOT_TRADABLE:<статус>` | `trade_status` пары запрещает эту сторону | Ждать, пока Gate откроет торговлю |
| `SIDE_DISABLED`, `STOP_BUY`, `STOP_SELL`, `STOP_TRADING` | Сторона выключена `pair.mode` или STOP-файлом | Раздел 10 |
| `STOP_ACCOUNT_REVIEW` | Посторонняя заявка или сделка на паре | Раздел 10 |
| `CANCEL_RECONCILIATION` | Есть отмены без подтверждения: новые заявки и DMS ждут | Ждать; повторы отмены идут через 5, 10, 20, 40, 60 с |
| `WAIT_ACCOUNT_RECONCILIATION`, `ACCOUNT_READ_FAILED` | Чтения аккаунта сбоят; после 12 с (`read_error_grace_seconds`) все заявки снимаются | Сеть, ключ, лимиты Gate |
| `REST_BUDGET_WAIT` | Сверка отложена локальным лимитом запросов | Кратко — норма |
| `TELEGRAM_UNAVAILABLE` | В live нет подтверждённой доставки Telegram | Проверьте токен, chat id, Start у бота |
| `DMS_NOT_CONFIRMED`, `DMS_TOO_CLOSE_FOR_CREATE`, `DMS_OR_ACTION_UNKNOWN` | DMS не продлён, до его срока слишком мало времени, ответ DMS неизвестен | Обычно сетевые задержки; следите за повторяемостью |
| `UNKNOWN_CREATE_RESULT`, `UNKNOWN_ORDER_KEEP_RESERVATION:<text>` | Ответ на создание потерян. Резерв держится, заявка ищется по `text` в открытых, завершённых и `my_trades`. Если её не нашли, до конца окна `x-gate-exptime` (в примере 50 с) снимаются все заявки бота, затем строка закрывается как непринятая | Ждать; при повторении проверьте сеть |
| `REJECTED:<label>`, `NOT_SENT:<причина>` | Явный отказ Gate или отказ до отправки | Смотрите label в журнале. `GATE_AUTH:<label>` останавливает бота (ключ, IP, права) |
| `CLOCK_SKEW:<мс>` | Часы расходятся с Gate больше 1000 мс | Синхронизируйте время |
| `AWAITING_DECISION`, `ORDER_SENT`, `NOT_EVALUATED` | Заявка готовится или отправлена; слот ещё не оценивался | Ждать |

## 14. Открытые вопросы к заказчику

1. **Веса источников.** В примере Bybit 0.4, Bitget 0.25, BingX 0.15, Gate futures 0.1, Lighter 0.1.
   В снимке рынка из задания нет цен и объёмов Bitget и BingX, поэтому их веса не выведены из данных.
   Пришлите 24-часовые объёмы STONK-перпетуалов на Bitget и BingX или утвердите веса по цифрам `index-watch`.
   Должен ли Bybit оставаться `required`? Сейчас любой его сбой блокирует торговлю.
2. **Размеры и лимиты.** Размер каждого уровня (в примере 20/30 USDT), `max_inventory_usdt` (200), дневные и
   накопительные лимиты для первого live; число уровней и их отступы (1% и 2%).
3. **Режим.** `pair.mode` для первого live: только `buy`, только `sell` или `both`. Нужен ли
   `sell.min_profit_pct`, и есть ли на субаккаунте STONK с известной себестоимостью (`initial_avg_price`)?
4. **Тип аккаунта.** Классический спот или unified; отдельный субаккаунт; привязка ключа к IP.
5. **Источник FX USDC→USDT для Lighter.** `fixed` (по умолчанию; депег меньше 1% не виден), `binance_spot`,
   `gate_spot` или отключить Lighter. Существование пар `USDCUSDT` на Binance и `USDC_USDT` на Gate и доступ к
   ним с вашего сервера проверит `doctor`.
6. **«Тихий» Bybit.** Если замер этапа 1 покажет, что `ts` у повторов снимка не обновляется, нужно решение:
   правка кода (время повтора по получению) или Bybit без `required`.
7. **Iceberg.** Нужен ли он вообще, если скрытая часть оплачивается по taker-ставке, а совместимость с
   post-only не документирована? Если нужен, требуется отдельное «да» на проверку iceberg+poc (API_NOTES §9.2,
   шаг 5.9). Отдельно — на проверку пути отказа post-only (шаг 5.7): если `poc` не сработает, минимальная
   заявка исполнится как taker.
8. **Перестановки.** Согласны ли начать с `band_tolerance_pct: 0.3` при `reprice_threshold_pct: 0.15` (меньше
   отмен и запросов), и включать ли `reprice_via_amend` после проверки этапа 5?
