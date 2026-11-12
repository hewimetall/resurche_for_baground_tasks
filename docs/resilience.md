# Устойчивость

Все строки ниже — шкала `BENCH_SCALE=short`, если не сказано иное. Полный список файлов: `results/<lib>/*.json`. Сценарий, которого нет в каталоге, не запускался.

Общие параметры шкалы: пачка 8, быстрый прогон 80, блокировка 4 с, долгая задача 4×1 с, grace бесконечной задачи 6 с, Redis недоступен 5 с. 12-минутная задача, OOM cgroup на каждой доставке и 10 000 задач не запускались: одновременно с kind на машине с 16 ГБ без swap это выходило за запас, который стенд сам для себя обозначил (остановка ниже 800 МБ свободных).

## 1. Пачка на нескольких процессах

`parallel`. Инвариант потерь и дублей: 0 у simple-q, Dramatiq, Celery, arq, taskiq. Статус `done`.

## 2. Recycle по числу задач

simple-q: 12 элементов, pid сменились (5 значений), потерь 0. Celery: отдельный `recycle` тоже с инвариантом 0. arq: в JSON прямо сказано, что recycle по числу задач нет. taskiq: `--max-tasks-per-child` оборвал пачку, дошёл 1 элемент из 12, потеряны 11 (`results/taskiq/recycle.json`).

## 3. Soft-лимит памяти

`CHAOS_START=alloc:30` на пачке из 8.

| Библиотека | Что записано |
| --- | --- |
| simple-q | pid 9649 → 9669, потерь 0, статус done |
| Celery | pid 12234 → 12242, потерь 0, статус done. Проверка лимита у Celery происходит после задачи |
| Dramatiq, arq, taskiq | сценарий не измерял лимит: в JSON причина «лимита RSS процесса в библиотеке нет» |

Разовый всплеск 300 МиБ и OOM на каждой доставке не ставились.

## 4. SIGKILL посреди задачи

Окно задавалось окружением (simple-q heartbeat 5 с × 2, Dramatiq 8 с, Celery visibility 12 с, taskiq idle 8 с).

| Библиотека | Попытки в JSON | Вывод по этому окну |
| --- | --- | --- |
| simple-q | `block-start` deliveries 1, затем deliveries 2 через ~10.2 с от kill | повтор есть, растёт `deliveries`, не `retries` |
| Dramatiq | одна `block-start` | за окно сценария повтор не записан |
| Celery | одна `block-start` | за окно сценария повтор не записан |
| arq | см. `results/arq/sigkill.json`: процессный stop после kill вышел с кодом 0, gap пустой | отдельного счётчика доставок нет |
| taskiq | одна `block-start` | при `--ack-type when_executed` подтверждение уходит в момент старта |

Пять SIGKILL подряд — в `docs/rollout.md`. Итог short: poison с хуком только у simple-q.

## 5. SIGTERM с grace и kill без grace

Короткая задача (`sigterm_short`): simple-q вышел за 5.0 с с кодом 0, Dramatiq за 6.4 с с кодом 0, Celery за 1.2 с с кодом 0, arq за 5.6 с с кодом 0, taskiq за 5.2 с с кодом 0.

Бесконечная блокировка и grace 6 с (`sigterm_infinite`): simple-q, Dramatiq, arq, taskiq — код −9, `T_drain` около 6 с. Celery в этом файле вышел с кодом 0 за 1.2 с.

Kind с grace 12 с и живой блокировкой: все четверо (simple-q, Dramatiq, Celery, taskiq) получили 137 ровно на дедлайне. См. `docs/rollout.md`.

## 6. Rolling restart и сигнал в первые секунды

Процессы, simple-q: задержки 0.4, 1.5, 2.5 с, каждый раз `T_drain` 0.2 с, код 0, процесс после вызова stop уже не жив. Двойной ранний рестарт — те же 0.2 с и код 0. `bash -c` и `exec` в `shell_sigterm` тоже завершились с кодом 0 за 0.2 с. Потеря SIGTERM в первые ~3 с, известная по rc6, на rc7 в этих прогонах не повторилась.

Dramatiq на раннем SIGTERM при коротком shutdown ушёл в −9 примерно через 5 с. Celery и taskiq выходят за ~1 с с кодом 0, arq за 0.2 с.

Kind: см. таблицу двойного rollout в `docs/rollout.md`. Код 0, единицы секунд, деплой доходит до Ready.

## 7. Poison

simple-q, пять kill: статус `failed`, `poison deliveries=6 retries=0`. Таймаут `limitjob`: статус `failed`, заметка `timeout deliveries=1 retries=1`.

Dramatiq `limit`: `TimeLimitExceeded`, статус `failed`. Пять kill: статус остался `running`, видна одна попытка.

Celery `limit`: `SoftTimeLimitExceeded`, статус `failed`. Пять kill: статус `running`.

arq `limit`: статус остался `running`, заметок хука нет. Пять kill: две попытки, статус `running`.

taskiq `limit`: `TimeoutError`, статус `failed`. Пять kill: четыре старта `long`, статус `running`. Хук считает исключения handler, не смерть процесса.

## 8. Долгая блокировка, пока процесс жив

`blocking`, 4 с, и `block_lease` у simple-q (20 с при окне 10 с).

simple-q `block_lease`: одна доставка, есть `block-end`, повторной выдачи нет. `blocking` 4 с: start и end на `deliveries=1`.

Dramatiq, Celery, arq, taskiq: блокировка 4 с завершилась одной парой start/end. Окно владения в этих прогонах было длиннее 4 с (у arq timeout 34 с, у taskiq idle 8 с). Задача длиннее окна на стенде не запускалась. По коду брокера такая задача будет выдана повторно, пока первый исполнитель ещё работает: Celery `visibility_timeout`, arq TTL in-progress, taskiq `idle_timeout`. У simple-q heartbeat пишет супервизор, и измеренный 20-секундный `time.sleep` это подтвердил.

12 минут asyncio и 12 минут `time.sleep` не запускались.

## 9. Дедуп

| Библиотека | Второй send | После завершения | Инвариант |
| --- | --- | --- | --- |
| simple-q | `accepted: false`, id `null` | после ack новый send принят | 1 вызов mock, дублей 0 |
| Dramatiq | дубль исполнен | — | `duplicated: 1` |
| Celery | дубль исполнен | — | `duplicated: 1` |
| arq | второй send отклонён | после ack тоже отклонён: ключ `_job_id` остаётся вместе с результатом | дублей исполнения 0 |
| taskiq | оба send приняты с одним task id | третий тоже | mock `calls: 3` |

Дубль после SIGKILL отдельным ключом измерен у simple-q в `sigkill`: то же message id, `deliveries` вырос. Сбой Redis ровно в момент снятия `unique_key` отдельным сценарием не выделялся. Близкое, что есть: `redis_loss` и `redis_down`.

## 10. Отмена

`cancel_before` и `cancel_mid`. У всех пяти библиотек отмена — флаг в БД, который handler проверяет сам. Элементы без записи в `results` попадают в поле `lost` инварианта (5 и 12 на short) при `duplicated: 0`. Это пропущенные по флагу элементы, не потерянные брокером сообщения. Снять уже выполняющийся handler сигналом библиотеки прогон не проверял. `dramatiq-abort` в адаптер не подключён.

## 11. Redis

`redis_loss`, рестарт без AOF: у simple-q, Dramatiq, Celery, arq и taskiq три ожидаемых элемента потеряны (`lost: 3`), попыток 0. С AOF те же три элемента дошли (`lost: 0`).

`redis_down`, пауза 5 с:

| Библиотека | Воркер жив | Дошло элементов |
| --- | --- | --- |
| simple-q | да | 0 из 6, статус running |
| Dramatiq | да | 6 из 6, done |
| Celery | да | 6 из 6, done |
| arq | нет | 0 из 6 |
| taskiq | да | 6 из 6, done |

simple-q за эти 5 с очередь не добрал: период опроса heartbeat длиннее паузы. arq после обрыва соединения сам вышел (код 1 в stop).

`maxmemory` + `noeviction` прогнан, файл `results/<lib>/maxmemory.json`. На short это проверка, что брокер не принимает запись при полном Redis, а не замер деградации.

## 12. Короткий прогон 80 задач

Цифры и оговорка про p50 simple-q — в `docs/bench.md`.
