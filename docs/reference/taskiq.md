# taskiq: что делает пустой воркер

Стенд: taskiq 0.13.0 и taskiq-redis 1.2.4.
Команда: `taskiq worker bench.taskiq_app:broker --workers 1 --max-async-tasks 1`.
Обзор: [taskiq-python.github.io](https://taskiq-python.github.io/).

`RedisStreamBroker.__init__` принимает `xread_block: int = 2000` и сохраняет его в `self.block`.
`listen` крутит `xreadgroup(..., block=self.block, count=self.count)`. На стенде группа `taskiq`, поток `bench`.
Режим стенда `--ack-type when_executed` к форме ожидания не относится: блок стоит на `XREADGROUP` в любом из режимов ack. `XACK` в этом режиме уходит после возврата handler.

## Трейс

`results/traces/taskiq.json`. Простой 2.820 с, `pickup_ms` 405.

| Процесс | wchan | py-spy |
| --- | --- | --- |
| родитель CLI | `hrtimer_nanosleep` | `ProcessManager.start` |
| ребёнок | `ep_poll` | `EpollSelector.select` → `start_listen` |

`MONITOR` в простое:

```text
XREADGROUP GROUP taskiq <consumer> COUNT 100 BLOCK 2000 STREAMS bench >
```

Два таких вызова за окно: блок 2 с истёк, следующий ушёл сразу. После короткой задачи следующий `XACK` стоит через несколько миллисекунд: handler уже вернулся. Разрез, где `sent_ms` отделён от команды, — в {doc}`/reference/phase-series`: там `XADD` на +416 мс, файл через 2.5 мс, `XACK` на +419.9 мс. 405 мс этого одиночного трейса приходятся на время до `XADD`.

## Когда вызывается XACK

Установлены taskiq 0.13.0 и taskiq-redis 1.2.4. `RedisStreamBroker.listen` читает `XREADGROUP` с `noack=False` и отдаёт `AckableMessage`. Сам `listen` `XACK` не шлёт. Вызов сидит в `_ack_generator` (`taskiq_redis/redis_broker.py`). Момент выбирает `Receiver.callback` (`taskiq/receiver/receiver.py`):

| Режим | Когда `AckController.ack` |
| --- | --- |
| `when_received` | после строки `Executing task`, до `run_task` |
| `when_executed` | после возврата `run_task`, до `post_execute` и `set_result` |
| `when_saved` | после `set_result` и `post_save` |
| `manual` | только из `Context.ack` |

`run_task` дожидается возврата handler или пойманного исключения. CLI по умолчанию ставит `when_saved` (`taskiq/cli/worker/args.py`). Стенд в `bench/workers.py` и `scripts/kind_rollout.py` передаёт `--ack-type when_executed`. Задачи стенда `Context.ack` не вызывают.

Пустой `fetched` делает `continue` до `xautoclaim` (`redis_broker.py`). `idle_timeout` — это `min_idle_time` того `XAUTOCLAIM`, и на пустом чтении он не применяется. `XREADGROUP` с `>` не возвращает id, которые уже в pending.

## Kill во время handler

Один прогон на `127.0.0.1:6396`, Redis после него остановлен с `SHUTDOWN NOSAVE`. Воркер — тот же `--ack-type when_executed`, `TASKIQ_IDLE_MS` не задан (в адаптере это 600000). Задача написала `ENTRY` и ушла в `asyncio.sleep(20)`. Процесс убит после `ENTRY` и до выхода из сна. Сырой `MONITOR` в репозиторий не положен; числа прогона — `results/traces/taskiq-ack.json`.

| Момент | `XPENDING bench taskiq` |
| --- | --- |
| во время сна | count 1, id `1794567753041-0`, deliveries 1, idle 72 мс |
| после SIGKILL, воркера нет | count 1, deliveries 1, idle 392 мс |
| второй воркер слушал 8 с | count 1, deliveries 1, idle 8805 мс, `XLEN` 1 |

`XADD` один. `XACK` 0. `XAUTOCLAIM` 0. Второй воркер строку `Executing task` не написал. Запись осталась pending у первого consumer. Повторной доставки за эти 8 с не было, и причиной был не `XACK`: подтверждения не случилось. Пустое чтение до `XAUTOCLAIM` не дошло. Режимы `when_received`, `when_saved` и recycle этим прогоном не убивались.
