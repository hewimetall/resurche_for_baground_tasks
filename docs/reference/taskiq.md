# taskiq: что делает пустой воркер

Стенд: taskiq 0.13.0 и taskiq-redis 1.2.4.
Команда: `taskiq worker bench.taskiq_app:broker --workers 1 --max-async-tasks 1`.
Обзор: [taskiq-python.github.io](https://taskiq-python.github.io/).

`RedisStreamBroker.__init__` принимает `xread_block: int = 2000` и сохраняет его в `self.block`.
`listen` крутит `xreadgroup(..., block=self.block, count=self.count)`. На стенде группа `taskiq`, поток `bench`.
Подтверждение — `XACK` после того, как сообщение отдано на обработку. Режим стенда `--ack-type when_executed` к форме ожидания не относится: блок стоит на `XREADGROUP` в любом из режимов ack.

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

Два таких вызова за окно: блок 2 с истёк, следующий ушёл сразу. После `XADD` от клиента следующий `XACK` стоит через несколько миллисекунд в том же соединении ребёнка. 405 мс — остаток текущего блока, не полный интервал 2 с.
