# Один трейс пустой очереди

Нужны Redis на `127.0.0.1:6391` и Postgres `bench` на `127.0.0.1:5432`.
Скрипт поднимает Redis сам, если порта нет. Порт 6379 он не открывает.

```bash
cd /workspace
PYTHONPATH=/workspace .venv/bin/python scripts/wait_trace.py celery
```

Аргумент — имя библиотеки: `simpleq`, `dramatiq`, `celery`, `arq`, `taskiq`.
Без аргумента скрипт идёт по всем пяти по очереди.

Скрипт пишет:

| Файл | Что внутри |
| --- | --- |
| `results/traces/<lib>.json` | потоки, wchan, py-spy, `pickup_ms` |
| `results/traces/<lib>.monitor.txt` | `MONITOR` этого прогона |
| `results/traces/<lib>.worker.log` | stdout воркера |

`pickup_ms` — разница полей в `LAT_PATH`. Это момент `fast_record`, не момент, когда Redis принял команду.

simple-q и arq на старте вызывают `core.open_deps()`. Без `PG_DSN` актор не доходит до записи файла. Скрипт ставит `postgresql://ubuntu@127.0.0.1:5432/bench` и `MOCK_URL=http://127.0.0.1:8090`.

Сборка этих страниц:

```bash
.venv/bin/pip install -r docs/requirements.txt
.venv/bin/sphinx-build -b html docs docs/_build/html
```
