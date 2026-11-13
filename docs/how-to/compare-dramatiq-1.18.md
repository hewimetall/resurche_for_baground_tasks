# Как сравнить Dramatiq 1.18.0 и 2.2.1

1.18 стоит в отдельном интерпретаторе. В основной `.venv` его не ставить: там 2.2.1 и другой потолок `redis`.

```bash
python3.12 -m venv /tmp/venv-d118
/tmp/venv-d118/bin/pip install 'dramatiq==1.18.0' 'redis>=4.2,<6'
```

На этой машине поставились Dramatiq 1.18.0 и redis-py 5.3.1. Сервер — Redis 7.0.15 на `127.0.0.1:6392`.

Прогон:

```bash
.venv/bin/python scripts/dramatiq_118_gap.py
```

Актор — `scripts/d118_probe.py`, очередь `bench`, `--processes 1 --threads 1`.
Холодный запуск ждёт 1.5 с и шлёт одно сообщение. Прогретый ждёт 16 с.
Каждая версия — три холодных и три прогретых запуска.
Формула `compute_backoff` считается в том же интерпретаторе, без Redis: 2000 первых значений и 400 значений после 24 промахов при `max_backoff=5000`.

Результат: `results/traces/dramatiq-118.json`.
Числа и то, чего прогон не покрывает, — в {doc}`/reference/dramatiq-1.18`.
Почему потолок на живом воркере 1000 мс, а не аргумент по умолчанию у `consume`, — в {doc}`/explanation/dramatiq-1.18`.
