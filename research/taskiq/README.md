# Taskiq-only research stand

Углублённое исследование Taskiq 0.13.0 + taskiq-redis 1.2.4 (`RedisStreamBroker`).

- Отчёт: [`docs/reference/taskiq-deep.md`](../../docs/reference/taskiq-deep.md)
- Артефакты: [`results/taskiq-deep/`](../../results/taskiq-deep/)
- Приложение: `app/broker.py`
- Эксперименты: `experiments/e01_*.py` … `e11_*.py`

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.lock
redis-server --port 6413 --bind 127.0.0.1 --daemonize yes --save '' --dir /tmp/redis-taskiq-research
export PYTHONPATH=$PWD
python -m research.taskiq.experiments.e01_batch_pel
python -m research.taskiq.run_all
```

AAA: readiness handshake, bounded waits, monotonic time, unique stream/group namespaces, real Redis, real processes, `Popen.poll`/`wait`.
