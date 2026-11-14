# Retry-enqueue fault stand

```bash
cd /workspace   # or repo root
python3 -m venv .venv-sq120
. .venv-sq120/bin/activate
pip install 'simple-q-runtime==1.2.0' 'simple-q-worker==1.2.0' redis

python scripts/simpleq_retry_enqueue_fault.py
echo exit:$?
```

Ports: Redis `6420`, proxy `6421`. Dir: `results/long/retry-enqueue-6420`.

Exit `0` — all scenarios `complete` and armed faults fired. Exit `2` — incomplete or fault did not fire.

Key outputs:

- `baseline.json` / `baseline-fault-rerun.json`
- `run.json`
- `{scenario}.effect`, `.monitor`, `.summary.json`, `.timeline.json`
- `worker-*.log`
- `delay-probe.json`
