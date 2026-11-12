#!/usr/bin/env bash
# Имитация RollingUpdate maxSurge: новые контейнеры поднимаются, затем старые
# получают SIGTERM. На этой VM Docker не было, скрипт не исполнялся.
# Воркер в compose должен быть exec-формой, без sh -c.
set -euo pipefail
svc=${1:?service}
grace=${2:-60}
prestop=${PRESTOP:-1}
old=$(docker compose ps -q "$svc")
count=$(wc -w <<<"$old")
docker compose up -d --no-recreate --scale "$svc=$(( count * 2 ))" "$svc"
if [[ $prestop == 1 ]]; then
  for c in $old; do
    docker exec "$c" touch /tmp/draining || true
  done
fi
echo "sigterm_at=$(date +%s.%N)"
for c in $old; do
  docker stop -t "$grace" "$c" &
done
wait
for c in $old; do
  docker inspect -f '{{.Name}} {{.State.FinishedAt}} exit={{.State.ExitCode}}' "$c"
done
