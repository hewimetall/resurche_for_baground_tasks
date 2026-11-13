#!/usr/bin/env bash
# Имитация RollingUpdate maxSurge для одного профиля compose.
# Измеренный rollout в results/kind снят kind, не этим скриптом.
# Воркер в compose — exec-форма, без sh -c.
set -euo pipefail
svc=${1:?service}
grace=${2:-60}
prestop=${PRESTOP:-1}
compose=(docker compose --profile "$svc")
old=$("${compose[@]}" ps -q "$svc")
count=$(wc -w <<<"$old")
"${compose[@]}" up -d --no-recreate --scale "$svc=$(( count * 2 ))" "$svc"
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
