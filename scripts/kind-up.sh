#!/usr/bin/env bash
# Однонодовый kind без systemd: dockerd уже должен слушать /var/run/docker.sock.
# Память ноды ограничена, чтобы кластер не забрал хост. Воркеры сюда не входят.
set -euo pipefail
cd /workspace

avail_kb=$(awk '/MemAvailable/{print $2}' /proc/meminfo)
echo "mem_available_kb=${avail_kb}"
if (( avail_kb < 2200000 )); then
  echo "мало свободной памяти для kind, стоп" >&2
  exit 1
fi

sudo chmod 666 /var/run/docker.sock
export KUBECONFIG=/tmp/kind.kubeconfig

if ! kind get clusters | grep -qx bench; then
  kind create cluster --name bench --config deploy/kind/cluster.yaml --kubeconfig "$KUBECONFIG" --wait 180s
fi
kubectl cluster-info
kubectl get nodes -o wide

node=$(docker ps -qf 'name=bench-control-plane')
if [[ -z "$node" ]]; then
  echo "контейнер ноды не найден" >&2
  exit 1
fi
# Потолок 3 ГиБ на всю ноду вместе с control plane. --memory-swap на cgroup v2
# может не приняться: тогда только --memory.
if ! docker update --memory 3g --memory-swap 3g "$node"; then
  docker update --memory 3g "$node"
fi
docker stats --no-stream --format '{{.Name}} mem={{.MemUsage}} cpu={{.CPUPerc}}' "$node"
echo "KUBECONFIG=$KUBECONFIG"
