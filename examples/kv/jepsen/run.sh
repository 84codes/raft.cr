#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "Building Jepsen test cluster..."
docker compose build

echo "Starting DB nodes..."
docker compose up -d n1 n2 n3 n4 n5

echo "Waiting for SSH to be ready on all nodes..."
for node in n1 n2 n3 n4 n5; do
  until docker exec jepsen-$node bash -c "ss -tlnp | grep -q :22" 2>/dev/null; do
    sleep 0.5
  done
  echo "  $node ready"
done

echo "Running Jepsen test..."
docker compose run --rm control lein run test \
  --nodes "n1,n2,n3,n4,n5" \
  --time-limit "${TIME_LIMIT:-60}" \
  --concurrency "${CONCURRENCY:-25}" \
  "$@"

echo ""
echo "Results in: examples/kv/jepsen/store/latest/"
echo "To browse results: docker compose run --rm -p 8080:8080 control lein run serve"
