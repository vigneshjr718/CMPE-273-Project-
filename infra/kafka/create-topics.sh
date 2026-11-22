#!/bin/sh
# Creates the PartStream Kafka topics. Owner: D.
# The source of truth is contracts/events/topics.md. Keep this list in sync with it.
#
# Safe to run any number of times: existing topics are left alone, but a
# partition-count mismatch FAILS loudly. (Changing the partition count changes
# which partition each order_id key maps to, which breaks per-order ordering.)
#
# Usually run by the kafka-init compose service. Manual re-run (from the repo root):
#   docker compose -f infra/docker-compose.yml run --rm kafka-init
set -eu

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:9092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"   # single broker

# name|partitions|retention.ms   (-1 = keep forever; see topics.md "Retention")
TOPICS="
order.created|3|-1
workflow.events|3|-1
"

topics_cmd() {
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" "$@"
}

echo "create-topics: waiting for broker at $BOOTSTRAP_SERVER"
i=0
until topics_cmd --list >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 30 ]; then
    echo "create-topics: broker not reachable after 30 attempts" >&2
    exit 1
  fi
  sleep 2
done

status=0
for spec in $TOPICS; do
  name=$(echo "$spec" | cut -d'|' -f1)
  partitions=$(echo "$spec" | cut -d'|' -f2)
  retention=$(echo "$spec" | cut -d'|' -f3)

  topics_cmd --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$REPLICATION_FACTOR" \
    --config "retention.ms=$retention" \
    --config cleanup.policy=delete \
    --config min.insync.replicas=1 >/dev/null

  actual=$(topics_cmd --describe --topic "$name" \
    | sed -n 's/.*PartitionCount: *\([0-9][0-9]*\).*/\1/p' | head -n 1)
  if [ "$actual" != "$partitions" ]; then
    echo "create-topics: ERROR $name has $actual partitions, expected $partitions" >&2
    status=1
  else
    echo "create-topics: ok $name partitions=$actual retention.ms=$retention"
  fi
done

exit "$status"
