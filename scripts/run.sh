#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
COMPOSE=(docker compose --project-name debezium-sequence-lab --file "$ROOT_DIR/docker-compose.yml")
TOPIC=debezium-sequence-lab.lab.events

compose_step() {
  if [[ "${LAB_QUIET:-0}" == "1" ]]; then
    local output
    if ! output=$("${COMPOSE[@]}" "$@" 2>&1); then
      printf '%s\n' "$output" >&2
      return 1
    fi
    return 0
  fi

  "${COMPOSE[@]}" "$@"
}

sql_value() {
  local service=$1
  local sql=$2
  "${COMPOSE[@]}" exec -T "$service" psql -XAtq -U postgres -d lab -c "$sql"
}

connect_api() {
  "${COMPOSE[@]}" exec -T connect curl --fail --silent "$@"
}

wait_for_value() {
  local service=$1
  local sql=$2
  local expected=$3
  local description=$4
  local actual=""

  for _ in $(seq 1 120); do
    actual=$(sql_value "$service" "$sql" 2>/dev/null || true)
    if [[ "$actual" == "$expected" ]]; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for $description; last value: ${actual:-<empty>}" >&2
  "${COMPOSE[@]}" logs --tail 160 connect >&2
  return 1
}

wait_for_connector() {
  local connector=$1
  local status=""

  for _ in $(seq 1 120); do
    status=$(connect_api "http://localhost:8083/connectors/$connector/status" 2>/dev/null || true)
    if [[ $(grep -o '"state":"RUNNING"' <<< "$status" | wc -l) -ge 2 ]]; then
      return 0
    fi
    if [[ "$status" == *'"state":"FAILED"'* ]]; then
      echo "Connector $connector failed: $status" >&2
      "${COMPOSE[@]}" logs --tail 160 connect >&2
      return 1
    fi
    sleep 1
  done

  echo "Timed out waiting for connector $connector: ${status:-<empty>}" >&2
  "${COMPOSE[@]}" logs --tail 160 connect >&2
  return 1
}

register_connector() {
  local config_file=$1
  connect_api \
    --request POST \
    --header "Content-Type: application/json" \
    --data-binary "@/config/$config_file" \
    http://localhost:8083/connectors >/dev/null
}

echo "==> Resetting the lab"
"${COMPOSE[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true

echo "==> Starting PostgreSQL, Kafka, and Debezium Connect"
compose_step up -d --wait source target kafka connect

echo "==> Verifying the source and JDBC sink plugins"
plugins=$(connect_api http://localhost:8083/connector-plugins)
for connector_class in \
  io.debezium.connector.postgresql.PostgresConnector \
  io.debezium.connector.jdbc.JdbcSinkConnector; do
  if [[ "$plugins" != *"$connector_class"* ]]; then
    echo "Required connector is missing: $connector_class" >&2
    exit 1
  fi
done

echo "==> Seeding 1,000 source rows with an explicit CREATE SEQUENCE default"
"${COMPOSE[@]}" exec -T source psql -X -U postgres -d lab < "$ROOT_DIR/db/seed.sql" >/dev/null

echo "==> Taking the initial snapshot with pg_dump"
"${COMPOSE[@]}" exec -T source pg_dump -U postgres --no-owner --no-privileges lab \
  | "${COMPOSE[@]}" exec -T target psql -X -v ON_ERROR_STOP=1 -U postgres -d lab >/dev/null

source_snapshot=$(sql_value source "SELECT max(id) || ':' || (SELECT last_value FROM lab.id_sequence) FROM lab.events;")
target_snapshot=$(sql_value target "SELECT max(id) || ':' || (SELECT last_value FROM lab.id_sequence) FROM lab.events;")
if [[ "$source_snapshot" != "1000:1000" || "$target_snapshot" != "1000:1000" ]]; then
  echo "Snapshot invariant failed: source=$source_snapshot target=$target_snapshot" >&2
  exit 1
fi

echo "==> Creating the table topic before starting the sink"
"${COMPOSE[@]}" exec -T kafka /kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions 1 \
  --replication-factor 1 >/dev/null

echo "==> Registering the PostgreSQL source connector without a data snapshot"
register_connector source.json
wait_for_connector debezium-source
wait_for_value source \
  "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'debezium_sequence_lab_slot';" \
  "1" \
  "the Debezium replication slot"

echo "==> Registering the Debezium JDBC sink against the restored target table"
register_connector sink.json
wait_for_connector debezium-sink

echo "==> Inserting 500 additional rows at the source"
sql_value source "INSERT INTO lab.events (payload) SELECT 'cdc-' || value FROM generate_series(1, 500) AS value;" >/dev/null
wait_for_value target "SELECT count(*) FROM lab.events;" "1500" "1,500 rows at the target"

if [[ "${SHOW_KAFKA_EVENT:-0}" == "1" ]]; then
  echo
  echo "First CDC event stored in Kafka:"
  kafka_event=$("${COMPOSE[@]}" exec -T kafka /kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --topic "$TOPIC" \
    --from-beginning \
    --max-messages 1 \
    --timeout-ms 10000 2>/dev/null)
  printf '%s\n' "$kafka_event"
  echo
fi

echo "==> Stopping Debezium Connect and testing a target-side default insert"
compose_step stop connect

source_state=$(sql_value source "SELECT max(id) || ':' || (SELECT last_value FROM lab.id_sequence) FROM lab.events;")
target_state=$(sql_value target "SELECT max(id) || ':' || (SELECT last_value FROM lab.id_sequence) FROM lab.events;")

set +e
cutover_output=$("${COMPOSE[@]}" exec -T target psql -XAtq -v ON_ERROR_STOP=1 -U postgres -d lab \
  -c "INSERT INTO lab.events (payload) VALUES ('target-cutover') RETURNING id;" 2>&1)
cutover_status=$?
set -e

echo
echo "Debezium:                3.6.1.Final"
echo "After snapshot (max:seq): source=$source_snapshot target=$target_snapshot"
echo "After CDC (max:seq):      source=$source_state target=$target_state"

if [[ "$source_state" != "1500:1500" || "$target_state" != "1500:1000" ]]; then
  echo "FAIL: the observed sequence state did not match the expected Debezium behavior" >&2
  exit 1
fi
if [[ $cutover_status -eq 0 || "$cutover_output" != *"duplicate key value violates unique constraint"* ]]; then
  echo "FAIL: expected the target default insert to collide on id 1001" >&2
  echo "$cutover_output" >&2
  exit 1
fi

echo "Target default insert output:"
while IFS= read -r line; do
  printf '  %s\n' "$line"
done <<< "$cutover_output"
echo "Result:                   expected duplicate-key failure on id 1001"
echo "PASS: Debezium reproduces the explicit-sequence cutover problem"
echo
echo "The containers are still available for inspection. Run: make clean"
