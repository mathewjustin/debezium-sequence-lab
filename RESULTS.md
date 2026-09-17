# Validated results

Validated locally on 2026-09-17 with Docker Compose v5.3.1.

Command:

```bash
./demo.sh
```

Result:

```text
Debezium:                 3.6.1.Final
After snapshot (max:seq): source=1000:1000 target=1000:1000
After CDC (max:seq):      source=1500:1500 target=1500:1000
Target default insert output:
  ERROR:  duplicate key value violates unique constraint "events_pkey"
  DETAIL:  Key (id)=(1001) already exists.
Result:                   expected duplicate-key failure on id 1001
PASS: Debezium reproduces the explicit-sequence cutover problem
```

The first Kafka change event contained the row value explicitly:

```json
{
  "after": {
    "id": 1001,
    "payload": "cdc-1"
  },
  "source": {
    "version": "3.6.1.Final",
    "connector": "postgresql",
    "schema": "lab",
    "table": "events"
  },
  "op": "c"
}
```

The JDBC sink consumed all 500 events and inserted IDs 1001 through 1500. No
event carried `lab.id_sequence.last_value`, and no sink operation invoked
`nextval` or `setval` on the target sequence.
