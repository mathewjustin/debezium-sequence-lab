# Debezium sequence lab

A small Docker Compose lab that tests whether Debezium keeps an explicitly
created PostgreSQL sequence synchronized while replicating row changes to
another PostgreSQL database.

**Start reading:** [Debezium architecture: from reader to writer](ARCHITECTURE.md)
explains the components, deployment, event flow, and where the proposed
sequence-advancement feature belongs. It uses a vertical diagram and short
sections for reading alongside the code.

**Verified result:** Debezium 3.6.1.Final replicates the 500 new rows and their
explicit IDs, but it does not advance the target sequence. The first
target-side default insert collides on `id=1001`.

The schema matches the edge case from
[xataio/pgstream#1203](https://github.com/xataio/pgstream/issues/1203):

```sql
CREATE SEQUENCE lab.id_sequence;

CREATE TABLE lab.events (
    id bigint PRIMARY KEY DEFAULT nextval('lab.id_sequence'::regclass),
    payload text NOT NULL
);
```

The sequence is deliberately not `OWNED BY` the table column.

## Run it

Requirements: Docker with Compose and about 1.5 GB of free memory.

```bash
./demo.sh
```

The script:

1. Inserts 1,000 source rows.
2. Uses `pg_dump` to restore the initial schema, rows, and sequence state to the target.
3. Starts a Debezium PostgreSQL source connector with `snapshot.mode=no_data`.
4. Starts the Debezium JDBC sink against the restored target table.
5. Inserts another 500 rows at the source and waits for them at the target.
6. Stops CDC and tries a target insert that relies on the default sequence.

The source connector emits the row `id`, and the JDBC sink writes that explicit
value. The experiment checks whether anything also advances the target sequence.

## Expected output

```text
Debezium:                 3.6.1.Final
After snapshot (max:seq): source=1000:1000 target=1000:1000
After CDC (max:seq):      source=1500:1500 target=1500:1000
Result:                   expected duplicate-key failure on id 1001
PASS: Debezium reproduces the explicit-sequence cutover problem
```

See [`RESULTS.md`](RESULTS.md) for the validated run and
[`HOW_IT_WORKS.md`](HOW_IT_WORKS.md) for the event-by-event explanation.

## Inspect it

The containers remain running after the test:

```bash
docker compose exec target psql -U postgres -d lab
docker compose ps
make logs
make clean
```

The source, target, and Kafka Connect REST API are exposed at `localhost:56432`,
`localhost:56433`, and `localhost:58083` respectively.

## Versions

- Debezium Connect and Kafka: `3.6.1.Final`
- PostgreSQL source and target: `16.11`
- Debezium source connector: `io.debezium.connector.postgresql.PostgresConnector`
- Debezium sink connector: `io.debezium.connector.jdbc.JdbcSinkConnector`

## References

- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- [Debezium JDBC sink connector](https://debezium.io/documentation/reference/stable/connectors/jdbc.html)
