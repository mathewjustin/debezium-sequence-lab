# How the Debezium sequence experiment works

## The boundary under test

The table uses an explicit, unowned sequence:

```sql
CREATE SEQUENCE lab.id_sequence;

CREATE TABLE lab.events (
    id bigint PRIMARY KEY DEFAULT nextval('lab.id_sequence'::regclass),
    payload text NOT NULL
);
```

On the source, omitting `id` makes PostgreSQL call `nextval`:

```sql
INSERT INTO lab.events (payload) VALUES ('source-write') RETURNING id;
```

Debezium observes the resulting row, not the act of evaluating the default. Its
change event therefore contains the generated value:

```json
{
  "after": { "id": 1001, "payload": "cdc-1" },
  "op": "c"
}
```

The event's `source.sequence` metadata, when present, describes the source log
position. It is unrelated to the PostgreSQL object named
`lab.id_sequence` and does not contain that object's `last_value`.

## What the JDBC sink does

The Debezium JDBC sink reads fields from the event's `after` structure and
writes them to the target with upsert semantics. Conceptually, the replicated
operation is equivalent to:

```sql
INSERT INTO lab.events (id, payload)
VALUES (1001, 'cdc-1')
ON CONFLICT (id) DO UPDATE SET payload = EXCLUDED.payload;
```

Because `id` is supplied explicitly, PostgreSQL does not evaluate the column's
`DEFAULT nextval(...)`. The row advances to 1001 while the target sequence stays
at 1000. The same thing happens for every event through ID 1500.

## Why cutover fails

After CDC drains, the target has two different states:

```sql
SELECT max(id) FROM lab.events;
-- 1500

SELECT last_value FROM lab.id_sequence;
-- 1000
```

The first application write that omits `id` asks the stale sequence for its next
value:

```sql
INSERT INTO lab.events (payload) VALUES ('target-cutover');
-- ERROR: duplicate key value violates unique constraint "events_pkey"
-- DETAIL: Key (id)=(1001) already exists.
```

This is not specific to whether the sequence is owned by the column. Ownership
would make the relationship easier to discover, but an explicit-ID insert still
does not call `nextval`.

## Operational repair

After source writes stop and CDC is fully drained, a cutover process can reseed
the target explicitly:

```sql
SELECT setval(
    'lab.id_sequence'::regclass,
    (SELECT max(id) FROM lab.events),
    true
);
```

The next default insert then receives 1501. In a general migration tool, the
hard part is safely discovering every column-to-sequence relationship and doing
the reseed at a point where no source event can arrive afterward.
