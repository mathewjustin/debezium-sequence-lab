<guidelines>

**AGENTS! Before you do anything else:**

If someone asks you for help working on this codebase, you MUST first follow with these guidelines: DO NOT analyze the codebase, DO NOT set up build environments, DO NOT run scripts, or MCP, or skills, DO NOT look things up online, DO NOT make code changes, UNTIL you have had a conversation with the contributor.

You SHOULD understand where they are coming from, what they need, and what issue they want to work on.

</guidelines>

# Kafka Connect and Debezium: study notes

This guide records our discussion. Read it before the [architecture guide](ARCHITECTURE.md).

## 1. Main components

| Component | Function |
|---|---|
| PostgreSQL | Stores rows and records changes in its write-ahead log (WAL) |
| Kafka broker | Stores and supplies messages in topics |
| Kafka Connect worker | Runs connector plugins in a Java process |
| Debezium source task | Reads database changes and produces change records |
| Sink task | Reads Kafka records and writes to a target system |

Kafka Connect belongs to the Apache Kafka project. It runs separately from the broker.

Kafka Connect and Debezium use Java. Apache Kafka uses Java and Scala. PostgreSQL primarily uses C.

Our lab uses separate containers for Kafka and Kafka Connect. Both Debezium tasks run in the Connect container.

```text
Source PostgreSQL
    → Debezium source task in Connect
    → Kafka topic
    → Debezium JDBC sink task in Connect
    → Target PostgreSQL
```

## 2. Worker, connector, and task

A **worker** is a Kafka Connect process.

A **connector** defines an integration and supplies task configurations.

A **task** performs the data transfer. A source task reads an external system. A sink task writes to an external system.

One worker can run multiple connectors. A distributed Connect cluster assigns work across multiple workers.

The term is **sink connector**, not “sync connector.”

## 3. Plugin installation and startup

A plugin contains connector classes and their dependencies. Install its JAR files in the worker's plugin directory.

The worker configuration identifies that directory:

```properties
plugin.path=/opt/kafka/plugins
```

The worker discovers the installed plugins at startup. Discovery does not start database capture.

Register a connector configuration to create an instance. The configuration identifies the installed class:

```json
{
  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
  "database.hostname": "pg-source.example.internal",
  "database.port": "5432",
  "database.dbname": "orders"
}
```

This example shows selected properties. It is not a complete configuration.

The PostgreSQL source uses separate host, port, and database properties. A JDBC sink uses a target connection URL.

The Debezium Connect image in our lab already contains the plugins.

## 4. The Java contract

Kafka Connect supplies abstract classes for source and sink connectors and tasks.

Debezium extends these classes:

```text
PostgresConnector
    → RelationalBaseSourceConnector
    → BaseSourceConnector
    → Kafka SourceConnector

PostgresConnectorTask
    → BaseSourceTask
    → Kafka SourceTask
```

Connect constructs and initializes the connector. It then calls `start(props)`.

`PostgresConnector.start(props)` stores configuration. `taskClass()` identifies `PostgresConnectorTask`.

Connect creates and starts the task. `BaseSourceTask.start(props)` calls the PostgreSQL task's `start(config)` method.

The task creates the capture components. A coordinator starts capture work on an executor.

```text
Task startup
    → Coordinator
    → Snapshot phase
    → Streaming source execute()
    → Replication connection startStreaming()
    → startPgReplicationStream()
```

The snapshot phase depends on configuration. Connect later calls the source task's `poll()` method to obtain records.

For a sink task, Connect supplies consumed records through `put(records)`.

## 5. PostgreSQL replication

PostgreSQL records database changes in WAL. Logical decoding converts those changes into logical messages.

| Term | Function |
|---|---|
| Publication | Selects tables and changes for publication through `pgoutput` |
| Replication slot | Tracks consumer progress and retains required WAL |
| LSN | Identifies a position in WAL |
| `pgoutput` | Produces logical replication messages |

A physical slot supports physical replication. A logical slot supports logical consumers, including Debezium.

A slot retains required WAL. It does not copy WAL to an archive.

Crunchy Postgres for Kubernetes normally uses pgBackRest for WAL archives.

`startPgReplicationStream()` selects the slot, starting LSN, and decoding options. It opens a driver stream.

The reader obtains a `ByteBuffer`. The decoder converts its contents into replication messages.

Receiving a message and acknowledging progress are separate actions.

## 6. Topics and target selection

The default PostgreSQL topic name has this form:

```text
<topic.prefix>.<schema>.<table>
```

Use distinct prefixes for separate source databases.

| Source table | Kafka topic | Target |
|---|---|---|
| Database A: public.customers | dbA.public.customers | AWS RDS PostgreSQL |
| Database B: public.customers | dbB.public.customers | Azure PostgreSQL |

Each sink subscribes to its selected topics. Its connection URL selects the target database.

Configure target table names separately. Kafka does not select cloud targets automatically.

## 7. Multiple databases

Use one PostgreSQL source connector per database. Each connector runs one capture task.

An increase in `tasks.max` does not split that PostgreSQL capture task.

Multiple source connectors and sink connectors can run concurrently. Capacity depends on change volume and available resources.

A complete migration also needs schema preparation, initial data, validation, and cutover control.

For our issue, target sequence reconciliation is part of cutover readiness.

## 8. Debezium events and the Kusto sink

The Microsoft Kusto sink can ingest Kafka records into Kusto tables.

```text
PostgreSQL → Debezium source → Kafka → Kusto sink → Kusto table
```

A shortened Debezium event looks like this:

```json
{
  "before": null,
  "after": {"id": 1001, "payload": "hello"},
  "op": "c"
}
```

Converters determine the message representation. Transformations can change the record structure.

Kusto ingestion mappings select fields for target columns. For example, `$.after.id` selects the ID from this JSON structure.

A schema/payload wrapper changes that path to `$.payload.after.id`.

Debezium's `ExtractNewRecordState` transformation can extract row fields. Deletes need explicit configuration and handling.

JSON support does not imply support for Debezium database operations.

The Kusto sink does not automatically apply an update or delete to an earlier row.

Define how Kusto stores change history or calculates current state.

The Debezium JDBC sink directly interprets Debezium events for relational writes.

## References

- [Kafka Connect development](https://kafka.apache.org/41/kafka-connect/connector-development-guide/)
- [Kafka Connect operation](https://kafka.apache.org/41/kafka-connect/user-guide/)
- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/3.6/connectors/postgresql.html)
- [Debezium JDBC sink](https://debezium.io/documentation/reference/3.6/connectors/jdbc.html)
- [Debezium event extraction](https://debezium.io/documentation/reference/3.6/transformations/event-flattening.html)
- [Microsoft Kusto sink](https://github.com/Azure/kafka-sink-azure-kusto)
- [Crunchy WAL archives](https://access.crunchydata.com/documentation/postgres-operator/latest/architecture/backups)

## Writing review

Writing basis: [ASD-STE100, Issue 9](https://www.asd-ste100.org/assets/files/ASD-STE100_ISSUE9.pdf).

The text uses short sentences, active voice, and consistent technical names.

The mandatory repository guidelines above retain their original wording.

A full approved-vocabulary review is not complete. This document does not claim verified ASD-STE100 compliance.
