# Conduit Connector for pgvector

The pgvector connector is a [Conduit](https://github.com/ConduitIO/conduit)
plugin. It provides a **destination** connector for
[pgvector](https://github.com/pgvector/pgvector), the open-source vector-store
extension for PostgreSQL.

It is the flagship vector sink for Conduit's AI data pipeline
(**CDC → chunk → embed → pgvector**), designed to keep a RAG index fresh from
Postgres. See the design document
[`20260724-ai-pipeline-components.md`](https://github.com/ConduitIO/conduit/blob/main/docs/design-documents/20260724-ai-pipeline-components.md)
in the core repository.

<!-- readmegen:description -->
The pgvector destination connector is the flagship vector sink for Conduit's
AI data pipeline (CDC -> chunk -> embed -> pgvector), keeping a RAG index
fresh from Postgres.

It is a write-only connector. Each incoming embedding record is upserted into
a pgvector table keyed by the record's stable key (the chunk_id), using
`INSERT ... ON CONFLICT (<key>) DO UPDATE`, so at-least-once redelivery
converges to a single row rather than duplicating it.

At pipeline start the connector validates the configured embedding
`dimension` against the target table's `vector` column and fails fast with a
stable, actionable error (`ai.vector_dimension_mismatch`) if they disagree,
instead of failing silently on the first write.
<!-- /readmegen:description -->

## Destination

The destination connector writes embedding records into a pgvector table.

### How records map to rows

| Record part | Target |
| --- | --- |
| `Record.Key` (the stable `chunk_id`) | the `keyColumn` (upsert conflict target) |
| `Record.Payload.After[vectorField]` (a numeric array) | the `vectorColumn` (a `vector(N)` column) |
| `Record.Metadata` | the `metadataColumn` (`jsonb`), if configured |

### Upsert semantics (idempotent under retry)

Every create/update/snapshot record is written with:

```sql
INSERT INTO <table> (<keyColumn>, <vectorColumn>, <metadataColumn>)
VALUES ($1, $2, $3)
ON CONFLICT (<keyColumn>) DO UPDATE
  SET <vectorColumn> = EXCLUDED.<vectorColumn>,
      <metadataColumn> = EXCLUDED.<metadataColumn>
```

The connector registers pgvector's `vector` type on the connection at startup,
so the embedding binds by the column's real type (reported by the prepared
statement) rather than a SQL cast.

Because the conflict key is the record's stable `chunk_id` and the whole
vector+metadata pair is replaced in a single statement, redelivering the same
record (at-least-once) converges to exactly one row — no duplicates, no
half-written rows. A batch of records is executed as a single transaction: if
any statement fails, none are committed and the connector reports `0` records
written so the engine can retry or route to the DLQ. **No record is acked
before it is durably written** (data-integrity invariants 1 and 3).

Delete records remove the row whose `keyColumn` equals the record key.

### Dimension validation at pipeline start (fail fast)

When the target `table` is a static name, the connector reads the target
`vector` column's declared dimension from the Postgres catalog on `Open` and
compares it against the configured `dimension`. On mismatch it refuses to start
with a stable, actionable error rather than failing silently on the first
write, and never truncates or pads a vector to force-fit a mismatched column
(invariant 6):

```text
ai.vector_dimension_mismatch: configured embedding dimension 1536 does not match
target column "docs"."embedding" dimension 768 (config path: "dimension"); fix:
set "dimension" to 768 to match the table, or point "table" at a vector(1536)
column, or migrate with ALTER TABLE "docs" ALTER COLUMN "embedding" TYPE vector(1536)
```

When `table` is a Go template (resolved per record), the target cannot be
introspected at startup; validation is deferred to a per-record dimension
guard, which is logged at startup so the deferral is never hidden.

### Error codes

Every user-facing error carries a stable, machine-matchable code, the failing
config path where known, and a suggested fix.

| Code | Raised when |
| --- | --- |
| `ai.vector_dimension_mismatch` | Configured `dimension` does not match the target vector column (at startup or per record). |
| `pgvector.invalid_config` | Configuration failed validation (bad URL, non-positive dimension, empty required column names). |
| `pgvector.connection_failed` | The connector could not connect to or introspect the target database. |
| `pgvector.target_table_missing` | The configured `table` does not exist or is not visible to the connection role. |
| `pgvector.vector_column_missing` | The configured `vectorColumn` does not exist on the target table. |
| `pgvector.vector_column_type_mismatch` | The configured `vectorColumn` exists but is not a pgvector `vector` column. |
| `pgvector.missing_vector_field` | A record carries no valid embedding vector in `vectorField`. |
| `pgvector.missing_key` | A record has no usable key to derive the upsert conflict target. |
| `pgvector.write_failed` | The batch upsert/delete failed to execute. |

### Example pipeline

The target table must exist and the `pgvector` extension must be installed
(`CREATE EXTENSION vector;`):

```sql
CREATE TABLE docs (
  id        text PRIMARY KEY,
  embedding vector(768),
  metadata  jsonb
);
```

```yaml
version: 2.2
pipelines:
  - id: rag-sync
    status: running
    connectors:
      # ... a source + chunking + embedding processors upstream ...
      - id: pgvector
        type: destination
        plugin: standalone:pgvector
        settings:
          url: postgres://user:pass@localhost:5432/rag?sslmode=disable
          table: docs
          dimension: "768"
          vectorColumn: embedding
          keyColumn: id
          vectorField: vector
          metadataColumn: metadata
```

<!-- readmegen:destination.parameters.yaml -->
```yaml
version: 2.2
pipelines:
  - id: example
    status: running
    connectors:
      - id: example
        plugin: "pgvector"
        settings:
          # Dimension is the embedding dimension the upstream model produces. It
          # is validated against the target table's vector column at pipeline
          # start (fail fast). There is no safe default, so it is required.
          # Type: int
          # Required: yes
          dimension: "0"
          # URL is the connection string for the target Postgres/pgvector
          # database.
          # Type: string
          # Required: yes
          url: ""
          # KeyColumn is the primary-key column used as the ON CONFLICT target
          # for idempotent upserts. It holds the record's stable chunk_id.
          # Type: string
          # Required: no
          keyColumn: "id"
          # MetadataColumn is the JSONB column that receives the record's
          # metadata. Leave empty to disable metadata writes.
          # Type: string
          # Required: no
          metadataColumn: "metadata"
          # Table is the target table into which embedding rows are upserted.
          # Supports a Go template evaluated per record (default: the record's
          # OpenCDC collection), matching the Postgres connector's convention.
          # Type: string
          # Required: no
          table: "{{ index .Metadata "opencdc.collection" }}"
          # VectorColumn is the name of the pgvector `vector` column on the
          # target table.
          # Type: string
          # Required: no
          vectorColumn: "embedding"
          # VectorField is the record payload field that carries the embedding
          # vector (a numeric array).
          # Type: string
          # Required: no
          vectorField: "vector"
          # Maximum delay before an incomplete batch is written to the
          # destination.
          # Type: duration
          # Required: no
          sdk.batch.delay: "0"
          # Maximum size of batch before it gets written to the destination.
          # Type: int
          # Required: no
          sdk.batch.size: "0"
          # Allow bursts of at most X records (0 or less means that bursts are
          # not limited). Only takes effect if a rate limit per second is set.
          # Note that if `sdk.batch.size` is bigger than `sdk.rate.burst`, the
          # effective batch size will be equal to `sdk.rate.burst`.
          # Type: int
          # Required: no
          sdk.rate.burst: "0"
          # Maximum number of records written per second (0 means no rate
          # limit).
          # Type: float
          # Required: no
          sdk.rate.perSecond: "0"
          # The format of the output record. See the Conduit documentation for a
          # full list of supported formats
          # (https://conduit.io/docs/using/connectors/configuration-parameters/output-format).
          # Type: string
          # Required: no
          sdk.record.format: "opencdc/json"
          # Options to configure the chosen output record format. Options are
          # normally key=value pairs separated with comma (e.g.
          # opt1=val2,opt2=val2), except for the `template` record format, where
          # options are a Go template.
          # Type: string
          # Required: no
          sdk.record.format.options: ""
          # Whether to extract and decode the record key with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.key.enabled: "true"
          # Whether to extract and decode the record payload with a schema.
          # Type: bool
          # Required: no
          sdk.schema.extract.payload.enabled: "true"
```
<!-- /readmegen:destination.parameters.yaml -->

## Testing

Run `make test` to run all tests. This requires Docker to be installed and
running; it brings up a `pgvector/pgvector` Postgres via
`test/docker-compose.yml`. Run `make test-unit` for the unit-only suite (no
Docker required).

## Delivery semantics

At-least-once. Duplicate delivery is safe: upserts are idempotent by
`chunk_id`. Deletes for a non-existent key are a no-op.
