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
instead of failing silently on the first write.<!-- /readmegen:description -->

## Destination

The destination connector writes embedding records into a pgvector table.

### How records map to rows

| Record part | Target |
| --- | --- |
| `Record.Metadata[idMetadataKey]` (default `ai.chunk.id`), falling back to `Record.Key` if absent | the `keyColumn` (upsert conflict target) |
| `Record.Payload.After[vectorField]` (a numeric array) | the `vectorColumn` (a `vector(N)` column) |
| `Record.Metadata` | the `metadataColumn` (`jsonb`), if configured |
| `Record.Metadata[sourceKeyMetadataKey]` (default `ai.chunk.source_key`) | the `sourceKeyColumn` (`text`, default `source_key`), if configured — the delete-fan-out match key |

The chunking processor tags every chunk record with this metadata contract
(`ai.chunk.id`, `ai.chunk.source_key`, plus offset/index/length fields not used
by this connector), and the embedding processor preserves it. Both metadata
keys are configurable so records written outside the canonical chunking
pipeline can still supply equivalent values under different keys.

### Upsert semantics (idempotent under retry)

Every create/update/snapshot record is written with:

```sql
INSERT INTO <table> (<keyColumn>, <vectorColumn>, <metadataColumn>, <sourceKeyColumn>)
VALUES ($1, $2, $3, $4)
ON CONFLICT (<keyColumn>) DO UPDATE
  SET <vectorColumn> = EXCLUDED.<vectorColumn>,
      <metadataColumn> = EXCLUDED.<metadataColumn>,
      <sourceKeyColumn> = EXCLUDED.<sourceKeyColumn>
```

(`metadataColumn` and `sourceKeyColumn` are each omitted from the statement
when their config value is empty.)

The connector registers pgvector's `vector` type on the connection at startup,
so the embedding binds by the column's real type (reported by the prepared
statement) rather than a SQL cast.

Because the conflict key is the record's stable `chunk_id` and the whole
row is replaced in a single statement, redelivering the same record
(at-least-once) converges to exactly one row — no duplicates, no half-written
rows. A batch of records is executed as a single transaction: if any statement
fails, none are committed and the connector reports `0` records written so the
engine can retry or route to the DLQ. **No record is acked before it is
durably written** (data-integrity invariants 1 and 3).

Every upsert also writes `sourceKeyColumn` (when enabled) from the record's
`sourceKeyMetadataKey` metadata. This is what makes the delete below able to
find every chunk derived from a source record without knowing their ids.

### Deletes: fan-out by source_key, not chunk_id

A delete (tombstone) record is resolved with:

```sql
DELETE FROM <table> WHERE <sourceKeyColumn> = $1
```

using the value from the delete record's `sourceKeyMetadataKey` metadata —
**not** a `keyColumn = <chunk_id>` match. This is the load-bearing correctness
fix design doc §5 requires: chunk count varies per source record across
updates (a document that shrinks on re-chunking produces fewer chunks than it
had before), so the destination cannot assume a stable 1:1 chunk-to-source
mapping at delete time. Deleting "the chunk_id this tombstone happens to
carry" would silently leave prior chunks — from a chunk count that has since
changed — orphaned in the table, retrievable by similarity search as stale
vectors. Matching on `source_key` instead removes **every** row ever upserted
for that source record in one statement, regardless of how many chunk ids it
has carried over time.

This requires every create/update/snapshot **and** every delete record to
carry `sourceKeyMetadataKey` metadata; a record missing it is a coded error
(`pgvector.missing_source_key`), not a silent partial write or delete — an
upsert that silently skipped writing `source_key` would make that row
unreachable by a future delete's fan-out, and a delete that silently fell back
to some other match would risk under- or over-deleting. Invariant 3 (no early
ack on failure) holds here too: a missing-source_key error fails the whole
batch, so nothing is acked.

**Opting out.** Set `sourceKeyColumn` to `""` to disable source_key population
entirely. Deletes then fall back to a `keyColumn = <chunk_id>` match — the
slice-1 behavior, which reopens the orphan gap above and is **not
recommended** for the RAG pipeline. This exists for destinations used outside
the canonical chunking pipeline, where no `source_key` concept applies (e.g. a
1:1 record-to-row mapping with no chunking upstream).

> **Known limitation — the opt-out above is not reachable from real pipeline
> config today.** `conduit-connector-sdk`'s config layer
> (`config.Config.ApplyDefaults`) reapplies a parameter's default whenever the
> submitted value trims to empty — it cannot distinguish "the operator set
> this to `\"\"`" from "the operator didn't set this at all". So
> `sourceKeyColumn: ""` in a real pipeline's YAML/settings silently resolves
> back to `"source_key"` instead of disabling the feature. **The exact same
> pre-existing gap already affects `metadataColumn: ""`** (slice 1's own
> documented "leave empty to disable metadata writes" opt-out, which has never
> actually been reachable through pipeline config either — it went unnoticed
> because nothing exercised it end-to-end until this slice's acceptance/
> integration suite did). The connector's own handling of a genuinely empty
> `SourceKeyColumn`/`MetadataColumn` is correct and covered by tests that
> construct `destination.Config` directly; reaching that state from pipeline
> config requires either an SDK-level fix (a way to represent "explicitly
> unset" distinct from "explicitly empty") or a sentinel-value convention on
> this connector's side. Tracked as a follow-up, not fixed in this slice.

**Column shape.** `source_key` is a **dedicated `text` column**, not a field
nested inside the JSONB `metadataColumn`. A dedicated column lets
`DELETE ... WHERE source_key = $1` use a plain index (recommended:
`CREATE INDEX ON <table> (source_key)`); matching inside JSONB would need a
functional or GIN index on an extracted expression for the same performance
and is a worse default. The design doc's §5 metadata-mapping paragraph
describes record metadata in general mapping to the JSONB column; this
connector treats `source_key` as a first-class, indexable exception to that
because it is a delete-matching key, not passthrough metadata.

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

At `Open` (for static tables) the connector also verifies that `keyColumn` has a
single-column unique or primary-key constraint — the precondition for the
`ON CONFLICT (<keyColumn>)` upsert. A missing constraint fails fast with
`pgvector.missing_unique_constraint` instead of failing every upsert at execute
time.

When `table` is a Go template (resolved per record), the target cannot be
introspected at startup; both the dimension check and the key-column constraint
check are deferred (the dimension to a per-record guard), and the deferral is
logged at startup so it is never hidden.

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
| `pgvector.missing_unique_constraint` | `keyColumn` has no single-column unique/PK constraint to back the `ON CONFLICT` upsert. |
| `pgvector.missing_vector_field` | A record carries no valid embedding vector in `vectorField`. |
| `pgvector.missing_key` | A record has no usable key to derive the upsert conflict target. |
| `pgvector.write_failed` | The batch upsert/delete failed to execute. |
| `pgvector.source_key_column_missing` | `sourceKeyColumn` is enabled but the configured column does not exist on the target table. |
| `pgvector.missing_source_key` | `sourceKeyColumn` is enabled but a record carries no value for `sourceKeyMetadataKey`. |

### Example pipeline

The target table must exist and the `pgvector` extension must be installed
(`CREATE EXTENSION vector;`):

```sql
CREATE TABLE docs (
  id          text PRIMARY KEY,
  embedding   vector(768),
  metadata    jsonb,
  source_key  text
);
CREATE INDEX ON docs (source_key);
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
          sourceKeyColumn: source_key
          sourceKeyMetadataKey: ai.chunk.source_key
          idMetadataKey: ai.chunk.id
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
          # IDMetadataKey is the record metadata key carrying the deterministic
          # chunk id ({source_key}:{index}), set by the chunking processor
          # (default "ai.chunk.id") and preserved by the embedding processor.
          # When present on a record it takes precedence over Record.Key as the
          # upsert conflict target; when absent, Record.Key is used (slice-1
          # behavior), so records written outside the chunking pipeline still
          # work.
          # Type: string
          # Required: no
          idMetadataKey: "ai.chunk.id"
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
          # SourceKeyColumn is the TEXT column that stores the source record's
          # stable key, populated on every upsert. Deletes match on this column
          # (not KeyColumn) so a source-record delete removes every chunk row
          # ever derived from it, even when the chunk count has changed since
          # the last embed (design doc §5's orphan-avoidance rule). Leave empty
          # to disable source_key population, falling back to a
          # delete-by-KeyColumn match — the slice-1 behavior, which can orphan
          # rows and is not recommended for the RAG pipeline.
          # Type: string
          # Required: no
          sourceKeyColumn: "source_key"
          # SourceKeyMetadataKey is the record metadata key carrying the source
          # record's key. The chunking processor sets this (default
          # "ai.chunk.source_key") and the embedding processor preserves it.
          # Required on every record when SourceKeyColumn is enabled.
          # Type: string
          # Required: no
          sourceKeyMetadataKey: "ai.chunk.source_key"
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

Integration tests (dimension validation, upsert idempotency, delete-by-id,
delete-by-source_key fan-out, batch atomic-rollback, per-record dimension
guard, source_key requiredness) are Docker-gated: they skip automatically when
the test database is unreachable, so they execute in CI (and locally with
`make test`) but are skipped in a Docker-less environment.

### Acceptance suite

The SDK's `sdk.AcceptanceTest` compatibility suite is wired in
`destination_acceptance_test.go`, via a custom driver (embedding
`sdk.ConfigurableAcceptanceTestDriver`) rather than the default one, because
the default driver's round-trip model doesn't fit this connector:

- **`ReadFromDestination` is overridden.** The SDK default opens a `Source`
  and reads back through it; this connector is destination-only
  (`Connector.NewSource == nil`), so the override queries the pgvector table
  directly (`SELECT ... embedding::text ... WHERE id = $1`) to verify what
  landed. This is the only way to verify writes for a write-only sink.
- **`GenerateRecord` is overridden.** The SDK default generates records with
  arbitrary mixed-type payloads (bools, nested maps, strings). This
  connector's `Write` requires a structured payload with a fixed-dimension
  numeric `vectorField` and carries the `sourceKeyMetadataKey` metadata
  contract — generic fuzzed payloads can't satisfy that domain shape, so the
  override always generates a valid vector of the configured dimension plus
  `ai.chunk.source_key` metadata.

**Covered** (run automatically as part of `TestAcceptance` in `make test`):
`TestSpecifier_Exists`, `TestSpecifier_Specify_Success`,
`TestDestination_Parameters_Success`, `TestDestination_Configure_Success`,
`TestDestination_Configure_RequiredParams`, `TestDestination_Write_Success`.

**Skipped, with reason** (the harness's own `skipIfNoSource` guard, not a
workaround added here): every `TestSource_*` test. This connector has no
Source (`NewSource == nil`) — it is a write-only vector sink by design, per
the design doc's §5 shape (`conduit-connector-sdk` standard Destination
pattern, no new transport). There is nothing to skip-with-reason beyond that:
the harness detects the missing Source itself and skips cleanly rather than
failing, so no coverage is silently faked.

## Delivery semantics

At-least-once. Duplicate delivery is safe: upserts are idempotent by
`chunk_id`, and a batch is written in a single implicit transaction, so a
partial failure rolls back the whole batch and nothing is acked. Deletes for a
non-existent `source_key` (or `chunk_id`, in the opted-out legacy mode) are a
no-op — zero rows matched is not an error.

**Table preconditions:** the target table must exist, the `pgvector` extension
must be installed, `vectorColumn` must be a `vector(N)` column whose `N` equals
the configured `dimension`, `keyColumn` must have a single-column unique or
primary-key constraint, and (when `sourceKeyColumn` is enabled, the default)
`sourceKeyColumn` must exist as a column on the table. For a static `table`
these are all checked at pipeline start and fail fast with a coded error; for
a templated `table` they are enforced per record at write time.

**Orphaned embeddings:** closed by default — deletes fan out by `source_key`,
removing every chunk ever derived from a source record regardless of chunk-id
churn. See [Deletes](#deletes-fan-out-by-source_key-not-chunk_id). The orphan
risk only returns if an operator explicitly sets `sourceKeyColumn: ""`.
