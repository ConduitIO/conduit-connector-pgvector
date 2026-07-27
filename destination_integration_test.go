// Copyright © 2026 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgvector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-pgvector/destination"
	"github.com/conduitio/conduit-connector-pgvector/internal"
	"github.com/conduitio/conduit-connector-pgvector/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

// These tests require the pgvector Postgres from test/docker-compose.yml. They
// skip (via test.Connect) when the database is unreachable, so `go test -short`
// and CI without docker do not fail on them.

func openDestination(ctx context.Context, t *testing.T, settings map[string]string) sdk.Destination {
	t.Helper()
	is := is.New(t)
	d := NewDestination()
	err := sdk.Util.ParseConfig(ctx, settings, d.Config(), Connector.NewSpecification().DestinationParams)
	is.NoErr(err)
	return d
}

func embeddingRecord(op opencdc.Operation, key string, vec []float32, meta map[string]string) opencdc.Record {
	return opencdc.Record{
		Operation: op,
		Metadata:  meta,
		Key:       opencdc.RawData(key),
		Payload: opencdc.Change{
			After: opencdc.StructuredData{"vector": vec},
		},
	}
}

// sourceKeyMeta builds the metadata map carrying the ai.chunk.source_key
// contract (the default sourceKeyMetadataKey) that the chunking/embedding
// processors set on every record and that this connector's default config
// (sourceKeyColumn enabled) now requires on every create/update/snapshot and
// every delete.
func sourceKeyMeta(sourceKey string) map[string]string {
	return map[string]string{"ai.chunk.source_key": sourceKey}
}

func assertCode(t *testing.T, err error, code string) {
	t.Helper()
	is := is.New(t)
	is.True(err != nil)
	var ce *internal.CodedError
	is.True(errors.As(err, &ce))
	is.Equal(ce.Code, code)
}

// TestDestination_DimensionMismatch_FailFast is the canonical fail-fast test:
// a table with vector(768) and a configured dimension of 1536 must fail at
// Open with the stable ai.vector_dimension_mismatch code — never on first write.
func TestDestination_DimensionMismatch_FailFast(t *testing.T) {
	ctx := test.Context(t)
	conn := test.Connect(ctx, t)
	test.EnsureExtension(ctx, t, conn)
	table := test.RandomIdentifier(t)
	test.SetupVectorTable(ctx, t, conn, table, 768)

	d := openDestination(ctx, t, map[string]string{
		"url":       test.ConnString,
		"table":     table,
		"dimension": "1536",
	})
	err := d.Open(ctx)
	assertCode(t, err, internal.CodeVectorDimensionMismatch)
}

func TestDestination_Open_DimensionMatch(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.Connect(ctx, t)
	test.EnsureExtension(ctx, t, conn)
	table := test.RandomIdentifier(t)
	test.SetupVectorTable(ctx, t, conn, table, 768)

	d := openDestination(ctx, t, map[string]string{
		"url":       test.ConnString,
		"table":     table,
		"dimension": "768",
	})
	is.NoErr(d.Open(ctx))
	is.NoErr(d.Teardown(ctx))
}

func TestDestination_TargetTableMissing(t *testing.T) {
	ctx := test.Context(t)
	conn := test.Connect(ctx, t)
	test.EnsureExtension(ctx, t, conn)

	d := openDestination(ctx, t, map[string]string{
		"url":       test.ConnString,
		"table":     "does_not_exist_" + test.RandomIdentifier(t),
		"dimension": "768",
	})
	assertCode(t, d.Open(ctx), internal.CodeTargetTableMissing)
}

// TestDestination_SourceKeyColumnMissing_FailFast is the source_key analogue
// of TestDestination_DimensionMismatch_FailFast: source_key population is
// enabled by default, so a static table missing the configured
// sourceKeyColumn must fail fast at Open with a coded, actionable error —
// never a surprise on the first upsert.
func TestDestination_SourceKeyColumnMissing_FailFast(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.Connect(ctx, t)
	test.EnsureExtension(ctx, t, conn)
	table := test.RandomIdentifier(t)

	// A table with no source_key column at all (unlike test.SetupVectorTable).
	_, err := conn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id text PRIMARY KEY, embedding vector(4), metadata jsonb)`, quoteForTest(table)))
	is.NoErr(err)
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+quoteForTest(table))
	})

	d := openDestination(ctx, t, map[string]string{
		"url":       test.ConnString,
		"table":     table,
		"dimension": "4",
	})
	assertCode(t, d.Open(ctx), internal.CodeSourceKeyColumnMissing)
}

// TestDestination_Upsert_Idempotent proves the invariant-3/§6 property:
// writing the same keyed record twice converges to exactly one row (no
// duplicate), and a re-embed replaces the whole row — vector, metadata, and
// source_key together — with no half-written column set.
func TestDestination_Upsert_Idempotent(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.Connect(ctx, t)
	test.EnsureExtension(ctx, t, conn)
	table := test.RandomIdentifier(t)
	test.SetupVectorTable(ctx, t, conn, table, 4)

	d := openDestination(ctx, t, map[string]string{
		"url":       test.ConnString,
		"table":     table,
		"dimension": "4",
	})
	is.NoErr(d.Open(ctx))
	defer func() { is.NoErr(d.Teardown(ctx)) }()

	rec := embeddingRecord(opencdc.OperationCreate, "doc1:0",
		[]float32{0.1, 0.2, 0.3, 0.4}, sourceKeyMeta("doc1"))

	// First write.
	n, err := d.Write(ctx, []opencdc.Record{rec})
	is.NoErr(err)
	is.Equal(n, 1)

	// Redelivery of the same record (a retry after a crash, or a duplicate
	// from an at-least-once redelivery) with a re-embedded vector. Per design
	// doc §6 this must converge to the same row, not a second one.
	rec2 := embeddingRecord(opencdc.OperationUpdate, "doc1:0",
		[]float32{0.5, 0.6, 0.7, 0.8}, sourceKeyMeta("doc1"))
	n, err = d.Write(ctx, []opencdc.Record{rec2})
	is.NoErr(err)
	is.Equal(n, 1)

	// Exactly one row, carrying the latest vector.
	var count int
	is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+quoteForTest(table)+" WHERE id = $1", "doc1:0").Scan(&count))
	is.Equal(count, 1)

	// The whole row converged together: vector, metadata, AND source_key all
	// reflect the second write — no column lags behind from the first
	// (design doc §6 point 3: a retry replaces the whole row, never a partial
	// column update).
	var (
		vecText, sourceKey string
		metaJSON           []byte
	)
	is.NoErr(conn.QueryRow(ctx,
		"SELECT embedding::text, source_key, metadata FROM "+quoteForTest(table)+" WHERE id = $1", "doc1:0",
	).Scan(&vecText, &sourceKey, &metaJSON))
	is.Equal(vecText, "[0.5,0.6,0.7,0.8]")
	is.Equal(sourceKey, "doc1")

	gotVec, err := internal.ParseVectorText(vecText)
	is.NoErr(err)
	is.Equal(gotVec, []float32{0.5, 0.6, 0.7, 0.8})

	var gotMeta map[string]string
	is.NoErr(json.Unmarshal(metaJSON, &gotMeta))
	is.Equal(gotMeta, map[string]string{"ai.chunk.source_key": "doc1"})
}

// TestDestination_Delete_LegacyByID_WhenSourceKeyColumnDisabled covers the
// opt-out path: with sourceKeyColumn disabled (empty), deletes fall back to
// the slice-1 by-id match, and no source_key metadata is required.
//
// This constructs destination.Config directly instead of going through
// openDestination (sdk.Util.ParseConfig): conduit-commons's
// Config.ApplyDefaults reapplies a parameter's default whenever the
// configured value trims to empty ("if strings.TrimSpace(c[key]) == "" &&
// param.Default != "" { c[key] = param.Default }"), so a real pipeline
// setting "sourceKeyColumn": "" is indistinguishable from an absent key and
// silently resolves back to "source_key" — confirmed by actually running
// this test through openDestination first and watching it fail with
// pgvector.missing_source_key instead of exercising the opt-out. The exact
// same pre-existing limitation already silently affects "metadataColumn": ""
// from slice 1; see the README's "Opting out" note. This test proves the
// connector's own handling of a genuinely empty SourceKeyColumn is correct;
// reaching that state from real pipeline YAML is a tracked follow-up.
func TestDestination_Delete_LegacyByID_WhenSourceKeyColumnDisabled(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.Connect(ctx, t)
	test.EnsureExtension(ctx, t, conn)
	table := test.RandomIdentifier(t)
	test.SetupVectorTable(ctx, t, conn, table, 4)

	d := &Destination{config: destination.Config{
		URL:            test.ConnString,
		Table:          table,
		Dimension:      4,
		VectorColumn:   "embedding",
		KeyColumn:      "id",
		VectorField:    "vector",
		MetadataColumn: "metadata",
		// SourceKeyColumn intentionally left "" (disabled).
	}}
	is.NoErr(d.Open(ctx))
	defer func() { is.NoErr(d.Teardown(ctx)) }()

	rec := embeddingRecord(opencdc.OperationCreate, "doc1:0",
		[]float32{0.1, 0.2, 0.3, 0.4}, nil) // no source_key metadata needed
	_, err := d.Write(ctx, []opencdc.Record{rec})
	is.NoErr(err)

	del := opencdc.Record{Operation: opencdc.OperationDelete, Key: opencdc.RawData("doc1:0")}
	_, err = d.Write(ctx, []opencdc.Record{del})
	is.NoErr(err)

	var count int
	is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+quoteForTest(table)+" WHERE id = $1", "doc1:0").Scan(&count))
	is.Equal(count, 0)
}

// TestDestination_DeleteBySourceKey_RemovesAllChunksAcrossChunkCountChange is
// the load-bearing correctness test for design doc §5: a source record that
// has accumulated chunk rows under a changing chunk count (simulating
// re-chunking after edits — 5 chunks, then re-embedded down to 2) must have
// EVERY row derived from it removed by a single delete tombstone carrying
// only the source_key, not the ids of whichever chunk count happened to be
// current. A "delete today's expected chunk_id" implementation would leave
// the higher-numbered, now-stale chunk ids as orphaned rows; this test proves
// they are gone too.
func TestDestination_DeleteBySourceKey_RemovesAllChunksAcrossChunkCountChange(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.Connect(ctx, t)
	test.EnsureExtension(ctx, t, conn)
	table := test.RandomIdentifier(t)
	test.SetupVectorTable(ctx, t, conn, table, 4)

	d := openDestination(ctx, t, map[string]string{
		"url":       test.ConnString,
		"table":     table,
		"dimension": "4",
	})
	is.NoErr(d.Open(ctx))
	defer func() { is.NoErr(d.Teardown(ctx)) }()

	vec := []float32{0.1, 0.2, 0.3, 0.4}

	// First embed: the source record chunks into 5 pieces (doc1:0..doc1:4).
	var first []opencdc.Record
	for i := range 5 {
		first = append(first, embeddingRecord(opencdc.OperationCreate,
			fmt.Sprintf("doc1:%d", i), vec, sourceKeyMeta("doc1")))
	}
	n, err := d.Write(ctx, first)
	is.NoErr(err)
	is.Equal(n, 5)

	// The source record is edited and shrinks: re-chunking now produces only
	// 2 pieces. The chunking processor re-upserts doc1:0 and doc1:1 (updating
	// them in place) but never touches doc1:2..doc1:4 — those ids simply stop
	// being produced. Without source_key-based delete fan-out those 3 rows
	// would remain in the table forever (the orphan gap design doc §5
	// closes).
	var second []opencdc.Record
	for i := range 2 {
		second = append(second, embeddingRecord(opencdc.OperationUpdate,
			fmt.Sprintf("doc1:%d", i), vec, sourceKeyMeta("doc1")))
	}
	n, err = d.Write(ctx, second)
	is.NoErr(err)
	is.Equal(n, 2)

	var preDeleteCount int
	is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+quoteForTest(table)+" WHERE source_key = $1", "doc1").
		Scan(&preDeleteCount))
	is.Equal(preDeleteCount, 5) // doc1:0, doc1:1 (updated) + doc1:2, doc1:3, doc1:4 (stale, still present)

	// Now the source record itself is deleted upstream. The chunking
	// processor re-emits a single delete tombstone carrying only the
	// source_key — it does not (and structurally cannot, in general) know
	// every chunk_id ever derived from this source record.
	del := opencdc.Record{
		Operation: opencdc.OperationDelete,
		Metadata:  sourceKeyMeta("doc1"),
	}
	n, err = d.Write(ctx, []opencdc.Record{del})
	is.NoErr(err)
	is.Equal(n, 1)

	// Every row for source_key "doc1" is gone — including the stale doc1:2,
	// doc1:3, doc1:4 rows from the chunk count that has since changed. This
	// is the property a chunk_id-guessing delete cannot provide.
	var postDeleteCount int
	is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+quoteForTest(table)+" WHERE source_key = $1", "doc1").
		Scan(&postDeleteCount))
	is.Equal(postDeleteCount, 0)
}

// TestDestination_Delete_UnrelatedSourceKeyUnaffected proves the fan-out
// delete is scoped to its own source_key: deleting doc1's chunks must not
// touch doc2's rows.
func TestDestination_Delete_UnrelatedSourceKeyUnaffected(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.Connect(ctx, t)
	test.EnsureExtension(ctx, t, conn)
	table := test.RandomIdentifier(t)
	test.SetupVectorTable(ctx, t, conn, table, 4)

	d := openDestination(ctx, t, map[string]string{
		"url":       test.ConnString,
		"table":     table,
		"dimension": "4",
	})
	is.NoErr(d.Open(ctx))
	defer func() { is.NoErr(d.Teardown(ctx)) }()

	vec := []float32{0.1, 0.2, 0.3, 0.4}
	_, err := d.Write(ctx, []opencdc.Record{
		embeddingRecord(opencdc.OperationCreate, "doc1:0", vec, sourceKeyMeta("doc1")),
		embeddingRecord(opencdc.OperationCreate, "doc2:0", vec, sourceKeyMeta("doc2")),
	})
	is.NoErr(err)

	del := opencdc.Record{Operation: opencdc.OperationDelete, Metadata: sourceKeyMeta("doc1")}
	_, err = d.Write(ctx, []opencdc.Record{del})
	is.NoErr(err)

	var doc1Count, doc2Count int
	is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+quoteForTest(table)+" WHERE source_key = $1", "doc1").Scan(&doc1Count))
	is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+quoteForTest(table)+" WHERE source_key = $1", "doc2").Scan(&doc2Count))
	is.Equal(doc1Count, 0)
	is.Equal(doc2Count, 1)
}

// TestDestination_Upsert_MissingSourceKey_CodedError proves invariant 3 (no
// early ack) for the upsert side of source_key population: a
// create/update/snapshot record with no source_key metadata must fail with a
// coded error — never a silent write that skips the column (which would make
// that row unreachable by a future delete's fan-out).
func TestDestination_Upsert_MissingSourceKey_CodedError(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.Connect(ctx, t)
	test.EnsureExtension(ctx, t, conn)
	table := test.RandomIdentifier(t)
	test.SetupVectorTable(ctx, t, conn, table, 4)

	d := openDestination(ctx, t, map[string]string{
		"url":       test.ConnString,
		"table":     table,
		"dimension": "4",
	})
	is.NoErr(d.Open(ctx))
	defer func() { is.NoErr(d.Teardown(ctx)) }()

	rec := embeddingRecord(opencdc.OperationCreate, "doc1:0", []float32{0.1, 0.2, 0.3, 0.4}, nil)
	n, err := d.Write(ctx, []opencdc.Record{rec})
	is.Equal(n, 0) // nothing acked
	assertCode(t, err, internal.CodeMissingSourceKey)

	var count int
	is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+quoteForTest(table)).Scan(&count))
	is.Equal(count, 0)
}

// TestDestination_Delete_MissingSourceKey_CodedError is the delete-side
// counterpart: a delete tombstone with no source_key metadata cannot resolve
// its fan-out, so it must fail with a coded error rather than silently
// no-op'ing (which would look like a successful delete while leaving every
// row for that source record in place) or falling back to some other guess.
func TestDestination_Delete_MissingSourceKey_CodedError(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.Connect(ctx, t)
	test.EnsureExtension(ctx, t, conn)
	table := test.RandomIdentifier(t)
	test.SetupVectorTable(ctx, t, conn, table, 4)

	d := openDestination(ctx, t, map[string]string{
		"url":       test.ConnString,
		"table":     table,
		"dimension": "4",
	})
	is.NoErr(d.Open(ctx))
	defer func() { is.NoErr(d.Teardown(ctx)) }()

	rec := embeddingRecord(opencdc.OperationCreate, "doc1:0", []float32{0.1, 0.2, 0.3, 0.4}, sourceKeyMeta("doc1"))
	_, err := d.Write(ctx, []opencdc.Record{rec})
	is.NoErr(err)

	del := opencdc.Record{Operation: opencdc.OperationDelete, Key: opencdc.RawData("doc1:0")} // no metadata
	n, err := d.Write(ctx, []opencdc.Record{del})
	is.Equal(n, 0)
	assertCode(t, err, internal.CodeMissingSourceKey)

	// The row must still be there: the failed delete must not have partially
	// applied.
	var count int
	is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+quoteForTest(table)+" WHERE id = $1", "doc1:0").Scan(&count))
	is.Equal(count, 1)
}

// TestDestination_IDMetadataKey_TakesPrecedenceOverRecordKey proves the
// id-resolution contract: when a record carries ai.chunk.id metadata, it is
// used as the upsert conflict key instead of Record.Key.
func TestDestination_IDMetadataKey_TakesPrecedenceOverRecordKey(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.Connect(ctx, t)
	test.EnsureExtension(ctx, t, conn)
	table := test.RandomIdentifier(t)
	test.SetupVectorTable(ctx, t, conn, table, 4)

	d := openDestination(ctx, t, map[string]string{
		"url":       test.ConnString,
		"table":     table,
		"dimension": "4",
	})
	is.NoErr(d.Open(ctx))
	defer func() { is.NoErr(d.Teardown(ctx)) }()

	meta := sourceKeyMeta("doc1")
	meta["ai.chunk.id"] = "doc1:0"
	// Record.Key deliberately disagrees with ai.chunk.id metadata; the
	// metadata contract must win.
	rec := opencdc.Record{
		Operation: opencdc.OperationCreate,
		Key:       opencdc.RawData("some-other-key"),
		Metadata:  meta,
		Payload: opencdc.Change{
			After: opencdc.StructuredData{"vector": []float32{0.1, 0.2, 0.3, 0.4}},
		},
	}
	_, err := d.Write(ctx, []opencdc.Record{rec})
	is.NoErr(err)

	var count int
	is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+quoteForTest(table)+" WHERE id = $1", "doc1:0").Scan(&count))
	is.Equal(count, 1)
	is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+quoteForTest(table)+" WHERE id = $1", "some-other-key").Scan(&count))
	is.Equal(count, 0)
}

// TestDestination_PerRecordDimensionGuard verifies the per-record backstop: a
// vector whose length disagrees with the configured dimension is a coded
// error, never a silent write. This must fire before source_key resolution
// (checked at record 0, which has no source_key metadata either) — dimension
// is the more specific, more actionable diagnosis.
func TestDestination_PerRecordDimensionGuard(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.Connect(ctx, t)
	test.EnsureExtension(ctx, t, conn)
	table := test.RandomIdentifier(t)
	test.SetupVectorTable(ctx, t, conn, table, 4)

	d := openDestination(ctx, t, map[string]string{
		"url":       test.ConnString,
		"table":     table,
		"dimension": "4",
	})
	is.NoErr(d.Open(ctx))
	defer func() { is.NoErr(d.Teardown(ctx)) }()

	rec := embeddingRecord(opencdc.OperationCreate, "doc1:0", []float32{0.1, 0.2}, nil) // wrong length
	n, err := d.Write(ctx, []opencdc.Record{rec})
	is.Equal(n, 0)
	assertCode(t, err, internal.CodeVectorDimensionMismatch)
}

// TestDestination_BatchAtomicRollback is the load-bearing invariant-1/3 test:
// a batch of N valid records plus one record that fails AT POSTGRES (a CHECK
// violation that passes the connector's local validation) must leave the table
// empty — full-success-or-nothing, no partial write, and Write must report
// (0, err) so nothing is acked. This exercises pgx's implicit-transaction
// rollback across the batch, which the local-validation guard test cannot.
func TestDestination_BatchAtomicRollback(t *testing.T) {
	is := is.New(t)
	ctx := test.Context(t)
	conn := test.Connect(ctx, t)
	test.EnsureExtension(ctx, t, conn)
	table := test.RandomIdentifier(t)

	// A CHECK on id rejects the literal 'BAD' at execute time. The connector's
	// local validation (key present, vector length correct, source_key
	// present) passes, so the failure originates at Postgres — exactly the
	// case the invariant covers.
	_, err := conn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id text PRIMARY KEY CHECK (id <> 'BAD'), embedding vector(4), metadata jsonb, source_key text)`,
		quoteForTest(table)))
	is.NoErr(err)
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+quoteForTest(table))
	})

	d := openDestination(ctx, t, map[string]string{
		"url":       test.ConnString,
		"table":     table,
		"dimension": "4",
	})
	is.NoErr(d.Open(ctx))
	defer func() { is.NoErr(d.Teardown(ctx)) }()

	vec := []float32{0.1, 0.2, 0.3, 0.4}
	recs := []opencdc.Record{
		embeddingRecord(opencdc.OperationCreate, "good:0", vec, sourceKeyMeta("doc1")),
		embeddingRecord(opencdc.OperationCreate, "good:1", vec, sourceKeyMeta("doc1")),
		embeddingRecord(opencdc.OperationCreate, "BAD", vec, sourceKeyMeta("doc1")), // violates CHECK at Postgres
	}

	n, err := d.Write(ctx, recs)
	is.Equal(n, 0) // nothing acked
	assertCode(t, err, internal.CodeWriteFailed)

	// Invariant 1/3: the two valid records must NOT have landed — the whole
	// batch rolled back atomically.
	var count int
	is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+quoteForTest(table)).Scan(&count))
	is.Equal(count, 0)
}

// quoteForTest quotes an identifier for use in raw test SQL.
func quoteForTest(ident string) string {
	return pgx.Identifier{ident}.Sanitize()
}
