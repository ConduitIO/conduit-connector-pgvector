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
	"errors"
	"fmt"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
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

// TestDestination_Upsert_Idempotent proves the invariant-3 property: writing the
// same keyed record twice converges to exactly one row (no duplicate), and a
// re-embed replaces the whole vector.
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
		[]float32{0.1, 0.2, 0.3, 0.4}, map[string]string{"source_key": "doc1"})

	// First write.
	n, err := d.Write(ctx, []opencdc.Record{rec})
	is.NoErr(err)
	is.Equal(n, 1)

	// Redelivery of the same record with an updated vector.
	rec2 := embeddingRecord(opencdc.OperationUpdate, "doc1:0",
		[]float32{0.5, 0.6, 0.7, 0.8}, map[string]string{"source_key": "doc1"})
	n, err = d.Write(ctx, []opencdc.Record{rec2})
	is.NoErr(err)
	is.Equal(n, 1)

	// Exactly one row, carrying the latest vector.
	var count int
	is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+quoteForTest(table)+" WHERE id = $1", "doc1:0").Scan(&count))
	is.Equal(count, 1)

	var got string
	is.NoErr(conn.QueryRow(ctx, "SELECT embedding::text FROM "+quoteForTest(table)+" WHERE id = $1", "doc1:0").Scan(&got))
	is.Equal(got, "[0.5,0.6,0.7,0.8]")
}

func TestDestination_Delete(t *testing.T) {
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
		[]float32{0.1, 0.2, 0.3, 0.4}, nil)
	_, err := d.Write(ctx, []opencdc.Record{rec})
	is.NoErr(err)

	del := opencdc.Record{Operation: opencdc.OperationDelete, Key: opencdc.RawData("doc1:0")}
	_, err = d.Write(ctx, []opencdc.Record{del})
	is.NoErr(err)

	var count int
	is.NoErr(conn.QueryRow(ctx, "SELECT count(*) FROM "+quoteForTest(table)+" WHERE id = $1", "doc1:0").Scan(&count))
	is.Equal(count, 0)
}

// TestDestination_PerRecordDimensionGuard verifies the per-record backstop: a
// vector whose length disagrees with the configured dimension is a coded error,
// never a silent write.
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
	// local validation (key present, vector length correct) passes, so the
	// failure originates at Postgres — exactly the case the invariant covers.
	_, err := conn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s (id text PRIMARY KEY CHECK (id <> 'BAD'), embedding vector(4), metadata jsonb)`,
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
		embeddingRecord(opencdc.OperationCreate, "good:0", vec, nil),
		embeddingRecord(opencdc.OperationCreate, "good:1", vec, nil),
		embeddingRecord(opencdc.OperationCreate, "BAD", vec, nil), // violates CHECK at Postgres
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
