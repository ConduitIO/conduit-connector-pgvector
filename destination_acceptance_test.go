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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-pgvector/internal"
	"github.com/conduitio/conduit-connector-pgvector/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

// acceptanceDim is the embedding dimension used throughout the acceptance
// suite: small, and chosen so generated values are exact binary fractions
// (n + k/4 for integer n, k in 0..3), so pgvector's text round-trip never
// introduces float-rounding noise into the SDK's byte-equality comparisons.
const acceptanceDim = 4

// acceptanceRecordCounter makes every generated record's vector distinct
// (see acceptanceDriver.GenerateRecord for why distinctness matters, not just
// validity).
var acceptanceRecordCounter atomic.Uint64

// acceptanceDriver adapts the SDK's generic destination acceptance suite
// (sdk.AcceptanceTest, see conduit-connector-sdk's acceptance_testing.go) to
// pgvector's domain-specific write contract, by embedding the SDK's default
// driver and overriding exactly the two methods that don't fit a
// domain-shaped, write-only connector:
//
//   - GenerateRecord: the SDK default generates arbitrary mixed-type payloads
//     (bools, nested maps, strings, ints). This connector's Write requires a
//     structured payload with a fixed-dimension numeric vectorField and the
//     sourceKeyMetadataKey ("ai.chunk.source_key" by default) metadata
//     contract — a generic fuzzed payload can't satisfy that shape, so every
//     generated record carries both.
//   - ReadFromDestination: the SDK default opens a Source and reads back
//     through it (`if d.Connector().NewSource == nil { t.Fatal(...) }`).
//     pgvector is destination-only (Connector.NewSource == nil per
//     connector.go) — there is no Source to read through — so writes are
//     verified by querying the target table directly instead. This is the
//     only way to verify a write-only sink's acceptance suite; see the
//     "Acceptance suite" section of README.md for the full covered/skipped
//     breakdown.
type acceptanceDriver struct {
	sdk.ConfigurableAcceptanceTestDriver
	conn  *pgx.Conn
	table string
}

func newAcceptanceDriver(t *testing.T) *acceptanceDriver {
	t.Helper()
	ctx := test.Context(t)
	conn := test.Connect(ctx, t)
	test.EnsureExtension(ctx, t, conn)
	table := test.RandomIdentifier(t)
	test.SetupVectorTable(ctx, t, conn, table, acceptanceDim)

	return &acceptanceDriver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Context:   ctx,
				Connector: Connector,
				DestinationConfig: config.Config{
					"url":       test.ConnString,
					"table":     table,
					"dimension": fmt.Sprintf("%d", acceptanceDim),
				},
				// keyColumn/vectorColumn/vectorField/metadataColumn/
				// sourceKeyColumn/sourceKeyMetadataKey/idMetadataKey all use
				// their config defaults, which match the columns
				// test.SetupVectorTable creates (id, embedding, metadata,
				// source_key) and the metadata keys GenerateRecord sets
				// below.
			},
		},
		conn:  conn,
		table: table,
	}
}

// GenerateRecord builds a record shaped the way this connector's Write
// requires: a unique key, a valid fixed-dimension vector, and the
// source_key metadata every create/update/snapshot record must carry once
// source_key population is enabled (the default; see design doc §5 and the
// README's "Deletes: fan-out by source_key" section).
//
// The vector is distinct per record (an integer offset plus a quarter-step
// per dimension), not just individually valid: sdk.AcceptanceTest's
// isEqualRecords matches want/got pairs by sorting both slices on
// Payload.After.Bytes() and comparing index-for-index. If every generated
// record carried the identical vector, that sort would be a set of ties with
// unspecified relative order, and the resulting want[i]/got[i] pairing could
// compare an unrelated pair's keys against each other — a flaky failure
// unrelated to any real bug. Distinct payload bytes make the sort
// deterministic per record.
func (d *acceptanceDriver) GenerateRecord(t *testing.T, op opencdc.Operation) opencdc.Record {
	t.Helper()
	n := acceptanceRecordCounter.Add(1)
	key := test.RandomIdentifier(t)

	vec := make([]float32, acceptanceDim)
	for i := range vec {
		vec[i] = float32(n) + float32(i)/float32(acceptanceDim)
	}

	return opencdc.Record{
		Operation: op,
		Key:       opencdc.RawData(key),
		Metadata:  opencdc.Metadata{"ai.chunk.source_key": key},
		Payload: opencdc.Change{
			After: opencdc.StructuredData{"vector": vec},
		},
	}
}

// ReadFromDestination verifies what the destination wrote by querying the
// target table directly (see the type doc for why: this connector has no
// Source to read through). For each input record it looks up the row by key
// and reconstructs a record with the same operation/key and the vector read
// back from Postgres, which sdk.AcceptanceTest then compares against the
// original.
func (d *acceptanceDriver) ReadFromDestination(t *testing.T, records []opencdc.Record) []opencdc.Record {
	t.Helper()
	is := is.New(t)
	ctx := d.Context()

	out := make([]opencdc.Record, 0, len(records))
	for _, rec := range records {
		key, ok := rec.Key.(opencdc.RawData)
		if !ok {
			t.Fatalf("acceptanceDriver.ReadFromDestination: record key is %T, want opencdc.RawData", rec.Key)
		}

		var vecText string
		err := d.conn.QueryRow(ctx,
			"SELECT embedding::text FROM "+quoteForTest(d.table)+" WHERE id = $1", string(key),
		).Scan(&vecText)
		is.NoErr(err) // record written by the destination could not be read back from the table

		vec, err := internal.ParseVectorText(vecText)
		is.NoErr(err)

		out = append(out, opencdc.Record{
			Operation: rec.Operation,
			Key:       rec.Key,
			Payload: opencdc.Change{
				After: opencdc.StructuredData{"vector": vec},
			},
		})
	}
	return out
}

// TestAcceptance runs the SDK's compatibility suite via acceptanceDriver.
// Docker-gated like the other integration tests in this package (via
// test.Connect, transitively through newAcceptanceDriver): it skips when the
// pgvector test database is unreachable, so `go test -short` and CI without
// docker do not fail on it, and it runs in CI (and locally with `make test`)
// otherwise.
//
// See README.md's "Acceptance suite" section for exactly which of the
// harness's subtests run (Destination*, Specifier*) versus skip with reason
// (every Source* test — this connector has no Source by design).
func TestAcceptance(t *testing.T) {
	driver := newAcceptanceDriver(t)
	sdk.AcceptanceTest(t, driver)
}
