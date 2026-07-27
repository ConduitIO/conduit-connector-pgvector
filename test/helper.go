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

// Package test contains helpers for the pgvector connector's integration and
// acceptance tests. It expects the pgvector Postgres from test/docker-compose.yml.
package test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
	"github.com/rs/zerolog"
)

// ConnString is the connection string for the pgvector test database brought up
// by test/docker-compose.yml.
const ConnString = "postgres://conduit:conduit@127.0.0.1:5433/conduit_test?sslmode=disable"

// Context returns a context with a test-scoped logger.
func Context(_ *testing.T) context.Context {
	writer := zerolog.NewConsoleWriter()
	writer.NoColor = true
	writer.PartsExclude = []string{zerolog.TimestampFieldName}
	logger := zerolog.New(writer).Level(zerolog.DebugLevel)
	return logger.WithContext(context.Background())
}

// Connect opens a connection to the test database, skipping the test if the
// database is unreachable so unit-only runs (and CI without docker) do not fail.
func Connect(ctx context.Context, t *testing.T) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, ConnString)
	if err != nil {
		t.Skipf("skipping: pgvector test database not reachable (%v); run `make test` for docker-backed tests", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// EnsureExtension creates the pgvector extension if it is not already present.
func EnsureExtension(ctx context.Context, t *testing.T, conn *pgx.Conn) {
	t.Helper()
	is := is.New(t)
	_, err := conn.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS vector`)
	is.NoErr(err)
}

// SetupVectorTable creates a fresh vector table with the given dimension and
// drops it on cleanup. The table has an id text primary key, an embedding
// vector(dim) column, a metadata jsonb column, and a source_key text column
// (indexed) — the shape the connector's default config (sourceKeyColumn
// enabled) expects, matching the README's recommended schema.
func SetupVectorTable(ctx context.Context, t *testing.T, conn *pgx.Conn, table string, dim int) {
	t.Helper()
	is := is.New(t)
	_, err := conn.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %q (id text PRIMARY KEY, embedding vector(%d), metadata jsonb, source_key text)`, table, dim))
	is.NoErr(err)
	_, err = conn.Exec(ctx, fmt.Sprintf(`CREATE INDEX ON %q (source_key)`, table))
	is.NoErr(err)
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), fmt.Sprintf(`DROP TABLE IF EXISTS %q`, table))
	})
}

// identifierCounter makes RandomIdentifier collision-free within a test run
// without a (gosec-flagged) weak RNG.
var identifierCounter atomic.Uint64

// RandomIdentifier returns a lowercase table-safe identifier unique to the test.
func RandomIdentifier(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("conduit_pgvec_%d_%d", time.Now().UnixNano(), identifierCounter.Add(1))
}
