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

package internal

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Querier is the subset of pgx used for database introspection. It is satisfied
// by both *pgx.Conn and pgxpool.Pool, which keeps db_info unit-testable against
// a lightweight fake.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// VectorColumnInfo describes a target table's vector column as introspected
// from the Postgres catalog.
type VectorColumnInfo struct {
	// TypeName is the base Postgres type name (e.g. "vector"). pgvector's
	// extension type is named "vector".
	TypeName string
	// Dimension is the fixed dimension declared on the column
	// (vector(Dimension)). It is DimensionUnspecified when the column is an
	// unconstrained `vector` with no declared dimension.
	Dimension int
}

// DimensionUnspecified is the sentinel Dimension value for an unconstrained
// `vector` column (declared as `vector`, not `vector(N)`). pgvector stores the
// dimension directly in pg_attribute.atttypmod, using -1 for "unspecified".
const DimensionUnspecified = -1

// PgVectorTypeName is the type name pgvector registers for its vector type.
const PgVectorTypeName = "vector"

// GetVectorColumnInfo introspects the target table's vector column from the
// Postgres catalog. It returns the base type name and the column's declared
// dimension (from atttypmod). It returns a wrapped ErrColumnNotFound if the
// table or column does not exist so callers can map it to a stable connector
// error code.
//
// The dimension is read from pg_attribute.atttypmod directly: pgvector's
// typmod_in stores the declared dimension there verbatim (no VARHDRSZ offset),
// so atttypmod is N for vector(N) and -1 for an unconstrained vector.
func GetVectorColumnInfo(ctx context.Context, q Querier, table, column string) (VectorColumnInfo, error) {
	schema, tbl := splitSchemaTable(table)

	const query = `
		SELECT t.typname, a.atttypmod
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_type t ON t.oid = a.atttypid
		WHERE n.nspname = $1
		  AND c.relname = $2
		  AND a.attname = $3
		  AND a.attnum > 0
		  AND NOT a.attisdropped
	`

	var (
		typeName string
		typmod   int
	)
	err := q.QueryRow(ctx, query, schema, tbl, column).Scan(&typeName, &typmod)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return VectorColumnInfo{}, fmt.Errorf("%w: %s.%s.%s", ErrColumnNotFound, schema, tbl, column)
		}
		return VectorColumnInfo{}, fmt.Errorf("failed introspecting vector column %s.%s.%s: %w", schema, tbl, column, err)
	}

	return VectorColumnInfo{TypeName: typeName, Dimension: typmod}, nil
}

// ErrColumnNotFound is returned by GetVectorColumnInfo when the target table or
// column is not present in the catalog. It is a sentinel so callers can map it
// to the appropriate stable connector error code with errors.Is.
var ErrColumnNotFound = errors.New("target table or column not found")

// splitSchemaTable splits a possibly schema-qualified table reference into its
// schema and table parts, defaulting to the "public" schema. It intentionally
// treats only the first dot as the separator; pgvector target tables are simple
// identifiers, not arbitrary quoted paths (deeper qualification handling is a
// later slice).
func splitSchemaTable(table string) (schema, tbl string) {
	if i := strings.Index(table, "."); i >= 0 {
		return table[:i], table[i+1:]
	}
	return "public", table
}
