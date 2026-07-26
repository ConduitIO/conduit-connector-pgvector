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

package destination

import (
	"bytes"
	"context"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-pgvector/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
)

// TableFn resolves the target table for a given record, supporting either a
// static table name or a Go-template-derived one.
type TableFn func(opencdc.Record) (string, error)

// Config is the pgvector destination configuration. It maps an incoming
// embedding record onto a pgvector table row: the record key becomes the
// upsert conflict key (KeyColumn / the stable chunk_id), the VectorField in the
// payload becomes the VectorColumn value, and the remaining metadata is written
// to MetadataColumn as JSONB. See design doc §5.
type Config struct {
	sdk.DefaultDestinationMiddleware

	// URL is the connection string for the target Postgres/pgvector database.
	URL string `json:"url" validate:"required"`
	// Table is the target table into which embedding rows are upserted.
	// Supports a Go template evaluated per record (default: the record's
	// OpenCDC collection), matching the Postgres connector's convention.
	Table string `json:"table" default:"{{ index .Metadata \"opencdc.collection\" }}"`
	// Dimension is the embedding dimension the upstream model produces. It is
	// validated against the target table's vector column at pipeline start
	// (fail fast). There is no safe default, so it is required.
	Dimension int `json:"dimension" validate:"required"`
	// VectorColumn is the name of the pgvector `vector` column on the target
	// table.
	VectorColumn string `json:"vectorColumn" default:"embedding"`
	// KeyColumn is the primary-key column used as the ON CONFLICT target for
	// idempotent upserts. It holds the record's stable chunk_id.
	KeyColumn string `json:"keyColumn" default:"id"`
	// VectorField is the record payload field that carries the embedding
	// vector (a numeric array).
	VectorField string `json:"vectorField" default:"vector"`
	// MetadataColumn is the JSONB column that receives the record's metadata.
	// Leave empty to disable metadata writes.
	MetadataColumn string `json:"metadataColumn" default:"metadata"`
}

// Validate performs config-time sanity checks and returns machine-actionable,
// coded errors. It does NOT touch the database — the dimension-vs-table check
// that needs a connection is the fail-fast check in Destination.Open.
func (c *Config) Validate(ctx context.Context) error {
	if _, err := pgx.ParseConfig(c.URL); err != nil {
		return internal.NewCodedError(internal.CodeInvalidConfig, "invalid Postgres connection URL").
			WithConfigPath("url").
			WithSuggestion("provide a valid Postgres connection string, e.g. postgres://user:pass@host:5432/db?sslmode=disable").
			Wrapping(err)
	}

	if c.Dimension <= 0 {
		return internal.NewCodedError(internal.CodeInvalidConfig, "embedding dimension must be a positive integer").
			WithConfigPath("dimension").
			WithSuggestion("set \"dimension\" to the output dimension of your embedding model (e.g. 1536 for text-embedding-3-small, 768 for nomic-embed-text)")
	}

	if strings.TrimSpace(c.VectorColumn) == "" {
		return internal.NewCodedError(internal.CodeInvalidConfig, "vector column name must not be empty").
			WithConfigPath("vectorColumn").
			WithSuggestion("set \"vectorColumn\" to the name of the pgvector vector column on the target table")
	}

	if strings.TrimSpace(c.KeyColumn) == "" {
		return internal.NewCodedError(internal.CodeInvalidConfig, "key column name must not be empty").
			WithConfigPath("keyColumn").
			WithSuggestion("set \"keyColumn\" to the primary-key column used as the upsert conflict target")
	}

	if strings.TrimSpace(c.VectorField) == "" {
		return internal.NewCodedError(internal.CodeInvalidConfig, "vector field name must not be empty").
			WithConfigPath("vectorField").
			WithSuggestion("set \"vectorField\" to the record payload field that carries the embedding vector")
	}

	if _, err := c.TableFunction(); err != nil {
		return internal.NewCodedError(internal.CodeInvalidConfig, "invalid table name or table template").
			WithConfigPath("table").
			WithSuggestion("provide a static table name or a valid Go template").
			Wrapping(err)
	}

	if err := c.DefaultDestinationMiddleware.Validate(ctx); err != nil {
		return internal.NewCodedError(internal.CodeInvalidConfig, "connector middleware validation failed").Wrapping(err)
	}

	return nil
}

// TableFunction returns a function that resolves the target table for each
// record. If the configured table is not a Go template it returns a function
// that always yields the static name; otherwise it compiles the template once
// and evaluates it per record. Mirrors the Postgres connector's behavior.
func (c *Config) TableFunction() (TableFn, error) {
	if !strings.HasPrefix(c.Table, "{{") && !strings.HasSuffix(c.Table, "}}") {
		return func(_ opencdc.Record) (string, error) {
			return c.Table, nil
		}, nil
	}

	t, err := template.New("table").Funcs(sprig.FuncMap()).Parse(c.Table)
	if err != nil {
		return nil, err //nolint:wrapcheck // wrapped with a coded error by the caller (Validate).
	}

	var buf bytes.Buffer
	return func(r opencdc.Record) (string, error) {
		buf.Reset()
		if err := t.Execute(&buf, r); err != nil {
			return "", err //nolint:wrapcheck // template execution error surfaced verbatim to the caller.
		}
		return buf.String(), nil
	}, nil
}
