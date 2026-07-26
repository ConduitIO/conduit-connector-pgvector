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
	"strings"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-pgvector/destination"
	"github.com/conduitio/conduit-connector-pgvector/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// Destination writes embedding records into a pgvector table. Each record is
// upserted by its stable key (chunk_id) so redelivery under at-least-once
// converges to a single row (Invariant 3; see design doc §6).
type Destination struct {
	sdk.UnimplementedDestination

	config       destination.Config
	getTableName destination.TableFn

	conn *pgx.Conn
}

func (d *Destination) Config() sdk.DestinationConfig {
	return &d.config
}

// NewDestination returns the pgvector destination wrapped with the SDK's
// default destination middleware.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{})
}

// Open connects to the target database and performs fail-fast dimension
// validation: for a statically-named target table it reads the vector column's
// declared dimension and refuses to start if it does not match the configured
// embedding dimension (Invariant 6; design doc §5). A templated (per-record)
// table name cannot be introspected at startup, so validation for that case is
// deferred to the per-record guard in Write, and the deferral is logged rather
// than hidden.
func (d *Destination) Open(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, d.config.URL)
	if err != nil {
		return internal.NewCodedError(internal.CodeConnectionFailed, "failed to connect to the target database").
			WithConfigPath("url").
			WithSuggestion("verify the database is reachable and the credentials in \"url\" are correct").
			Wrapping(err)
	}
	d.conn = conn

	d.getTableName, err = d.config.TableFunction()
	if err != nil {
		return internal.NewCodedError(internal.CodeInvalidConfig, "invalid table name or table template").
			WithConfigPath("table").
			Wrapping(err)
	}

	// Register pgvector's `vector` type with this connection's type map so a
	// formatted vector literal binds as a text-encoded `vector` parameter. This
	// makes the upsert rely on the prepared-statement's parameter OIDs (which
	// Postgres reports as the real column types) rather than fragile SQL-cast
	// type inference, and avoids a hard dependency on a pgvector-specific pgx
	// client library.
	d.registerVectorType(ctx)

	if table, ok := d.staticTable(); ok {
		return d.validateDimension(ctx, table)
	}

	sdk.Logger(ctx).Warn().
		Str("table", d.config.Table).
		Msg("target table is templated (resolved per record); deferring vector dimension validation to the per-record write guard")
	return nil
}

// registerVectorType looks up pgvector's `vector` type OID and registers it on
// the connection with a text codec, so a formatted vector literal (a Go string)
// encodes correctly for a `vector` column. If pgvector is not installed the OID
// lookup returns no rows; registration is skipped and a clear pgx encode error
// surfaces at write time for any vector column (static tables are already
// caught earlier by validateDimension).
func (d *Destination) registerVectorType(ctx context.Context) {
	var oid uint32
	err := d.conn.QueryRow(ctx, `SELECT oid FROM pg_type WHERE typname = $1`, internal.PgVectorTypeName).Scan(&oid)
	if err != nil {
		sdk.Logger(ctx).Warn().Err(err).
			Msg("could not resolve the pgvector 'vector' type OID; vector binding falls back to default pgx encoding")
		return
	}
	d.conn.TypeMap().RegisterType(&pgtype.Type{
		Name:  internal.PgVectorTypeName,
		OID:   oid,
		Codec: pgtype.TextCodec{},
	})
}

// validateDimension is the canonical fail-fast check. It distinguishes a
// missing table, a missing/wrong-typed vector column, and a true dimension
// mismatch, and returns a coded, actionable error naming both the table's
// dimension and the configured dimension for the mismatch case.
func (d *Destination) validateDimension(ctx context.Context, table string) error {
	exists, err := internal.TableExists(ctx, d.conn, table)
	if err != nil {
		return internal.NewCodedError(internal.CodeConnectionFailed, "failed to introspect the target table").
			WithConfigPath("table").
			Wrapping(err)
	}
	if !exists {
		return internal.NewCodedError(internal.CodeTargetTableMissing,
			fmt.Sprintf("target table %q does not exist or is not visible to the connection role", table)).
			WithConfigPath("table").
			WithSuggestion(fmt.Sprintf("create the table, e.g. CREATE TABLE %s (%s text PRIMARY KEY, %s vector(%d), %s jsonb)",
				internal.QuoteTable(table), internal.QuoteIdent(d.config.KeyColumn),
				internal.QuoteIdent(d.config.VectorColumn), d.config.Dimension,
				metadataColumnForHint(d.config.MetadataColumn)))
	}

	info, err := internal.GetVectorColumnInfo(ctx, d.conn, table, d.config.VectorColumn)
	if err != nil {
		if errors.Is(err, internal.ErrColumnNotFound) {
			return internal.NewCodedError(internal.CodeVectorColumnMissing,
				fmt.Sprintf("vector column %q does not exist on table %q", d.config.VectorColumn, table)).
				WithConfigPath("vectorColumn").
				WithSuggestion(fmt.Sprintf("add the column, e.g. ALTER TABLE %s ADD COLUMN %s vector(%d)",
					internal.QuoteTable(table), internal.QuoteIdent(d.config.VectorColumn), d.config.Dimension))
		}
		return internal.NewCodedError(internal.CodeConnectionFailed, "failed to introspect the target vector column").
			WithConfigPath("vectorColumn").
			Wrapping(err)
	}

	if info.TypeName != internal.PgVectorTypeName {
		return internal.NewCodedError(internal.CodeVectorColumnTypeMismatch,
			fmt.Sprintf("column %q on table %q is of type %q, not pgvector %q",
				d.config.VectorColumn, table, info.TypeName, internal.PgVectorTypeName)).
			WithConfigPath("vectorColumn").
			WithSuggestion("point \"vectorColumn\" at a column declared as vector(N), or install the pgvector extension and recreate the column")
	}

	if info.Dimension == internal.DimensionUnspecified {
		sdk.Logger(ctx).Warn().
			Str("table", table).
			Str("column", d.config.VectorColumn).
			Msg("target vector column has no declared dimension; skipping strict startup validation (per-record dimension guard still applies)")
		return nil
	}

	// Invariant 6: never silently write a mismatched vector. A dimension
	// mismatch is a coded, actionable startup failure — not a first-upsert
	// surprise, and never a silent truncate/pad.
	if info.Dimension != d.config.Dimension {
		return internal.NewCodedError(internal.CodeVectorDimensionMismatch,
			fmt.Sprintf("configured embedding dimension %d does not match target column %q.%q dimension %d",
				d.config.Dimension, table, d.config.VectorColumn, info.Dimension)).
			WithConfigPath("dimension").
			WithSuggestion(fmt.Sprintf("set \"dimension\" to %d to match the table, or point \"table\" at a vector(%d) column, or migrate with ALTER TABLE %s ALTER COLUMN %s TYPE vector(%d)",
				info.Dimension, d.config.Dimension,
				internal.QuoteTable(table), internal.QuoteIdent(d.config.VectorColumn), d.config.Dimension))
	}

	sdk.Logger(ctx).Info().
		Str("table", table).
		Str("column", d.config.VectorColumn).
		Int("dimension", info.Dimension).
		Msg("vector dimension validated against target table")
	return nil
}

// Write upserts embedding records and deletes tombstones. The whole slice is
// executed as one implicit transaction via pgx batch, so a partial failure
// returns n=0 with an error and the engine retries/DLQs the batch — never an
// early ack for records that did not durably land (Invariants 1 & 3).
func (d *Destination) Write(ctx context.Context, recs []opencdc.Record) (int, error) {
	batch := &pgx.Batch{}
	for _, rec := range recs {
		table, err := d.getTableName(rec)
		if err != nil {
			return 0, internal.NewCodedError(internal.CodeInvalidConfig, "failed to resolve target table for record").
				WithConfigPath("table").
				Wrapping(err)
		}

		switch rec.Operation {
		case opencdc.OperationCreate, opencdc.OperationSnapshot, opencdc.OperationUpdate:
			if err := d.queueUpsert(batch, table, rec); err != nil {
				return 0, err
			}
		case opencdc.OperationDelete:
			if err := d.queueDelete(batch, table, rec); err != nil {
				return 0, err
			}
		default:
			return 0, internal.NewCodedError(internal.CodeWriteFailed,
				fmt.Sprintf("unsupported record operation %q", rec.Operation))
		}
	}

	br := d.conn.SendBatch(ctx, batch)
	defer br.Close()

	for i := range recs {
		if _, err := br.Exec(); err != nil {
			// The batch runs in a single implicit transaction: if one statement
			// fails, none are committed, so we report n=0 and let the engine
			// apply its retry/DLQ policy to the whole slice.
			return 0, internal.NewCodedError(internal.CodeWriteFailed,
				fmt.Sprintf("failed to execute upsert/delete for record %d", i)).
				Wrapping(err)
		}
	}

	return len(recs), nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	if d.conn != nil {
		return d.conn.Close(ctx) //nolint:wrapcheck // close error surfaced verbatim.
	}
	return nil
}

// queueUpsert builds the idempotent upsert:
//
//	INSERT INTO <table> (<key>, <vector>[, <metadata>])
//	VALUES ($1, $2::vector[, $3::jsonb])
//	ON CONFLICT (<key>) DO UPDATE SET <vector> = EXCLUDED.<vector>[, <metadata> = EXCLUDED.<metadata>]
//
// Keying the conflict on the stable chunk_id and replacing the whole
// vector+metadata pair in one statement is what makes redelivery converge to a
// single, consistent row (Invariant 3; design doc §6).
func (d *Destination) queueUpsert(batch *pgx.Batch, table string, rec opencdc.Record) error {
	key, err := extractKey(rec)
	if err != nil {
		return err
	}

	payload, err := structuredPayload(rec)
	if err != nil {
		return err
	}

	raw, ok := payload[d.config.VectorField]
	if !ok {
		return internal.NewCodedError(internal.CodeMissingVectorField,
			fmt.Sprintf("record payload has no field %q carrying the embedding vector", d.config.VectorField)).
			WithConfigPath("vectorField").
			WithSuggestion("ensure the upstream embedding processor writes the vector to this payload field")
	}

	vec, err := internal.ParseVector(raw)
	if err != nil {
		return internal.NewCodedError(internal.CodeMissingVectorField,
			fmt.Sprintf("field %q is not a valid embedding vector", d.config.VectorField)).
			WithConfigPath("vectorField").
			Wrapping(err)
	}

	// Per-record dimension guard (backstop to the startup check, and the only
	// dimension check available for templated tables). Invariant 6: refuse a
	// mismatched vector rather than let pgvector reject or silently coerce it.
	if len(vec) != d.config.Dimension {
		return internal.NewCodedError(internal.CodeVectorDimensionMismatch,
			fmt.Sprintf("record vector has dimension %d but the connector is configured for dimension %d", len(vec), d.config.Dimension)).
			WithConfigPath("dimension").
			WithSuggestion("ensure the embedding model's output dimension matches \"dimension\"")
	}

	// No SQL casts: the prepared statement's parameter OIDs (the real column
	// types — vector, jsonb) drive encoding. The vector type is registered on
	// the connection in Open; jsonb is handled by pgx's built-in codec.
	cols := []string{internal.QuoteIdent(d.config.KeyColumn), internal.QuoteIdent(d.config.VectorColumn)}
	placeholders := []string{"$1", "$2"}
	args := []any{key, internal.FormatVector(vec)}
	var setClauses []string
	setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s",
		internal.QuoteIdent(d.config.VectorColumn), internal.QuoteIdent(d.config.VectorColumn)))

	if d.config.MetadataColumn != "" {
		metaJSON, err := buildMetadata(rec)
		if err != nil {
			return err
		}
		cols = append(cols, internal.QuoteIdent(d.config.MetadataColumn))
		placeholders = append(placeholders, "$3")
		args = append(args, metaJSON)
		setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s",
			internal.QuoteIdent(d.config.MetadataColumn), internal.QuoteIdent(d.config.MetadataColumn)))
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
		internal.QuoteTable(table),
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
		internal.QuoteIdent(d.config.KeyColumn),
		strings.Join(setClauses, ", "),
	)
	batch.Queue(query, args...)
	return nil
}

// queueDelete removes the row for a tombstone's key. Slice 1 deletes by the
// record's own key (chunk_id). Deleting all chunks derived from a source record
// by a source_key metadata match (design doc §5's orphan-avoidance rule) is a
// deliberate later slice.
func (d *Destination) queueDelete(batch *pgx.Batch, table string, rec opencdc.Record) error {
	key, err := extractKey(rec)
	if err != nil {
		return err
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = $1",
		internal.QuoteTable(table), internal.QuoteIdent(d.config.KeyColumn))
	batch.Queue(query, key)
	return nil
}

// staticTable returns the configured table name and true when it is a static
// identifier (not a Go template), the case that can be dimension-validated at
// startup.
func (d *Destination) staticTable() (string, bool) {
	t := d.config.Table
	if strings.HasPrefix(t, "{{") || strings.HasSuffix(t, "}}") {
		return "", false
	}
	if strings.TrimSpace(t) == "" {
		return "", false
	}
	return t, true
}

// extractKey derives the upsert/delete key (the stable chunk_id) from the
// record key. It accepts a raw key (used verbatim as text) or structured data
// (the configured key field, or a lone field). A record with no usable key is a
// coded error rather than a silent skip (Invariant 3: no silent drop).
func extractKey(rec opencdc.Record) (any, error) {
	switch k := rec.Key.(type) {
	case opencdc.RawData:
		if len(k.Bytes()) == 0 {
			return nil, missingKeyErr()
		}
		return string(k.Bytes()), nil
	case opencdc.StructuredData:
		if len(k) == 0 {
			return nil, missingKeyErr()
		}
		if len(k) == 1 {
			for _, v := range k {
				return v, nil
			}
		}
		// Multiple key fields but no single obvious chunk_id: serialize
		// deterministically so redelivery still converges to one conflict key.
		b, err := json.Marshal(k)
		if err != nil {
			return nil, internal.NewCodedError(internal.CodeMissingKey, "failed to serialize composite record key").Wrapping(err)
		}
		return string(b), nil
	case nil:
		return nil, missingKeyErr()
	default:
		return nil, missingKeyErr()
	}
}

func missingKeyErr() error {
	return internal.NewCodedError(internal.CodeMissingKey,
		"record has no key to use as the upsert conflict target (chunk_id)").
		WithSuggestion("ensure the record carries a stable key (e.g. the chunk_id set by the chunking processor)")
}

// structuredPayload returns the record's after-payload as a map, JSON-decoding
// raw payloads. An embedding processor emits structured payloads; raw JSON is
// accepted defensively.
func structuredPayload(rec opencdc.Record) (map[string]any, error) {
	switch p := rec.Payload.After.(type) {
	case opencdc.StructuredData:
		return p, nil
	case opencdc.RawData:
		if len(p.Bytes()) == 0 {
			return map[string]any{}, nil
		}
		m := make(map[string]any)
		if err := json.Unmarshal(p.Bytes(), &m); err != nil {
			return nil, internal.NewCodedError(internal.CodeMissingVectorField, "failed to decode record payload as JSON").Wrapping(err)
		}
		return m, nil
	case nil:
		return map[string]any{}, nil
	default:
		return nil, internal.NewCodedError(internal.CodeMissingVectorField,
			fmt.Sprintf("unexpected payload type %T", rec.Payload.After))
	}
}

// buildMetadata serializes the record metadata to a JSON object for the JSONB
// metadata column. Slice 1 writes the full OpenCDC metadata map; configurable
// field selection and typed mapping is a later slice.
func buildMetadata(rec opencdc.Record) ([]byte, error) {
	md := rec.Metadata
	if md == nil {
		md = opencdc.Metadata{}
	}
	b, err := json.Marshal(md)
	if err != nil {
		return nil, internal.NewCodedError(internal.CodeWriteFailed, "failed to serialize record metadata to JSON").Wrapping(err)
	}
	return b, nil
}

// metadataColumnForHint returns a metadata column name to use in a CREATE TABLE
// hint, defaulting to "metadata" when metadata writes are disabled so the hint
// is still valid, illustrative SQL.
func metadataColumnForHint(col string) string {
	if col == "" {
		return "metadata"
	}
	return col
}
