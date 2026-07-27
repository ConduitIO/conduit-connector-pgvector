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

package internal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/conduitio/conduit-connector-pgvector/internal"
	"github.com/jackc/pgx/v5"
	"github.com/matryer/is"
)

// fakeRow returns preset scan values, or an error.
type fakeRow struct {
	typeName string
	typmod   int
	err      error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 2 {
		return errors.New("unexpected scan arity")
	}
	*dest[0].(*string) = r.typeName
	*dest[1].(*int) = r.typmod
	return nil
}

// fakeQuerier returns a preset row for QueryRow.
type fakeQuerier struct {
	row       fakeRow
	lastArgs  []any
	lastQuery string
}

func (q *fakeQuerier) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	q.lastQuery = sql
	q.lastArgs = args
	return q.row
}

func TestGetVectorColumnInfo_Dimension(t *testing.T) {
	is := is.New(t)
	q := &fakeQuerier{row: fakeRow{typeName: "vector", typmod: 768}}
	info, err := internal.GetVectorColumnInfo(context.Background(), q, "docs", "embedding")
	is.NoErr(err)
	is.Equal(info.TypeName, "vector")
	is.Equal(info.Dimension, 768)
	// default schema applied, column passed through
	is.Equal(q.lastArgs, []any{"public", "docs", "embedding"})
}

func TestGetVectorColumnInfo_SchemaQualified(t *testing.T) {
	is := is.New(t)
	q := &fakeQuerier{row: fakeRow{typeName: "vector", typmod: 1536}}
	info, err := internal.GetVectorColumnInfo(context.Background(), q, "rag.chunks", "embedding")
	is.NoErr(err)
	is.Equal(info.Dimension, 1536)
	is.Equal(q.lastArgs, []any{"rag", "chunks", "embedding"})
}

func TestGetVectorColumnInfo_Unspecified(t *testing.T) {
	is := is.New(t)
	q := &fakeQuerier{row: fakeRow{typeName: "vector", typmod: -1}}
	info, err := internal.GetVectorColumnInfo(context.Background(), q, "docs", "embedding")
	is.NoErr(err)
	is.Equal(info.Dimension, internal.DimensionUnspecified)
}

func TestGetVectorColumnInfo_ColumnNotFound(t *testing.T) {
	is := is.New(t)
	q := &fakeQuerier{row: fakeRow{err: pgx.ErrNoRows}}
	_, err := internal.GetVectorColumnInfo(context.Background(), q, "docs", "embedding")
	is.True(errors.Is(err, internal.ErrColumnNotFound))
}

// fakeBoolRow scans a single bool, for EXISTS-style queries.
type fakeBoolRow struct {
	val bool
	err error
}

func (r fakeBoolRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(*bool) = r.val
	return nil
}

type fakeBoolQuerier struct {
	row      fakeBoolRow
	lastArgs []any
}

func (q *fakeBoolQuerier) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	q.lastArgs = args
	return q.row
}

func TestColumnExists(t *testing.T) {
	is := is.New(t)

	q := &fakeBoolQuerier{row: fakeBoolRow{val: true}}
	ok, err := internal.ColumnExists(context.Background(), q, "rag.chunks", "source_key")
	is.NoErr(err)
	is.True(ok)
	is.Equal(q.lastArgs, []any{"rag", "chunks", "source_key"})

	q = &fakeBoolQuerier{row: fakeBoolRow{val: false}}
	ok, err = internal.ColumnExists(context.Background(), q, "docs", "source_key")
	is.NoErr(err)
	is.True(!ok)
}

func TestColumnExists_QueryError(t *testing.T) {
	is := is.New(t)
	q := &fakeBoolQuerier{row: fakeBoolRow{err: errors.New("connection reset")}}
	_, err := internal.ColumnExists(context.Background(), q, "docs", "source_key")
	is.True(err != nil)
}

func TestHasSingleColumnUniqueConstraint(t *testing.T) {
	is := is.New(t)

	q := &fakeBoolQuerier{row: fakeBoolRow{val: true}}
	ok, err := internal.HasSingleColumnUniqueConstraint(context.Background(), q, "rag.chunks", "id")
	is.NoErr(err)
	is.True(ok)
	is.Equal(q.lastArgs, []any{"rag", "chunks", "id"})

	q = &fakeBoolQuerier{row: fakeBoolRow{val: false}}
	ok, err = internal.HasSingleColumnUniqueConstraint(context.Background(), q, "docs", "id")
	is.NoErr(err)
	is.True(!ok)
}
