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
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

// QuoteIdent wraps a Postgres identifier in double quotes, allowing uppercase
// letters and special characters in identifier names. Mirrors the Postgres
// connector's WrapSQLIdent.
func QuoteIdent(ident string) string {
	return strconv.Quote(ident)
}

// QuoteTable wraps a possibly schema-qualified table reference, quoting the
// schema and table parts independently so "public.docs" becomes
// "public"."docs" (not the single, wrong identifier "public.docs"). A bare
// table name is quoted as-is without forcing a schema prefix.
func QuoteTable(table string) string {
	if i := strings.Index(table, "."); i >= 0 {
		return strconv.Quote(table[:i]) + "." + strconv.Quote(table[i+1:])
	}
	return strconv.Quote(table)
}

// TableExists reports whether the (possibly schema-qualified) table is present
// and visible to the connection role, using to_regclass.
func TableExists(ctx context.Context, q Querier, table string) (bool, error) {
	schema, tbl := splitSchemaTable(table)
	regName := fmt.Sprintf("%s.%s", strconv.Quote(schema), strconv.Quote(tbl))

	var exists bool
	err := q.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, regName).Scan(&exists)
	if err != nil {
		if err == pgx.ErrNoRows { //nolint:errorlint // to_regclass always returns a row; defensive only.
			return false, nil
		}
		return false, fmt.Errorf("failed checking table existence for %q: %w", table, err)
	}
	return exists, nil
}
