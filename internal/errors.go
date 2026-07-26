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
	"encoding/json"
	"fmt"
	"strings"
)

// Stable, machine-actionable error codes surfaced by the pgvector connector.
//
// Codes are a public contract (CLAUDE.md: "Errors are API"). Agents and
// operators match on the code, so they are versioned like any other public
// surface: renaming or repurposing a code is a breaking change.
//
// The connector deliberately uses two namespaces:
//
//   - "ai.*"       — codes shared across the AI-pipeline subsystem, defined by
//     the design doc's §9 error table (20260724-ai-pipeline-components.md). The
//     dimension-mismatch code MUST match the doc so the whole subsystem speaks
//     one vocabulary for the canonical fail-fast case.
//   - "pgvector.*" — codes specific to this connector (config, connection,
//     target-table introspection).
const (
	// CodeVectorDimensionMismatch is the canonical fail-fast case: the
	// configured embedding dimension does not match the target table's vector
	// column dimension. Raised at pipeline start (Open), never silently on the
	// first upsert. Matches design doc §5/§9 exactly.
	CodeVectorDimensionMismatch = "ai.vector_dimension_mismatch"

	// CodeInvalidConfig is raised when connector configuration fails validation
	// before any connection is opened.
	CodeInvalidConfig = "pgvector.invalid_config"

	// CodeConnectionFailed is raised when the connector cannot open a
	// connection to the target Postgres/pgvector database.
	CodeConnectionFailed = "pgvector.connection_failed"

	// CodeTargetTableMissing is raised when the configured target table does
	// not exist (or is not visible to the connection role).
	CodeTargetTableMissing = "pgvector.target_table_missing"

	// CodeVectorColumnMissing is raised when the configured vector column does
	// not exist on the target table.
	CodeVectorColumnMissing = "pgvector.vector_column_missing"

	// CodeVectorColumnTypeMismatch is raised when the configured vector column
	// exists but is not a pgvector `vector` column.
	CodeVectorColumnTypeMismatch = "pgvector.vector_column_type_mismatch"

	// CodeMissingUniqueConstraint is raised when the key column has no
	// single-column unique/primary-key constraint to back the upsert's
	// ON CONFLICT target.
	CodeMissingUniqueConstraint = "pgvector.missing_unique_constraint"

	// CodeMissingVectorField is raised when a record does not carry the
	// configured embedding-vector field, or the field is not a numeric array.
	CodeMissingVectorField = "pgvector.missing_vector_field"

	// CodeMissingKey is raised when a record has no usable key to derive the
	// upsert conflict target (the stable chunk_id).
	CodeMissingKey = "pgvector.missing_key"

	// CodeWriteFailed is raised when the batch upsert/delete against pgvector
	// fails to execute.
	CodeWriteFailed = "pgvector.write_failed"
)

// CodedError is a machine-actionable connector error. It carries a stable code,
// the failing config path (where known), and a suggested fix — the three things
// CLAUDE.md requires of every user-facing connector error. It renders as a
// single actionable line for logs and marshals to a stable JSON envelope so a
// `--json`-consuming CLI/agent gets structured, coded output rather than an
// opaque string.
type CodedError struct {
	// Code is a stable, matchable error code (see the constants above).
	Code string `json:"code"`
	// Message is a human-readable description of what went wrong.
	Message string `json:"message"`
	// ConfigPath is the connector-config field the error is about, if any
	// (e.g. "dimension", "table"). Empty when the error is not attributable to
	// a single config field.
	ConfigPath string `json:"config_path,omitempty"`
	// Suggestion is a concrete, actionable next step for the operator/agent.
	Suggestion string `json:"suggestion,omitempty"`
	// Err is the underlying cause, if any. Preserved for errors.Is/As; never
	// the primary user-facing message.
	Err error `json:"-"`
}

// NewCodedError constructs a CodedError.
func NewCodedError(code, message string) *CodedError {
	return &CodedError{Code: code, Message: message}
}

// WithConfigPath sets the failing config field and returns the error for
// chaining.
func (e *CodedError) WithConfigPath(path string) *CodedError {
	e.ConfigPath = path
	return e
}

// WithSuggestion sets the actionable fix and returns the error for chaining.
func (e *CodedError) WithSuggestion(suggestion string) *CodedError {
	e.Suggestion = suggestion
	return e
}

// Wrapping sets the underlying cause and returns the error for chaining.
func (e *CodedError) Wrapping(err error) *CodedError {
	e.Err = err
	return e
}

// Error renders the error as a single, stable, actionable line:
//
//	<code>: <message> (config path: "<path>"); fix: <suggestion>: <cause>
func (e *CodedError) Error() string {
	var b strings.Builder
	b.WriteString(e.Code)
	b.WriteString(": ")
	b.WriteString(e.Message)
	if e.ConfigPath != "" {
		fmt.Fprintf(&b, " (config path: %q)", e.ConfigPath)
	}
	if e.Suggestion != "" {
		b.WriteString("; fix: ")
		b.WriteString(e.Suggestion)
	}
	if e.Err != nil {
		b.WriteString(": ")
		b.WriteString(e.Err.Error())
	}
	return b.String()
}

// Unwrap exposes the underlying cause for errors.Is / errors.As.
func (e *CodedError) Unwrap() error {
	return e.Err
}

// JSON returns the stable structured envelope for this error. NOTE: this
// connector is served as a gRPC plugin and has no CLI surface of its own, so
// nothing in this repo renders errors as JSON today — errors reach the operator
// as the Error() string via Conduit's own error reporting. JSON() is provided
// for a future host/CLI that chooses to render coded connector errors
// structurally; it is deliberately not claimed as a live `--json` surface here.
// The wrapped cause is flattened to its string form so the envelope is always
// serializable.
func (e *CodedError) JSON() ([]byte, error) {
	type envelope struct {
		Code       string `json:"code"`
		Message    string `json:"message"`
		ConfigPath string `json:"config_path,omitempty"`
		Suggestion string `json:"suggestion,omitempty"`
		Cause      string `json:"cause,omitempty"`
	}
	env := envelope{
		Code:       e.Code,
		Message:    e.Message,
		ConfigPath: e.ConfigPath,
		Suggestion: e.Suggestion,
	}
	if e.Err != nil {
		env.Cause = e.Err.Error()
	}
	return json.Marshal(env)
}
