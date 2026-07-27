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

// Package pgvector implements the Conduit destination connector for pgvector,
// the Postgres vector-store extension. It is the flagship vector sink for
// Conduit's AI data pipeline (CDC -> chunk -> embed -> pgvector), designed in
// docs/design-documents/20260724-ai-pipeline-components.md in the core repo.
//
// The connector is write-only: it upserts embedding records into a pgvector
// table keyed by each record's stable chunk_id, so at-least-once redelivery
// converges to a single row (Invariant 3). At pipeline start it validates the
// configured embedding dimension against the target table's vector column and
// fails fast with a coded, actionable error on mismatch (Invariant 6). Every
// upsert also writes a source_key column (from the chunking processor's
// ai.chunk.source_key metadata), which deletes match on instead of the
// chunk_id: a source-record delete removes every chunk row ever derived from
// it, not just the chunk_ids the current chunk count happens to produce (see
// design doc §5's orphan-avoidance rule).
//
// Invariants upheld here (see the core repo's data-integrity invariants):
//
//   - Invariant 3 (at-least-once, no silent drop): upsert is keyed on the
//     stable chunk_id via ON CONFLICT DO UPDATE, and a batch executes as one
//     transaction so a partial failure returns n=0 for engine retry/DLQ — never
//     an early ack. A record missing required source_key metadata (when
//     source_key population is enabled) is the same kind of failure: a coded
//     error, never a silent partial write or delete.
//   - Invariant 6 (no silent schema/dimension mangling): dimension mismatches
//     are coded startup errors, and per-record dimension mismatches are coded
//     write errors — never a silent truncate/pad. A delete never silently
//     under-matches: it either resolves its full source_key fan-out or fails.
package pgvector
