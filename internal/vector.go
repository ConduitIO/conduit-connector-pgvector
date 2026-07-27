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
	"strconv"
	"strings"
)

// ParseVector coerces an embedding value carried on a record into a []float32.
// It accepts the shapes an upstream embedding processor realistically produces:
// []float32, []float64, and []any of JSON numbers (float64 / json.Number),
// which is what a vector round-tripped through JSON/OpenCDC structured data
// looks like. Any other element type is a coded data error (invariant 6: never
// silently coerce or mangle) rather than a best-effort guess.
func ParseVector(v any) ([]float32, error) {
	switch vec := v.(type) {
	case []float32:
		return vec, nil
	case []float64:
		out := make([]float32, len(vec))
		for i, f := range vec {
			out[i] = float32(f)
		}
		return out, nil
	case []any:
		out := make([]float32, len(vec))
		for i, e := range vec {
			f, err := toFloat32(e)
			if err != nil {
				return nil, fmt.Errorf("vector element %d: %w", i, err)
			}
			out[i] = f
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported vector type %T, expected a numeric array", v)
	}
}

func toFloat32(e any) (float32, error) {
	switch n := e.(type) {
	case float64:
		return float32(n), nil
	case float32:
		return n, nil
	case int:
		return float32(n), nil
	case int64:
		return float32(n), nil
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return 0, fmt.Errorf("invalid numeric %q: %w", n.String(), err)
		}
		return float32(f), nil
	default:
		return 0, fmt.Errorf("unsupported element type %T", e)
	}
}

// ParseVectorText parses pgvector's text output format (e.g. "[0.1,0.2,0.3]",
// as returned by casting a vector column to ::text) back into a []float32.
// It is FormatVector's inverse, used by tests that read a written row back
// from Postgres to verify upsert/round-trip correctness.
func ParseVectorText(s string) ([]float32, error) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {
		return nil, fmt.Errorf("invalid pgvector text literal %q: expected \"[...]\"", s)
	}
	inner := s[1 : len(s)-1]
	if inner == "" {
		return []float32{}, nil
	}
	parts := strings.Split(inner, ",")
	out := make([]float32, len(parts))
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			return nil, fmt.Errorf("invalid vector element %d (%q): %w", i, p, err)
		}
		out[i] = float32(f)
	}
	return out, nil
}

// FormatVector renders a []float32 as a pgvector text literal, e.g.
// "[0.1,0.2,0.3]". This is the wire form pgvector's input function accepts; the
// connector binds it as a text parameter and casts to `vector` in SQL, avoiding
// a hard dependency on a pgvector-specific pgx type registration in this first
// slice (a pgx type-map registration is a candidate later refinement).
func FormatVector(vec []float32) string {
	var b strings.Builder
	b.Grow(len(vec)*8 + 2)
	b.WriteByte('[')
	for i, f := range vec {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(f), 'g', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}
