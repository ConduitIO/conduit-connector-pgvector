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
	"encoding/json"
	"testing"

	"github.com/conduitio/conduit-connector-pgvector/internal"
	"github.com/matryer/is"
)

func TestParseVector_Float32(t *testing.T) {
	is := is.New(t)
	got, err := internal.ParseVector([]float32{0.1, 0.2, 0.3})
	is.NoErr(err)
	is.Equal(got, []float32{0.1, 0.2, 0.3})
}

func TestParseVector_Float64(t *testing.T) {
	is := is.New(t)
	got, err := internal.ParseVector([]float64{1, 2, 3})
	is.NoErr(err)
	is.Equal(got, []float32{1, 2, 3})
}

func TestParseVector_AnySliceFromJSON(t *testing.T) {
	is := is.New(t)
	// Simulates a vector round-tripped through JSON/OpenCDC structured data.
	var decoded any
	err := json.Unmarshal([]byte(`[0.5, 1.5, 2.5]`), &decoded)
	is.NoErr(err)
	got, err := internal.ParseVector(decoded)
	is.NoErr(err)
	is.Equal(got, []float32{0.5, 1.5, 2.5})
}

func TestParseVector_JSONNumber(t *testing.T) {
	is := is.New(t)
	got, err := internal.ParseVector([]any{json.Number("0.25"), json.Number("0.75")})
	is.NoErr(err)
	is.Equal(got, []float32{0.25, 0.75})
}

func TestParseVector_UnsupportedType(t *testing.T) {
	is := is.New(t)
	_, err := internal.ParseVector("not-a-vector")
	is.True(err != nil)
}

func TestParseVector_UnsupportedElement(t *testing.T) {
	is := is.New(t)
	_, err := internal.ParseVector([]any{0.1, "oops"})
	is.True(err != nil)
}

func TestFormatVector(t *testing.T) {
	is := is.New(t)
	is.Equal(internal.FormatVector([]float32{0.1, 0.2, 0.3}), "[0.1,0.2,0.3]")
	is.Equal(internal.FormatVector([]float32{1, 2, 3}), "[1,2,3]")
	is.Equal(internal.FormatVector(nil), "[]")
}

func TestParseVectorText(t *testing.T) {
	is := is.New(t)
	got, err := internal.ParseVectorText("[0.1,0.2,0.3]")
	is.NoErr(err)
	is.Equal(got, []float32{0.1, 0.2, 0.3})

	got, err = internal.ParseVectorText("[1,2,3]")
	is.NoErr(err)
	is.Equal(got, []float32{1, 2, 3})

	got, err = internal.ParseVectorText("[]")
	is.NoErr(err)
	is.Equal(got, []float32{})
}

func TestParseVectorText_Invalid(t *testing.T) {
	is := is.New(t)
	_, err := internal.ParseVectorText("not-a-vector")
	is.True(err != nil)

	_, err = internal.ParseVectorText("[0.1,oops,0.3]")
	is.True(err != nil)
}

// TestFormatVector_ParseVectorText_RoundTrip proves FormatVector and
// ParseVectorText are exact inverses for values that survive pgvector's
// single-precision text codec unchanged — the property the upsert
// idempotency test (TestDestination_Upsert_Idempotent) and the acceptance
// suite's read-back (acceptanceDriver.ReadFromDestination) both depend on.
func TestFormatVector_ParseVectorText_RoundTrip(t *testing.T) {
	is := is.New(t)
	want := []float32{0.25, 0.5, 0.75, 1, -2.5, 0}
	got, err := internal.ParseVectorText(internal.FormatVector(want))
	is.NoErr(err)
	is.Equal(got, want)
}
