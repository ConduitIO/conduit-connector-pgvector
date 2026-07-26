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
	"errors"
	"strings"
	"testing"

	"github.com/conduitio/conduit-connector-pgvector/internal"
	"github.com/matryer/is"
)

func TestCodedError_Error(t *testing.T) {
	is := is.New(t)
	err := internal.NewCodedError(internal.CodeVectorDimensionMismatch, "dimension mismatch").
		WithConfigPath("dimension").
		WithSuggestion("set dimension to 768")
	msg := err.Error()
	is.True(strings.HasPrefix(msg, "ai.vector_dimension_mismatch: "))
	is.True(strings.Contains(msg, `(config path: "dimension")`))
	is.True(strings.Contains(msg, "fix: set dimension to 768"))
}

func TestCodedError_Unwrap(t *testing.T) {
	is := is.New(t)
	cause := errors.New("boom")
	err := internal.NewCodedError(internal.CodeConnectionFailed, "connect failed").Wrapping(cause)
	is.True(errors.Is(err, cause))
	is.True(strings.Contains(err.Error(), "boom"))
}

func TestCodedError_JSON(t *testing.T) {
	is := is.New(t)
	err := internal.NewCodedError(internal.CodeVectorDimensionMismatch, "dimension mismatch").
		WithConfigPath("dimension").
		WithSuggestion("set dimension to 768").
		Wrapping(errors.New("underlying"))

	b, jerr := err.JSON()
	is.NoErr(jerr)

	var env map[string]any
	is.NoErr(json.Unmarshal(b, &env))
	is.Equal(env["code"], "ai.vector_dimension_mismatch")
	is.Equal(env["message"], "dimension mismatch")
	is.Equal(env["config_path"], "dimension")
	is.Equal(env["suggestion"], "set dimension to 768")
	is.Equal(env["cause"], "underlying")
}
