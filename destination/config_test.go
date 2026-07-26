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

package destination_test

import (
	"context"
	"errors"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-pgvector/destination"
	"github.com/conduitio/conduit-connector-pgvector/internal"
	"github.com/matryer/is"
)

func record() opencdc.Record {
	return opencdc.Record{
		Operation: opencdc.OperationCreate,
		Metadata:  opencdc.Metadata{},
	}
}

func validConfig() destination.Config {
	return destination.Config{
		URL:            "postgres://user:pass@127.0.0.1:5432/db?sslmode=disable",
		Table:          "docs",
		Dimension:      768,
		VectorColumn:   "embedding",
		KeyColumn:      "id",
		VectorField:    "vector",
		MetadataColumn: "metadata",
	}
}

func codeOf(t *testing.T, err error) string {
	t.Helper()
	var ce *internal.CodedError
	if !errors.As(err, &ce) {
		t.Fatalf("expected a *internal.CodedError, got %T: %v", err, err)
	}
	return ce.Code
}

func TestConfig_Validate_OK(t *testing.T) {
	is := is.New(t)
	c := validConfig()
	is.NoErr(c.Validate(context.Background()))
}

func TestConfig_Validate_InvalidURL(t *testing.T) {
	is := is.New(t)
	c := validConfig()
	c.URL = "://not a url"
	err := c.Validate(context.Background())
	is.True(err != nil)
	is.Equal(codeOf(t, err), internal.CodeInvalidConfig)
}

func TestConfig_Validate_DimensionMustBePositive(t *testing.T) {
	is := is.New(t)
	c := validConfig()
	c.Dimension = 0
	err := c.Validate(context.Background())
	is.True(err != nil)
	is.Equal(codeOf(t, err), internal.CodeInvalidConfig)

	var ce *internal.CodedError
	is.True(errors.As(err, &ce))
	is.Equal(ce.ConfigPath, "dimension")
}

func TestConfig_Validate_EmptyVectorColumn(t *testing.T) {
	is := is.New(t)
	c := validConfig()
	c.VectorColumn = "  "
	err := c.Validate(context.Background())
	is.True(err != nil)
	is.Equal(codeOf(t, err), internal.CodeInvalidConfig)
}

func TestConfig_Validate_EmptyKeyColumn(t *testing.T) {
	is := is.New(t)
	c := validConfig()
	c.KeyColumn = ""
	err := c.Validate(context.Background())
	is.True(err != nil)
	is.Equal(codeOf(t, err), internal.CodeInvalidConfig)
}

func TestConfig_TableFunction_Static(t *testing.T) {
	is := is.New(t)
	c := validConfig()
	fn, err := c.TableFunction()
	is.NoErr(err)
	got, err := fn(record())
	is.NoErr(err)
	is.Equal(got, "docs")
}

func TestConfig_TableFunction_Template(t *testing.T) {
	is := is.New(t)
	c := validConfig()
	c.Table = `{{ index .Metadata "opencdc.collection" }}`
	fn, err := c.TableFunction()
	is.NoErr(err)
	r := record()
	r.Metadata = map[string]string{"opencdc.collection": "chunks"}
	got, err := fn(r)
	is.NoErr(err)
	is.Equal(got, "chunks")
}
