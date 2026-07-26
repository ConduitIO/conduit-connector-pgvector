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

//go:generate conn-sdk-cli specgen

package pgvector

import (
	_ "embed"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

//go:embed connector.yaml
var specs string

var version = "(devel)"

// Connector is the pgvector destination connector. It has no Source; pgvector
// is a write-only vector sink in the Conduit AI pipeline
// (CDC -> chunk -> embed -> pgvector), per design doc
// 20260724-ai-pipeline-components.md.
var Connector = sdk.Connector{
	NewSpecification: sdk.YAMLSpecification(specs, version),
	NewSource:        nil,
	NewDestination:   NewDestination,
}
