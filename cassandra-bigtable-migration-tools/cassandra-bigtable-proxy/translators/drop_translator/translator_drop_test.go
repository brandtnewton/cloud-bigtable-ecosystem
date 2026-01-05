/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package drop_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/parser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/testing/mockdata"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTranslateDropTableToBigtable(t *testing.T) {
	type Want struct {
		Keyspace types.Keyspace
		Table    types.TableName
		IfExists bool
	}

	var tests = []struct {
		name            string
		query           string
		want            *Want
		error           string
		defaultKeyspace types.Keyspace
	}{

		{
			name:  "Drop table with explicit keyspace",
			query: "DROP TABLE my_keyspace.my_table",
			want: &Want{
				Table:    "my_table",
				Keyspace: "my_keyspace",
				IfExists: false,
			},
			defaultKeyspace: "",
		},
		{
			name:  "Drop table with IF EXISTS clause",
			query: "DROP TABLE IF EXISTS my_keyspace.my_table",
			want: &Want{
				Table:    "my_table",
				Keyspace: "my_keyspace",
				IfExists: true,
			},
			defaultKeyspace: "",
		},
		{
			name:  "Drop table with explicit keyspace and default keyspace",
			query: "DROP TABLE my_keyspace.my_table;",
			want: &Want{
				Table:    "my_table",
				Keyspace: "my_keyspace",
				IfExists: false,
			},
			defaultKeyspace: "my_keyspace",
		},
		{
			name:  "Drop table with default keyspace",
			query: "DROP TABLE my_table",
			want: &Want{
				Table:    "my_table",
				Keyspace: "my_keyspace",
				IfExists: false,
			},
			defaultKeyspace: "my_keyspace",
		},
		{
			name:            "Drop table without keyspace and no default keyspace",
			query:           "DROP TABLE my_table",
			want:            nil,
			error:           "no keyspace specified",
			defaultKeyspace: "",
		},
		{
			name:  "Drop table without semicolon",
			query: "DROP TABLE my_keyspace.my_table",
			want: &Want{
				Table:    "my_table",
				Keyspace: "my_keyspace",
				IfExists: false,
			},
			defaultKeyspace: "my_keyspace",
		},
		{
			name:            "Drop table with empty table name",
			query:           "DROP TABLE my_keyspace.;",
			want:            nil,
			error:           "parsing error",
			defaultKeyspace: "my_keyspace",
		},
		{
			name:            "Drop table with empty keyspace(Should return error as query syntax is not valid)",
			query:           "DROP TABLE .my_table",
			want:            nil,
			error:           "parsing error",
			defaultKeyspace: "my_keyspace",
		},
		{
			name:  "IF EXISTS without keyspace, with default keyspace",
			query: "DROP TABLE IF EXISTS my_table;",
			want: &Want{
				Table:    "my_table",
				Keyspace: "my_keyspace",
				IfExists: true,
			},
			defaultKeyspace: "my_keyspace",
		},
		{
			name:            "IF EXISTS without keyspace, without default keyspace (should error)",
			query:           "DROP TABLE IF EXISTS my_table",
			want:            nil,
			error:           "no keyspace specified",
			defaultKeyspace: "",
		},
		{
			name:            "completely invalid syntax (should error)",
			query:           "DROP my_keyspace.my_table",
			want:            nil,
			error:           "parsing error",
			defaultKeyspace: "my_keyspace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := NewDropTranslator(mockdata.GetSchemaMappingConfig())
			got, err := tr.Translate(types.NewRawQuery(nil, tt.defaultKeyspace, tt.query, parser.NewParser(tt.query), types.QueryTypeDelete), tt.defaultKeyspace)
			if tt.error != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.error)
				return
			}

			require.NoError(t, err)
			gotDrop := got.(*types.DropTableQuery)
			assert.Equal(t, tt.want.Keyspace, gotDrop.Keyspace())
			assert.Equal(t, tt.want.Table, gotDrop.Table())
			assert.Equal(t, tt.want.IfExists, gotDrop.IfExists)

		})
	}
}
