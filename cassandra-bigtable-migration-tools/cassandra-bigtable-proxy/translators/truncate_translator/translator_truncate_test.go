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

package truncate_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/parser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/testing/mockdata"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTranslateTruncateTableToBigtable(t *testing.T) {
	var tests = []struct {
		name            string
		query           string
		want            *types.TruncateTableStatementMap
		wantErr         string
		defaultKeyspace types.Keyspace
	}{
		{
			name:            "explicit keyspace",
			query:           "TRUNCATE TABLE test_keyspace.test_table",
			want:            types.NewTruncateTableStatementMap("test_keyspace", "test_table", "TRUNCATE TABLE test_keyspace.test_table"),
			defaultKeyspace: "",
		},
		{
			name:            "no explicit keyspace",
			query:           "TRUNCATE TABLE test_table",
			want:            types.NewTruncateTableStatementMap("test_keyspace", "test_table", "TRUNCATE TABLE test_table"),
			defaultKeyspace: "test_keyspace",
		},

		{
			name:            "explicit keyspace AND session keyspace",
			query:           "TRUNCATE TABLE test_keyspace.test_table",
			want:            types.NewTruncateTableStatementMap("test_keyspace", "test_table", "TRUNCATE TABLE test_keyspace.test_table"),
			defaultKeyspace: "other_keyspace",
		},
		{
			name:            "missing keyspace",
			query:           "TRUNCATE TABLE test_table",
			want:            nil,
			wantErr:         "no keyspace specified",
			defaultKeyspace: "",
		},
		{
			name:            "no table anme",
			query:           "TRUNCATE TABLE",
			want:            nil,
			wantErr:         "parsing error",
			defaultKeyspace: "",
		},
		{
			name:            "malformed keyspace",
			query:           "TRUNCATE TABLE .table",
			want:            nil,
			wantErr:         "parsing error",
			defaultKeyspace: "",
		},
		{
			name:            "unknown table",
			query:           "TRUNCATE TABLE test_keyspace.unknown_table",
			want:            nil,
			wantErr:         "table 'unknown_table' does not exist",
			defaultKeyspace: "",
		},
		{
			name:            "unknown keyspace",
			query:           "TRUNCATE TABLE unknown_keyspace.test_table",
			want:            nil,
			wantErr:         "keyspace 'unknown_keyspace' does not exist",
			defaultKeyspace: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			tr := NewTruncateTranslator(mockdata.GetSchemaMappingConfig())
			got, err := tr.Translate(types.NewRawQuery(nil, tt.defaultKeyspace, tt.query, parser.NewParser(tt.query), types.QueryTypeDelete), tt.defaultKeyspace)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			gotTruncate := got.(*types.TruncateTableStatementMap)
			assert.Equal(t, tt.want, gotTruncate)
		})
	}
}
