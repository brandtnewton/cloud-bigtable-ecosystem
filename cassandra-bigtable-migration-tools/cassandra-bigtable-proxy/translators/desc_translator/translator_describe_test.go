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

package desc_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/parser"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/testing/mockdata"
	"github.com/stretchr/testify/assert"
)

func TestTranslator_TranslateDescribeQuery(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		want            types.IDescribeQueryVariant
		wantErr         string
		defaultKeyspace types.Keyspace
	}{
		{
			name:            "keyspaces",
			query:           "DESCRIBE KEYSPACES",
			want:            types.NewDescribeKeyspacesQuery(),
			wantErr:         "",
			defaultKeyspace: "test_keyspace",
		},
		{
			name:            "keyspaces",
			query:           "DESC KEYSPACES",
			want:            types.NewDescribeKeyspacesQuery(),
			wantErr:         "",
			defaultKeyspace: "test_keyspace",
		},
		{
			name:            "keyspace",
			query:           "DESCRIBE KEYSPACE test_keyspace",
			want:            types.NewDescribeKeyspaceQuery("test_keyspace"),
			wantErr:         "",
			defaultKeyspace: "test_keyspace",
		},
		{
			name:            "keyspace",
			query:           "DESC KEYSPACE test_keyspace",
			want:            types.NewDescribeKeyspaceQuery("test_keyspace"),
			wantErr:         "",
			defaultKeyspace: "test_keyspace",
		},
		{
			name:            "keyspace",
			query:           "DESC KEYSPACE",
			want:            nil,
			wantErr:         "parsing error",
			defaultKeyspace: "test_keyspace",
		},
		{
			name:            "tables",
			query:           "DESC TABLES",
			want:            types.NewDescribeTablesQuery(),
			wantErr:         "",
			defaultKeyspace: "test_keyspace",
		},
		{
			name:            "tables",
			query:           "DESCRIBE TABLES",
			want:            types.NewDescribeTablesQuery(),
			wantErr:         "",
			defaultKeyspace: "test_keyspace",
		},
		{
			name:            "table",
			query:           "DESCRIBE TABLE test_keyspace.user_info",
			want:            types.NewDescribeTableQuery("test_keyspace", "user_info"),
			wantErr:         "",
			defaultKeyspace: "test_keyspace",
		},
		{
			name:            "table",
			query:           "DESCRIBE TABLE test_keyspace.",
			want:            nil,
			wantErr:         "parsing error",
			defaultKeyspace: "test_keyspace",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := NewDescTranslator(mockdata.GetSchemaMappingConfig())
			got, err := tr.Translate(types.NewRawQuery(nil, tt.defaultKeyspace, tt.query, parser.NewParser(tt.query), types.QueryTypeDelete), tt.defaultKeyspace)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, tt.want)
			gotDesc := got.(*types.DescribeQuery)
			assert.Equal(t, tt.want, gotDesc.Desc())
		})
	}
}
