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

package delete_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/parser"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/testing/mockdata"
	"github.com/stretchr/testify/assert"
)

func TestTranslator_TranslateDeleteQuerytoBigtable(t *testing.T) {
	type Want struct {
		Keyspace        types.Keyspace
		Table           types.TableName
		IfExists        bool
		Conditions      []types.Condition
		SelectedColumns []types.SelectedColumn
		InitialValues   map[types.Placeholder]types.GoValue
	}

	tests := []struct {
		name            string
		query           string
		want            *Want
		wantErr         string
		defaultKeyspace types.Keyspace
	}{
		{
			name:            "simple DELETE query without WHERE clause",
			query:           "DELETE FROM test_keyspace.user_info",
			want:            nil,
			wantErr:         "parsing error",
			defaultKeyspace: "test_keyspace",
		},
		{
			name:            "DELETE query with single WHERE clause missing primary key",
			query:           "DELETE FROM test_keyspace.user_info WHERE name='test'",
			wantErr:         "missing primary key in where clause: 'age'",
			defaultKeyspace: "test_keyspace",
		},
		{
			name:  "DELETE query with multiple WHERE clauses",
			query: "DELETE FROM test_keyspace.user_info WHERE name='test' AND age=15",
			want: &Want{
				Keyspace: "test_keyspace",
				Table:    "user_info",
				IfExists: false,
				Conditions: []types.Condition{
					{
						Column:            mockdata.GetColumnOrDie("test_keyspace", "user_info", "name"),
						Operator:          types.EQ,
						ValuePlaceholder:  "@value0",
						ValuePlaceholder2: "",
					},
					{
						Column:            mockdata.GetColumnOrDie("test_keyspace", "user_info", "age"),
						Operator:          types.EQ,
						ValuePlaceholder:  "@value1",
						ValuePlaceholder2: "",
					},
				},
				SelectedColumns: nil,
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": "test",
					"@value1": int64(15),
				},
			},
			defaultKeyspace: "test_keyspace",
		},
		{
			name:  "DELETE with different column order",
			query: "DELETE FROM test_keyspace.user_info WHERE age=15 AND name='test'",
			want: &Want{
				Keyspace: "test_keyspace",
				Table:    "user_info",
				IfExists: false,
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "user_info", "age"),
						Operator:         types.EQ,
						ValuePlaceholder: "@value0",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "user_info", "name"),
						Operator:         types.EQ,
						ValuePlaceholder: "@value1",
					},
				},
				SelectedColumns: nil,
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": int64(15),
					"@value1": "test",
				},
			},
			defaultKeyspace: "test_keyspace",
		},
		{
			name:            "DELETE query with missing keyspace or table",
			query:           "DELETE FROM .user_info WHERE name='test' AND age=15",
			wantErr:         "parsing error",
			defaultKeyspace: "",
		},
		{
			name:    "DELETE query with incorrect keyword positions",
			query:   "DELETE test_keyspace.user_info WHERE name='test' AND age=15",
			wantErr: "parsing error",
		},
		{
			name:  "DELETE query with ifExists condition",
			query: `DELETE FROM test_keyspace.user_info WHERE name='test' AND age=15 IF EXISTS`,
			want: &Want{
				Keyspace: "test_keyspace",
				Table:    "user_info",
				IfExists: true,
				Conditions: []types.Condition{
					{
						Column:            mockdata.GetColumnOrDie("test_keyspace", "user_info", "name"),
						Operator:          types.EQ,
						ValuePlaceholder:  "@value0",
						ValuePlaceholder2: "",
					},
					{
						Column:            mockdata.GetColumnOrDie("test_keyspace", "user_info", "age"),
						Operator:          types.EQ,
						ValuePlaceholder:  "@value1",
						ValuePlaceholder2: "",
					},
				},
				SelectedColumns: nil,
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": "test",
					"@value1": int64(15),
				},
			},
			defaultKeyspace: "test_keyspace",
		},
		{
			name:  "DELETE query with escaped single quotes",
			query: `DELETE FROM test_keyspace.user_info WHERE name='tes''t' AND age=15 IF EXISTS`,
			want: &Want{
				Keyspace: "test_keyspace",
				Table:    "user_info",
				IfExists: true,
				Conditions: []types.Condition{
					{
						Column:            mockdata.GetColumnOrDie("test_keyspace", "user_info", "name"),
						Operator:          types.EQ,
						ValuePlaceholder:  "@value0",
						ValuePlaceholder2: "",
					},
					{
						Column:            mockdata.GetColumnOrDie("test_keyspace", "user_info", "age"),
						Operator:          types.EQ,
						ValuePlaceholder:  "@value1",
						ValuePlaceholder2: "",
					},
				},
				SelectedColumns: nil,
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": "tes't",
					"@value1": int64(15),
				},
			},
			defaultKeyspace: "test_keyspace",
		},
		{
			name:  "DELETE with session keyspace",
			query: `DELETE FROM user_info WHERE name='test' AND age=15`,
			want: &Want{
				Keyspace: "test_keyspace",
				Table:    "user_info",
				IfExists: false,
				Conditions: []types.Condition{
					{
						Column:            mockdata.GetColumnOrDie("test_keyspace", "user_info", "name"),
						Operator:          types.EQ,
						ValuePlaceholder:  "@value0",
						ValuePlaceholder2: "",
					},
					{
						Column:            mockdata.GetColumnOrDie("test_keyspace", "user_info", "age"),
						Operator:          types.EQ,
						ValuePlaceholder:  "@value1",
						ValuePlaceholder2: "",
					},
				},
				SelectedColumns: nil,
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": "test",
					"@value1": int64(15),
				},
			},
			defaultKeyspace: "test_keyspace",
		},
		{
			name:            "non-existent keyspace/table",
			query:           "DELETE FROM non_existent_keyspace.non_existent_table WHERE name='test'",
			wantErr:         "keyspace 'non_existent_keyspace' does not exist",
			defaultKeyspace: "test_keyspace",
		},
		{
			name:            "invalid query syntax (should error)",
			query:           "DELETE FROM test_keyspace.test_table",
			want:            nil,
			wantErr:         "parsing error",
			defaultKeyspace: "test_keyspace",
		},
		{
			name:            "parser returns empty table (should error)",
			query:           "DELETE FROM test_keyspace. WHERE column1 = 'abc';",
			want:            nil,
			wantErr:         "parsing error",
			defaultKeyspace: "test_keyspace",
		},
		{
			name:            "table does not exist (should error)",
			query:           "DELETE FROM test_keyspace.invalid_table WHERE column1 = 'abc';",
			want:            nil,
			wantErr:         "table 'invalid_table' does not exist",
			defaultKeyspace: "test_keyspace",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := NewDeleteTranslator(mockdata.GetSchemaMappingConfig())
			got, err := tr.Translate(types.NewRawQuery(nil, tt.defaultKeyspace, tt.query, parser.NewParser(tt.query), types.QueryTypeDelete), tt.defaultKeyspace)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, tt.want)
			gotDelete := got.(*types.PreparedDeleteQuery)
			assert.Equal(t, tt.want.Keyspace, gotDelete.Keyspace())
			assert.Equal(t, tt.want.Table, gotDelete.Table())
			assert.Equal(t, tt.want.IfExists, gotDelete.IfExists)
			assert.Equal(t, tt.want.Conditions, gotDelete.Conditions)
			assert.Equal(t, tt.want.SelectedColumns, gotDelete.SelectedColumns)
			assert.Equal(t, tt.want.InitialValues, gotDelete.InitialValues())
		})
	}
}
