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

package insert_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/parser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/testing/mockdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

type Want struct {
	Keyspace      types.Keyspace
	Table         types.TableName
	IfNotExists   bool
	Assignments   []types.Assignment
	InitialValues map[types.Placeholder]types.GoValue
	AllParams     []types.Placeholder
}

func TestSelectTranslator_Translate(t *testing.T) {
	tests := []struct {
		name            string
		query           string
		sessionKeyspace types.Keyspace
		want            *Want
		wantErr         string
	}{
		{
			name:  "success with prepared query",
			query: "INSERT INTO test_keyspace.test_table (pk1, pk2) VALUES (?, ?)",
			want: &Want{
				Keyspace:    "test_keyspace",
				Table:       "test_table",
				IfNotExists: false,
				Assignments: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"), "@value0"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"), "@value1"),
				},
				InitialValues: make(map[types.Placeholder]types.GoValue, 0),
				AllParams: []types.Placeholder{
					"@value0",
					"@value1",
				},
			},
		},
		{
			name:  "success with prepared query and USING TIMESTAMP",
			query: "INSERT INTO test_keyspace.test_table (pk2, col_blob, col_bool, list_text, col_int, col_bigint, pk1) VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
			want: &Want{
				Keyspace:    "test_keyspace",
				Table:       "test_table",
				IfNotExists: false,
				Assignments: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"), "@value0"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_blob"), "@value1"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bool"), "@value2"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "list_text"), "@value3"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_int"), "@value4"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bigint"), "@value5"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"), "@value6"),
				},
				InitialValues: make(map[types.Placeholder]types.GoValue, 0),
				AllParams: []types.Placeholder{
					"@value0",
					"@value1",
					"@value2",
					"@value3",
					"@value4",
					"@value5",
					"@value6",
					types.UsingTimePlaceholder,
				},
			},
		},
		{
			name:  "success with adhoc query and USING TIMESTAMP",
			query: "INSERT INTO test_keyspace.test_table (pk2, col_blob, col_bool, list_text, col_int, col_bigint, pk1) VALUES ('u123', '0x0000003', true, ['item1', 'item2', 'item1'], 3, 8242842848, 'org1') USING TIMESTAMP 234242424",
			want: &Want{
				Keyspace:    "test_keyspace",
				Table:       "test_table",
				IfNotExists: false,
				Assignments: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"), "@value0"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_blob"), "@value1"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bool"), "@value2"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "list_text"), "@value3"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_int"), "@value4"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bigint"), "@value5"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"), "@value6"),
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0":                  "u123",
					"@value1":                  "0x0000003",
					"@value2":                  true,
					"@value3":                  []types.GoValue{"item1", "item2", "item1"},
					"@value4":                  int32(3),
					"@value5":                  int64(8242842848),
					"@value6":                  "org1",
					types.UsingTimePlaceholder: int64(234242424),
				},
				AllParams: []types.Placeholder{
					"@value0",
					"@value1",
					"@value2",
					"@value3",
					"@value4",
					"@value5",
					"@value6",
					types.UsingTimePlaceholder,
				},
			},
		},
		{
			name:  "session keyspace",
			query: "INSERT INTO test_table (pk1, pk2) VALUES (?, ?)",
			want: &Want{
				Keyspace:    "test_keyspace",
				Table:       "test_table",
				IfNotExists: false,
				Assignments: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"), "@value0"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"), "@value1"),
				},
				InitialValues: make(map[types.Placeholder]types.GoValue, 0),
				AllParams: []types.Placeholder{
					"@value0",
					"@value1",
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "insert a map with special characters and key words",
			query: "INSERT INTO test_keyspace.test_table (pk1, pk2, map_text_text) VALUES ('abc', 'pkval', {'foo': 'bar', 'key:': ':value', 'k}': '{v:k}'})",
			want: &Want{
				Table:    "test_table",
				Keyspace: "test_keyspace",
				Assignments: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"), "@value0"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"), "@value1"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "map_text_text"), "@value2"),
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": "abc",
					"@value1": "pkval",
					"@value2": map[types.GoValue]types.GoValue{"foo": "bar", "key:": ":value", "k}": "{v:k}"},
				},
				AllParams: []types.Placeholder{
					"@value0",
					"@value1",
					"@value2",
				},
			},
		},
		{
			name:  "escaped single quotes",
			query: "INSERT INTO test_keyspace.test_table (pk1, pk2, col_int) VALUES ('abc', 'pkva''l', 3)",
			want: &Want{
				Table:    "test_table",
				Keyspace: "test_keyspace",
				Assignments: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"), "@value0"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"), "@value1"),
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_int"), "@value2"),
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": "abc",
					"@value1": "pkva'l",
					"@value2": int32(3),
				},
				AllParams: []types.Placeholder{
					"@value0",
					"@value1",
					"@value2",
				},
			},
		},
		{
			name:    "no keyspace",
			query:   "INSERT INTO test_table (pk1, pk2, col_int) VALUES ('abc', 'pkva''l', 3)",
			want:    nil,
			wantErr: "no keyspace specified",
		},
		{
			name:    "no such table",
			query:   "INSERT INTO test_keyspace.no_such_table (pk1, pk2, col_int) VALUES ('abc', 'pkva''l', 3)",
			want:    nil,
			wantErr: "table 'no_such_table' does not exist",
		},
		{
			name:            "no such keyspace",
			query:           "INSERT INTO no_such_keyspace.test_table (pk1, pk2, col_int) VALUES ('abc', 'pkva''l', 3)",
			want:            nil,
			wantErr:         "keyspace 'no_such_keyspace' does not exist",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:    "invalid query syntax (should error)",
			query:   "INSERT INTO test_keyspace.test_table",
			want:    nil,
			wantErr: "parsing error",
		},
		{
			name:    "invalid query syntax (should error)",
			query:   "INSERT INTO test_keyspace.",
			want:    nil,
			wantErr: "parsing error",
		},

		{
			name:    "invalid query syntax (should error)",
			query:   "INSERT INTO .test_table",
			want:    nil,
			wantErr: "parsing error",
		},

		{
			name:    "invalid query syntax (should error)",
			query:   "INSERT INTO test_keyspace.test_table () VALUES ()",
			want:    nil,
			wantErr: "parsing error",
		},
		{
			name:    "invalid query syntax (should error)",
			query:   "INSERT INTO test_keyspace.test_table (col_ts, col_int, pk1) VALUES (?, ?,?)",
			want:    nil,
			wantErr: "missing primary key: 'pk2'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := NewInsertTranslator(mockdata.GetSchemaMappingConfig())
			got, err := tr.Translate(types.NewRawQuery(nil, tt.sessionKeyspace, tt.query, parser.NewParser(tt.query), types.QueryTypeDelete), tt.sessionKeyspace)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			gotInsert := got.(*types.PreparedInsertQuery)
			assert.Equal(t, tt.want.Keyspace, gotInsert.Keyspace())
			assert.Equal(t, tt.want.Table, gotInsert.Table())
			assert.Equal(t, tt.want.IfNotExists, gotInsert.IfNotExists)
			assert.Equal(t, tt.want.Assignments, gotInsert.Assignments)
			assert.Equal(t, tt.want.InitialValues, gotInsert.InitialValues())
			assert.Equal(t, tt.want.AllParams, gotInsert.Parameters().AllKeys())
		})
	}
}
