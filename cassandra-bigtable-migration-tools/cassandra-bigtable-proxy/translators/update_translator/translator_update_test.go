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

package update_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/parser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/testing/mockdata"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Want struct {
	Keyspace      types.Keyspace
	Table         types.TableName
	IfExists      bool
	Values        []types.Assignment
	Clauses       []types.Condition
	AllParams     []types.Placeholder
	InitialValues map[types.Placeholder]types.GoValue
}

func TestTranslator_TranslateUpdateQuerytoBigtable(t *testing.T) {
	tests := []struct {
		name            string
		sessionKeyspace types.Keyspace
		query           string
		want            *Want
		wantErr         string
	}{
		{
			name:  "update blob column",
			query: "UPDATE test_keyspace.test_table SET col_blob = '0x0000000000000003' WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": "0x0000000000000003",
					"@value1": "testText",
					"@value2": "pk2",
				},
				IfExists: false,
				Values: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_blob"), "@value0"),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:  "update boolean column",
			query: "UPDATE test_keyspace.test_table SET col_bool = true WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				IfExists:  false,
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": true,
					"@value1": "testText",
					"@value2": "pk2",
				},
				Values: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bool"), "@value0"),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:  "update timestamp column",
			query: "UPDATE test_keyspace.test_table SET col_ts = '2024-08-12T12:34:56Z' WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": time.Date(2024, 8, 12, 12, 34, 56, 0, time.UTC),
					"@value1": "testText",
					"@value2": "pk2",
				},
				Values: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_ts"), "@value0"),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:  "update int column",
			query: "UPDATE test_keyspace.test_table SET col_int = 123 WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": int32(123),
					"@value1": "testText",
					"@value2": "pk2",
				},
				Values: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_int"), "@value0"),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:  "update set<varchar> column",
			query: "UPDATE test_keyspace.test_table SET set_text = {'item1', 'item2'} WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": []types.GoValue{"item1", "item2"},
					"@value1": "testText",
					"@value2": "pk2",
				},
				Values: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "set_text"), "@value0"),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:  "update map<varchar,boolean> column",
			query: "UPDATE test_keyspace.test_table SET map_text_bool = {'key1': true, 'key2': false} WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": map[types.GoValue]types.GoValue{"key1": true, "key2": false},
					"@value1": "testText",
					"@value2": "pk2",
				},
				Values: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "map_text_bool"), "@value0"),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:  "update bigint column",
			query: "UPDATE test_keyspace.test_table SET col_bigint = 1234567890 WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": int64(1234567890),
					"@value1": "testText",
					"@value2": "pk2",
				},
				Values: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bigint"), "@value0"),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:  "with keyspace in query, without default keyspace",
			query: "UPDATE test_keyspace.test_table SET col_blob = 'abc' WHERE pk2 = 'pkval' AND pk1 = 'abc';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": "abc",
					"@value1": "pkval",
					"@value2": "abc",
				},
				Values: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_blob"), "@value0"),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:  "append to set column with + operator",
			query: "UPDATE test_keyspace.test_table SET set_text = set_text + {'item3'} WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": []types.GoValue{"item3"},
					"@value1": "testText",
					"@value2": "pk2",
				},
				Values: []types.Assignment{
					types.NewComplexAssignmentAppend(mockdata.GetColumnOrDie("test_keyspace", "test_table", "set_text"), types.PLUS, "@value0", false),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:  "subtract from set column with - operator",
			query: "UPDATE test_keyspace.test_table SET set_text = set_text - {'item2'} WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": []types.GoValue{"item2"},
					"@value1": "testText",
					"@value2": "pk2",
				},
				Values: []types.Assignment{
					types.NewComplexAssignmentAppend(mockdata.GetColumnOrDie("test_keyspace", "test_table", "set_text"), types.MINUS, "@value0", false),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:  "counter operation",
			query: "UPDATE test_keyspace.test_table SET col_counter = col_counter + 1 WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": int64(1),
					"@value1": "testText",
					"@value2": "pk2",
				},
				Values: []types.Assignment{
					types.NewAssignmentCounterIncrement(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_counter"), types.PLUS, "@value0"),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:    "counter operation with invalid operator",
			query:   "UPDATE test_keyspace.test_table SET col_counter = col_counter * 1 WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			wantErr: "parsing error",
		},
		{
			name:  "counter operation decrement",
			query: "UPDATE test_keyspace.test_table SET col_counter = col_counter - 9 WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				Values: []types.Assignment{
					types.NewAssignmentCounterIncrement(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_counter"), types.MINUS, "@value0"),
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": int64(9),
					"@value1": "testText",
					"@value2": "pk2",
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:  "counter operation increment a negative value",
			query: "UPDATE test_keyspace.test_table SET col_counter = col_counter + -9 WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": int64(-9),
					"@value1": "testText",
					"@value2": "pk2",
				},
				Values: []types.Assignment{
					types.NewAssignmentCounterIncrement(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_counter"), types.PLUS, "@value0"),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:  "with keyspace in query, with default keyspace",
			query: "UPDATE test_keyspace.test_table SET col_blob = 'abc' WHERE pk2 = 'pkval' AND pk1 = 'abc';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": "abc",
					"@value1": "pkval",
					"@value2": "abc",
				},
				Values: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_blob"), "@value0"),
				},
				Clauses: []types.Condition{

					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:    "assign list to set",
			query:   "UPDATE test_keyspace.test_table SET set_text = ['item1'] WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			wantErr: "cannot parse list value for non-list type: set<text>",
		},
		{
			name:  "update with list assignment",
			query: "UPDATE test_keyspace.test_table SET set_text = {'item1', 'item2'} WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": []types.GoValue{"item1", "item2"},
					"@value1": "testText",
					"@value2": "pk2",
				},
				Values: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "set_text"), "@value0"),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:  "update with map assignment",
			query: "UPDATE test_keyspace.test_table SET map_text_bool = {'key1': true, 'key2': false} WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": map[types.GoValue]types.GoValue{"key1": true, "key2": false},
					"@value1": "testText",
					"@value2": "pk2",
				},
				Values: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "map_text_bool"), "@value0"),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:  "update with set assignment",
			query: "UPDATE test_keyspace.test_table SET set_text = {'item1', 'item2'} WHERE pk1 = 'testText' AND pk2 = 'pk2';",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": []types.GoValue{"item1", "item2"},
					"@value1": "testText",
					"@value2": "pk2",
				},
				Values: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "set_text"), "@value0"),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:    "attempt to update primary key with collection (should error)",
			query:   "UPDATE test_keyspace.test_table SET pk1 = ['item1'] WHERE pk2 = 'pk2';",
			wantErr: "cannot parse list value for non-list type: varchar",
		},
		{
			name:    "invalid collection syntax (should error)",
			query:   "UPDATE test_keyspace.test_table SET set_text = ['item1', WHERE pk1 = 'testText';",
			wantErr: "parsing error",
		},
		{
			name:    "collection assignment to non-collection column (should error)",
			query:   "UPDATE test_keyspace.test_table SET col_blob = ['item1'] WHERE pk1 = 'testText';",
			wantErr: "cannot parse list value for non-list type: blob",
		},
		{
			name:            "without keyspace in query, with default keyspace",
			query:           "UPDATE test_table SET col_blob = 'abc' WHERE pk2 = 'pkval' AND pk1 = 'abc';",
			sessionKeyspace: "test_keyspace",
			wantErr:         "",
			want: &Want{
				Keyspace:  "test_keyspace",
				Table:     "test_table",
				AllParams: []types.Placeholder{"@value0", "@value1", "@value2"},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0": "abc",
					"@value1": "pkval",
					"@value2": "abc",
				},
				Values: []types.Assignment{
					types.NewComplexAssignmentSet(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_blob"), "@value0"),
				},
				Clauses: []types.Condition{
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"),
						Operator: types.EQ,
						Value:    "@value1",
					},
					{
						Column:   mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator: types.EQ,
						Value:    "@value2",
					},
				},
			},
		},
		{
			name:    "invalid index update missing bracket (should error)",
			query:   "UPDATE test_keyspace.test_table SET column71 = 'newItem' WHERE pk1 = 'testText';",
			wantErr: "unknown column 'column71'",
		},
		{
			name:    "without keyspace in query, without default keyspace (should error)",
			query:   "UPDATE test_table SET col_blob = 'abc' WHERE pk2 = 'pkval' AND pk1 = 'abc';",
			wantErr: "no keyspace specified",
		},
		{
			name:    "invalid query syntax (should error)",
			query:   "UPDATE test_keyspace.test_table",
			wantErr: "parsing error",
		},
		{
			name:    "parser returns empty table (should error)",
			query:   "UPDATE test_keyspace. SET pk1 = 'abc' WHERE pk2 = 'pkval';",
			wantErr: "parsing error",
		},
		{
			name:    "parser returns empty keyspace (should error)",
			query:   "UPDATE .test_table SET pk1 = 'abc' WHERE pk2 = 'pkval';",
			wantErr: "parsing error",
		},
		{
			name:    "parser returns empty set clause (should error)",
			query:   "UPDATE test_keyspace.test_table SET WHERE pk2 = 'pkval';",
			wantErr: "parsing error",
		},
		{
			name:    "keyspace does not exist (should error)",
			query:   "UPDATE invalid_keyspace.test_table SET pk1 = 'abc' WHERE pk2 = 'pkval';",
			wantErr: "keyspace 'invalid_keyspace' does not exist",
		},
		{
			name:    "table does not exist (should error)",
			query:   "UPDATE test_keyspace.invalid_table SET pk1 = 'abc' WHERE pk2 = 'pkval';",
			wantErr: "table 'invalid_table' does not exist",
		},
		{
			name:    "missing primary key in where clause (should error)",
			query:   "UPDATE test_keyspace.test_table SET col_blob = 'abc' WHERE pk1 = 'testText'", // Missing pk2
			wantErr: "missing primary key in where clause: 'pk2'",
			want:    nil,
		},
		{
			name:    "missing all primary keys in where clause (should error)",
			query:   "UPDATE test_keyspace.test_table SET col_blob = 'abc' WHERE column3 = true", // No PK columns
			wantErr: "unknown column 'column3' in table test_keyspace.test_table",
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := NewUpdateTranslator(mockdata.GetSchemaMappingConfig())
			got, err := tr.Translate(types.NewRawQuery(nil, tt.sessionKeyspace, tt.query, parser.NewParser(tt.query), types.QueryTypeUpdate), tt.sessionKeyspace)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			gotUpdate := got.(*types.PreparedUpdateQuery)
			assert.Equal(t, tt.want.Keyspace, gotUpdate.Keyspace())
			assert.Equal(t, tt.want.Table, gotUpdate.Table())
			assert.Equal(t, tt.want.IfExists, gotUpdate.IfExists)
			assert.Equal(t, tt.want.Values, gotUpdate.Values)
			assert.Equal(t, tt.want.Clauses, gotUpdate.Clauses)
			assert.Equal(t, tt.want.AllParams, gotUpdate.Parameters().AllKeys())
			assert.Equal(t, tt.want.InitialValues, gotUpdate.InitialValues())
		})
	}
}
