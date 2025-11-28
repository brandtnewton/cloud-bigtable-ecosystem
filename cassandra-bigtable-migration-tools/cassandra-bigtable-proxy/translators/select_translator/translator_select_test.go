/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package select_translator

import (
	"cloud.google.com/go/bigtable"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/parser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/testing/mockdata"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/stretchr/testify/assert"
)

type Want struct {
	Keyspace        types.Keyspace
	Table           types.TableName
	TranslatedQuery string
	SelectClause    *types.SelectClause
	Conditions      []types.Condition
	InitialValues   map[types.Placeholder]types.GoValue
	CachedBTPrepare *bigtable.PreparedStatement
	OrderBy         types.OrderBy
	GroupByColumns  []string
	AllParams       []types.Placeholder
	ParamKeys       []types.Placeholder
}

var (
	inputPreparedQuery = "select pk1, col_int, col_bool from test_keyspace.test_table where pk1 = ? AND col_int=? AND col_bool=? AND col_ts=? AND col_int=? AND col_bigint=?;"
	timeStamp, _       = time.Parse("2006-01-02 15:04:05.999", "2015-05-03 13:30:54.234")
)

func TestTranslator_TranslateSelectQuerytoBigtable(t *testing.T) {

	tests := []struct {
		name            string
		query           string
		want            *Want
		wantErr         string
		sessionKeyspace types.Keyspace
	}{
		{
			name: "Select query with list contains key clause",
			query: `select pk1, col_int, col_bool from  test_keyspace.test_table
 where pk1 = 'test' AND col_bool='true'
 AND col_ts <= '2015-05-03 13:30:54.234' AND col_int >= '123'
 AND col_bigint > '-10000000' LIMIT 20000;`,
			want: &Want{
				TranslatedQuery: "SELECT pk1, TO_INT64(cf1['col_int']), TO_INT64(cf1['col_bool']) FROM test_table WHERE pk1 = @value0 AND TO_INT64(cf1['col_bool']) = @value1 AND TO_TIME(cf1['col_ts']) <= @value2 AND TO_INT64(cf1['col_int']) >= @value3 AND TO_INT64(cf1['col_bigint']) > @value4 LIMIT @limitValue;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				SelectClause: &types.SelectClause{
					Columns: []types.SelectedColumn{
						*types.NewSelectedColumn("pk1", "pk1", "", types.TypeVarchar),
						*types.NewSelectedColumn("col_int", "col_int", "", types.TypeInt),
						*types.NewSelectedColumn("col_bool", "col_bool", "", types.TypeBoolean),
					},
				},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         types.EQ,
						ValuePlaceholder: "@value0",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bool"),
						Operator:         types.EQ,
						ValuePlaceholder: "@value1",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_ts"),
						Operator:         types.LTE,
						ValuePlaceholder: "@value2",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_int"),
						Operator:         types.GTE,
						ValuePlaceholder: "@value3",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bigint"),
						Operator:         types.GT,
						ValuePlaceholder: "@value4",
					},
				},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value0":              "test",
					"@value1":              true,
					"@value2":              time.Date(2015, 05, 03, 13, 30, 54, 234000000, time.UTC),
					"@value3":              int32(123),
					"@value4":              int64(-10000000),
					types.LimitPlaceholder: int32(20000),
				},
				AllParams: []types.Placeholder{
					"@value0",
					"@value1",
					"@value2",
					"@value3",
					"@value4",
					types.LimitPlaceholder,
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "Select query with list contains key clause",
			query: `select col_int as name from test_keyspace.test_table where list_text CONTAINS 'test';`,
			want: &Want{
				TranslatedQuery: "SELECT cf1['col_int'] as name FROM test_table WHERE ARRAY_INCLUDES(MAP_VALUES(`list_text`), @value0);",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "list_text"),
						Operator:         types.ARRAY_INCLUDES,
						ValuePlaceholder: "@value0",
					},
				},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": "test",
				},
				AllParams: []types.Placeholder{
					"@value0",
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "Select query with map contains key clause",
			query: `select col_int as name from test_keyspace.test_table where set_text CONTAINS KEY 'test';`,
			want: &Want{
				TranslatedQuery: "SELECT cf1['col_int'] as name FROM test_table WHERE MAP_CONTAINS_KEY(`set_text`, @value1);",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "set_text"),
						Operator:         "MAP_CONTAINS_KEY",
						ValuePlaceholder: "@value1",
					},
				},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"@value1": "test",
				},
				ParamKeys: []types.Placeholder{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "Select query with set contains key clause",
			query: `select col_int as name from test_keyspace.test_table where set_text CONTAINS 'test';`,
			want: &Want{
				TranslatedQuery: "SELECT cf1['col_int'] as name FROM test_table WHERE MAP_CONTAINS_KEY(`set_text`, @value1);",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "set_text"),
						Operator:         "MAP_CONTAINS_KEY", // We are considering set as map internally
						ValuePlaceholder: "@value1",
					},
				},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": []byte("test"),
				},
				ParamKeys: []types.Placeholder{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "Writetime CqlQuery without as keyword",
			query: `select pk1, WRITETIME(col_int) from test_keyspace.test_table where pk1 = 'test';`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,WRITE_TIMESTAMP(cf1, 'col_int') FROM test_table WHERE pk1 = @value1;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "=",
						ValuePlaceholder: "@value1",
					},
				},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": "test",
				},
				ParamKeys: []types.Placeholder{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with integer values",
			query: `select pk1 from test_keyspace.test_table where col_int IN (1, 2, 3);`,
			want: &Want{
				TranslatedQuery: "SELECT pk1 FROM test_table WHERE TO_INT64(cf1['col_int']) IN UNNEST(@value1);",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_int"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": []int{1, 2, 3},
				},
				ParamKeys: []types.Placeholder{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with bigint values",
			query: `select pk1 from test_keyspace.test_table where col_bigint IN (1234567890, 9876543210);`,
			want: &Want{
				TranslatedQuery: `SELECT pk1 FROM test_table WHERE TO_INT64(cf1['col_bigint']) IN UNNEST(@value1);`,
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bigint"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": []int64{1234567890, 9876543210},
				},
				ParamKeys: []types.Placeholder{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with float values",
			query: `select pk1 from test_keyspace.test_table where col_float IN (1.5, 2.5, 3.5);`,
			want: &Want{
				TranslatedQuery: "SELECT pk1 FROM test_table WHERE TO_FLOAT32(cf1['col_float']) IN UNNEST(@value1);",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_float"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": []float32{1.5, 2.5, 3.5},
				},
				ParamKeys: []types.Placeholder{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with double values",
			query: `select pk1 from test_keyspace.test_table where col_double IN (3.1415926535, 2.7182818284);`,
			want: &Want{
				TranslatedQuery: "SELECT pk1 FROM test_table WHERE TO_FLOAT64(cf1['col_double']) IN UNNEST(@value1);",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_double"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": []float64{3.1415926535, 2.7182818284},
				},
				ParamKeys: []types.Placeholder{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with boolean values",
			query: `select pk1 from test_keyspace.test_table where col_bool IN (true, false);`,
			want: &Want{
				TranslatedQuery: "SELECT pk1 FROM test_table WHERE TO_INT64(cf1['col_bool']) IN UNNEST(@value1);",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bool"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": []bool{true, false},
				},
				ParamKeys: []types.Placeholder{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "IN operator with mixed types (should error)",
			query:           `select pk1 from test_keyspace.test_table where int_column IN (1, 'two', 3);`,
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with prepared statement placeholder for Int",
			query: `select pk1 from test_keyspace.test_table where col_int IN (?);`,
			want: &Want{
				TranslatedQuery: "SELECT pk1 FROM test_table WHERE TO_INT64(cf1['col_int']) IN UNNEST(@value1);",
				Keyspace:        "test_keyspace",
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": []int{},
				},
				ParamKeys: []types.Placeholder{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_int"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with prepared statement placeholder for Bigint",
			query: `select pk1 from test_keyspace.test_table where col_bigint IN (?);`,
			want: &Want{
				TranslatedQuery: "SELECT pk1 FROM test_table WHERE TO_INT64(cf1['col_bigint']) IN UNNEST(@value1);",
				Keyspace:        "test_keyspace",
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": []int64{},
				},
				ParamKeys: []types.Placeholder{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bigint"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with prepared statement placeholder for Float",
			query: `select pk1 from test_keyspace.test_table where col_float IN (?);`,
			want: &Want{
				TranslatedQuery: "SELECT pk1 FROM test_table WHERE TO_FLOAT32(cf1['col_float']) IN UNNEST(@value1);",
				Keyspace:        "test_keyspace",
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": []float32{},
				},
				ParamKeys: []types.Placeholder{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_float"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with prepared statement placeholder for Double",
			query: `select pk1 from test_keyspace.test_table where col_double IN (?);`,
			want: &Want{
				TranslatedQuery: "SELECT pk1 FROM test_table WHERE TO_FLOAT64(cf1['col_double']) IN UNNEST(@value1);",
				Keyspace:        "test_keyspace",
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": []float64{},
				},
				ParamKeys: []types.Placeholder{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_double"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with prepared statement placeholder for Boolean",
			query: `select pk1 from test_keyspace.test_table where col_bool IN (?);`,
			want: &Want{
				TranslatedQuery: "SELECT pk1 FROM test_table WHERE TO_INT64(cf1['col_bool']) IN UNNEST(@value1);",
				Keyspace:        "test_keyspace",
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": []bool{},
				},
				ParamKeys: []types.Placeholder{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bool"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with prepared statement placeholder for Blob",
			query: `select pk1 from test_keyspace.test_table where col_int IN (?);`,
			want: &Want{
				TranslatedQuery: "SELECT pk1 FROM test_table WHERE TO_BLOB(cf1['col_int']) IN UNNEST(@value1);",
				Keyspace:        "test_keyspace",
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": [][]byte{},
				},
				ParamKeys: []types.Placeholder{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_int"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "IN operator with unsupported CQL type",
			query:           `select pk1 from test_keyspace.test_table where custom_column IN (?);`,
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "Writetime CqlQuery with as keyword",
			query: `select pk1, WRITETIME(col_int) as name from test_keyspace.test_table where pk1 = 'test';`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,WRITE_TIMESTAMP(cf1, 'col_int') as name FROM test_table WHERE pk1 = @value1;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "=",
						ValuePlaceholder: "@value1",
					},
				},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": "test",
				},
				ParamKeys: []types.Placeholder{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "As CqlQuery",
			query: `select col_int as name from test_keyspace.test_table where pk1 = 'test';`,
			want: &Want{
				TranslatedQuery: "SELECT cf1['col_int'] as name FROM test_table WHERE pk1 = @value1;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "=",
						ValuePlaceholder: "@value1",
					},
				},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": "test",
				},
				ParamKeys: []types.Placeholder{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "CqlQuery without Columns",
			query:           `select from key_space.test_table where pk1 = 'test'`,
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "test for raw query when column name not exist in schema mapping table",
			query:           "select pk101, col_int, col_bool from  key_space.test_table where pk1 = 'test' AND col_bool='true' AND col_ts <= '2015-05-03 13:30:54.234' AND col_int >= '123' AND col_bigint > '-10000000' LIMIT 20000;",
			want:            nil,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "CqlQuery without keyspace name",
			query:           `select pk1, col_int from test_table where pk1 = '?' and pk1 in ('?', '?');`,
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "",
		},
		{
			name:            "MALFORMED QUERY",
			query:           "MALFORMED QUERY",
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "test for raw query success",
			query: "select pk1, col_int, col_bool from  test_keyspace.test_table where pk1 = 'test' AND col_bool='true' AND col_ts <= '2015-05-03 13:30:54.234' AND col_int >= '123' AND col_bigint > '-10000000' AND col_bigint < '10' LIMIT 20000;",
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['col_int'],cf1['col_bool'] FROM test_table WHERE pk1 = @value1 AND TO_INT64(cf1['col_bool']) = @value2 AND TO_TIME(cf1['col_ts']) <= @value3 AND TO_INT64(cf1['col_int']) >= @value4 AND TO_INT64(cf1['col_bigint']) > @value5 AND TO_INT64(cf1['col_bigint']) < @value6 LIMIT 20000;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "=",
						ValuePlaceholder: "@value1",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bool"),
						Operator:         "=",
						ValuePlaceholder: "@value2",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_ts"),
						Operator:         "<=",
						ValuePlaceholder: "@value3",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_int"),
						Operator:         ">=",
						ValuePlaceholder: "@value4",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bigint"),
						Operator:         ">",
						ValuePlaceholder: "@value5",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bigint"),
						Operator:         "<",
						ValuePlaceholder: "@value6",
					},
				},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": "test",
					"value2": int64(1),
					"value3": timeStamp,
					"value4": int32(123),
					"value5": int64(-10000000),
					"value6": int64(10),
				},
				ParamKeys: []types.Placeholder{"value1", "value2", "value3", "value4", "value5", "value6"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "test for prepared query success",
			query: inputPreparedQuery,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['col_int'],cf1['col_bool'] FROM test_table WHERE pk1 = @value1 AND TO_BLOB(cf1['col_int']) = @value2 AND TO_INT64(cf1['col_bool']) = @value3 AND TO_TIME(cf1['col_ts']) = @value4 AND TO_INT64(cf1['col_int']) = @value5 AND TO_INT64(cf1['col_bigint']) = @value6;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "=",
						ValuePlaceholder: "@value1",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_int"),
						Operator:         "=",
						ValuePlaceholder: "@value2",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bool"),
						Operator:         "=",
						ValuePlaceholder: "@value3",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_ts"),
						Operator:         "=",
						ValuePlaceholder: "@value4",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_int"),
						Operator:         "=",
						ValuePlaceholder: "@value5",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_bigint"),
						Operator:         "=",
						ValuePlaceholder: "@value6",
					},
				},
				InitialValues: map[types.Placeholder]types.GoValue{"value1": "", "value2": []uint8{}, "value3": false, "value4": time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC), "value5": int32(0), "value6": int64(0)},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				ParamKeys: []types.Placeholder{"value1", "value2", "value3", "value4", "value5", "value6"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "test for query without clause success",
			query: `select pk1, col_int, col_bool from test_keyspace.test_table ORDER BY pk1 LIMIT 20000;`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['col_int'],cf1['col_bool'] FROM test_table ORDER BY pk1 asc LIMIT 20000;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				SelectClause: &types.SelectClause{
					IsStar:  false,
					Columns: []types.SelectedColumn{{Sql: "pk1"}, {Sql: "col_int"}, {Sql: "col_bool"}},
				},
				OrderBy: types.OrderBy{
					IsOrderBy: true,
					Columns: []types.OrderByColumn{
						{
							Column:    "pk1",
							Operation: types.Asc,
						},
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "error at Columns parsing",
			query:           "select  from table;",
			want:            nil,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "error at Condition parsing when column type not found",
			query:           "select * from test_keyspace.table_name where name=test;",
			want:            nil,
			wantErr:         "column with name 'name' not found in table 'table_name'",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "error at Condition parsing when value invalid",
			query:           "select * from test_keyspace.test_table where pk1=",
			want:            nil,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "test with IN operator raw query",
			query: `select pk1, col_int from test_keyspace.test_table where pk1 = 'test' and pk1 in ('abc', 'xyz');`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['col_int'] FROM test_table WHERE pk1 = @value1 AND pk1 IN UNNEST(@value2);",
				Keyspace:        "test_keyspace",
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": "test",
					"value2": []string{"abc", "xyz"},
				},
				ParamKeys: []types.Placeholder{"value1", "value2"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "=",
						ValuePlaceholder: "@value1",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "IN",
						ValuePlaceholder: "@value2",
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "test with IN operator prepared query",
			query: `select pk1, col_int from test_keyspace.test_table where pk1 = '?' and pk1 in ('?', '?');`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['col_int'] FROM test_table WHERE pk1 = @value1 AND pk1 IN UNNEST(@value2);",
				Keyspace:        "test_keyspace",
				InitialValues:   map[types.Placeholder]types.GoValue{"value1": "", "value2": []string{}},
				ParamKeys:       []types.Placeholder{"value1", "value2"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "=",
						ValuePlaceholder: "@value1",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "IN",
						ValuePlaceholder: "@value2",
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "test with LIKE keyword raw query",
			query: `select pk1, col_int from test_keyspace.test_table where pk1 like 'test%';`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['col_int'] FROM test_table WHERE pk1 LIKE @value1;",
				Keyspace:        "test_keyspace",
				InitialValues:   map[types.Placeholder]types.GoValue{"value1": "test%"},
				ParamKeys:       []types.Placeholder{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "LIKE",
						ValuePlaceholder: "@value1",
					},
				},
			},
		},
		{
			name:  "test with BETWEEN operator raw query",
			query: `select pk1, col_int from test_keyspace.test_table where pk1 between 'te''st' and 'test2';`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['col_int'] FROM test_table WHERE pk1 BETWEEN @value1 AND @value2;",
				Keyspace:        "test_keyspace",
				InitialValues:   map[types.Placeholder]types.GoValue{"value1": "te'st", "value2": "test2"},
				ParamKeys:       []types.Placeholder{"value1", "value2"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "BETWEEN",
						ValuePlaceholder: "@value1",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "BETWEEN-AND",
						ValuePlaceholder: "@value2",
					},
				},
			},
		},
		{
			name:    "test with BETWEEN operator raw query with single value",
			query:   `select pk1, col_int from test_keyspace.test_table where pk1 between 'test';`,
			wantErr: "todo",
			want:    nil,
		},
		{
			name:  "test with LIKE keyword prepared query",
			query: `select pk1, col_int from test_keyspace.test_table where pk1 like '?';`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['col_int'] FROM test_table WHERE pk1 LIKE @value1;",
				Keyspace:        "test_keyspace",
				InitialValues:   map[types.Placeholder]types.GoValue{"value1": ""},
				ParamKeys:       []types.Placeholder{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "LIKE",
						ValuePlaceholder: "@value1",
					},
				},
			},
		},
		{
			name:  "test with BETWEEN operator prepared query",
			query: `select pk1, col_int from test_keyspace.test_table where pk1 between ? and ?;`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['col_int'] FROM test_table WHERE pk1 BETWEEN @value1 AND @value2;",
				Keyspace:        "test_keyspace",
				InitialValues:   map[types.Placeholder]types.GoValue{"value1": "", "value2": ""},
				ParamKeys:       []types.Placeholder{"value1", "value2"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "BETWEEN",
						ValuePlaceholder: "@value1",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "BETWEEN-AND",
						ValuePlaceholder: "@value2",
					},
				},
			},
		},
		{
			name:    "test with BETWEEN operator prepared query with single value",
			query:   `select pk1, col_int from test_keyspace.test_table where pk1 between ?;`,
			wantErr: "todo",
			want:    nil,
		},
		{
			name:  "test with LIKE keyword raw query",
			query: `select pk1, col_int from test_keyspace.test_table where pk1 like 'test%';`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['col_int'] FROM test_table WHERE pk1 LIKE @value1;",
				Keyspace:        "test_keyspace",
				InitialValues:   map[types.Placeholder]types.GoValue{"value1": "test%"},
				ParamKeys:       []types.Placeholder{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "LIKE",
						ValuePlaceholder: "@value1",
					},
				},
			},
		},
		{
			name:  "test with BETWEEN operator raw query",
			query: `select pk1, col_int from test_keyspace.test_table where pk1 between 'test' and 'test2';`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['col_int'] FROM test_table WHERE pk1 BETWEEN @value1 AND @value2;",
				Keyspace:        "test_keyspace",
				InitialValues:   map[types.Placeholder]types.GoValue{"value1": "test", "value2": "test2"},
				ParamKeys:       []types.Placeholder{"value1", "value2"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "BETWEEN",
						ValuePlaceholder: "@value1",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"),
						Operator:         "BETWEEN-AND",
						ValuePlaceholder: "@value2",
					},
				},
			},
		},
		{
			name:    "test with BETWEEN operator raw query without any value",
			query:   `select pk1, col_int from test_keyspace.test_table where pk1 between`,
			wantErr: "todo",
			want:    nil,
		},
		{
			name:            "Empty CqlQuery",
			query:           "",
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "CqlQuery Without Select Object",
			query:           "UPDATE table_name SET pk1 = 'new_value1', col_int = 'new_value2' WHERE condition;",
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "query with missing limit value",
			query:           `select pk1, col_int, col_bool from test_keyspace.test_table ORDER BY pk1 LIMIT;`,
			want:            nil,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "query with LIMIT before ORDER BY",
			query:           `select pk1, col_int, col_bool from test_keyspace.test_table LIMIT 100 ORDER BY pk1;`,
			want:            nil,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "query with non-existent column in ORDER BY",
			query:           `select pk1, col_int, col_bool from test_keyspace.test_table ORDER BY pk12343 LIMIT 100;`,
			want:            nil,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "query with negative LIMIT value",
			query:           `select pk1, col_int, col_bool from test_keyspace.test_table ORDER BY pk1 LIMIT -100;`,
			want:            nil,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "query with duplicate negative LIMIT value (potential duplicate test case)",
			query:           `select pk1, col_int, col_bool from test_keyspace.test_table ORDER BY pk1 LIMIT -100;`,
			want:            nil,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "With keyspace in query, with default keyspace (should use query keyspace)",
			query: `select pk1 from test_keyspace.test_table where pk1 = 'abc';`,
			want: &Want{
				Keyspace:        "test_keyspace",
				Table:           "test_table",
				TranslatedQuery: "SELECT pk1 FROM test_table WHERE pk1 = @value1;",
				InitialValues:   map[types.Placeholder]types.GoValue{"value1": "abc"},
				ParamKeys:       []types.Placeholder{"value1"},
				Conditions:      []types.Condition{{Column: mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"), Operator: "=", ValuePlaceholder: "@value1"}},
			},
			sessionKeyspace: "other_keyspace",
		},
		{
			name:  "Without keyspace in query, with default keyspace (should use default)",
			query: `select pk1 from test_table where pk1 = 'abc';`,
			want: &Want{
				Keyspace:        "test_keyspace",
				Table:           "test_table",
				TranslatedQuery: "SELECT pk1 FROM test_table WHERE pk1 = @value1;",
				InitialValues:   map[types.Placeholder]types.GoValue{"value1": "abc"},
				ParamKeys:       []types.Placeholder{"value1"},
				Conditions:      []types.Condition{{Column: mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"), Operator: "=", ValuePlaceholder: "@value1"}},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "Without keyspace in query, without default keyspace (should error)",
			query:           `select pk1 from test_table where pk1 = 'abc';`,
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "",
		},
		{
			name:  "With keyspace in query, with default keyspace (should ignore default)",
			query: `select pk1 from test_keyspace.test_table where pk1 = 'abc';`,
			want: &Want{
				Keyspace:        "test_keyspace",
				Table:           "test_table",
				TranslatedQuery: "SELECT pk1 FROM test_table WHERE pk1 = @value1;",
				InitialValues:   map[types.Placeholder]types.GoValue{"value1": "abc"},
				ParamKeys:       []types.Placeholder{"value1"},
				Conditions:      []types.Condition{{Column: mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"), Operator: "=", ValuePlaceholder: "@value1"}},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "Invalid keyspace/table (not in schema)",
			query:           `select pk1 from invalid_keyspace.invalid_table where pk1 = 'abc';`,
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "Parser returns nil/empty for table or keyspace (simulate parser edge cases)",
			query:           `select from ;`,
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "CqlQuery with only table, no keyspace, and sessionKeyspace is empty (should error)",
			query:           `select pk1 from test_table;`,
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "",
		},
		{
			name:  "CqlQuery with complex GROUP BY and HAVING",
			query: `select pk1, count(col_int) as count_col2 from test_keyspace.test_table GROUP BY pk1 HAVING count(col_int) > 5;`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,count(TO_BLOB(cf1['col_int'])) as count_col2 FROM test_table GROUP BY pk1;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				SelectClause: &types.SelectClause{
					IsStar: false,
					Columns: []types.SelectedColumn{
						{Sql: "pk1"},
						{Sql: "count_col2", Func: types.FuncCodeCount, ColumnName: "col_int", Alias: "count_col2"},
					},
				},
				GroupByColumns: []string{},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "CqlQuery with complex WHERE conditions",
			query:           `select pk1, col_int from test_keyspace.test_table where pk1 = 'test' AND (col_int > 100 OR col_int < 50) AND col_bool IN ('a', 'b', 'c');`,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "CqlQuery with multiple aggregate functions",
			query:           `select pk1, avg(col_int) as avg_col2, max(col_bool) as max_col3, min(column4) as min_col4 from test_keyspace.test_table GROUP BY pk1;`,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "CqlQuery with LIMIT and OFFSET",
			query:           `select pk1, col_int from test_keyspace.test_table LIMIT 10;`,
			wantErr:         "offset not supported",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:    "Invalid ORDER BY with non-grouped column",
			query:   `select pk1, col_int, col_bool from test_keyspace.test_table where pk1 = 'test' AND col_bool='true' AND col_ts = '2015-05-03 13:30:54.234' AND col_int = '123' AND col_bigint = '-10000000' LIMIT 20000 ORDER BY pk12343;`,
			want:    nil,
			wantErr: "todo",
		},
		{
			name:    "Valid GROUP BY with aggregate and ORDER BY",
			query:   `select pk1, col_int, col_bool from test_keyspace.test_table where pk1 = 'test' AND col_bool='true' AND col_ts = '2015-05-03 13:30:54.234' AND col_int = '123' AND col_bigint = '-10000000' LIMIT 20000 ORDER BY pk12343;`,
			want:    nil,
			wantErr: "todo",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := NewSelectTranslator(mockdata.GetSchemaMappingConfig())
			got, err := tr.Translate(types.NewRawQuery(nil, tt.sessionKeyspace, tt.query, parser.NewParser(tt.query), types.QueryTypeSelect), tt.sessionKeyspace)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			gotSelect := got.(*types.PreparedSelectQuery)
			assert.Equal(t, tt.want.Keyspace, gotSelect.Keyspace())
			assert.Equal(t, tt.want.Table, gotSelect.Table())
			assert.Equal(t, tt.want.TranslatedQuery, gotSelect.TranslatedQuery)
			assert.Equal(t, tt.want.SelectClause, gotSelect.SelectClause)
			assert.Equal(t, tt.want.Conditions, gotSelect.Conditions)
			assert.Equal(t, tt.want.InitialValues, gotSelect.InitialValues())
			assert.Equal(t, tt.want.CachedBTPrepare, gotSelect.CachedBTPrepare)
			assert.Equal(t, tt.want.OrderBy, gotSelect.OrderBy)
			assert.Equal(t, tt.want.GroupByColumns, gotSelect.GroupByColumns)
			assert.Equal(t, tt.want.AllParams, gotSelect.Params.AllKeys())
		})
	}
}
