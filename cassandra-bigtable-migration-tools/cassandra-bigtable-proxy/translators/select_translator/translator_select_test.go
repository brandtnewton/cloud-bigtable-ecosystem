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
	Params          map[string]interface{}
	ParamKeys       []string
	Limit           types.Limit
}

var (
	inputPreparedQuery = "select pk1, column2, column3 from test_keyspace.test_table where column1 = ? AND column2=? AND column3=? AND column5=? AND column6=? AND column9=?;"
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
			query: `select pk1, column2, column3 from  test_keyspace.test_table
 where column1 = 'test' AND column3='true'
 AND column5 <= '2015-05-03 13:30:54.234' AND column6 >= '123'
 AND column9 > '-10000000' LIMIT 20000;`,
			want: &Want{
				TranslatedQuery: "todo",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions:      []types.Condition{},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				AllParams: []types.Placeholder{
					"@value0",
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": "test",
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "Select query with list contains key clause",
			query: `select column2 as name from test_keyspace.test_table where list_text CONTAINS 'test';`,
			want: &Want{
				TranslatedQuery: "SELECT cf1['column2'] as name FROM test_table WHERE ARRAY_INCLUDES(MAP_VALUES(`list_text`), @value0);",
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
				AllParams: []types.Placeholder{
					"@value0",
				},
				InitialValues: map[types.Placeholder]types.GoValue{
					"value1": "test",
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "Select query with map contains key clause",
			query: `select column2 as name from test_keyspace.test_table where column8 CONTAINS KEY 'test';`,
			want: &Want{
				TranslatedQuery: "SELECT cf1['column2'] as name FROM test_table WHERE MAP_CONTAINS_KEY(`column8`, @value1);",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column8"),
						Operator:         "MAP_CONTAINS_KEY",
						ValuePlaceholder: "@value1",
					},
				},
				Limit: types.Limit{
					IsLimit: false,
				},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				Params: map[string]interface{}{
					"value1": []byte("test"),
				},
				ParamKeys: []string{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "Select query with set contains key clause",
			query: `select column2 as name from test_keyspace.test_table where column7 CONTAINS 'test';`,
			want: &Want{
				TranslatedQuery: "SELECT cf1['column2'] as name FROM test_table WHERE MAP_CONTAINS_KEY(`column7`, @value1);",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column7"),
						Operator:         "MAP_CONTAINS_KEY", // We are considering set as map internally
						ValuePlaceholder: "@value1",
					},
				},
				Limit: types.Limit{
					IsLimit: false,
				},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				Params: map[string]interface{}{
					"value1": []byte("test"),
				},
				ParamKeys: []string{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "Writetime CqlQuery without as keyword",
			query: `select pk1, WRITETIME(column2) from test_keyspace.test_table where column1 = 'test';`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,WRITE_TIMESTAMP(cf1, 'column2') FROM test_table WHERE column1 = @value1;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "=",
						ValuePlaceholder: "@value1",
						IsPrimaryKey:     true,
					},
				},
				Limit: types.Limit{
					IsLimit: false,
				},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				Params: map[string]interface{}{
					"value1": "test",
				},
				ParamKeys: []string{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with integer values",
			query: `select column1 from test_keyspace.test_table where column6 IN (1, 2, 3);`,
			want: &Want{
				TranslatedQuery: "SELECT column1 FROM test_table WHERE TO_INT64(cf1['column6']) IN UNNEST(@value1);",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column6"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
				Params: map[string]interface{}{
					"value1": []int{1, 2, 3},
				},
				ParamKeys: []string{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with bigint values",
			query: `select column1 from test_keyspace.test_table where column9 IN (1234567890, 9876543210);`,
			want: &Want{
				TranslatedQuery: `SELECT column1 FROM test_table WHERE TO_INT64(cf1['column9']) IN UNNEST(@value1);`,
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column9"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
				Params: map[string]interface{}{
					"value1": []int64{1234567890, 9876543210},
				},
				ParamKeys: []string{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with float values",
			query: `select column1 from test_keyspace.test_table where float_col IN (1.5, 2.5, 3.5);`,
			want: &Want{
				TranslatedQuery: "SELECT column1 FROM test_table WHERE TO_FLOAT32(cf1['float_col']) IN UNNEST(@value1);",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "float_col"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
				Params: map[string]interface{}{
					"value1": []float32{1.5, 2.5, 3.5},
				},
				ParamKeys: []string{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with double values",
			query: `select column1 from test_keyspace.test_table where double_col IN (3.1415926535, 2.7182818284);`,
			want: &Want{
				TranslatedQuery: "SELECT column1 FROM test_table WHERE TO_FLOAT64(cf1['double_col']) IN UNNEST(@value1);",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "double_col"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
				Params: map[string]interface{}{
					"value1": []float64{3.1415926535, 2.7182818284},
				},
				ParamKeys: []string{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with boolean values",
			query: `select column1 from test_keyspace.test_table where column3 IN (true, false);`,
			want: &Want{
				TranslatedQuery: "SELECT column1 FROM test_table WHERE TO_INT64(cf1['column3']) IN UNNEST(@value1);",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column3"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
				Params: map[string]interface{}{
					"value1": []bool{true, false},
				},
				ParamKeys: []string{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "IN operator with mixed types (should error)",
			query:           `select column1 from test_keyspace.test_table where int_column IN (1, 'two', 3);`,
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with prepared statement placeholder for Int",
			query: `select column1 from test_keyspace.test_table where column6 IN (?);`,
			want: &Want{
				TranslatedQuery: "SELECT column1 FROM test_table WHERE TO_INT64(cf1['column6']) IN UNNEST(@value1);",
				Keyspace:        "test_keyspace",
				Params: map[string]interface{}{
					"value1": []int{},
				},
				ParamKeys: []string{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column6"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with prepared statement placeholder for Bigint",
			query: `select column1 from test_keyspace.test_table where column9 IN (?);`,
			want: &Want{
				TranslatedQuery: "SELECT column1 FROM test_table WHERE TO_INT64(cf1['column9']) IN UNNEST(@value1);",
				Keyspace:        "test_keyspace",
				Params: map[string]interface{}{
					"value1": []int64{},
				},
				ParamKeys: []string{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column9"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with prepared statement placeholder for Float",
			query: `select column1 from test_keyspace.test_table where float_col IN (?);`,
			want: &Want{
				TranslatedQuery: "SELECT column1 FROM test_table WHERE TO_FLOAT32(cf1['float_col']) IN UNNEST(@value1);",
				Keyspace:        "test_keyspace",
				Params: map[string]interface{}{
					"value1": []float32{},
				},
				ParamKeys: []string{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "float_col"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with prepared statement placeholder for Double",
			query: `select column1 from test_keyspace.test_table where double_col IN (?);`,
			want: &Want{
				TranslatedQuery: "SELECT column1 FROM test_table WHERE TO_FLOAT64(cf1['double_col']) IN UNNEST(@value1);",
				Keyspace:        "test_keyspace",
				Params: map[string]interface{}{
					"value1": []float64{},
				},
				ParamKeys: []string{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "double_col"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with prepared statement placeholder for Boolean",
			query: `select column1 from test_keyspace.test_table where column3 IN (?);`,
			want: &Want{
				TranslatedQuery: "SELECT column1 FROM test_table WHERE TO_INT64(cf1['column3']) IN UNNEST(@value1);",
				Keyspace:        "test_keyspace",
				Params: map[string]interface{}{
					"value1": []bool{},
				},
				ParamKeys: []string{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column3"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "IN operator with prepared statement placeholder for Blob",
			query: `select column1 from test_keyspace.test_table where column2 IN (?);`,
			want: &Want{
				TranslatedQuery: "SELECT column1 FROM test_table WHERE TO_BLOB(cf1['column2']) IN UNNEST(@value1);",
				Keyspace:        "test_keyspace",
				Params: map[string]interface{}{
					"value1": [][]byte{},
				},
				ParamKeys: []string{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column2"),
						Operator:         "IN",
						ValuePlaceholder: "@value1",
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "IN operator with unsupported CQL type",
			query:           `select column1 from test_keyspace.test_table where custom_column IN (?);`,
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "Writetime CqlQuery with as keyword",
			query: `select pk1, WRITETIME(column2) as name from test_keyspace.test_table where column1 = 'test';`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,WRITE_TIMESTAMP(cf1, 'column2') as name FROM test_table WHERE column1 = @value1;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "=",
						ValuePlaceholder: "@value1",
						IsPrimaryKey:     true,
					},
				},
				Limit: types.Limit{
					IsLimit: false,
				},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				Params: map[string]interface{}{
					"value1": "test",
				},
				ParamKeys: []string{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "As CqlQuery",
			query: `select column2 as name from test_keyspace.test_table where column1 = 'test';`,
			want: &Want{
				TranslatedQuery: "SELECT cf1['column2'] as name FROM test_table WHERE column1 = @value1;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "=",
						ValuePlaceholder: "@value1",
						IsPrimaryKey:     true,
					},
				},
				Limit: types.Limit{
					IsLimit: false,
				},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				Params: map[string]interface{}{
					"value1": "test",
				},
				ParamKeys: []string{"value1"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "CqlQuery without Columns",
			query:           `select from key_space.test_table where column1 = 'test'`,
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "test for raw query when column name not exist in schema mapping table",
			query:           "select column101, column2, column3 from  key_space.test_table where column1 = 'test' AND column3='true' AND column5 <= '2015-05-03 13:30:54.234' AND column6 >= '123' AND column9 > '-10000000' LIMIT 20000;",
			want:            nil,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "CqlQuery without keyspace name",
			query:           `select pk1, column2 from test_table where column1 = '?' and column1 in ('?', '?');`,
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
			query: "select pk1, column2, column3 from  test_keyspace.test_table where column1 = 'test' AND column3='true' AND column5 <= '2015-05-03 13:30:54.234' AND column6 >= '123' AND column9 > '-10000000' AND column9 < '10' LIMIT 20000;",
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['column2'],cf1['column3'] FROM test_table WHERE column1 = @value1 AND TO_INT64(cf1['column3']) = @value2 AND TO_TIME(cf1['column5']) <= @value3 AND TO_INT64(cf1['column6']) >= @value4 AND TO_INT64(cf1['column9']) > @value5 AND TO_INT64(cf1['column9']) < @value6 LIMIT 20000;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "=",
						ValuePlaceholder: "@value1",
						IsPrimaryKey:     true,
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column3"),
						Operator:         "=",
						ValuePlaceholder: "@value2",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column5"),
						Operator:         "<=",
						ValuePlaceholder: "@value3",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column6"),
						Operator:         ">=",
						ValuePlaceholder: "@value4",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column9"),
						Operator:         ">",
						ValuePlaceholder: "@value5",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column9"),
						Operator:         "<",
						ValuePlaceholder: "@value6",
					},
				},
				Limit: types.Limit{
					IsLimit: true,
					Value:   20000,
				},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				Params: map[string]interface{}{
					"value1": "test",
					"value2": int64(1),
					"value3": timeStamp,
					"value4": int32(123),
					"value5": int64(-10000000),
					"value6": int64(10),
				},
				ParamKeys: []string{"value1", "value2", "value3", "value4", "value5", "value6"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "test for prepared query success",
			query: inputPreparedQuery,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['column2'],cf1['column3'] FROM test_table WHERE column1 = @value1 AND TO_BLOB(cf1['column2']) = @value2 AND TO_INT64(cf1['column3']) = @value3 AND TO_TIME(cf1['column5']) = @value4 AND TO_INT64(cf1['column6']) = @value5 AND TO_INT64(cf1['column9']) = @value6;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "=",
						ValuePlaceholder: "@value1",
						IsPrimaryKey:     true,
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column2"),
						Operator:         "=",
						ValuePlaceholder: "@value2",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column3"),
						Operator:         "=",
						ValuePlaceholder: "@value3",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column5"),
						Operator:         "=",
						ValuePlaceholder: "@value4",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column6"),
						Operator:         "=",
						ValuePlaceholder: "@value5",
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column9"),
						Operator:         "=",
						ValuePlaceholder: "@value6",
					},
				},
				Params: map[string]interface{}{"value1": "", "value2": []uint8{}, "value3": false, "value4": time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC), "value5": int32(0), "value6": int64(0)},
				Limit: types.Limit{
					IsLimit: true,
					Value:   2000,
				},
				OrderBy: types.OrderBy{
					IsOrderBy: false,
				},
				ParamKeys: []string{"value1", "value2", "value3", "value4", "value5", "value6"},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "test for query without clause success",
			query: `select pk1, column2, column3 from test_keyspace.test_table ORDER BY column1 LIMIT 20000;`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['column2'],cf1['column3'] FROM test_table ORDER BY column1 asc LIMIT 20000;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				SelectClause: &types.SelectClause{
					IsStar:  false,
					Columns: []types.SelectedColumn{{Sql: "column1"}, {Sql: "column2"}, {Sql: "column3"}},
				},
				Limit: types.Limit{
					IsLimit: true,
					Value:   20000,
				},
				OrderBy: types.OrderBy{
					IsOrderBy: true,
					Columns: []types.OrderByColumn{
						{
							Column:    "column1",
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
			query:           "select * from test_keyspace.test_table where column1=",
			want:            nil,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "test with IN operator raw query",
			query: `select pk1, column2 from test_keyspace.test_table where column1 = 'test' and column1 in ('abc', 'xyz');`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['column2'] FROM test_table WHERE column1 = @value1 AND column1 IN UNNEST(@value2);",
				Keyspace:        "test_keyspace",
				Params: map[string]interface{}{
					"value1": "test",
					"value2": []string{"abc", "xyz"},
				},
				ParamKeys: []string{"value1", "value2"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "=",
						ValuePlaceholder: "@value1",
						IsPrimaryKey:     true,
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "IN",
						ValuePlaceholder: "@value2",
						IsPrimaryKey:     true,
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "test with IN operator prepared query",
			query: `select pk1, column2 from test_keyspace.test_table where column1 = '?' and column1 in ('?', '?');`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['column2'] FROM test_table WHERE column1 = @value1 AND column1 IN UNNEST(@value2);",
				Keyspace:        "test_keyspace",
				Params:          map[string]interface{}{"value1": "", "value2": []string{}},
				ParamKeys:       []string{"value1", "value2"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "=",
						ValuePlaceholder: "@value1",
						IsPrimaryKey:     true,
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "IN",
						ValuePlaceholder: "@value2",
						IsPrimaryKey:     true,
					},
				},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "test with LIKE keyword raw query",
			query: `select pk1, column2 from test_keyspace.test_table where column1 like 'test%';`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['column2'] FROM test_table WHERE column1 LIKE @value1;",
				Keyspace:        "test_keyspace",
				Params:          map[string]interface{}{"value1": "test%"},
				ParamKeys:       []string{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "LIKE",
						ValuePlaceholder: "@value1",
						IsPrimaryKey:     true,
					},
				},
			},
		},
		{
			name:  "test with BETWEEN operator raw query",
			query: `select pk1, column2 from test_keyspace.test_table where column1 between 'te''st' and 'test2';`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['column2'] FROM test_table WHERE column1 BETWEEN @value1 AND @value2;",
				Keyspace:        "test_keyspace",
				Params:          map[string]interface{}{"value1": "te'st", "value2": "test2"},
				ParamKeys:       []string{"value1", "value2"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "BETWEEN",
						ValuePlaceholder: "@value1",
						IsPrimaryKey:     true,
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "BETWEEN-AND",
						ValuePlaceholder: "@value2",
						IsPrimaryKey:     true,
					},
				},
			},
		},
		{
			name:    "test with BETWEEN operator raw query with single value",
			query:   `select pk1, column2 from test_keyspace.test_table where column1 between 'test';`,
			wantErr: "todo",
			want:    nil,
		},
		{
			name:  "test with LIKE keyword prepared query",
			query: `select pk1, column2 from test_keyspace.test_table where column1 like '?';`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['column2'] FROM test_table WHERE column1 LIKE @value1;",
				Keyspace:        "test_keyspace",
				Params:          map[string]interface{}{"value1": ""},
				ParamKeys:       []string{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "LIKE",
						ValuePlaceholder: "@value1",
						IsPrimaryKey:     true,
					},
				},
			},
		},
		{
			name:  "test with BETWEEN operator prepared query",
			query: `select pk1, column2 from test_keyspace.test_table where column1 between ? and ?;`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['column2'] FROM test_table WHERE column1 BETWEEN @value1 AND @value2;",
				Keyspace:        "test_keyspace",
				Params:          map[string]interface{}{"value1": "", "value2": ""},
				ParamKeys:       []string{"value1", "value2"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "BETWEEN",
						ValuePlaceholder: "@value1",
						IsPrimaryKey:     true,
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "BETWEEN-AND",
						ValuePlaceholder: "@value2",
						IsPrimaryKey:     true,
					},
				},
			},
		},
		{
			name:    "test with BETWEEN operator prepared query with single value",
			query:   `select pk1, column2 from test_keyspace.test_table where column1 between ?;`,
			wantErr: "todo",
			want:    nil,
		},
		{
			name:  "test with LIKE keyword raw query",
			query: `select pk1, column2 from test_keyspace.test_table where column1 like 'test%';`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['column2'] FROM test_table WHERE column1 LIKE @value1;",
				Keyspace:        "test_keyspace",
				Params:          map[string]interface{}{"value1": "test%"},
				ParamKeys:       []string{"value1"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "LIKE",
						ValuePlaceholder: "@value1",
						IsPrimaryKey:     true,
					},
				},
			},
		},
		{
			name:  "test with BETWEEN operator raw query",
			query: `select pk1, column2 from test_keyspace.test_table where column1 between 'test' and 'test2';`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,cf1['column2'] FROM test_table WHERE column1 BETWEEN @value1 AND @value2;",
				Keyspace:        "test_keyspace",
				Params:          map[string]interface{}{"value1": "test", "value2": "test2"},
				ParamKeys:       []string{"value1", "value2"},
				Conditions: []types.Condition{
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "BETWEEN",
						ValuePlaceholder: "@value1",
						IsPrimaryKey:     true,
					},
					{
						Column:           mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"),
						Operator:         "BETWEEN-AND",
						ValuePlaceholder: "@value2",
						IsPrimaryKey:     true,
					},
				},
			},
		},
		{
			name:    "test with BETWEEN operator raw query without any value",
			query:   `select pk1, column2 from test_keyspace.test_table where column1 between`,
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
			query:           "UPDATE table_name SET column1 = 'new_value1', column2 = 'new_value2' WHERE condition;",
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "query with missing limit value",
			query:           `select pk1, column2, column3 from test_keyspace.test_table ORDER BY column1 LIMIT;`,
			want:            nil,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "query with LIMIT before ORDER BY",
			query:           `select pk1, column2, column3 from test_keyspace.test_table LIMIT 100 ORDER BY column1;`,
			want:            nil,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "query with non-existent column in ORDER BY",
			query:           `select pk1, column2, column3 from test_keyspace.test_table ORDER BY column12343 LIMIT 100;`,
			want:            nil,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "query with negative LIMIT value",
			query:           `select pk1, column2, column3 from test_keyspace.test_table ORDER BY column1 LIMIT -100;`,
			want:            nil,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "query with duplicate negative LIMIT value (potential duplicate test case)",
			query:           `select pk1, column2, column3 from test_keyspace.test_table ORDER BY column1 LIMIT -100;`,
			want:            nil,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:  "With keyspace in query, with default keyspace (should use query keyspace)",
			query: `select column1 from test_keyspace.test_table where column1 = 'abc';`,
			want: &Want{
				Keyspace:        "test_keyspace",
				Table:           "test_table",
				TranslatedQuery: "SELECT column1 FROM test_table WHERE column1 = @value1;",
				Params:          map[string]interface{}{"value1": "abc"},
				ParamKeys:       []string{"value1"},
				Conditions:      []types.Condition{{Column: mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"), Operator: "=", ValuePlaceholder: "@value1", IsPrimaryKey: true}},
			},
			sessionKeyspace: "other_keyspace",
		},
		{
			name:  "Without keyspace in query, with default keyspace (should use default)",
			query: `select column1 from test_table where column1 = 'abc';`,
			want: &Want{
				Keyspace:        "test_keyspace",
				Table:           "test_table",
				TranslatedQuery: "SELECT column1 FROM test_table WHERE column1 = @value1;",
				Params:          map[string]interface{}{"value1": "abc"},
				ParamKeys:       []string{"value1"},
				Conditions:      []types.Condition{{Column: mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"), Operator: "=", ValuePlaceholder: "@value1", IsPrimaryKey: true}},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "Without keyspace in query, without default keyspace (should error)",
			query:           `select column1 from test_table where column1 = 'abc';`,
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "",
		},
		{
			name:  "With keyspace in query, with default keyspace (should ignore default)",
			query: `select column1 from test_keyspace.test_table where column1 = 'abc';`,
			want: &Want{
				Keyspace:        "test_keyspace",
				Table:           "test_table",
				TranslatedQuery: "SELECT column1 FROM test_table WHERE column1 = @value1;",
				Params:          map[string]interface{}{"value1": "abc"},
				ParamKeys:       []string{"value1"},
				Conditions:      []types.Condition{{Column: mockdata.GetColumnOrDie("test_keyspace", "test_table", "column1"), Operator: "=", ValuePlaceholder: "@value1", IsPrimaryKey: true}},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "Invalid keyspace/table (not in schema)",
			query:           `select column1 from invalid_keyspace.invalid_table where column1 = 'abc';`,
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
			query:           `select column1 from test_table;`,
			wantErr:         "",
			want:            nil,
			sessionKeyspace: "",
		},
		{
			name:  "CqlQuery with complex GROUP BY and HAVING",
			query: `select pk1, count(column2) as count_col2 from test_keyspace.test_table GROUP BY column1 HAVING count(column2) > 5;`,
			want: &Want{
				TranslatedQuery: "SELECT pk1,count(TO_BLOB(cf1['column2'])) as count_col2 FROM test_table GROUP BY column1;",
				Table:           "test_table",
				Keyspace:        "test_keyspace",
				SelectClause: &types.SelectClause{
					IsStar: false,
					Columns: []types.SelectedColumn{
						{Sql: "column1"},
						{Sql: "count_col2", Func: types.FuncCodeCount, ColumnName: "column2", Alias: "count_col2"},
					},
				},
				GroupByColumns: []string{},
			},
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "CqlQuery with complex WHERE conditions",
			query:           `select pk1, column2 from test_keyspace.test_table where column1 = 'test' AND (column2 > 100 OR column2 < 50) AND column3 IN ('a', 'b', 'c');`,
			wantErr:         "",
			sessionKeyspace: "test_keyspace",
		},
		{
			name:            "CqlQuery with multiple aggregate functions",
			query:           `select pk1, avg(column2) as avg_col2, max(column3) as max_col3, min(column4) as min_col4 from test_keyspace.test_table GROUP BY column1;`,
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
			query:   `select pk1, column2, column3 from test_keyspace.test_table where column1 = 'test' AND column3='true' AND column5 = '2015-05-03 13:30:54.234' AND column6 = '123' AND column9 = '-10000000' LIMIT 20000 ORDER BY column12343;`,
			want:    nil,
			wantErr: "todo",
		},
		{
			name:    "Valid GROUP BY with aggregate and ORDER BY",
			query:   `select pk1, column2, column3 from test_keyspace.test_table where column1 = 'test' AND column3='true' AND column5 = '2015-05-03 13:30:54.234' AND column6 = '123' AND column9 = '-10000000' LIMIT 20000 ORDER BY column12343;`,
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
