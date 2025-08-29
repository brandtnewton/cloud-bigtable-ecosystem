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
package compliance

import (
	"fmt"
	"math"
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ensures that various int row keys are handled correctly in all CRUD operations
func TestIntRowKeys(t *testing.T) {
	t.Parallel()
	tests := []struct {
		i32 int32
		i64 int64
	}{
		{i32: 0, i64: 0},
		{i32: 1, i64: 1},
		{i32: -1, i64: -1},
		{i32: math.MinInt32, i64: math.MinInt64},
		{i32: math.MaxInt32, i64: math.MaxInt64},
		{i32: math.MaxInt32, i64: math.MinInt64},
		{i32: math.MinInt32, i64: math.MaxInt64},
		// min int64
		{i32: -1, i64: math.MinInt64},
		{i32: -1, i64: math.MinInt64 + 1},
		{i32: -1, i64: math.MinInt64 + 2},
		{i32: -1, i64: math.MinInt64 + 100},
		{i32: -1, i64: math.MinInt64 + 1000},
		{i32: -1, i64: math.MinInt64 + 10000},
		{i32: -1, i64: math.MinInt64 + 100000},

		// max int64
		{i32: -1, i64: math.MaxInt64},
		{i32: -1, i64: math.MaxInt64 - 1},
		{i32: -1, i64: math.MaxInt64 - 2},
		{i32: -1, i64: math.MaxInt64 - 100},
		{i32: -1, i64: math.MaxInt64 - 1000},
		{i32: -1, i64: math.MaxInt64 - 10000},
		{i32: -1, i64: math.MaxInt64 - 100000},

		// min int32
		{i32: math.MinInt32, i64: 1},
		{i32: math.MinInt32 + 1, i64: 1},
		{i32: math.MinInt32 + 2, i64: 1},
		{i32: math.MinInt32 + 100, i64: 1},
		{i32: math.MinInt32 + 1000, i64: 1},
		{i32: math.MinInt32 + 10000, i64: 1},
		{i32: math.MinInt32 + 100000, i64: 1},

		// max int32
		{i32: math.MaxInt32, i64: 1},
		{i32: math.MaxInt32 - 1, i64: 1},
		{i32: math.MaxInt32 - 2, i64: 1},
		{i32: math.MaxInt32 - 100, i64: 1},
		{i32: math.MaxInt32 - 1000, i64: 1},
		{i32: math.MaxInt32 - 10000, i64: 1},
		{i32: math.MaxInt32 - 100000, i64: 1},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("int keys i32 %d and i64 %d", tc.i32, tc.i64), func(t *testing.T) {
			t.Parallel()
			name := uuid.New().String()
			require.NoError(t, session.Query("insert into bigtabledevinstance.multiple_int_keys (user_id, order_num, name) values (?, ?, ?)", tc.i64, tc.i32, name).Exec())

			var gotUserId int64
			var gotOrderNum int32
			var gotName string
			query := session.Query("select user_id, order_num, name from bigtabledevinstance.multiple_int_keys where user_id=? and order_num=?", tc.i64, tc.i32)
			require.NoError(t, query.Scan(&gotUserId, &gotOrderNum, &gotName))
			assert.Equal(t, name, gotName)
			assert.Equal(t, tc.i64, gotUserId)
			assert.Equal(t, tc.i32, gotOrderNum)

			name = name + "2"
			require.NoError(t, session.Query("update bigtabledevinstance.multiple_int_keys set name=? where user_id=? and order_num=?", name, tc.i64, tc.i32).Exec())

			require.NoError(t, query.Scan(&gotUserId, &gotOrderNum, &gotName))
			assert.Equal(t, name, gotName)
			assert.Equal(t, tc.i64, gotUserId)
			assert.Equal(t, tc.i32, gotOrderNum)

			require.NoError(t, session.Query("delete from bigtabledevinstance.multiple_int_keys where user_id=? and order_num=?", tc.i64, tc.i32).Exec())
			err := query.Scan()
			assert.Equal(t, gocql.ErrNotFound, err, "The record should be deleted")
		})
	}
}

func TestLexicographicOrder(t *testing.T) {
	if testTarget == TestTargetCassandra {
		t.Skip()
		return
	}
	require.NoError(t, session.Query("CREATE TABLE IF NOT EXISTS lex_test_ordered_code (org BIGINT, id INT, username TEXT, row_index INT, PRIMARY KEY (org, id, username))").Exec())
	require.NoError(t, session.Query("TRUNCATE TABLE lex_test_ordered_code").Exec())

	orderedValues := []map[string]interface{}{
		{"org": math.MinInt64, "id": math.MinInt32, "username": ""},
		{"org": math.MinInt64, "id": math.MinInt32 + 1, "username": ""},
		{"org": math.MinInt64, "id": math.MinInt32 + 1, "username": "a"},
		{"org": math.MinInt64, "id": math.MinInt32 + 1, "username": "b"},
		{"org": math.MinInt64 + 1, "id": math.MinInt32, "username": ""},
		{"org": -1000, "id": math.MinInt32, "username": ""},
		{"org": -1, "id": math.MinInt32, "username": ""},
		{"org": 0, "id": math.MinInt32, "username": ""},
		{"org": 1, "id": math.MinInt32, "username": ""},
		{"org": 1000, "id": math.MinInt32, "username": ""},
		{"org": 99999, "id": math.MinInt32, "username": ""},
		{"org": math.MaxInt64 - 1, "id": math.MinInt32, "username": ""},
		{"org": math.MaxInt64, "id": math.MinInt32, "username": ""},
		{"org": math.MaxInt64, "id": math.MinInt32 + 1, "username": ""},
		{"org": math.MaxInt64, "id": -1000, "username": ""},
		{"org": math.MaxInt64, "id": -1, "username": ""},
		{"org": math.MaxInt64, "id": 0, "username": ""},
		{"org": math.MaxInt64, "id": 0, "username": "D"},
		{"org": math.MaxInt64, "id": 0, "username": "a"},
		{"org": math.MaxInt64, "id": 0, "username": "b"},
		{"org": math.MaxInt64, "id": 1, "username": ""},
		{"org": math.MaxInt64, "id": 1000, "username": ""},
		{"org": math.MaxInt64, "id": 99999, "username": ""},
		{"org": math.MaxInt64, "id": math.MaxInt32 - 1, "username": ""},
		{"org": math.MaxInt64, "id": math.MaxInt32, "username": ""},
		{"org": math.MaxInt64, "id": math.MaxInt32, "username": "10a"},
		{"org": math.MaxInt64, "id": math.MaxInt32, "username": "A"},
		{"org": math.MaxInt64, "id": math.MaxInt32, "username": "Aa"},
		{"org": math.MaxInt64, "id": math.MaxInt32, "username": "Z"},
		{"org": math.MaxInt64, "id": math.MaxInt32, "username": "a"},
		{"org": math.MaxInt64, "id": math.MaxInt32, "username": "b"},
		{"org": math.MaxInt64, "id": math.MaxInt32, "username": "c"},
		{"org": math.MaxInt64, "id": math.MaxInt32, "username": "d"},
		{"org": math.MaxInt64, "id": math.MaxInt32, "username": "defghi"},
		{"org": math.MaxInt64, "id": math.MaxInt32, "username": "dz"},
		{"org": math.MaxInt64, "id": math.MaxInt32, "username": "y"},
		{"org": math.MaxInt64, "id": math.MaxInt32, "username": "z"},
	}

	testLexOrder(t, orderedValues, "lex_test_ordered_code")
}
