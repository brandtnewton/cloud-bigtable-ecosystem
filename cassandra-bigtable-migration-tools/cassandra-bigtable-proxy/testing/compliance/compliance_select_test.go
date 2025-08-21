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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnsupportedFunctionInSelectQuery validates that using a function that doesn't exist fails as expected.
func TestUnsupportedFunctionInSelectQuery(t *testing.T) {
	query := `SELECT xxxx(name) FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`
	err := session.Query(query, "Carls", int64(45)).Exec()

	require.Error(t, err, "Expected an error for an unsupported function, but got none")
	assert.Contains(t, err.Error(), "unknown function 'xxxx'", "Error message did not match expected output")
}

// TestSelectAndValidateDataFromTestTable verifies a standard INSERT and SELECT operation.
func TestSelectAndValidateDataFromTestTable(t *testing.T) {
	// 1. Insert a record with various data types
	birthDate := time.UnixMicro(915148800000) // Corrected timestamp value
	err := session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code, credited, balance, is_active, birth_date, zip_code, extra_info, tags) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"Carls", int64(45), 123, 1500.5, float32(500.0), true, birthDate, int64(12345),
		map[string]string{"info_key": "info_value"}, []string{"tag1", "tag2"},
	).Exec()
	require.NoError(t, err, "Failed to insert record")

	// 2. Perform a SELECT query to validate the inserted data
	var name string
	var age int64
	err = session.Query(`SELECT name, age FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`, "Carls", int64(45)).Scan(&name, &age)
	require.NoError(t, err, "Failed to select the inserted record")

	assert.Equal(t, "Carls", name)
	assert.Equal(t, int64(45), age)
}

// TestSelectAllRowsWithoutWhereClause checks that a SELECT without a WHERE clause returns results.
func TestSelectAllRowsWithoutWhereClause(t *testing.T) {
	// 1. Insert a record to ensure the table is not empty
	err := session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`,
		"Arena", int64(30), 999).Exec()
	require.NoError(t, err, "Failed to insert record")

	// 2. Select all rows and count them
	iter := session.Query(`SELECT name, age, code FROM bigtabledevinstance.user_info`).Iter()
	rowCount := iter.NumRows()
	require.NoError(t, iter.Close(), "Failed to close iterator after counting rows")

	// The test ensures that the query runs and returns at least the one record we inserted.
	assert.GreaterOrEqual(t, rowCount, 1, "Expected to retrieve at least one row")
}

// TestValidatingWritetimeFunctionality checks the WRITETIME function with and without an alias.
func TestValidatingWritetimeFunctionality(t *testing.T) {
	// 1. Insert records with specific timestamps
	tsAlice := int64(1734516831000000)
	tsBob := int64(2683260983500000)

	err := session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?) USING TIMESTAMP ?`,
		"Alice", int64(35), 999, tsAlice).Exec()
	require.NoError(t, err, "Failed to insert Alice's record")

	err = session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?) USING TIMESTAMP ?`,
		"Bob", int64(40), 123, tsBob).Exec()
	require.NoError(t, err, "Failed to insert Bob's record")

	// 2. Validate WRITETIME without an alias
	t.Run("writetime without alias", func(t *testing.T) {
		var name string
		var age int64
		var code int
		var writeTime int64
		err := session.Query(`SELECT name, age, code, WRITETIME(code) FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`,
			"Alice", int64(35)).Scan(&name, &age, &code, &writeTime)
		require.NoError(t, err)
		assert.Equal(t, "Alice", name)
		assert.Equal(t, tsAlice, writeTime, "WRITETIME(code) did not match the insertion timestamp")
	})

	// 3. Validate WRITETIME with an alias
	t.Run("writetime with alias", func(t *testing.T) {
		var name string
		var age int64
		var code int
		var codeTimestamp int64
		err := session.Query(`SELECT name, age, code, WRITETIME(code) as code_timestamp FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`,
			"Alice", int64(35)).Scan(&name, &age, &code, &codeTimestamp)
		require.NoError(t, err)
		assert.Equal(t, "Alice", name)
		assert.Equal(t, tsAlice, codeTimestamp, "Aliased WRITETIME(code) did not match")
	})
}

// TestSelectStarWithAllDatatypes inserts a record with a wide variety of data types and validates them using SELECT *.
func TestSelectStarWithAllDatatypes(t *testing.T) {
	// 1. Prepare complex data for insertion
	birthDate := time.UnixMicro(1672531200000)
	ts1 := time.UnixMicro(1672531200000).UTC()

	// 2. Insert the comprehensive record
	err := session.Query(`
		INSERT INTO bigtabledevinstance.user_info (name, age, code, credited, balance, is_active, birth_date, zip_code, extra_info, tags, set_int, list_text, map_text_int, ts_text_map) 
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"TestUserStar", int64(25), 101, 1000.50, float32(500.25), true, birthDate, int64(54321),
		map[string]string{"key1": "value1"}, []string{"tag1", "tag2"}, []int{1, 2}, []string{"item1", "item2"},
		map[string]int{"key1": 1}, map[time.Time]string{ts1: "value1"},
	).Exec()
	require.NoError(t, err, "Failed to insert the comprehensive record")

	// 3. Retrieve the record using SELECT * and validate using MapScan
	iter := session.Query(`SELECT * FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`, "TestUserStar", int64(25)).Iter()
	require.NotNil(t, iter)

	resultMap := make(map[string]interface{})
	if !iter.MapScan(resultMap) {
		t.Fatal("MapScan failed to retrieve any data")
	}
	require.NoError(t, iter.Close(), "Failed to close iterator")

	// 4. Assert all values from the map
	assert.Equal(t, "TestUserStar", resultMap["name"])
	assert.Equal(t, int64(25), resultMap["age"])
	assert.Equal(t, 101, resultMap["code"])
	assert.Equal(t, 1000.50, resultMap["credited"])
	assert.Equal(t, float32(500.25), resultMap["balance"])
	assert.Equal(t, true, resultMap["is_active"])
	assert.Equal(t, birthDate.UTC(), resultMap["birth_date"].(time.Time).UTC())
	assert.Equal(t, int64(54321), resultMap["zip_code"])
	assert.Equal(t, map[string]string{"key1": "value1"}, resultMap["extra_info"])
	assert.ElementsMatch(t, []string{"tag1", "tag2"}, resultMap["tags"])
	assert.ElementsMatch(t, []int{1, 2}, resultMap["set_int"])
	assert.Equal(t, []string{"item1", "item2"}, resultMap["list_text"])
	assert.Equal(t, map[string]int{"key1": 1}, resultMap["map_text_int"])
	assert.Equal(t, map[time.Time]string{ts1: "value1"}, resultMap["ts_text_map"])
}

// TestSelectWithDifferentWhereOperators validates operators like <, >, <=, >=.
// These tests require ALLOW FILTERING as they don't operate on a primary key.
func TestSelectWithDifferentWhereOperators(t *testing.T) {
	// 1. Insert boundary records
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Della", int64(1), 987).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Andre", int64(2), 987).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Simon", int64(1500), 987).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Nivi", int64(1550), 987).Exec())

	// 2. Test >= operator
	iterGtEq := session.Query(`SELECT name FROM bigtabledevinstance.user_info WHERE age >= ? ALLOW FILTERING`, 1500).Iter()
	namesGtEq, err := iterGtEq.SliceMap()
	require.NoError(t, err)
	assert.ElementsMatch(t, []map[string]interface{}{{"name": "Simon"}, {"name": "Nivi"}}, namesGtEq)

	// 3. Test > operator
	iterGt := session.Query(`SELECT name FROM bigtabledevinstance.user_info WHERE age > ? ALLOW FILTERING`, 1500).Iter()
	namesGt, err := iterGt.SliceMap()
	require.NoError(t, err)
	assert.ElementsMatch(t, []map[string]interface{}{{"name": "Nivi"}}, namesGt)

	// 4. Test <= operator
	iterLtEq := session.Query(`SELECT name FROM bigtabledevinstance.user_info WHERE age <= ? ALLOW FILTERING`, 2).Iter()
	namesLtEq, err := iterLtEq.SliceMap()
	require.NoError(t, err)
	assert.ElementsMatch(t, []map[string]interface{}{{"name": "Della"}, {"name": "Andre"}}, namesLtEq)

	// 5. Test < operator
	iterLt := session.Query(`SELECT name FROM bigtabledevinstance.user_info WHERE age < ? ALLOW FILTERING`, 2).Iter()
	namesLt, err := iterLt.SliceMap()
	require.NoError(t, err)
	assert.ElementsMatch(t, []map[string]interface{}{{"name": "Della"}}, namesLt)
}

// TestSelectWithBetweenOperator validates that the BETWEEN operator is not supported and fails correctly.
func TestSelectWithBetweenOperator(t *testing.T) {
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Bob", int64(41220), 987).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Jack", int64(41230), 987).Exec())
	iter := session.Query(`SELECT name FROM bigtabledevinstance.user_info WHERE age BETWEEN ? AND ?`, int64(41225), int64(41235)).Iter()
	namesGtEq, err := iter.SliceMap()

	if testTarget == TestTargetCassandra {
		require.Error(t, err, "Expected an error for BETWEEN operator, but got none")
		assert.Contains(t, err.Error(), "line 1:62 no viable alternative at input 'BETWEEN' (...age FROM bigtabledevinstance.user_info WHERE [age] BETWEEN...)")
	} else {
		assert.ElementsMatch(t, []map[string]interface{}{{"name": "Jack"}}, namesGtEq)
	}
}

// TestSelectWithLikeOperator validates that the LIKE operator fails correctly on non-indexed columns.
func TestSelectWithLikeOperator(t *testing.T) {
	// 1. Insert test data
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Silver Hunter", int64(1300), 987).Exec())

	// 2. Test that LIKE fails without a proper index
	iter := session.Query(`SELECT name, age FROM bigtabledevinstance.user_info WHERE name LIKE ?`, "Silver H%").Iter()
	results, err := iter.SliceMap()

	if testTarget == TestTargetCassandra {
		require.Error(t, err, "Expected an error for LIKE on a non-indexed column, but got none")
		assert.Contains(t, err.Error(), "LIKE restriction is only supported on properly indexed columns", "Error message did not match expected output")
	} else {
		assert.ElementsMatch(t, []map[string]interface{}{{"name": "Silver Hunter", "age": int64(1300)}}, results)
	}
}
