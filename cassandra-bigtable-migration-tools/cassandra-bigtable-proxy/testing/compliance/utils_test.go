/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, ProtocolVersion 2.0 (the "License"); you may not
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
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
)

// Helper to parse RFC3339 timestamp strings used in the tests
func parseTime(t *testing.T, ts string) time.Time {
	t.Helper()
	parsedTime, err := time.Parse(time.RFC3339, ts)
	require.NoError(t, err, "Failed to parse timestamp string")
	return parsedTime.UTC()
}

// Helper to parse "YYYY-MM-DD HH:MM:SS" timestamp strings used in the tests.
func parseSimpleTime(t *testing.T, ts string) time.Time {
	t.Helper()
	parsedTime, err := time.Parse("2006-01-02 15:04:05", ts)
	require.NoError(t, err, "Failed to parse timestamp string")
	return parsedTime.UTC()
}

func uniqueTableName(prefix string) string {
	// add an underscore separator
	if len(prefix) != 0 && prefix[len(prefix)-1] != '_' {
		prefix = prefix + "_"
	}
	return prefix + strings.ReplaceAll(uuid.New().String(), "-", "_")
}

// note: table must have a non-key row_index column
// note: input map must have the same keys
func testLexOrder(t *testing.T, input []map[string]interface{}, table string) {
	require.NotEqual(t, 0, len(input), "input cannot be empty")

	// used to weed out duplicate keys which are not allowed
	var inputMap = make(map[string]int)
	for i, v := range input {
		existing, ok := inputMap[fmt.Sprintf("%v", v)]
		require.False(t, ok, fmt.Sprintf("duplicate input '%v' at index %d and %d. duplicates are not allowed and indicate an incorrectly ordered input.", v, existing, i))

		values := []interface{}{i}
		queryPlaceholders := []string{"?"}
		columns := []string{"row_index"}
		for _, k := range maps.Keys(v) {
			queryPlaceholders = append(queryPlaceholders, "?")
			columns = append(columns, k)
			values = append(values, v[k])
		}

		queryString := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(columns, ", "), strings.Join(queryPlaceholders, ", "))
		insertQuery := session.Query(queryString, values...)
		require.NoError(t, insertQuery.Exec(), "error inserting row %d", i)
		inputMap[fmt.Sprintf("%v", v)] = i
	}

	scanner := session.Query("SELECT row_index FROM " + table).Iter().Scanner()

	var results []int32 = nil
	for scanner.Next() {
		var rowIndex int32
		err := scanner.Scan(
			&rowIndex,
		)
		require.NoError(t, err)
		results = append(results, rowIndex)
	}

	require.Equal(t, len(input), len(results), "missing or extra result row?")
	for i, result := range results {
		require.True(t, int(result) >= 0 && int(result) < len(input), fmt.Sprintf("unexpected result index: %d your environment is probably not clean", result))
		assert.Equal(t, int32(i), result, fmt.Sprintf("out of order result: expected %v but got %v", input[i], input[result]))
	}
}
