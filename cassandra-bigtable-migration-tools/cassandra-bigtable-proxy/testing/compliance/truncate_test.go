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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ensures that various int row keys are handled correctly in all CRUD operations
func TestTruncate(t *testing.T) {
	t.Parallel()

	require.NoError(t, session.Query("CREATE TABLE IF NOT EXISTS truncate_test (id INT PRIMARY KEY, name TEXT)").Exec())

	// add some data to truncate
	require.NoError(t, session.Query("INSERT INTO truncate_test (id, name) VALUES (?, ?)", 1, "foo").Exec())
	require.NoError(t, session.Query("INSERT INTO truncate_test (id, name) VALUES (?, ?)", 2, "bar").Exec())

	var gotCount int32
	query := session.Query("select count(*) from truncate_test")
	require.NoError(t, query.Scan(&gotCount))

	// ensure the data is there
	assert.Equal(t, int32(2), gotCount)

	require.NoError(t, session.Query("TRUNCATE TABLE truncate_test").Exec())

	require.NoError(t, query.Scan(&gotCount))

	// ensure the data is gone
	assert.Equal(t, int32(0), gotCount)
}

func TestNegativeTruncateCases(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		query         string
		expectedError string
		skipCassandra bool
	}{
		{
			name:          "A missing table",
			query:         "TRUNCATE TABLE no_such_truncate_table",
			expectedError: "does not exist",
		},
		{
			name:          "malformed truncate command",
			query:         "TRUNCATE",
			expectedError: "failed to parse table name",
		},
		{
			name:          "malformed truncate command",
			query:         "TRUNCATE TABLE ",
			expectedError: "failed to parse table name",
		},
		{
			name:          "truncate schema_mapping table",
			query:         "TRUNCATE TABLE schema_mapping",
			expectedError: "table name cannot be the same as the configured schema mapping table name 'schema_mapping'",
		},
	}

	for _, tc := range testCases {
		if tc.skipCassandra && testTarget == TestTargetCassandra {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := session.Query(tc.query).Exec()
			if testTarget == TestTargetCassandra {
				// we don't care about validating the cassandra error message, just that we got an error
				require.Error(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedError)
		})
	}
}
