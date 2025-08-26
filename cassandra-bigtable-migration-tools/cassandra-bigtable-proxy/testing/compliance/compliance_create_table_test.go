package compliance

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateIfNotExist(t *testing.T) {
	t.Parallel()
	// dropping a random table that definitely doesn't exist should be ok
	table := "create_" + strings.ReplaceAll(uuid.New().String(), "-", "_")
	defer cleanupTable(t, table)
	err := session.Query(fmt.Sprintf("CREATE TABLE %s (id TEXT PRIMARY KEY, name TEXT)", table)).Exec()
	assert.NoError(t, err)
	err = session.Query(fmt.Sprintf("CREATE TABLE %s (id TEXT PRIMARY KEY, name TEXT)", table)).Exec()
	assert.Error(t, err)
	err = session.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id TEXT PRIMARY KEY, name TEXT)", table)).Exec()
	assert.NoError(t, err)
}

func TestNegativeTestCasesForCreateTable(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name          string
		query         string
		expectedError string
		skipCassandra bool
	}{
		{
			name:          "Create table with no primary key",
			query:         "CREATE TABLE fail_no_primary_key (num INT, big_num BIGINT)",
			expectedError: "no primary key found in create table statement",
		},
		{
			name:          "multiple inline primary keys",
			query:         "CREATE TABLE fail_multiple_inline_pmk (num INT PRIMARY KEY, big_num BIGINT, PRIMARY KEY (num, big_num))",
			expectedError: "cannot specify both primary key clause and inline primary key",
		},
		{
			name:          "Create table with empty keys",
			query:         "CREATE TABLE fail_empty_primary_key (num INT, big_num BIGINT, PRIMARY KEY ())",
			expectedError: "no primary key found in create table statement",
		},
		{
			name:          "Create table no column definition for pmk",
			query:         "CREATE TABLE fail_missing_pmk_col (num INT, big_num BIGINT, PRIMARY KEY (foo))",
			expectedError: "primary key 'foo' has no column definition in create table statement",
		},
		{
			name:          "Create table with invalid key type",
			query:         "CREATE TABLE fail_invalid_pmk_type (k boolean, big_num BIGINT, PRIMARY KEY (k))",
			expectedError: "primary key cannot be of type boolean",
			skipCassandra: true,
		},
		{
			name:          "Create table with invalid column type",
			query:         "CREATE TABLE fail_invalid_col_type (num INT, big_num UUID, PRIMARY KEY (num))",
			expectedError: "column type 'uuid' is not supported",
			skipCassandra: true,
		},
		{
			name:          "Create table ending with a comma",
			query:         "CREATE TABLE eof_table (num INT, big_num UUID, PRIMARY KEY (num)),",
			expectedError: "column type 'uuid' is not supported",
			skipCassandra: true,
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
			require.Error(t, err, "Expected an error but got none")
			assert.True(t, strings.Contains(err.Error(), tc.expectedError), "Error message mismatch.\nExpected to contain: %s\nGot: %s", tc.expectedError, err.Error())
		})
	}
}
