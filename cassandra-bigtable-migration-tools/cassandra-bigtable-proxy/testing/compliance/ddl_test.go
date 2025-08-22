package compliance

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlterTable(t *testing.T) {
	table := "ddl_table_" + strings.ReplaceAll(uuid.New().String(), "-", "_")
	t.Logf("running test %s with random table name %s", t.Name(), table)
	err := session.Query(fmt.Sprintf("CREATE TABLE %s (id TEXT PRIMARY KEY, name TEXT)", table)).Exec()
	defer cleanupTable(t, table)
	assert.NoError(t, err)

	insertQuery := session.Query(fmt.Sprintf("INSERT INTO %s (id, name, age) VALUES (?, ?, ?)", table), "abc", "bob", 32)

	err = insertQuery.Exec()
	assert.Error(t, err, "insert should fail because there is no age column")

	err = session.Query(fmt.Sprintf("ALTER TABLE %s ADD age INT", table)).Exec()
	assert.NoError(t, err)

	err = insertQuery.Exec()
	assert.NoError(t, err)

	err = session.Query(fmt.Sprintf("ALTER TABLE %s DROP age", table)).Exec()
	assert.NoError(t, err)

	err = session.Query(fmt.Sprintf("ALTER TABLE %s ADD weight FLOAT", table)).Exec()
	assert.NoError(t, err)

	err = session.Query(fmt.Sprintf("INSERT INTO %s (id, name, weight) VALUES (?, ?, ?)", table), "abc", "bob", float32(190.5)).Exec()
	assert.NoError(t, err)

	err = session.Query(fmt.Sprintf("DROP TABLE  %s", table)).Exec()
	assert.NoError(t, err)
}

func TestCreateIfNotExist(t *testing.T) {
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

func TestDropTableIfExist(t *testing.T) {
	// dropping a random table that definitely doesn't exist should be ok
	id := strings.ReplaceAll(uuid.New().String(), "-", "_")
	err := session.Query(fmt.Sprintf("DROP TABLE IF EXISTS no_such_table_%s", id)).Exec()
	assert.NoError(t, err)
}

func TestNegativeTestCasesForCreateTable(t *testing.T) {
	testCases := []struct {
		name          string
		query         string
		expectedError string
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
		},
		{
			name:          "Create table with invalid column type",
			query:         "CREATE TABLE fail_invalid_col_type (num INT, big_num UUID, PRIMARY KEY (num))",
			expectedError: "column type 'uuid' is not supported",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := session.Query(tc.query).Exec()
			require.Error(t, err, "Expected an error but got none")
			assert.True(t, strings.Contains(err.Error(), tc.expectedError), "Error message mismatch.\nExpected to contain: %s\nGot: %s", tc.expectedError, err.Error())
		})
	}
}
