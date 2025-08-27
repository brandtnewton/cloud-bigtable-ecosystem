package compliance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ensures that various int row keys are handled correctly in all CRUD operations
func TestTruncate(t *testing.T) {
	t.Parallel()

	require.NoError(t, session.Query("CREATE TABLE IF NOT EXISTS truncate_test (id INT PRIMARY KEY, name TEXT").Exec())

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
			expectedError: "error while parsing truncate query",
		},
		{
			name:          "malformed truncate command",
			query:         "TRUNCATE TABLE ",
			expectedError: "error while parsing truncate query",
		},
	}

	for _, tc := range testCases {
		if tc.skipCassandra && testTarget == TestTargetCassandra {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			err := session.Query(tc.query).Exec()
			if testTarget == TestTargetCassandra {
				require.Error(t, err, "Expected an error but got none")
				// we don't care about validating the cassandra error message, just that we got an error
				require.Error(t, err)
				return
			}
			require.Error(t, err, "Expected an error but got none")
			assert.Contains(t, err.Error(), tc.expectedError, "Error message mismatch")
		})
	}
}
