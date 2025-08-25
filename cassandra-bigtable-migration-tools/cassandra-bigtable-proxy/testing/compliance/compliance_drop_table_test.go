package compliance

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDropTableIfExist(t *testing.T) {
	t.Parallel()
	// dropping a random table that definitely doesn't exist should be ok
	table := uniqueTableName("no_such_table")
	err := session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s", table)).Exec()
	assert.NoError(t, err)
}

func TestDropTableThatDoesntExist(t *testing.T) {
	t.Parallel()
	table := uniqueTableName("no_such_table")
	err := session.Query(fmt.Sprintf("DROP TABLE %s", table)).Exec()
	if testTarget == TestTargetCassandra {
		// we don't care about validating the cassandra error message, just that we got an error
		require.Error(t, err)
	} else {
		require.Error(t, err)
		assert.Contains(t, err.Error(), "because it does not exist")
	}
}

func TestDroppedTableWriteFails(t *testing.T) {
	t.Parallel()
	table := uniqueTableName("drop_table_")

	// 1. create a table
	err := session.Query(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, name text)", table)).Exec()
	require.NoError(t, err)

	// 2. drop it
	err = session.Query(fmt.Sprintf("DROP TABLE %s", table)).Exec()
	require.NoError(t, err)

	// 3. writing to the dropped table should be handled gracefully
	err = session.Query(fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (?, ?)`, table), 1, "foo").Exec()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}
