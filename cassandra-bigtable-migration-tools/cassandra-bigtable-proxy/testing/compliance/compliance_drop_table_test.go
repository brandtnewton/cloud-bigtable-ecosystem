package compliance

import (
	"fmt"
	"github.com/gocql/gocql"
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
		assert.Contains(t, err.Error(), "does not exist")
	}
}

func TestDroppedTableWriteFails(t *testing.T) {
	t.Parallel()
	table := uniqueTableName("drop_table_")

	// 1. create a table
	err := session.Query(fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, name text)", table)).Exec()
	require.NoError(t, err)

	// confirm system tables are updated
	systemTablesResult := make(map[string]any)
	err = session.Query("SELECT * FROM system_schema.tables WHERE table_name=?", table).MapScan(systemTablesResult)
	require.NoError(t, err)
	assert.Equal(t, systemTablesResult["keyspace_name"], "bigtabledevinstance")
	assert.Equal(t, systemTablesResult["table_name"], table)

	// confirm system columns are updated
	systemColumnsResult := make(map[string]any)
	err = session.Query("SELECT * FROM system_schema.columns WHERE table_name=? AND column_name=?", table, "id").MapScan(systemColumnsResult)
	require.NoError(t, err)
	assert.Equal(t, "bigtabledevinstance", systemColumnsResult["keyspace_name"])
	assert.Equal(t, table, systemColumnsResult["table_name"])
	assert.Equal(t, "id", systemColumnsResult["column_name"])
	assert.Equal(t, int(0), systemColumnsResult["position"])
	assert.Equal(t, "partition_key", systemColumnsResult["kind"])
	assert.Equal(t, "int", systemColumnsResult["type"])

	// 2. drop it
	err = session.Query(fmt.Sprintf("DROP TABLE %s", table)).Exec()
	require.NoError(t, err)

	// confirm system tables are updated
	err = session.Query("SELECT * FROM system_schema.tables WHERE table_name=?", table).MapScan(systemTablesResult)
	require.Error(t, err)
	assert.Error(t, gocql.ErrNotFound, err)

	// confirm system columns are updated
	err = session.Query("SELECT * FROM system_schema.columns WHERE table_name=? AND column_name=?", table, "id").MapScan(systemColumnsResult)
	require.Error(t, err)
	assert.Error(t, gocql.ErrNotFound, err)

	// 3. writing to the dropped table should be handled gracefully
	err = session.Query(fmt.Sprintf(`INSERT INTO %s (id, name) VALUES (?, ?)`, table), 1, "foo").Exec()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestDroppedSchemaMappingTableFails(t *testing.T) {
	if testTarget == TestTargetCassandra {
		t.Skip()
		return
	}
	t.Parallel()
	err := session.Query("DROP TABLE schema_mapping").Exec()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "table name cannot be the same as the configured schema mapping table name 'schema_mapping'")
}
