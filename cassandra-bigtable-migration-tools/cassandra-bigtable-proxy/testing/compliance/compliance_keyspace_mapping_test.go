package compliance

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicInsertUpdateDeleteAndValidation(t *testing.T) {
	// 1. INSERT a new record
	err := session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`,
		"John Doe", int64(30), 123).Exec()
	require.NoError(t, err, "Failed to insert record")

	// 2. SELECT the record to validate the insertion
	var name string
	var age int64
	var code int
	err = session.Query(`SELECT name, age, code FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`,
		"John Doe", int64(30)).Scan(&name, &age, &code)
	require.NoError(t, err, "Failed to select newly inserted record")
	assert.Equal(t, "John Doe", name)
	assert.Equal(t, int64(30), age)
	assert.Equal(t, 123, code)

	// 3. UPDATE the record's code
	err = session.Query(`UPDATE bigtabledevinstance.user_info SET code = ? WHERE name = ? AND age = ?`,
		456, "John Doe", int64(30)).Exec()
	require.NoError(t, err, "Failed to update record")

	// 4. SELECT the record again to validate the update
	err = session.Query(`SELECT name, age, code FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`,
		"John Doe", int64(30)).Scan(&name, &age, &code)
	require.NoError(t, err, "Failed to select updated record")
	assert.Equal(t, 456, code, "The 'code' field was not updated correctly")

	// 5. DELETE the record
	err = session.Query(`DELETE FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`,
		"John Doe", int64(30)).Exec()
	require.NoError(t, err, "Failed to delete record")

	// 6. SELECT the record one last time to confirm it's gone
	err = session.Query(`SELECT name FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`,
		"John Doe", int64(30)).Scan(&name)
	require.Error(t, err, "Expected an error when selecting a deleted record, but got none")
	assert.Equal(t, gocql.ErrNotFound, err, "Expected error to be 'not found' after deletion")
}
