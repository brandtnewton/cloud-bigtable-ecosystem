package compliance

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeleteOperationWithRecordValidation verifies the full lifecycle of inserting, validating, deleting, and re-validating a record.
func TestDeleteOperationWithRecordValidation(t *testing.T) {
	pkName, pkAge := "Michael", int64(45)

	err := session.Query(`INSERT INTO user_info (name, age, code, credited) VALUES (?, ?, ?, ?)`,
		pkName, pkAge, 987, 5000.0).Exec()
	require.NoError(t, err, "Setup: Failed to insert record")

	// Validate existence before delete
	var code int
	err = session.Query(`SELECT code FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&code)
	require.NoError(t, err, "Setup: Failed to select record for pre-validation")
	assert.Equal(t, 987, code)

	// Perform the delete
	err = session.Query(`DELETE FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Exec()
	require.NoError(t, err, "Delete operation failed")

	// Validate non-existence after delete
	err = session.Query(`SELECT code FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&code)
	require.Error(t, err, "Expected an error when selecting a deleted record")
	assert.Equal(t, gocql.ErrNotFound, err, "Expected error to be 'not found' after deletion")
}

// TestDeleteOperationWithTimestampFails verifies that DELETE with USING TIMESTAMP fails as expected.
// Note: This test assumes the target system (e.g., a proxy) rejects this, as it's valid in standard Cassandra.
func TestDeleteOperationWithTimestampFails(t *testing.T) {
	pkName, pkAge := "Jhon", int64(33)
	require.NoError(t, session.Query(`INSERT INTO user_info (name, age, code) VALUES (?, ?, ?)`, pkName, pkAge, 123).Exec())

	nowMicro := time.Now().UnixMicro()
	err := session.Query(`DELETE FROM user_info USING TIMESTAMP ? WHERE name = ? AND age = ?`,
		nowMicro, pkName, pkAge).Exec()

	// This test is based on the JSON expectation of an error.
	// In standard Cassandra, this query would succeed.
	// require.NoError(t, err)

	require.Error(t, err, "Expected an error for DELETE USING TIMESTAMP")
	assert.Contains(t, err.Error(), "delete using timestamp is not allowed")
}

// TestDeleteSpecificRecordByPrimaryKey confirms a targeted delete works correctly.
func TestDeleteSpecificRecordByPrimaryKey(t *testing.T) {
	pkName, pkAge := "John", int64(30)
	require.NoError(t, session.Query(`INSERT INTO user_info (name, age, code) VALUES (?, ?, ?)`, pkName, pkAge, 123).Exec())

	err := session.Query(`DELETE FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Exec()
	require.NoError(t, err)

	err = session.Query(`SELECT name FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&pkName)
	require.Error(t, err)
	assert.Equal(t, gocql.ErrNotFound, err)
}

// TestDeleteNonExistentRecord ensures deleting a record that doesn't exist completes without error.
func TestDeleteNonExistentRecord(t *testing.T) {
	pkName, pkAge := "NonExistent", int64(99)
	err := session.Query(`DELETE FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Exec()
	require.NoError(t, err, "Deleting a non-existent record should not produce an error")
}

// TestDeleteRecordWithIfExists verifies the IF EXISTS clause for conditional deletes.
func TestDeleteRecordWithIfExists(t *testing.T) {
	pkName, pkAge := "Emma", int64(28)
	require.NoError(t, session.Query(`INSERT INTO user_info (name, age, code) VALUES (?, ?, ?)`, pkName, pkAge, 112).Exec())

	// Delete existing record
	applied, err := session.Query(`DELETE FROM user_info WHERE name = ? AND age = ? IF EXISTS`, pkName, pkAge).ScanCAS()
	require.NoError(t, err)
	assert.True(t, applied, "DELETE IF EXISTS should be applied for an existing record")

	// Verify deletion
	err = session.Query(`SELECT name FROM user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&pkName)
	assert.Equal(t, gocql.ErrNotFound, err)

	// Delete non-existent record
	applied, err = session.Query(`DELETE FROM user_info WHERE name = ? AND age = ? IF EXISTS`, "NonExistentName", int64(99)).ScanCAS()
	require.NoError(t, err)
	assert.False(t, applied, "DELETE IF EXISTS should not be applied for a non-existent record")
}

// TestNegativeDeleteCases covers various invalid DELETE scenarios to ensure errors are handled correctly.
func TestNegativeDeleteCases(t *testing.T) {
	testCases := []struct {
		name          string
		query         string
		params        []interface{}
		expectedError string
	}{
		{"With Non-PK Condition", `DELETE FROM bigtabledevinstance.user_info WHERE name = ? AND age = ? AND balance = ?`, []interface{}{"Oliver", int64(50), float32(100.0)}, "Non PRIMARY KEY columns found in where clause"},
		{"Missing PK Part", `DELETE FROM bigtabledevinstance.user_info WHERE name = ? IF EXISTS`, []interface{}{"Michael"}, "Some primary key parts are missing: age"},
		{"Condition on Non-PK Only", `DELETE FROM bigtabledevinstance.user_info WHERE credited = ? IF EXISTS`, []interface{}{5000.0}, "Some primary key parts are missing"},
		{"Invalid Data Type", `DELETE FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`, []interface{}{"Michael", "invalid_age"}, "cannot marshal string to bigint"},
		{"Invalid Table Name", `DELETE FROM non_existent_table WHERE name = ? AND age = ?`, []interface{}{"Michael", int64(45)}, "table non_existent_table does not exist"},
		{"Invalid Keyspace", `DELETE FROM invalid_keyspace.user_info WHERE name = ? AND age = ?`, []interface{}{"Michael", int64(45)}, "keyspace invalid_keyspace does not exist"},
		{"Missing Keyspace", `DELETE FROM user_info WHERE name = ? AND age = ?`, []interface{}{"Michael", int64(45)}, "no keyspace provided"},
	}

	// Insert a record needed for the "With Non-PK Condition" test
	require.NoError(t, session.Query(`INSERT INTO user_info (name, age, balance) VALUES (?, ?, ?)`, "Oliver", int64(50), float32(100.0)).Exec())

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := session.Query(tc.query, tc.params...).Exec()
			require.Error(t, err, "Expected query to fail")
			assert.Contains(t, err.Error(), tc.expectedError)
		})
	}
}
