package compliance

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchUpsertOnSameRowKey(t *testing.T) {
	pkName, pkAge := "John", int64(45)

	// Create a new batch
	batch := session.NewBatch(gocql.LoggedBatch)
	batch.Query(
		"INSERT INTO bigtabledevinstance.user_info (name, age, code, credited) VALUES (?, ?, ?, ?)",
		[]interface{}{pkName, pkAge, 123, 1500.5},
	)
	batch.Query(
		"INSERT INTO bigtabledevinstance.user_info (name, age, code, credited) VALUES (?, ?, ?, ?)",
		[]interface{}{pkName, pkAge, 456, 1500.5},
	)
	batch.Query(
		"INSERT INTO bigtabledevinstance.user_info (name, age, code, credited) VALUES (?, ?, ?, ?)",
		[]interface{}{pkName, pkAge, 789, 1500.5},
	)

	// Execute the batch
	err := session.ExecuteBatch(batch)
	require.NoError(t, err, "Batch execution failed")

	// Validate that the last value for 'code' was applied
	var code int
	err = session.Query(`SELECT code FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&code)
	require.NoError(t, err, "Failed to select the record after batch insert")
	assert.Equal(t, 789, code, "The code should reflect the last value in the batch")
}

func TestBatchInsertMultipleTables(t *testing.T) {
	batch := session.NewBatch(gocql.LoggedBatch)
	batch.Query(
		"INSERT INTO bigtabledevinstance.user_info (name, age, code, credited) VALUES (?, ?, ?, ?)",
		[]interface{}{"John Batch", int64(100), 123, 1500.5},
	)
	batch.Query(
		"INSERT INTO orders (user_id, order_num, name) VALUES (?, ?, ?)",
		[]interface{}{"user1", 32, "diapers"},
	)

	err := session.ExecuteBatch(batch)
	require.NoError(t, err)

	// Validate insertion in bigtabledevinstance.user_info table
	var userName string
	err = session.Query(`SELECT name FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`, "John Batch", int64(100)).Scan(&userName)
	require.NoError(t, err)
	assert.Equal(t, "John Batch", userName)

	// Validate insertion in orders table
	var orderName string
	err = session.Query(`SELECT name FROM orders WHERE user_id = ? AND order_num = ?`, "user1", 32).Scan(&orderName)
	require.NoError(t, err)
	assert.Equal(t, "diapers", orderName)
}

func TestBatchInsertDifferentCompositeKeys(t *testing.T) {
	batch := session.NewBatch(gocql.LoggedBatch)
	batch.Query("INSERT INTO bigtabledevinstance.user_info (name, age, code, credited) VALUES (?, ?, ?, ?)", []interface{}{"Jhony", int64(32), 101, 1500.5})
	batch.Query("INSERT INTO bigtabledevinstance.user_info (name, age, code, credited) VALUES (?, ?, ?, ?)", []interface{}{"Jamess", int64(32), 102, 1600.0})
	batch.Query("INSERT INTO bigtabledevinstance.user_info (name, age, code, credited) VALUES (?, ?, ?, ?)", []interface{}{"Ronny", int64(32), 103, 1700.75})

	err := session.ExecuteBatch(batch)
	require.NoError(t, err)

	// Validate all three records
	var code int
	err = session.Query(`SELECT code FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`, "Jhony", int64(32)).Scan(&code)
	require.NoError(t, err)
	assert.Equal(t, 101, code)

	err = session.Query(`SELECT code FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`, "Jamess", int64(32)).Scan(&code)
	require.NoError(t, err)
	assert.Equal(t, 102, code)

	err = session.Query(`SELECT code FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`, "Ronny", int64(32)).Scan(&code)
	require.NoError(t, err)
	assert.Equal(t, 103, code)
}

func TestBatchInsertAndUpdateOnSameKey(t *testing.T) {
	pkName, pkAge := "Steave", int64(32)

	batch := session.NewBatch(gocql.LoggedBatch)
	batch.Query("INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)", []interface{}{pkName, pkAge, 123})
	batch.Query("UPDATE bigtabledevinstance.user_info SET code = ? WHERE name = ? AND age = ?", []interface{}{678, pkName, pkAge})

	err := session.ExecuteBatch(batch)
	require.NoError(t, err)

	var code int
	err = session.Query(`SELECT code FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&code)
	require.NoError(t, err)
	assert.Equal(t, 678, code, "The code should be the updated value")
}

func TestBatchInsertAndDeleteOnSameKey(t *testing.T) {
	pkName, pkAge := "Hazzlewood", int64(32)

	batch := session.NewBatch(gocql.LoggedBatch)
	batch.Query("INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)", []interface{}{pkName, pkAge, 123})
	batch.Query("DELETE FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?", []interface{}{pkName, pkAge})

	err := session.ExecuteBatch(batch)
	require.NoError(t, err)

	var code int
	err = session.Query(`SELECT code FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&code)
	require.Error(t, err)
	assert.Equal(t, gocql.ErrNotFound, err, "The record should be deleted")
}

func TestBatchMixedDataTypeInsert(t *testing.T) {
	pkName, pkAge := "Alice", int64(30)
	birthDate := time.UnixMicro(1736541455000)
	extraInfo := map[string]string{"key1": "value1", "key2": "value2"}
	mapTextInt := map[string]int{"field1": 100, "field2": 200}
	tags := []string{"tag1", "tag2", "tag3"}
	setFloat := []float32{10.5, 20.5, 30.75}

	batch := session.NewBatch(gocql.LoggedBatch)
	batch.Query(
		`INSERT INTO bigtabledevinstance.user_info (name, age, credited, balance, is_active, birth_date, zip_code, extra_info, map_text_int, tags, set_float) 
			   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		[]interface{}{pkName, pkAge, 1500.75, float32(2000.5), true, birthDate, int64(123456), extraInfo, mapTextInt, tags, setFloat},
	)

	err := session.ExecuteBatch(batch)
	require.NoError(t, err)

	var retrievedTags []string
	var retrievedSetFloat []float32
	err = session.Query(`SELECT tags, set_float FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&retrievedTags, &retrievedSetFloat)
	require.NoError(t, err)
	assert.ElementsMatch(t, tags, retrievedTags)
	assert.ElementsMatch(t, setFloat, retrievedSetFloat)
}

func TestBatchMixedDataTypeOperations(t *testing.T) {
	pkName, pkAge := "Alice", int64(30)
	birthDate, err := time.Parse("2006-01-02 15:04:05", "1995-05-15 10:30:00")
	require.NoError(t, err)

	batch := session.NewBatch(gocql.LoggedBatch)
	// 1. Insert initial record
	batch.Query(
		`INSERT INTO bigtabledevinstance.user_info (name, age, credited, balance, is_active, birth_date, zip_code, extra_info, map_text_int, tags, set_float) 
			   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		[]interface{}{
			pkName, pkAge, 1500.75, float32(2000.5), true, birthDate, int64(123456),
			map[string]string{"key1": "value1", "key2": "value2"}, map[string]int{"field1": 100},
			[]string{"tag1", "tag2"}, []float32{10.5},
		},
	)
	// 2. Update some fields
	batch.Query(
		`UPDATE bigtabledevinstance.user_info SET credited = ?, extra_info = ?, tags = ? WHERE name = ? AND age = ?`,
		[]interface{}{
			2500.0, map[string]string{"key1": "updated_value", "key3": "new_value"},
			[]string{"tag1", "tag4"}, pkName, pkAge,
		},
	)
	// 3. Delete the record
	batch.Query(
		`DELETE FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`,
		[]interface{}{pkName, pkAge},
	)

	err = session.ExecuteBatch(batch)
	require.NoError(t, err)

	// Validate the record is gone
	var name string
	err = session.Query(`SELECT name FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&name)
	require.Error(t, err)
	assert.Equal(t, gocql.ErrNotFound, err, "The final state of the batch should be that the record is deleted")
}

func TestBatchPartialUpdate(t *testing.T) {
	pkName, pkAge := "Eve", int64(35)

	batch := session.NewBatch(gocql.LoggedBatch)
	// Insert full record
	batch.Query(
		`INSERT INTO bigtabledevinstance.user_info (name, age, credited, balance, is_active) VALUES (?, ?, ?, ?, ?)`,
		[]interface{}{pkName, pkAge, 1200.0, float32(300.5), true},
	)
	// Insert with same PK to update a subset of columns
	batch.Query(
		`INSERT INTO bigtabledevinstance.user_info (name, age, balance, is_active) VALUES (?, ?, ?, ?)`,
		[]interface{}{pkName, pkAge, float32(400.0), false},
	)

	err := session.ExecuteBatch(batch)
	require.NoError(t, err)

	var credited float64
	var balance float32
	var isActive bool
	err = session.Query(`SELECT credited, balance, is_active FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`, pkName, pkAge).Scan(&credited, &balance, &isActive)
	require.NoError(t, err)

	assert.Equal(t, 1200.0, credited, "'credited' should be preserved from the first insert")
	assert.Equal(t, float32(400.0), balance, "'balance' should be updated")
	assert.False(t, isActive, "'is_active' should be updated")
}

func TestBatchUsingTimestampAndWritetime(t *testing.T) {
	ts1 := int64(1734516444000000)
	ts2 := int64(1747651510418000)

	batch := session.NewBatch(gocql.LoggedBatch)
	batch.Query(
		`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?) USING TIMESTAMP ?`,
		[]interface{}{"John Batch", int64(31), 987, ts1},
	)
	batch.Query(
		`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?) USING TIMESTAMP ?`,
		[]interface{}{"Alexa Batch", int64(32), 987, ts2},
	)

	err := session.ExecuteBatch(batch)
	require.NoError(t, err)

	// Validate writetime for John
	var writeTime int64
	err = session.Query(`SELECT writetime(code) FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`, "John Batch", int64(31)).Scan(&writeTime)
	require.NoError(t, err)
	assert.Equal(t, ts1, writeTime)

	// Validate writetime for Alexa
	err = session.Query(`SELECT writetime(code) FROM bigtabledevinstance.user_info WHERE name = ? AND age = ?`, "Alexa Batch", int64(32)).Scan(&writeTime)
	require.NoError(t, err)
	assert.Equal(t, ts2, writeTime)
}
