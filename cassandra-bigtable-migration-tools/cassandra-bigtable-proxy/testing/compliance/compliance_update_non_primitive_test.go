package compliance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestManipulationOfSetAndMapDataTypes validates INSERT and UPDATE operations on collection data types.
func TestManipulationOfSetAndMapDataTypes(t *testing.T) {
	// 1. Insert a record with a set and a map
	err := session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code, tags, extra_info) VALUES (?, ?, ?, ?, ?)`,
		"Lilly",
		int64(25),
		456,
		[]string{"tag1", "tag2"},
		map[string]string{"info_key": "info_value"},
	).Exec()
	require.NoError(t, err, "Failed to insert record with collections")

	// 2. Update the 'tags' set
	err = session.Query(`UPDATE bigtabledevinstance.user_info SET tags=? WHERE name=? and age=?`,
		[]string{"tag3", "tag4"},
		"Lilly",
		int64(25),
	).Exec()
	require.NoError(t, err, "Failed to update the tags set")

	// 3. Select the record to verify the 'tags' update
	var name string
	var age int64
	var code int
	var tags []string
	var extraInfo map[string]string

	err = session.Query(`SELECT name, age, code, tags, extra_info FROM bigtabledevinstance.user_info WHERE name=? AND age=?`,
		"Lilly", int64(25)).Scan(&name, &age, &code, &tags, &extraInfo)
	require.NoError(t, err, "Failed to select record after tags update")

	assert.Equal(t, "Lilly", name)
	assert.Equal(t, int64(25), age)
	assert.Equal(t, 456, code)
	assert.ElementsMatch(t, []string{"tag3", "tag4"}, tags, "Tags set was not updated correctly")
	assert.Equal(t, map[string]string{"info_key": "info_value"}, extraInfo)

	// 4. Update the 'extra_info' map, replacing its contents
	err = session.Query(`UPDATE bigtabledevinstance.user_info SET extra_info=? WHERE name=? and age=?`,
		map[string]string{"updated_info_key": "updated_info_value"},
		"Lilly",
		int64(25),
	).Exec()
	require.NoError(t, err, "Failed to update the extra_info map")

	// 5. Select the record to verify the 'extra_info' update
	err = session.Query(`SELECT name, age, code, tags, extra_info FROM bigtabledevinstance.user_info WHERE name=? AND age=?`,
		"Lilly", int64(25)).Scan(&name, &age, &code, &tags, &extraInfo)
	require.NoError(t, err, "Failed to select record after extra_info update")

	assert.ElementsMatch(t, []string{"tag3", "tag4"}, tags)
	assert.Equal(t, map[string]string{"updated_info_key": "updated_info_value"}, extraInfo, "Extra_info map was not updated correctly")

	// 6. Update the 'extra_info' map again with different content
	err = session.Query(`UPDATE bigtabledevinstance.user_info SET extra_info=? WHERE name=? and age=?`,
		map[string]string{"updated_info_key": "updated_info_value", "info_key": "updated_info_value"},
		"Lilly",
		int64(25),
	).Exec()
	require.NoError(t, err, "Failed to perform second update on extra_info map")

	// 7. Select the record to verify the final 'extra_info' update
	err = session.Query(`SELECT name, age, code, tags, extra_info FROM bigtabledevinstance.user_info WHERE name=? AND age=?`,
		"Lilly", int64(25)).Scan(&name, &age, &code, &tags, &extraInfo)
	require.NoError(t, err, "Failed to select record after final extra_info update")

	expectedMap := map[string]string{
		"updated_info_key": "updated_info_value",
		"info_key":         "updated_info_value",
	}
	assert.Equal(t, expectedMap, extraInfo, "Final extra_info map content is incorrect")
}
