package compliance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectWithInClause(t *testing.T) {
	t.Parallel()
	// 1. Insert a large number of records to create a diverse dataset.
	// We use require.NoError to stop the test immediately if data setup fails.
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code, credited, balance, is_active) VALUES (?, ?, ?, ?, ?, ?)`, "Ram", int64(45), 123, 12.14127, float32(12.1), true).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code, credited, balance, is_active) VALUES (?, ?, ?, ?, ?, ?)`, "Bob", int64(11), 173, 3.8014127, float32(10.71), false).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Ram", int64(98), 1).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Smith", int64(245), 8433).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Bob", int64(90), 0).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Ram", int64(50), 150).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Rahul", int64(298), 140).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Bob", int64(1001), 743).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Ram", int64(732), 10213).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Ram", int64(85), 1193).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Vishnu", int64(82), 934).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Ram", int64(10), 11).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Abc", int64(10), 2347).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Xyz", int64(10), 5847).Exec())
	require.NoError(t, session.Query(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES (?, ?, ?)`, "Ram", int64(93), 13).Exec())

	// 2. Run validation queries.
	t.Run("IN on clustering key with single value", func(t *testing.T) {
		t.Parallel()
		// IN on a clustering key is efficient if the partition key is also provided.
		// For this test, we assume a full table scan is acceptable and use ALLOW FILTERING.
		var name string
		var age int64
		err := session.Query(`SELECT name, age FROM bigtabledevinstance.user_info WHERE age IN ? ALLOW FILTERING`, []int64{82}).Scan(&name, &age)
		require.NoError(t, err)
		assert.Equal(t, "Vishnu", name)
		assert.Equal(t, int64(82), age)
	})

	t.Run("IN on clustering key with multiple values", func(t *testing.T) {
		t.Parallel()
		iter := session.Query(`SELECT name, age FROM bigtabledevinstance.user_info WHERE age IN ? ALLOW FILTERING`, []int64{298, 82}).Iter()
		results, err := iter.SliceMap()
		require.NoError(t, err)

		expected := []map[string]interface{}{
			{"name": "Rahul", "age": int64(298)},
			{"name": "Vishnu", "age": int64(82)},
		}
		assert.ElementsMatch(t, expected, results)
	})

	t.Run("IN on partition key", func(t *testing.T) {
		t.Parallel()
		// IN on a partition key is a valid and efficient query.
		iter := session.Query(`SELECT name, age FROM bigtabledevinstance.user_info WHERE name IN ?`, []string{"Vishnu"}).Iter()
		results, err := iter.SliceMap()
		if testTarget == TestTargetCassandra {
			require.Error(t, err)
			assert.Equal(t, "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING", err.Error())
		} else {
			require.NoError(t, err)
			expected := []map[string]interface{}{
				{"name": "Vishnu", "age": int64(82)},
			}
			assert.ElementsMatch(t, expected, results)
		}
	})

	t.Run("IN on non-key column requires ALLOW FILTERING", func(t *testing.T) {
		t.Parallel()
		// This query on a non-key 'code' column should fail without ALLOW FILTERING.
		iter := session.Query(`SELECT name, age FROM bigtabledevinstance.user_info WHERE code IN ?`, []int{11}).Iter()
		results, err := iter.SliceMap()
		if testTarget == TestTargetCassandra {
			require.Error(t, err, "Expected an error for IN on a non-indexed column")
		} else {
			require.NoError(t, err)
			expected := []map[string]interface{}{
				{"name": "Ram", "age": int64(10)},
			}
			assert.ElementsMatch(t, expected, results)
		}
	})

	t.Run("IN on non-key double column requires ALLOW FILTERING", func(t *testing.T) {
		t.Parallel()
		iter := session.Query(`SELECT name, age FROM bigtabledevinstance.user_info WHERE credited IN ?`, []float64{12.14127}).Iter()
		results, err := iter.SliceMap()
		if testTarget == TestTargetCassandra {
			require.Error(t, err, "Expected an error for IN on a non-indexed column")
		} else {
			require.NoError(t, err)
			expected := []map[string]interface{}{
				{"name": "Ram", "age": int64(45)},
			}
			assert.ElementsMatch(t, expected, results)
		}
	})
}
