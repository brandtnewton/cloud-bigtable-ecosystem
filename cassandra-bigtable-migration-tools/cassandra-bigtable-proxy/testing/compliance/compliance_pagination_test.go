/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package compliance

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBasicPagination(t *testing.T) {
	t.Parallel()
	tableName := uniqueTableName("pagination_test")
	err := session.Query(fmt.Sprintf(`CREATE TABLE %s (id text, val int, PRIMARY KEY (id, val))`, tableName)).Exec()
	require.NoError(t, err)
	defer cleanupTable(t, tableName)

	// Insert 10 rows for a single partition
	id := "partition1"
	for i := 0; i < 10; i++ {
		err = session.Query(fmt.Sprintf(`INSERT INTO %s (id, val) VALUES (?, ?)`, tableName), id, i).Exec()
		require.NoError(t, err)
	}

	// Read with page size 3
	// We expect multiple pages if pagination is supported.
	pageSize := 3
	var results []int
	var pageCount int
	var pageState []byte

	for {
		iter := session.Query(fmt.Sprintf(`SELECT val FROM %s WHERE id = ?`, tableName), id).
			PageSize(pageSize).
			PageState(pageState).
			Iter()

		var val int
		rowsInPage := 0
		for iter.Scan(&val) {
			results = append(results, val)
			rowsInPage++
		}

		pageCount++
		pageState = iter.PageState()
		err = iter.Close()
		require.NoError(t, err)

		if len(pageState) == 0 {
			break
		}

		assert.LessOrEqual(t, rowsInPage, pageSize)
		
		// Safety break to prevent infinite loops if something is wrong with PagingState
		if pageCount > 100 {
			t.Fatal("Too many pages, possible infinite loop")
		}
	}

	assert.Equal(t, 10, len(results))
	for i := 0; i < 10; i++ {
		assert.Equal(t, i, results[i])
	}

	if testTarget == TestTargetCassandra {
		assert.Greater(t, pageCount, 1, "Cassandra should have returned more than one page")
	} else {
		t.Logf("Proxy returned %d pages for 10 rows with page size %d", pageCount, pageSize)
	}
}

func TestPaginationWithLimit(t *testing.T) {
	t.Parallel()
	tableName := uniqueTableName("pagination_limit_test")
	err := session.Query(fmt.Sprintf(`CREATE TABLE %s (id text, val int, PRIMARY KEY (id, val))`, tableName)).Exec()
	require.NoError(t, err)
	defer cleanupTable(t, tableName)

	// Insert 20 rows
	id := "partition1"
	for i := 0; i < 20; i++ {
		err = session.Query(fmt.Sprintf(`INSERT INTO %s (id, val) VALUES (?, ?)`, tableName), id, i).Exec()
		require.NoError(t, err)
	}

	// Query with LIMIT 10 and PageSize 3
	pageSize := 3
	limit := 10
	var results []int
	var pageState []byte
	pageCount := 0

	for {
		iter := session.Query(fmt.Sprintf(`SELECT val FROM %s WHERE id = ? LIMIT %d`, tableName, limit), id).
			PageSize(pageSize).
			PageState(pageState).
			Iter()

		var val int
		for iter.Scan(&val) {
			results = append(results, val)
		}

		pageCount++
		pageState = iter.PageState()
		err = iter.Close()
		require.NoError(t, err)

		if len(pageState) == 0 {
			break
		}
		
		if pageCount > 100 {
			t.Fatal("Too many pages, possible infinite loop")
		}
	}

	assert.Equal(t, limit, len(results))
	for i := 0; i < limit; i++ {
		assert.Equal(t, i, results[i])
	}
}

func TestPaginationAcrossPartitions(t *testing.T) {
	t.Parallel()
	tableName := uniqueTableName("pagination_multi_partition")
	err := session.Query(fmt.Sprintf(`CREATE TABLE %s (id text PRIMARY KEY, val int)`, tableName)).Exec()
	require.NoError(t, err)
	defer cleanupTable(t, tableName)

	// Insert 10 rows with different partitions
	for i := 0; i < 10; i++ {
		err = session.Query(fmt.Sprintf(`INSERT INTO %s (id, val) VALUES (?, ?)`, tableName), fmt.Sprintf("p%d", i), i).Exec()
		require.NoError(t, err)
	}

	// Read all rows with page size 4
	pageSize := 4
	var results = make(map[int]bool)
	var pageState []byte
	pageCount := 0

	for {
		iter := session.Query(fmt.Sprintf(`SELECT val FROM %s`, tableName)).
			PageSize(pageSize).
			PageState(pageState).
			Iter()

		var val int
		for iter.Scan(&val) {
			results[val] = true
		}

		pageCount++
		pageState = iter.PageState()
		err = iter.Close()
		require.NoError(t, err)

		if len(pageState) == 0 {
			break
		}
		
		if pageCount > 100 {
			t.Fatal("Too many pages, possible infinite loop")
		}
	}

	assert.Equal(t, 10, len(results))
	for i := 0; i < 10; i++ {
		assert.True(t, results[i], "Missing value %d", i)
	}
}

func TestPaginationWithOrderBy(t *testing.T) {
	t.Parallel()
	tableName := uniqueTableName("pagination_order_by")
	err := session.Query(fmt.Sprintf(`CREATE TABLE %s (id text, val int, PRIMARY KEY (id, val))`, tableName)).Exec()
	require.NoError(t, err)
	defer cleanupTable(t, tableName)

	// Insert 10 rows
	id := "partition1"
	for i := 0; i < 10; i++ {
		err = session.Query(fmt.Sprintf(`INSERT INTO %s (id, val) VALUES (?, ?)`, tableName), id, i).Exec()
		require.NoError(t, err)
	}

	// Read with ORDER BY val DESC and page size 3
	pageSize := 3
	var results []int
	var pageState []byte
	pageCount := 0

	for {
		iter := session.Query(fmt.Sprintf(`SELECT val FROM %s WHERE id = ? ORDER BY val DESC`, tableName), id).
			PageSize(pageSize).
			PageState(pageState).
			Iter()

		var val int
		for iter.Scan(&val) {
			results = append(results, val)
		}

		pageCount++
		pageState = iter.PageState()
		err = iter.Close()
		require.NoError(t, err)

		if len(pageState) == 0 {
			break
		}

		if pageCount > 100 {
			t.Fatal("Too many pages, possible infinite loop")
		}
	}

	assert.Equal(t, 10, len(results))
	for i := 0; i < 10; i++ {
		assert.Equal(t, 9-i, results[i])
	}
}
