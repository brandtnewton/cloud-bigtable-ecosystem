package compliance

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// note: gocql doesn't support querying these tables
func TestSystemTables(t *testing.T) {
	t.Parallel()

	mapResults, err := cqlshScanToMap("SELECT * FROM system_schema.tables")
	require.NoError(t, err)
	var results []string = nil
	for _, r := range mapResults {
		results = append(results, fmt.Sprintf("%s.%s", r["keyspace_name"], r["table_name"]))
	}

	assert.Contains(t, results, "bigtabledevinstance.user_info")
	assert.Contains(t, results, "bigtabledevinstance.orders")
	assert.Contains(t, results, "bigtabledevinstance.aggregation_grouping_test")
	assert.Contains(t, results, "bigtabledevinstance.multiple_int_keys")
	assert.Contains(t, results, "bigtabledevinstance.test_int_key")
	assert.Contains(t, results, "bigtabledevinstance.social_posts")
	if testTarget == TestTargetProxy {
		assert.Contains(t, results, "bigtabledevinstance.orders_big_endian_encoded")
	}
}

// note: gocql doesn't support querying these tables
func TestSystemColumns(t *testing.T) {
	t.Parallel()

	mapResults, err := cqlshScanToMap("SELECT * FROM system_schema.columns")
	require.NoError(t, err)
	var results = make(map[string]map[string]string)
	for _, r := range mapResults {
		key := fmt.Sprintf("%s.%s.%s", r["keyspace_name"], r["table_name"], r["column_name"])
		results[key] = r
	}

	assert.Equal(t, map[string]string{
		"keyspace_name":    "bigtabledevinstance",
		"table_name":       "orders",
		"column_name":      "user_id",
		"kind":             "partition_key",
		"clustering_order": "none",
		"type":             "varchar",
		"position":         "0",
	}, results["bigtabledevinstance.orders.user_id"])
	assert.Equal(t, map[string]string{
		"keyspace_name":    "bigtabledevinstance",
		"table_name":       "orders",
		"column_name":      "order_num",
		"kind":             "clustering",
		"clustering_order": "none",
		"type":             "int",
		"position":         "1",
	}, results["bigtabledevinstance.orders.order_num"])
	assert.Equal(t, map[string]string{
		"keyspace_name":    "bigtabledevinstance",
		"table_name":       "orders",
		"column_name":      "name",
		"kind":             "regular",
		"clustering_order": "none",
		"type":             "varchar",
		"position":         "-1",
	}, results["bigtabledevinstance.orders.name"])
}
