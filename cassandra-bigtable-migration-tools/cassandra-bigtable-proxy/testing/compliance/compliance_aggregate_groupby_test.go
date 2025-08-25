package compliance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComprehensiveGroupByAndAggregateFunctions(t *testing.T) {
	require.NoError(t, session.Query(`INSERT INTO aggregation_grouping_test (region, category, item_id, sale_timestamp, quantity, price, discount, revenue_bigint) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"North", "Electronics", 101, time.UnixMicro(1736899200000), 5, float32(1200.50), 0.1, int64(6002)).Exec())
	require.NoError(t, session.Query(`INSERT INTO aggregation_grouping_test (region, category, item_id, sale_timestamp, quantity, price, discount, revenue_bigint) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"North", "Electronics", 102, time.UnixMicro(1736985600000), 2, float32(850.00), 0.05, int64(1700)).Exec())
	require.NoError(t, session.Query(`INSERT INTO aggregation_grouping_test (region, category, item_id, sale_timestamp, quantity, price, discount, revenue_bigint) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"South", "Apparel", 201, time.UnixMicro(1737072000000), 10, float32(45.99), 0.2, int64(459)).Exec())
	require.NoError(t, session.Query(`INSERT INTO aggregation_grouping_test (region, category, item_id, sale_timestamp, quantity, price, discount, revenue_bigint) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		"North", "Apparel", 202, time.UnixMicro(1737158400000), 15, float32(30.50), 0.15, int64(457)).Exec())

	// 2. Run validation queries
	t.Run("GROUP BY category with SUM and AVG", func(t *testing.T) {
		iter := session.Query(`SELECT category, SUM(quantity) AS total_items, AVG(price) AS avg_price FROM aggregation_grouping_test GROUP BY category`).Iter()
		results, err := iter.SliceMap()
		require.NoError(t, err)
		assert.ElementsMatch(t, []map[string]interface{}{
			{
				"category":    "Apparel",
				"avg_price":   float32(38.245003),
				"total_items": 25,
			},
			{
				"category":    "Electronics",
				"avg_price":   float32(1025.25),
				"total_items": 7,
			},
		}, results)
	})

	t.Run("GROUP BY region with COUNT, MIN, MAX", func(t *testing.T) {
		// As above, this query is invalid in standard Cassandra, and we test for the expected error.
		iter := session.Query(`SELECT region, COUNT(*) AS item_count, MIN(price) AS min_price, MAX(price) AS max_price FROM aggregation_grouping_test GROUP BY region`).Iter()
		results, err := iter.SliceMap()
		if testTarget == TestTargetCassandra {
			// we don't care about validating the cassandra error message, just that we got an error
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			assert.ElementsMatch(t, []map[string]interface{}{
				{
					"region":     "North",
					"min_price":  float32(30.50),
					"max_price":  float32(1200.50),
					"item_count": int64(3),
				},
				{
					"region":     "South",
					"min_price":  float32(45.99),
					"max_price":  float32(45.99),
					"item_count": int64(1),
				},
			}, results)
		}
	})

	t.Run("GROUP BY multiple columns", func(t *testing.T) {
		// This query is VALID in standard Cassandra because it groups by the full partition key.
		iter := session.Query(`SELECT region, category, SUM(revenue_bigint) AS total_revenue, AVG(discount) AS avg_discount FROM aggregation_grouping_test GROUP BY region, category ALLOW FILTERING`).Iter()
		results, err := iter.SliceMap()

		if testTarget == TestTargetCassandra {
			// we don't care about validating the cassandra error message, just that we got an error
			require.Error(t, err)
			return
		}

		require.NoError(t, err)

		// Use a helper to find a specific group in the results for easier validation
		findGroup := func(region, category string) map[string]interface{} {
			for _, r := range results {
				if r["region"] == region && r["category"] == category {
					return r
				}
			}
			return nil
		}

		northElec := findGroup("North", "Electronics")
		require.NotNil(t, northElec)
		assert.Equal(t, int64(7702), northElec["total_revenue"])
		assert.InDelta(t, 0.075, northElec["avg_discount"], 0.001)

		northApparel := findGroup("North", "Apparel")
		require.NotNil(t, northApparel)
		assert.Equal(t, int64(457), northApparel["total_revenue"])
		assert.InDelta(t, 0.15, northApparel["avg_discount"], 0.001)

		southApparel := findGroup("South", "Apparel")
		require.NotNil(t, southApparel)
		assert.Equal(t, int64(459), southApparel["total_revenue"])
		assert.InDelta(t, 0.2, southApparel["avg_discount"], 0.001)
	})

	t.Run("GROUP BY without aliases", func(t *testing.T) {
		iter := session.Query(`SELECT category, SUM(quantity), AVG(price) FROM aggregation_grouping_test GROUP BY category`).Iter()
		results, err := iter.SliceMap()
		require.NoError(t, err)
		assert.ElementsMatch(t, []map[string]interface{}{
			{
				"category":             "Apparel",
				"system.avg(price)":    float32(38.245003),
				"system.sum(quantity)": 25,
			},
			{
				"category":             "Electronics",
				"system.avg(price)":    float32(1025.25),
				"system.sum(quantity)": 7,
			},
		}, results)
	})
}
