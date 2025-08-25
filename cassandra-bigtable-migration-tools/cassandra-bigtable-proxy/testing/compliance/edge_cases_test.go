package compliance

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInt64RowKeyMaxValue(t *testing.T) {
	t.Parallel()
	err := session.Query("insert into bigtabledevinstance.multiple_int_keys (user_id, order_num, name) values (?, ?, ?)", int64(math.MaxInt64), int32(math.MaxInt32), "maxValue").Exec()
	require.NoError(t, err)

	var userId int64
	var orderNum int32
	var name string
	require.NoError(t, session.Query("select user_id, order_num, name from bigtabledevinstance.multiple_int_keys where user_id=? and order_num=?", int64(math.MaxInt64), int32(math.MaxInt32)).Scan(&userId, &orderNum, &name))

	assert.Equal(t, "maxValue", name)
	assert.Equal(t, int64(math.MaxInt64), userId)
	assert.Equal(t, int32(math.MaxInt32), orderNum)
}

func TestInt64RowKeyMinValue(t *testing.T) {
	t.Parallel()
	err := session.Query("insert into bigtabledevinstance.multiple_int_keys (user_id, order_num, name) values (?, ?, ?)", int64(math.MinInt64), int32(math.MinInt32), "minValue").Exec()

	// we don't care about validating the cassandra error message, just that we got an error
	if testTarget == TestTargetCassandra {
		return
	}
	// note: this test will need to be updated when ordered byte encoding becomes the default
	require.Error(t, err)
	assert.Contains(t, err.Error(), "row keys cannot contain negative integer values until ordered byte encoding is supported")
}
