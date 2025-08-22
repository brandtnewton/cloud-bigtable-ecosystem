package compliance

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInt64RowKeyMaxValue(t *testing.T) {
	require.NoError(t, session.Query("insert into bigtabledevinstance.user_info (name, age, code) values (?, ?, ?)", "id", int64(math.MaxInt64), 1).Exec())

	var name string
	var age int64
	var code int32
	require.NoError(t, session.Query("select name, age, code from bigtabledevinstance.user_info where name=? and age=?", "id", int64(math.MaxInt64)).Scan(&name, &age, &code))

	assert.Equal(t, "id", name)
	assert.Equal(t, int64(math.MaxInt64), age)
	assert.Equal(t, int32(1), code)
}

func TestInt64RowKeyMinValue(t *testing.T) {
	err := session.Query("insert into bigtabledevinstance.user_info (name, age, code) values (?, ?, ?)", "id", int64(math.MinInt64), -1).Exec()
	// note: this test will need to be updated when ordered byte encoding becomes the default
	require.Error(t, err)
	assert.Contains(t, err.Error(), "row keys cannot contain negative integer values until ordered byte encoding is supported")
}
