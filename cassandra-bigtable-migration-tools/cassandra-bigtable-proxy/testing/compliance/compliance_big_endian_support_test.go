package compliance

import (
	"math"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBigEndianDataSuccessCase(t *testing.T) {
	if testTarget == TestTargetCassandra {
		t.Skip()
		return
	}
	t.Parallel()
	err := session.Query(`INSERT INTO orders_big_endian_encoded (user_id, order_num, name) VALUES (?, ?, ?)`, "valid_int", int64(30), "foo").Exec()
	require.NoError(t, err)

	var userId string
	var orderNum int64
	var name string
	err = session.Query(`SELECT user_id, order_num, name FROM orders_big_endian_encoded WHERE user_id = ? AND order_num = ?`, "valid_int", int64(30)).Scan(&userId, &orderNum, &name)
	require.NoError(t, err, "Failed to select newly inserted record")
	assert.Equal(t, "valid_int", userId)
	assert.Equal(t, int64(30), orderNum)
	assert.Equal(t, "foo", name)
}

func TestBigEndianDataFailsOnNegativeIntKeys(t *testing.T) {
	if testTarget == TestTargetCassandra {
		t.Skip()
		return
	}
	t.Parallel()
	err := session.Query(`INSERT INTO orders_big_endian_encoded (user_id, order_num, name) VALUES (?, ?, ?)`, "invalid_int", int64(-1), "bad").Exec()
	require.Error(t, err)
	require.Contains(t, err.Error(), "row keys with big endian encoding cannot contain negative integer values")

	var name string
	err = session.Query(`SELECT  name FROM orders_big_endian_encoded WHERE user_id = ? AND order_num = ?`, "invalid_int", int64(30)).Scan(&name)
	assert.Equal(t, gocql.ErrNotFound, err, "not found because the insert should have failed")
}

func TestLexicographicOrderBigEndian(t *testing.T) {
	if testTarget == TestTargetCassandra {
		t.Skip()
		return
	}
	require.NoError(t, session.Query("CREATE TABLE IF NOT EXISTS lex_test_ordered_code_big_endian (org BIGINT, id INT, row_index INT, PRIMARY KEY (org, id)) WITH int_row_key_encoding='big_endian'").Exec())
	require.NoError(t, session.Query("TRUNCATE TABLE lex_test_ordered_code_big_endian").Exec())

	values := []map[string]interface{}{
		{"org": int64(0), "id": int32(0)},
		{"org": int64(0), "id": int32(1)},
		{"org": int64(0), "id": int32(2)},
		{"org": int64(1), "id": int32(0)},
		{"org": int64(1), "id": int32(1)},
		{"org": int64(1000), "id": int32(0)},
		{"org": int64(99999), "id": int32(0)},
		{"org": int64(math.MaxInt64 - 1), "id": int32(0)},
		{"org": int64(math.MaxInt64), "id": int32(0)},
		{"org": int64(math.MaxInt64), "id": int32(1)},
		{"org": int64(math.MaxInt64), "id": int32(1000)},
		{"org": int64(math.MaxInt64), "id": int32(99999)},
		{"org": int64(math.MaxInt64), "id": int32(math.MaxInt32 - 1)},
		{"org": int64(math.MaxInt64), "id": int32(math.MaxInt32)},
	}

	testLexOrder(t, values, "lex_test_ordered_code_big_endian")
}
