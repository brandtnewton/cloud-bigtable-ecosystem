package compliance

import (
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBlobs(t *testing.T) {
	t.Parallel()

	pk := []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46, 0x00, 0x01, 0x01, 0x01, 0x00, 0x48}
	val := []byte{0x01, 0x01, 0x01, 0x01, 0x00, 0x48, 0x46, 0x49, 0x46, 0x00}
	name := "basic"
	require.NoError(t, session.Query(`INSERT INTO blob_table (pk, val, name) VALUES (?, ?, ?)`, pk, val, name).Exec())

	var gotPk []byte
	var gotVal []byte
	var gotName string
	require.NoError(t, session.Query(`SELECT pk, val, name FROM blob_table WHERE pk=?`, pk).Scan(&gotPk, &gotVal, &gotName))
	assert.Equal(t, pk, gotPk)
	assert.Equal(t, val, gotVal)
	assert.Equal(t, name, gotName)

	val2 := []byte{0x01, 0xab, 0x01, 0x01, 0x00, 0x32, 0x46, 0x49, 0x46, 0x11}
	require.NoError(t, session.Query(`UPDATE blob_table SET val=? WHERE pk=?`, val2, pk).Exec())

	require.NoError(t, session.Query(`SELECT pk, val, name FROM blob_table WHERE pk=?`, pk).Scan(&gotPk, &gotVal, &gotName))
	assert.Equal(t, pk, gotPk)
	assert.Equal(t, val2, gotVal)
	assert.Equal(t, name, gotName)

	require.NoError(t, session.Query(`DELETE FROM blob_table WHERE pk=?`, pk).Exec())

	err := session.Query(`SELECT pk, val, name FROM blob_table WHERE pk=?`, pk).Scan(&gotPk, &gotVal, &gotName)
	require.Error(t, err)
	assert.Equal(t, gocql.ErrNotFound, err)
}
