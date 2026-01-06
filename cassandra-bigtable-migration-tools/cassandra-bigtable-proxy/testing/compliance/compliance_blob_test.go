package compliance

import (
	"crypto/rand"
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

func TestWriteALargeBlob(t *testing.T) {
	t.Parallel()

	pk := []byte{0x01}

	size := 1 * 1024 * 1024 // 1MB
	val := make([]byte, size)

	// fill random data
	var _, err = rand.Read(val)
	if err != nil {
		panic(err)
	}
	name := "big-blob"
	require.NoError(t, session.Query(`INSERT INTO blob_table (pk, val, name) VALUES (?, ?, ?)`, pk, val, name).Exec())

	var gotPk []byte
	var gotVal []byte
	var gotName string
	require.NoError(t, session.Query(`SELECT pk, val, name FROM blob_table WHERE pk=?`, pk).Scan(&gotPk, &gotVal, &gotName))
	assert.Equal(t, pk, gotPk)
	assert.Equal(t, val, gotVal)
	assert.Equal(t, name, gotName)
}

func TestBlobKeyOrder(t *testing.T) {
	t.Parallel()

	blobs := [][]byte{
		{0x00},
		{0x01},
		{0x01, 0x00},
		{0x02},
		{0x02, 0x00},
	}
	name := "blob-order"

	batch := session.NewBatch(gocql.LoggedBatch)
	for _, pk := range blobs {
		batch.Query(
			"INSERT INTO blob_table (pk, val, name) VALUES (?, ?, ?)", pk, []byte{0x01}, name)
	}
	require.NoError(t, session.ExecuteBatch(batch))

	scanner := session.Query(`SELECT pk, val, name FROM blob_table WHERE name=? ALLOW FILTERING`, name).Iter().Scanner()
	var got [][]byte = nil
	for scanner.Next() {
		var b []byte
		err := scanner.Scan(
			&b,
		)
		require.NoError(t, err)
		got = append(got, b)
	}
	require.NoError(t, scanner.Err())

	assert.Equal(t, blobs, got)
}

func TestBlobEdgeCases(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		pk       []byte
		val      []byte
		writeErr string
	}{
		{
			name:     "empty key",
			pk:       []byte{},
			val:      []byte{0x01},
			writeErr: "Row keys must be non-empty",
		},
		{
			name:     "empty col",
			pk:       []byte{0x01},
			val:      nil,
			writeErr: "",
		},
		{
			name:     "null bytes",
			pk:       []byte{0x00},
			val:      []byte{0x00},
			writeErr: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := session.Query(`INSERT INTO blob_table (pk, val, name) VALUES (?, ?, ?)`, tc.pk, tc.val, "edge-case").Exec()
			if tc.writeErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.writeErr)
				return
			}

			require.NoError(t, err)

			var gotPk []byte
			var gotVal []byte
			require.NoError(t, session.Query(`SELECT pk, val FROM blob_table WHERE pk=?`, tc.pk).Scan(&gotPk, &gotVal))
			assert.Equal(t, tc.pk, gotPk)
			assert.Equal(t, tc.val, gotVal)
		})
	}
}
