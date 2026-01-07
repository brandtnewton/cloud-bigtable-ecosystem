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
	require.NoError(t, session.Query(`INSERT INTO blob_table (pk, name, val) VALUES (?, ?, ?)`, pk, name, val).Exec())

	var gotPk []byte
	var gotVal []byte
	var gotName string
	require.NoError(t, session.Query(`SELECT pk, name, val FROM blob_table WHERE pk=? AND name=?`, pk, name).Scan(&gotPk, &gotName, &gotVal))
	assert.Equal(t, pk, gotPk)
	assert.Equal(t, val, gotVal)
	assert.Equal(t, name, gotName)

	val2 := []byte{0x01, 0xab, 0x01, 0x01, 0x00, 0x32, 0x46, 0x49, 0x46, 0x11}
	require.NoError(t, session.Query(`UPDATE blob_table SET val=? WHERE pk=? AND name=?`, val2, pk, name).Exec())

	require.NoError(t, session.Query(`SELECT pk, name, val FROM blob_table WHERE pk=? AND name=?`, pk, name).Scan(&gotPk, &gotName, &gotVal))
	assert.Equal(t, pk, gotPk)
	assert.Equal(t, val2, gotVal)
	assert.Equal(t, name, gotName)

	require.NoError(t, session.Query(`DELETE FROM blob_table WHERE pk=? AND name=?`, pk, name).Exec())

	err := session.Query(`SELECT pk, name, val FROM blob_table WHERE pk=? AND name=?`, pk, name).Scan(&gotPk, &gotName, &gotVal)
	require.Error(t, err)
	assert.Equal(t, gocql.ErrNotFound, err)
}
func TestBlobLiteral(t *testing.T) {
	t.Parallel()
	pkBytes := []byte{0x39, 0x38, 0x32, 0x33, 0x66, 0x6a, 0x61, 0x73, 0x2c, 0x76, 0x6d, 0x32, 0x66}
	valBytes := []byte{0x70, 0x6b, 0x6a, 0x78, 0x7a}

	require.NoError(t, session.Query(`INSERT INTO blob_table (pk, name, val) VALUES (0x39383233666A61732C766D3266, 'literal', 0x706B6A787A)`).Exec())

	var gotPk []byte
	var gotName string
	var gotVal []byte
	require.NoError(t, session.Query(`SELECT pk, name, val FROM blob_table WHERE pk=0x39383233666A61732C766D3266 AND val=0x706B6A787A ALLOW FILTERING`).Scan(&gotPk, &gotName, &gotVal))
	assert.Equal(t, pkBytes, gotPk)
	assert.Equal(t, "literal", gotName)
	assert.Equal(t, valBytes, gotVal)
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
	require.NoError(t, session.Query(`INSERT INTO blob_table (pk, name, val) VALUES (?, ?, ?)`, pk, name, val).Exec())

	var gotPk []byte
	var gotVal []byte
	var gotName string
	require.NoError(t, session.Query(`SELECT pk, name, val FROM blob_table WHERE pk=?`, pk).Scan(&gotPk, &gotName, &gotVal))
	assert.Equal(t, pk, gotPk)
	assert.Equal(t, val, gotVal)
	assert.Equal(t, name, gotName)
}

func TestBlobComparisonOperators(t *testing.T) {
	t.Parallel()

	blobs := [][]byte{
		{0x00},
		{0x01},
		{0x01, 0x00},
		{0x02},
		{0x02, 0x00},
	}
	name := "blob-comparison-operators"

	batch := session.NewBatch(gocql.LoggedBatch)
	for _, pk := range blobs {
		batch.Query(
			"INSERT INTO blob_table (pk, name, val) VALUES (?, ?, ?)", pk, name, []byte{0x01})
	}
	require.NoError(t, session.ExecuteBatch(batch))

	rows, err := readBlobRows(session.Query(`SELECT pk, val, name FROM blob_table where pk <= 0x02 AND name='blob-comparison-operators' ALLOW FILTERING`))
	require.NoError(t, err)

	var got [][]byte = nil
	for _, row := range rows {
		got = append(got, row.pk)
	}
	assert.ElementsMatch(t, [][]byte{
		{0x02},
		{0x01, 0x00},
		{0x01},
		{0x00},
	}, got)
}

func TestBlobEdgeCases(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		input    blobRow
		want     blobRow
		writeErr string
	}{
		{
			name: "empty key",
			input: blobRow{
				pk:   []byte{},
				val:  nil,
				name: "empty key",
			},
			want: blobRow{
				pk:   []byte{},
				val:  nil,
				name: "empty key",
			},
			writeErr: "key may not be empty",
		},
		{
			name: "empty col",
			input: blobRow{
				pk:   []byte{0x01},
				val:  nil,
				name: "empty col",
			},
			want: blobRow{
				pk:   []byte{0x01},
				val:  nil,
				name: "empty col",
			},
			writeErr: "",
		},
		{
			name: "empty val",
			input: blobRow{
				pk:   []byte{0x01},
				val:  []byte{},
				name: "empty val",
			},
			want: blobRow{
				pk:   []byte{0x01},
				val:  nil,
				name: "empty val",
			},
			writeErr: "",
		},
		{
			name: "null val",
			input: blobRow{
				pk:   []byte{0x01},
				val:  nil,
				name: "null val",
			},
			want: blobRow{
				pk:   []byte{0x01},
				val:  nil,
				name: "null val",
			},
			writeErr: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := session.Query(`INSERT INTO blob_table (pk, name, val) VALUES (?, ?, ?)`, tc.input.pk, tc.input.name, tc.input.val).Exec()
			if tc.writeErr != "" {
				require.Error(t, err)
				if testTarget == TestTargetProxy {
					require.Contains(t, err.Error(), tc.writeErr)
				}
				return
			}
			require.NoError(t, err)

			var gotPk []byte
			var gotVal []byte
			var gotName string
			require.NoError(t, session.Query(`SELECT pk, name, val FROM blob_table WHERE pk=? AND name=?`, tc.input.pk, tc.input.name).Scan(&gotPk, &gotName, &gotVal))
			assert.Equal(t, tc.want.pk, gotPk)
			assert.Equal(t, tc.want.name, gotName)
			assert.Equal(t, tc.want.val, gotVal)
		})
	}
}

type blobRow struct {
	pk   []byte
	val  []byte
	name string
}

func readBlobRows(q *gocql.Query) ([]blobRow, error) {
	scanner := q.Iter().Scanner()
	var results []blobRow = nil
	for scanner.Next() {
		blob := blobRow{}
		err := scanner.Scan(
			&blob.pk,
			&blob.name,
			&blob.val,
		)
		if err != nil {
			return nil, err
		}
		results = append(results, blob)
	}
	err := scanner.Err()
	if err != nil {
		return nil, err
	}
	return results, nil
}
