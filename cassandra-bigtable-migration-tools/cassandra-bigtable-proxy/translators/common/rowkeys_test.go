package common

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/testing/mockdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestBindRowKey(t *testing.T) {
	tests := []struct {
		name   string
		table  *schemaMapping.TableSchema
		values []types.GoValue
		want   types.RowKey
		err    string
	}{
		{
			name:  "success",
			table: mockdata.GetTableOrDie("test_keyspace", "test_table"),
			values: []types.GoValue{
				"abc",
				"def",
			},
			want: "abc\x00\x01def",
		},
		{
			name:  "mixed types",
			table: mockdata.GetTableOrDie("test_keyspace", "user_info"),
			values: []types.GoValue{
				"bobby",
				int64(14242),
			},
			want: "bobby\x00\x01\xe07\xa2",
		},
		{
			name:  "missing value",
			table: mockdata.GetTableOrDie("test_keyspace", "test_table"),
			values: []types.GoValue{
				"def",
			},
			want: "",
			err:  "missing primary key `pk1`",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var values []types.DynamicValue
			for _, v := range tt.values {
				values = append(values, types.NewLiteralValue(v))
			}
			rowKey, err := BindRowKey(tt.table, values, types.NewQueryParameterValues(types.NewQueryParameters(), time.Now()))
			if tt.err != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, rowKey)
		})
	}
}
