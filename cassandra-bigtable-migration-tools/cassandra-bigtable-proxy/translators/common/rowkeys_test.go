package common

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/testing/mockdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBindRowKey(t *testing.T) {
	tests := []struct {
		name   string
		table  *schemaMapping.TableConfig
		values *types.QueryParameterValues
		want   types.RowKey
		err    string
	}{
		{
			name:  "success",
			table: mockdata.GetTableOrDie("test_keyspace", "test_table"),
			values: mockdata.CreateQueryParameterValuesFromMap(map[*types.Column]types.GoValue{
				mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"): "abc",
				mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"): "def",
			}),
			want: "abc\x00\x01def",
		},
		{
			name:  "mixed types",
			table: mockdata.GetTableOrDie("test_keyspace", "user_info"),
			values: mockdata.CreateQueryParameterValuesFromMap(map[*types.Column]types.GoValue{
				mockdata.GetColumnOrDie("test_keyspace", "user_info", "name"): "bobby",
				mockdata.GetColumnOrDie("test_keyspace", "user_info", "age"):  int64(14242),
			}),
			want: "bobby\x00\x01\xe07\xa2",
		},
		{
			name:  "missing value",
			table: mockdata.GetTableOrDie("test_keyspace", "test_table"),
			values: mockdata.CreateQueryParameterValuesFromMap(map[*types.Column]types.GoValue{
				mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"): "def",
			}),
			want: "",
			err:  "missing primary key `pk1`",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rowKey, err := BindRowKey(tt.table, tt.values)
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
