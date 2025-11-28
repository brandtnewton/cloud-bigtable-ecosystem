package select_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCastColumns(t *testing.T) {
	tests := []struct {
		name         string
		colMeta      *types.Column
		columnFamily string
		want         string
		wantErr      bool
	}{
		{
			name: "integer type",
			colMeta: &types.Column{
				Name:    "age",
				CQLType: types.TypeInt,
			},
			columnFamily: "cf1",
			want:         "TO_INT64(cf1['age'])",
			wantErr:      false,
		},
		{
			name: "bigint type",
			colMeta: &types.Column{
				Name:    "timestamp",
				CQLType: types.TypeBigInt,
			},
			columnFamily: "cf1",
			want:         "TO_INT64(cf1['timestamp'])",
			wantErr:      false,
		},
		{
			name: "float type",
			colMeta: &types.Column{
				Name:    "price",
				CQLType: types.TypeFloat,
			},
			columnFamily: "cf1",
			want:         "TO_FLOAT32(cf1['price'])",
			wantErr:      false,
		},
		{
			name: "double type",
			colMeta: &types.Column{
				Name:    "value",
				CQLType: types.TypeDouble,
			},
			columnFamily: "cf1",
			want:         "TO_FLOAT64(cf1['value'])",
			wantErr:      false,
		},
		{
			name: "boolean type",
			colMeta: &types.Column{
				Name:    "active",
				CQLType: types.TypeBoolean,
			},
			columnFamily: "cf1",
			want:         "TO_INT64(cf1['active'])",
			wantErr:      false,
		},
		{
			name: "timestamp type",
			colMeta: &types.Column{
				Name:    "created_at",
				CQLType: types.TypeTimestamp,
			},
			columnFamily: "cf1",
			want:         "TO_TIME(cf1['created_at'])",
			wantErr:      false,
		},
		{
			name: "blob type",
			colMeta: &types.Column{
				Name:    "data",
				CQLType: types.TypeBlob,
			},
			columnFamily: "cf1",
			want:         "TO_BLOB(cf1['data'])",
			wantErr:      false,
		},
		{
			name: "text type",
			colMeta: &types.Column{
				Name:    "name",
				CQLType: types.TypeVarchar,
			},
			columnFamily: "cf1",
			want:         "cf1['name']",
			wantErr:      false,
		},
		{
			name: "handle special characters in column name",
			colMeta: &types.Column{
				Name:    "special-name",
				CQLType: types.TypeVarchar,
			},
			columnFamily: "cf1",
			want:         "cf1['special-name']",
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CastScalarColumn(tt.colMeta)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
