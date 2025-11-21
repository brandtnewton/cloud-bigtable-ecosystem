package schemaMapping

import (
	"testing"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/stretchr/testify/assert"
)

func TestTableConfig_Describe(t *testing.T) {
	tests := []struct {
		name  string
		table *TableConfig
		want  string
	}{
		{
			name: "Success",
			table: NewTableConfig("keyspace1", "table1", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{
					Name:         "name",
					ColumnFamily: "cf1",
					CQLType:      types.TypeVarchar,
					IsPrimaryKey: false,
					PkPrecedence: 0,
					KeyType:      types.KeyTypeRegular,
				},
				{
					Name:         "org_id",
					ColumnFamily: "cf1",
					CQLType:      types.TypeBigint,
					IsPrimaryKey: true,
					PkPrecedence: 1,
					KeyType:      types.KeyTypePartition,
				},
				{
					Name:         "user_id",
					ColumnFamily: "cf1",
					CQLType:      types.TypeBigint,
					IsPrimaryKey: true,
					PkPrecedence: 2,
					KeyType:      types.KeyTypeClustering,
				},
			}),
			want: "CREATE TABLE keyspace1.table1 (\n    org_id BIGINT,\n    user_id BIGINT,\n    name VARCHAR,\n    PRIMARY KEY (org_id, user_id)\n);",
		},
		{
			name: "two partition keys",
			table: NewTableConfig("keyspace1", "table1", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{
					Name:         "org_id",
					ColumnFamily: "cf1",
					CQLType:      types.TypeBigint,
					IsPrimaryKey: true,
					PkPrecedence: 1,
					KeyType:      types.KeyTypePartition,
				},
				{
					Name:         "user_id",
					ColumnFamily: "cf1",
					CQLType:      types.TypeBigint,
					IsPrimaryKey: true,
					PkPrecedence: 2,
					KeyType:      types.KeyTypePartition,
				},
				{
					Name:         "group_id",
					ColumnFamily: "cf1",
					CQLType:      types.TypeBigint,
					IsPrimaryKey: true,
					PkPrecedence: 3,
					KeyType:      types.KeyTypeClustering,
				},
				{
					Name:         "name",
					ColumnFamily: "cf1",
					CQLType:      types.TypeVarchar,
					IsPrimaryKey: false,
					PkPrecedence: 0,
					KeyType:      types.KeyTypeRegular,
				},
			}),
			want: "CREATE TABLE keyspace1.table1 (\n    org_id BIGINT,\n    user_id BIGINT,\n    group_id BIGINT,\n    name VARCHAR,\n    PRIMARY KEY ((org_id, user_id), group_id)\n);",
		},
		{
			name: "one partition key",
			table: NewTableConfig("keyspace1", "table1", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{
					Name:         "org_id",
					ColumnFamily: "cf1",
					CQLType:      types.TypeBigint,
					IsPrimaryKey: true,
					PkPrecedence: 1,
					KeyType:      types.KeyTypePartition,
				},
				{
					Name:         "user_id",
					ColumnFamily: "cf1",
					CQLType:      types.TypeBigint,
					IsPrimaryKey: false,
					PkPrecedence: 0,
					KeyType:      types.KeyTypeRegular,
				},
				{
					Name:         "name",
					ColumnFamily: "cf1",
					CQLType:      types.TypeVarchar,
					IsPrimaryKey: false,
					PkPrecedence: 0,
					KeyType:      types.KeyTypeRegular,
				},
			}),
			want: "CREATE TABLE keyspace1.table1 (\n    org_id BIGINT,\n    user_id BIGINT,\n    name VARCHAR,\n    PRIMARY KEY (org_id)\n);",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.table.Describe()
			assert.Equal(t, tt.want, got)
		})
	}
}
