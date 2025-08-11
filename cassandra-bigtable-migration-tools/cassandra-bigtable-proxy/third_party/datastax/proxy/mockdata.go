package proxy

import (
	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
)

func GetSchemaMappingConfig() *schemaMapping.SchemaMappingConfig {
	return &schemaMapping.SchemaMappingConfig{
		Tables:             mockTableConfig,
		SystemColumnFamily: "cf1",
	}
}

var mockTableConfig = map[string]map[string]*schemaMapping.TableConfig{
	"test_keyspace": {"test_table": &schemaMapping.TableConfig{
		Keyspace: "test_keyspace",
		Name:     "test_table",
		Logger:   nil,
		Columns: map[string]*types.Column{
			"column1": &types.Column{
				ColumnName:   "column1",
				CQLType:      datatype.Varchar,
				IsPrimaryKey: true,
				PkPrecedence: 1,
			},
			"column2": &types.Column{
				ColumnName:   "column2",
				CQLType:      datatype.Blob,
				IsPrimaryKey: false,
			},
			"column3": &types.Column{
				ColumnName:   "column3",
				CQLType:      datatype.Boolean,
				IsPrimaryKey: false,
			},
			"column4": &types.Column{
				ColumnName:   "column4",
				CQLType:      datatype.NewListType(datatype.Varchar),
				IsPrimaryKey: false,
				IsCollection: true,
			},
			"column5": &types.Column{
				ColumnName:   "column5",
				CQLType:      datatype.Timestamp,
				IsPrimaryKey: false,
			},
			"column6": &types.Column{
				ColumnName:   "column6",
				CQLType:      datatype.Int,
				IsPrimaryKey: false,
			},
			"column7": &types.Column{
				ColumnName:   "column7",
				CQLType:      datatype.NewSetType(datatype.Varchar),
				IsPrimaryKey: false,
				IsCollection: true,
			},
			"column8": &types.Column{
				ColumnName:   "column8",
				CQLType:      datatype.NewMapType(datatype.Varchar, datatype.Boolean),
				IsPrimaryKey: false,
				IsCollection: true,
			},
			"column9": &types.Column{
				ColumnName:   "column9",
				CQLType:      datatype.Bigint,
				IsPrimaryKey: false,
			},
			"column10": &types.Column{
				ColumnName:   "column10",
				CQLType:      datatype.Varchar,
				IsPrimaryKey: true,
				PkPrecedence: 2,
			},
			"column11": &types.Column{
				ColumnName:   "column11",
				CQLType:      datatype.NewSetType(datatype.Varchar),
				IsPrimaryKey: false,
				IsCollection: true,
			},
		},
		PrimaryKeys: []*types.Column{
			{
				ColumnName:   "column1",
				CQLType:      datatype.Varchar,
				IsPrimaryKey: true,
				PkPrecedence: 1,
			},
			{
				ColumnName:   "column10",
				CQLType:      datatype.Varchar,
				IsPrimaryKey: true,
				PkPrecedence: 2,
			},
		},
	},
		"user_info": &schemaMapping.TableConfig{
			Keyspace: "test_keyspace",
			Name:     "user_info",
			Logger:   nil,
			Columns: map[string]*types.Column{
				"name": &types.Column{
					ColumnName:   "name",
					CQLType:      datatype.Varchar,
					IsPrimaryKey: true,
					PkPrecedence: 0,
					IsCollection: false,
				},
			},
			PrimaryKeys: []*types.Column{
				{
					ColumnName:   "name",
					CQLType:      datatype.Varchar,
					IsPrimaryKey: true,
					PkPrecedence: 0,
				},
			},
		},
	},
}
