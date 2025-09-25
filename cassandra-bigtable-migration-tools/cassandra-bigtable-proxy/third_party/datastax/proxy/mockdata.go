package proxy

import (
	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"go.uber.org/zap"
)

func GetSchemaMappingConfig() *schemaMapping.SchemaMappingConfig {
	const systemColumnFamily = "cf1"

	testTableColumns := []*types.Column{
		{Name: "column1", TypeInfo: datatype.Varchar, KeyType: utilities.KEY_TYPE_PARTITION, PkPrecedence: 1},
		{Name: "column10", TypeInfo: datatype.Varchar, KeyType: utilities.KEY_TYPE_CLUSTERING, PkPrecedence: 2},
		{Name: "column2", TypeInfo: datatype.Blob, KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column3", TypeInfo: datatype.Boolean, KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column4", TypeInfo: datatype.NewListType(datatype.Varchar), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column5", TypeInfo: datatype.Timestamp, KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column6", TypeInfo: datatype.Int, KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column7", TypeInfo: datatype.NewSetType(datatype.Varchar), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column8", TypeInfo: datatype.NewMapType(datatype.Varchar, datatype.Boolean), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column9", TypeInfo: datatype.Bigint, KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column11", TypeInfo: datatype.NewSetType(datatype.Varchar), KeyType: utilities.KEY_TYPE_REGULAR},
	}

	userInfoColumns := []*types.Column{
		{Name: "name", TypeInfo: datatype.Varchar, KeyType: utilities.KEY_TYPE_PARTITION, PkPrecedence: 0},
		{Name: "age", TypeInfo: datatype.Varchar, KeyType: utilities.KEY_TYPE_REGULAR},
	}

	allTableConfigs := []*schemaMapping.TableConfig{
		schemaMapping.NewTableConfig("test_keyspace", "test_table", systemColumnFamily, types.OrderedCodeEncoding, testTableColumns),
		schemaMapping.NewTableConfig("test_keyspace", "user_info", systemColumnFamily, types.OrderedCodeEncoding, userInfoColumns),
	}

	return schemaMapping.NewSchemaMappingConfig("schema_mapping", systemColumnFamily, zap.NewNop(), allTableConfigs)
}
