package proxy

import (
	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"go.uber.org/zap"
)

func GetSchemaMappingConfig() *schemaMapping.SchemaMappingConfig {
	const systemColumnFamily = "cf1"

	testTableColumns := []*types.Column{
		{Name: "column1", TypeInfo: utilities.ParseCqlTypeOrDie("varchar"), KeyType: utilities.KEY_TYPE_PARTITION, PkPrecedence: 1},
		{Name: "column10", TypeInfo: utilities.ParseCqlTypeOrDie("varchar"), KeyType: utilities.KEY_TYPE_CLUSTERING, PkPrecedence: 2},
		{Name: "column2", TypeInfo: utilities.ParseCqlTypeOrDie("blob"), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column3", TypeInfo: utilities.ParseCqlTypeOrDie("boolean"), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column4", TypeInfo: utilities.ParseCqlTypeOrDie("list<text>"), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column5", TypeInfo: utilities.ParseCqlTypeOrDie("timestamp"), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column6", TypeInfo: utilities.ParseCqlTypeOrDie("int"), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column7", TypeInfo: utilities.ParseCqlTypeOrDie("set<text>"), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column8", TypeInfo: utilities.ParseCqlTypeOrDie("map<text,boolean>"), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column9", TypeInfo: utilities.ParseCqlTypeOrDie("bigint"), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column11", TypeInfo: utilities.ParseCqlTypeOrDie("set<text>"), KeyType: utilities.KEY_TYPE_REGULAR},
	}

	userInfoColumns := []*types.Column{
		{Name: "name", TypeInfo: utilities.ParseCqlTypeOrDie("varchar"), KeyType: utilities.KEY_TYPE_PARTITION, PkPrecedence: 0},
		{Name: "age", TypeInfo: utilities.ParseCqlTypeOrDie("varchar"), KeyType: utilities.KEY_TYPE_REGULAR},
	}

	allTableConfigs := []*schemaMapping.TableConfig{
		schemaMapping.NewTableConfig("test_keyspace", "test_table", systemColumnFamily, types.OrderedCodeEncoding, testTableColumns),
		schemaMapping.NewTableConfig("test_keyspace", "user_info", systemColumnFamily, types.OrderedCodeEncoding, userInfoColumns),
	}

	return schemaMapping.NewSchemaMappingConfig("schema_mapping", systemColumnFamily, zap.NewNop(), allTableConfigs)
}
