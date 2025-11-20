package alter_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
)

type AlterTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMappingConfig
}

func (t *AlterTranslator) QueryType() types.QueryType {
	return types.QueryTypeAlter
}

func NewAlterTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig) types.IQueryTranslator {
	return &AlterTranslator{schemaMappingConfig: schemaMappingConfig}
}
