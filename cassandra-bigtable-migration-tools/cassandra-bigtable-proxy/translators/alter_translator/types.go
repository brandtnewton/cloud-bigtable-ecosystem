package alter_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
)

type AlterTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMetadata
}

func (t *AlterTranslator) QueryType() types.QueryType {
	return types.QueryTypeAlter
}

func NewAlterTranslator(schemaMappingConfig *schemaMapping.SchemaMetadata) types.IQueryTranslator {
	return &AlterTranslator{schemaMappingConfig: schemaMappingConfig}
}
