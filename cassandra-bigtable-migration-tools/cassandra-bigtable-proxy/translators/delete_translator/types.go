package delete_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
)

type DeleteTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMappingConfig
}

func (t *DeleteTranslator) QueryType() types.QueryType {
	return types.QueryTypeDelete
}

func NewDeleteTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig) types.IQueryTranslator {
	return &DeleteTranslator{schemaMappingConfig: schemaMappingConfig}
}
