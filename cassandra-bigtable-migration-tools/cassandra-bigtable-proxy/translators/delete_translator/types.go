package delete_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
)

type DeleteTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMetadata
}

func (t *DeleteTranslator) QueryType() types.QueryType {
	return types.QueryTypeDelete
}

func NewDeleteTranslator(schemaMappingConfig *schemaMapping.SchemaMetadata) types.IQueryTranslator {
	return &DeleteTranslator{schemaMappingConfig: schemaMappingConfig}
}
