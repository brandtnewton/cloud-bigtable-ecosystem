package update_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
)

type UpdateTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMetadata
}

func (t *UpdateTranslator) QueryType() types.QueryType {
	return types.QueryTypeUpdate
}

func NewUpdateTranslator(schemaMappingConfig *schemaMapping.SchemaMetadata) types.IQueryTranslator {
	return &UpdateTranslator{schemaMappingConfig: schemaMappingConfig}
}
