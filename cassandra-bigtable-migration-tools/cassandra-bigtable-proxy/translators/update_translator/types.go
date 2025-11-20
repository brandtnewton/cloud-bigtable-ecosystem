package update_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
)

type UpdateTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMappingConfig
}

func NewUpdateTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig) types.IQueryTranslator {
	return &UpdateTranslator{schemaMappingConfig: schemaMappingConfig}
}
