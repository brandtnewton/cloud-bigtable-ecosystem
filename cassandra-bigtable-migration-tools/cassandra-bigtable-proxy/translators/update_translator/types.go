package update_translator

import (
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators"
)

type UpdateTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMappingConfig
}

func NewUpdateTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig) translators.IQueryTranslator {
	return &UpdateTranslator{schemaMappingConfig: schemaMappingConfig}
}
