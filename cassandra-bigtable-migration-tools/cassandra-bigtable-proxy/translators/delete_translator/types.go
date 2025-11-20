package delete_translator

import (
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
)

type DeleteTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMappingConfig
}

func NewDeleteTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig) types.IQueryTranslator {
	return &DeleteTranslator{schemaMappingConfig: schemaMappingConfig}
}
