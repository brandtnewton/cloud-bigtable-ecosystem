package alter_translator

import (
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
)

type AlterTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMappingConfig
}

func NewAlterTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig) types.IQueryTranslator {
	return &AlterTranslator{schemaMappingConfig: schemaMappingConfig}
}
