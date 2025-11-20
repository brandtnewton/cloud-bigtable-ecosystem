package insert_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
)

type InsertTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMappingConfig
}

func NewInsertTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig) types.IQueryTranslator {
	return &InsertTranslator{schemaMappingConfig: schemaMappingConfig}
}
