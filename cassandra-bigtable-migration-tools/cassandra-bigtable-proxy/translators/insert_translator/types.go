package insert_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
)

type InsertTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMetadata
}

func (t *InsertTranslator) QueryType() types.QueryType {
	return types.QueryTypeInsert
}

func NewInsertTranslator(schemaMappingConfig *schemaMapping.SchemaMetadata) types.IQueryTranslator {
	return &InsertTranslator{schemaMappingConfig: schemaMappingConfig}
}
