package select_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
)

type SelectTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMetadata
}

func (t *SelectTranslator) QueryType() types.QueryType {
	return types.QueryTypeSelect
}

func NewSelectTranslator(schemaMappingConfig *schemaMapping.SchemaMetadata) types.IQueryTranslator {
	return &SelectTranslator{schemaMappingConfig: schemaMappingConfig}
}
