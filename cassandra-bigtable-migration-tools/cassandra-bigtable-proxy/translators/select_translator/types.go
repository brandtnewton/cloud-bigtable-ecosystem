package select_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
)

type SelectTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMappingConfig
}

func NewSelectTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig) types.IQueryTranslator {
	return &SelectTranslator{schemaMappingConfig: schemaMappingConfig}
}
