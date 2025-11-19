package select_translator

import (
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators"
)

type SelectTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMappingConfig
}

func NewSelectTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig) translators.IQueryTranslator {
	return &SelectTranslator{schemaMappingConfig: schemaMappingConfig}
}
