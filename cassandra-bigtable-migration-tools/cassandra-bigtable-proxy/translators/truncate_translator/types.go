package truncate_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
)

type TruncateTranslator struct {
	schemaMappingConfig      *schemaMapping.SchemaMappingConfig
	defaultIntRowKeyEncoding types.IntRowKeyEncodingType
}

func (t *TruncateTranslator) QueryType() types.QueryType {
	return types.QueryTypeTruncate
}

func NewTruncateTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig) types.IQueryTranslator {
	return &TruncateTranslator{schemaMappingConfig: schemaMappingConfig}
}
