package truncate_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
)

type TruncateTranslator struct {
	schemaMappingConfig      *schemaMapping.SchemaMetadata
	defaultIntRowKeyEncoding types.IntRowKeyEncodingType
}

func (t *TruncateTranslator) QueryType() types.QueryType {
	return types.QueryTypeTruncate
}

func NewTruncateTranslator(schemaMappingConfig *schemaMapping.SchemaMetadata) types.IQueryTranslator {
	return &TruncateTranslator{schemaMappingConfig: schemaMappingConfig}
}
