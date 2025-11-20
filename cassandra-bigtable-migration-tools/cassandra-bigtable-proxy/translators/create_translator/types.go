package create_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
)

type CreateTranslator struct {
	schemaMappingConfig      *schemaMapping.SchemaMappingConfig
	defaultIntRowKeyEncoding types.IntRowKeyEncodingType
}

func NewCreateTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig, defaultIntRowKeyEncoding types.IntRowKeyEncodingType) types.IQueryTranslator {
	return &CreateTranslator{schemaMappingConfig: schemaMappingConfig, defaultIntRowKeyEncoding: defaultIntRowKeyEncoding}
}
