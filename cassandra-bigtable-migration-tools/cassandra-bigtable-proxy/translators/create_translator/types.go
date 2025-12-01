package create_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
)

type CreateTranslator struct {
	schemaMappingConfig      *schemaMapping.SchemaMetadata
	defaultIntRowKeyEncoding types.IntRowKeyEncodingType
}

func (t *CreateTranslator) QueryType() types.QueryType {
	return types.QueryTypeCreate
}

func NewCreateTranslator(schemaMappingConfig *schemaMapping.SchemaMetadata, defaultIntRowKeyEncoding types.IntRowKeyEncodingType) types.IQueryTranslator {
	return &CreateTranslator{schemaMappingConfig: schemaMappingConfig, defaultIntRowKeyEncoding: defaultIntRowKeyEncoding}
}
