package create_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
)

type CreateTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMetadata
	config              *types.BigtableConfig
}

func (t *CreateTranslator) QueryType() types.QueryType {
	return types.QueryTypeCreate
}

func NewCreateTranslator(schemaMappingConfig *schemaMapping.SchemaMetadata, config *types.BigtableConfig) types.IQueryTranslator {
	return &CreateTranslator{schemaMappingConfig: schemaMappingConfig, config: config}
}
