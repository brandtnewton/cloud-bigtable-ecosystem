package drop_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
)

type DropTranslator struct {
	schemaMappingConfig      *schemaMapping.SchemaMetadata
	defaultIntRowKeyEncoding types.IntRowKeyEncodingType
}

func (t *DropTranslator) QueryType() types.QueryType {
	return types.QueryTypeDrop
}

func NewDropTranslator(schemaMappingConfig *schemaMapping.SchemaMetadata) types.IQueryTranslator {
	return &DropTranslator{schemaMappingConfig: schemaMappingConfig}
}
