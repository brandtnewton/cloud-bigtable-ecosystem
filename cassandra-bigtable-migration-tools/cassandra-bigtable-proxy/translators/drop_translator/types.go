package drop_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators"
)

type DropTranslator struct {
	schemaMappingConfig      *schemaMapping.SchemaMappingConfig
	defaultIntRowKeyEncoding types.IntRowKeyEncodingType
}

func NewDropTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig) translators.IQueryTranslator {
	return &DropTranslator{schemaMappingConfig: schemaMappingConfig}
}
