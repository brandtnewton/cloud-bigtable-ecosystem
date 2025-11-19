package insert_translator

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators"
)

type InsertTranslator struct {
	schemaMappingConfig *schemaMapping.SchemaMappingConfig
}

func NewInsertTranslator(schemaMappingConfig *schemaMapping.SchemaMappingConfig) translators.IQueryTranslator {
	return &InsertTranslator{schemaMappingConfig: schemaMappingConfig}
}

// PreparedInsertQuery represents the mapping of an insert query along with its translation details.
type PreparedInsertQuery struct {
	cqlQuery    string
	keyspace    types.Keyspace
	table       types.TableName
	Params      *types.QueryParameters
	Assignments []translators.Assignment
	IfNotExists bool
}

func (p PreparedInsertQuery) Keyspace() types.Keyspace {
	return p.keyspace
}

func (p PreparedInsertQuery) Table() types.TableName {
	return p.table
}

func (p PreparedInsertQuery) CqlQuery() string {
	return p.cqlQuery
}
