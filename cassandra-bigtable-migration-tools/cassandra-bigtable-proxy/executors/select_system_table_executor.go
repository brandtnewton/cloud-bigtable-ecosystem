package executors

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/mem_table"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/responsehandler"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

type selectSystemTableExecutor struct {
	schemaMappings *schemaMapping.SchemaMetadata
	systemTables   *mem_table.InMemEngine
}

func newSelectSystemTableExecutor(schemaMappings *schemaMapping.SchemaMetadata, systemTables *mem_table.InMemEngine) IQueryExecutor {
	return &selectSystemTableExecutor{schemaMappings: schemaMappings, systemTables: systemTables}
}

func (d *selectSystemTableExecutor) CanRun(q types.IExecutableQuery) bool {
	return q.QueryType() == types.QueryTypeSelect && q.Keyspace().IsSystemKeyspace()
}

func (d *selectSystemTableExecutor) Execute(_ context.Context, c types.ICassandraClient, q types.IExecutableQuery) (message.Message, error) {
	query, ok := q.(*types.ExecutableSelectQuery)
	if !ok {
		return nil, fmt.Errorf("unsupported query")
	}

	rows, err := d.systemTables.Execute(query)
	if err != nil {
		return nil, err
	}

	response, err := responsehandler.BuildRowsResultResponse(query, rows, query.ProtocolVersion, nil)
	if err != nil {
		return nil, err
	}
	return response, nil

}
