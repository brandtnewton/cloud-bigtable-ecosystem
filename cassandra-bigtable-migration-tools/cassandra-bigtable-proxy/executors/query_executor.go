package executors

import (
	"context"
	"fmt"
	bigtableModule "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/bigtable"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/mem_table"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

type IQueryExecutor interface {
	CanRun(q types.IExecutableQuery) bool
	Execute(ctx context.Context, c types.ICassandraClient, q types.IExecutableQuery) (message.Message, error)
}

type QueryExecutorManager struct {
	executors []IQueryExecutor
}

func NewQueryExecutorManager(s *schemaMapping.SchemaMappingConfig, bt bigtableModule.BigTableClientIface, systemTables *mem_table.InMemEngine) *QueryExecutorManager {
	return &QueryExecutorManager{executors: []IQueryExecutor{
		newDescribeExecutor(s),
		newUseExecutor(s),
		newSelectSystemTableExecutor(s, systemTables),
		newBigtableExecutor(bt),
	}}
}

func (m *QueryExecutorManager) Execute(ctx context.Context, client types.ICassandraClient, q types.IExecutableQuery) (message.Message, error) {
	for _, e := range m.executors {
		if e.CanRun(q) {
			return e.Execute(ctx, client, q)
		}
	}
	return nil, fmt.Errorf("no executor found for query %s on keyspace %s", q.QueryType().String(), q.Keyspace())
}
