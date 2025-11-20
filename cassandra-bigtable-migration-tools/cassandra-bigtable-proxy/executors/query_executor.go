package executors

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

type IQueryExecutor interface {
	CanRun(q types.IExecutableQuery) bool
	Execute(ctx context.Context, q types.IExecutableQuery) (*message.RowsResult, error)
}

type QueryExecutorManager struct {
	executors []IQueryExecutor
}

func NewQueryExecutorManager(s *schemaMapping.SchemaMappingConfig) *QueryExecutorManager {
	return &QueryExecutorManager{executors: []IQueryExecutor{
		newDescribeExecutor(s),
	}}
}

func (m *QueryExecutorManager) Execute(ctx context.Context, q types.IExecutableQuery) (*message.RowsResult, error) {
	for _, e := range m.executors {
		if e.CanRun(q) {
			return e.Execute(ctx, q)
		}
	}
	return nil, fmt.Errorf("no executor found for query %s on keyspace %s", q.QueryType().String(), q.Keyspace())
}
