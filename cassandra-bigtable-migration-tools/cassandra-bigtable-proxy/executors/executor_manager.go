package executors

import (
	"context"
	"fmt"
	bigtableModule "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/bigtable"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/mem_table"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"go.uber.org/zap"
	"strings"
)

type IQueryExecutor interface {
	CanRun(q types.IExecutableQuery) bool
	Execute(ctx context.Context, c types.ICassandraClient, q types.IExecutableQuery) (message.Message, error)
}

type QueryExecutorManager struct {
	logger    *zap.Logger
	executors []IQueryExecutor
}

func NewQueryExecutorManager(logger *zap.Logger, s *schemaMapping.SchemaMappingConfig, bt *bigtableModule.BigtableDmlClient, systemTables *mem_table.InMemEngine) *QueryExecutorManager {
	return &QueryExecutorManager{
		logger: logger,
		executors: []IQueryExecutor{
			newDescribeExecutor(s),
			newUseExecutor(s),
			newSelectSystemTableExecutor(s, systemTables),
			newBigtableExecutor(bt),
		},
	}
}

func (m *QueryExecutorManager) Execute(ctx context.Context, client types.ICassandraClient, q types.IExecutableQuery) (message.Message, error) {
	for _, e := range m.executors {
		if e.CanRun(q) {
			m.logger.Debug("executing query", zap.String("cql", q.CqlQuery()), zap.String("btql", q.BigtableQuery()))
			return e.Execute(ctx, client, q)
		}
	}
	return nil, fmt.Errorf("no executor found for query %s on keyspace %s", strings.ToUpper(q.QueryType().String()), q.Keyspace())
}
