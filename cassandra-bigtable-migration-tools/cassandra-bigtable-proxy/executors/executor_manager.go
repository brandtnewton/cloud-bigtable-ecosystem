package executors

import (
	"context"
	"fmt"
	bigtableModule "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/bigtable"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/mem_table"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
	otelgo "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/otel"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
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
	trace     trace.Tracer
}

func NewQueryExecutorManager(logger *zap.Logger, s *schemaMapping.SchemaMetadata, bt *bigtableModule.BigtableAdapter, systemTables *mem_table.InMemEngine, otelInst *otelgo.OpenTelemetry) *QueryExecutorManager {
	return &QueryExecutorManager{
		logger: logger,
		executors: []IQueryExecutor{
			newDescribeExecutor(s),
			newUseExecutor(s),
			newSelectSystemTableExecutor(s, systemTables),
			newBigtableExecutor(bt),
		},
		trace: otel.GetTracerProvider().Tracer("executor"),
	}
}

func (m *QueryExecutorManager) getExecutor(q types.IExecutableQuery) (IQueryExecutor, error) {
	for _, e := range m.executors {
		if e.CanRun(q) {
			return e, nil
		}
	}
	return nil, fmt.Errorf("no executor found for query %s on keyspace %s", strings.ToUpper(q.QueryType().String()), q.Keyspace())
}

func (m *QueryExecutorManager) Execute(ctx context.Context, client types.ICassandraClient, q types.IExecutableQuery) (message.Message, error) {
	otelCtx, childSpan := m.trace.Start(ctx, "execute")
	defer childSpan.End()

	executor, err := m.getExecutor(q)
	if err != nil {
		childSpan.RecordError(err)
		return nil, err
	}

	msg, err := executor.Execute(otelCtx, client, q)
	if err != nil {
		childSpan.RecordError(err)
		return nil, err
	}
	return msg, nil
}
