package executors

import (
	"context"
	bigtableModule "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/bigtable"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

type bigtableExecutor struct {
	bt *bigtableModule.BigtableAdapter
}

func (d *bigtableExecutor) CanRun(q types.IExecutableQuery) bool {
	return !q.Keyspace().IsSystemKeyspace() &&
		q.QueryType() == types.QueryTypeSelect ||
		q.QueryType() == types.QueryTypeInsert ||
		q.QueryType() == types.QueryTypeDelete ||
		q.QueryType() == types.QueryTypeUpdate ||
		q.QueryType() == types.QueryTypeTruncate ||
		q.QueryType() == types.QueryTypeAlter ||
		q.QueryType() == types.QueryTypeDrop ||
		q.QueryType() == types.QueryTypeCreate
}

func (d *bigtableExecutor) Execute(ctx context.Context, _ types.ICassandraClient, q types.IExecutableQuery) (message.Message, error) {
	return d.bt.Execute(ctx, q)
}

func newBigtableExecutor(bt *bigtableModule.BigtableAdapter) IQueryExecutor {
	return &bigtableExecutor{bt: bt}
}
