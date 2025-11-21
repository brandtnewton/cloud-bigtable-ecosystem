package executors

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

type useExecutor struct {
	schemaMappings *schemaMapping.SchemaMappingConfig
}

func (d *useExecutor) CanRun(q types.IExecutableQuery) bool {
	return q.QueryType() == types.QueryTypeDescribe
}

func (d *useExecutor) Execute(_ context.Context, c types.ICassandraClient, q types.IExecutableQuery) (message.Message, error) {
	use, ok := q.(*types.UseTableStatementMap)
	if !ok {
		return nil, fmt.Errorf("invalid USE query type")
	}

	err := d.schemaMappings.ValidateKeyspace(use.Keyspace())
	if err != nil {
		return nil, err
	}
	c.SetSessionKeyspace(use.Keyspace())
	return &message.SetKeyspaceResult{Keyspace: string(use.Keyspace())}, nil
}

func newUseExecutor(schemaMappings *schemaMapping.SchemaMappingConfig) IQueryExecutor {
	return &useExecutor{schemaMappings: schemaMappings}
}
