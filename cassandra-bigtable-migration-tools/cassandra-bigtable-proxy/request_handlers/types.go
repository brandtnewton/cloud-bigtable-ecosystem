package request_handlers

import (
	"context"
	bigtableModule "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/bigtable"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/executors"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.uber.org/zap"
)

type IProxyRequestHandler interface {
	Name() string
	OpCode() primitive.OpCode
	HandleRequest(ctx context.Context, session IProxySession, raw *frame.RawFrame, m message.Message) (message.Message, error)
}

type IProxySession interface {
	types.ICassandraClient
	SessionKeyspace() types.Keyspace
	RegisterForEvents()
}

type IProxyServer interface {
	CQLVersion() string
	PreparedQueryCache() proxycore.PreparedCache[types.IPreparedQuery]
	BigtableClient() *bigtableModule.BigtableAdapter
	Translator() *translators.TranslatorManager
	Executor() *executors.QueryExecutorManager
	HandlePostDDLEvent(queryType types.QueryType, keyspace types.Keyspace, table types.TableName)
	Logger() *zap.Logger
}
