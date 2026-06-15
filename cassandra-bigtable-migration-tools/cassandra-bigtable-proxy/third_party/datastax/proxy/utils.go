package proxy

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxy/proxy_types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.uber.org/zap"
)

func handlePostDDLEvent(c proxy_types.IClient, queryType types.QueryType, keyspace types.Keyspace, table types.TableName) {
	var changeType primitive.SchemaChangeType
	switch queryType {
	case types.QueryTypeCreate:
		changeType = primitive.SchemaChangeTypeCreated
	case types.QueryTypeAlter:
		changeType = primitive.SchemaChangeTypeUpdated
	case types.QueryTypeDrop:
		changeType = primitive.SchemaChangeTypeDropped
	default:
		c.Proxy().Logger().Warn("unhandled ddl event type", zap.String("queryType", queryType.String()))
		return
	}

	// SendEvent all clients of schema change
	event := &proxycore.SchemaChangeEvent{
		Message: &message.SchemaChangeEvent{
			ChangeType: changeType,
			Target:     primitive.SchemaChangeTargetTable,
			Keyspace:   string(keyspace),
			Object:     string(table),
		},
	}
	c.Proxy().EventClients().Range(func(key, _ interface{}) bool {
		if client, ok := key.(proxy_types.IClient); ok {
			client.HandleEvent(event)
		}
		return true
	})
}
