package request_handlers

import (
	"context"
	"crypto/md5"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

func getQueryId(session IProxySession, msg *message.Prepare) [16]byte {
	return md5.Sum([]byte(msg.Query + string(session.SessionKeyspace())))
}

func prepareQuery(ctx context.Context, server IProxyServer, session IProxySession, query *types.RawQuery) (types.IPreparedQuery, error) {
	preparedQuery, err := server.Translator().TranslateQuery(ctx, query, session.SessionKeyspace())
	if err != nil {
		return nil, err
	}

	btPreparedQuery, err := server.BigtableClient().PrepareStatement(ctx, preparedQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare bigtable statement `%s`: %w", preparedQuery.BigtableQuery(), err)
	}
	preparedQuery.SetBigtablePreparedQuery(btPreparedQuery)

	return preparedQuery, err
}
