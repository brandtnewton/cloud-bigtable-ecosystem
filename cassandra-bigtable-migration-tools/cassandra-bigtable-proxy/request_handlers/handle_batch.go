// Copyright (c) DataStax, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package request_handlers

import (
	"context"
	"errors"
	"fmt"
	bigtableModule "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/bigtable"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxy/proxy_types"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.opentelemetry.io/otel/trace"
	"strings"
)

type BatchRequestHandler struct {
	server IProxyServer
}

func (b *BatchRequestHandler) Name() string {
	return "batch"
}

func (b *BatchRequestHandler) OpCode() primitive.OpCode {
	return primitive.OpCodeBatch
}

func (b *BatchRequestHandler) HandleRequest(ctx context.Context, session IProxySession, req *ProxyRequest) (message.Message, error) {
	msg := req.msg.(*proxy_types.PartialBatch)
	span := trace.SpanFromContext(ctx)

	bulkMutations, keyspace, err := b.bindBulkOperations(msg, session, req.header.Version)
	if err != nil {
		return &message.ConfigError{ErrorMessage: err.Error()}, err
	}
	span.AddEvent(proxy_types.SendingBulkApplyMutation)
	var errs []string
	for tableName, mutations := range bulkMutations.Mutations() {
		res, err := b.server.BigtableClient().ApplyBulkMutation(ctx, keyspace, tableName, mutations)
		if err != nil {
			errs = append(errs, err.Error())
		} else if res.FailedRows != "" {
			err = fmt.Errorf("failed rows for table %s: %s", tableName, res.FailedRows)
			errs = append(errs, res.FailedRows)
		}
	}
	span.AddEvent(proxy_types.GotBulkApplyResp)
	span.SetAttributes()
	if len(errs) > 0 {
		err := errors.New(strings.Join(errs, "\n"))
		return &message.ServerError{ErrorMessage: err.Error()}, err
	}

	return &message.VoidResult{}, nil
}

func (b *BatchRequestHandler) bindBulkOperations(msg *proxy_types.PartialBatch, session IProxySession, pv primitive.ProtocolVersion) (*bigtableModule.BigtableBulkMutation, types.Keyspace, error) {
	var keyspace types.Keyspace
	tableMutationsMap := bigtableModule.NewBigtableBulkMutation()
	for index, queryId := range msg.QueryOrIds {
		queryOrId, ok := queryId.([]byte)
		if !ok {
			return nil, "", fmt.Errorf("batch query id malformed")
		}
		id := proxy_types.PreparedIdKey(queryOrId)
		preparedStmt, ok := b.server.PreparedQueryCache().Load(id)
		if !ok {
			return nil, "", fmt.Errorf("prepared query not found in cache")
		}

		if preparedStmt.Keyspace() != "" {
			keyspace = preparedStmt.Keyspace()
		}

		// note: we don't support batch named queries at this time
		executableQuery, err := b.server.Translator().BindQuery(preparedStmt, msg.BatchPositionalValues[index], nil, pv)
		if err != nil {
			return nil, "", err
		}
		mutation, ok := executableQuery.AsBulkMutation()
		if !ok {
			return nil, "", fmt.Errorf("query type '%s' not compatible with bulk", executableQuery.QueryType().String())
		}
		tableMutationsMap.AddMutation(mutation)
	}
	if keyspace == "" {
		keyspace = session.SessionKeyspace()
	}
	return tableMutationsMap, keyspace, nil
}

func NewBatchRequestHandler(server IProxyServer) IProxyRequestHandler {
	return &BatchRequestHandler{server: server}
}
