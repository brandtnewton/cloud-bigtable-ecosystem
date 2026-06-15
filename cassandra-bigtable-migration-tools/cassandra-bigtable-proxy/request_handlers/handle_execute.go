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
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	otelgo "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/otel"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxy/proxy_types"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.opentelemetry.io/otel/trace"
)

type ExecuteRequestHandler struct {
	server IProxyServer
}

func (e *ExecuteRequestHandler) Name() string {
	return "execute"
}

func (e *ExecuteRequestHandler) OpCode() primitive.OpCode {
	return primitive.OpCodeExecute
}

func (e *ExecuteRequestHandler) HandleRequest(ctx context.Context, session IProxySession, raw *frame.RawFrame, m message.Message) (message.Message, error) {
	msg := m.(*proxy_types.PartialExecute)
	span := trace.SpanFromContext(ctx)

	var preparedStmt types.IPreparedQuery

	id := proxy_types.PreparedIdKey(msg.QueryId)

	var ok bool
	preparedStmt, ok = e.server.PreparedQueryCache().Load(id)
	if !ok {
		err := errors.New(proxy_types.ErrQueryNotPrepared)
		return &message.ServerError{ErrorMessage: proxy_types.ErrQueryNotPrepared}, err
	}

	otelgo.AddQueryAnnotations(span, preparedStmt)

	span.AddEvent("bind-query")
	boundQuery, err := e.server.Translator().BindQuery(preparedStmt, msg.PositionalValues, msg.NamedValues, raw.Header.Version)
	if err != nil {
		return &message.ConfigError{ErrorMessage: err.Error()}, err
	}

	span.AddEvent("execute-query")
	results, err := e.server.Executor().Execute(ctx, session, boundQuery)
	if err != nil {
		return &message.ConfigError{ErrorMessage: err.Error()}, err
	}
	if preparedStmt.QueryType().IsDDLType() {
		span.AddEvent("execute-ddl-event")
		e.server.HandlePostDDLEvent(preparedStmt.QueryType(), preparedStmt.Keyspace(), preparedStmt.Table())
	}
	return results, nil
}

func NewExecuteRequestHandler(server IProxyServer) IProxyRequestHandler {
	return &ExecuteRequestHandler{server: server}
}
