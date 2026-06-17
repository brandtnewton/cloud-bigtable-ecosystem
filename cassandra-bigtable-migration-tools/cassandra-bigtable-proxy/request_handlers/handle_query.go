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
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/parser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxy/proxy_types"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"time"
)

type QueryRequestHandler struct {
	server IProxyServer
}

func (q *QueryRequestHandler) Name() string {
	return "query"
}

func (q *QueryRequestHandler) OpCode() primitive.OpCode {
	return primitive.OpCodeQuery
}

func (q *QueryRequestHandler) HandleRequest(ctx context.Context, session IProxySession, req *ProxyRequest) (message.Message, error) {
	msg := req.msg.(*proxy_types.PartialQuery)
	span := trace.SpanFromContext(ctx)
	q.server.Logger().Debug("handling query", zap.String("encodedQuery", msg.Query), zap.Int16("stream", req.header.StreamId))

	p := parser.GetParser(msg.Query)
	var err error
	qt, err := parseQueryType(p)
	if err != nil {
		return &message.Invalid{ErrorMessage: err.Error()}, err
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.String(proxy_types.QueryTypeConst, qt.String()))
	}

	rawQuery := types.NewRawQuery(req.header, session.SessionKeyspace(), msg.Query, p, qt)

	query, err := prepareQuery(ctx, q.server, session, rawQuery)
	if err != nil {
		return &message.ServerError{ErrorMessage: err.Error()}, err
	}

	values := types.NewQueryParameterValues(query.Parameters(), time.Now())
	executableQuery, err := q.server.Translator().BindQueryParameters(query, values, req.header.Version)
	if err != nil {
		return &message.ConfigError{ErrorMessage: err.Error()}, err
	}

	span.AddEvent(proxy_types.ExecutingBigtableSQLAPIRequestEvent)
	selectResult, err := q.server.Executor().Execute(ctx, session, executableQuery)
	span.AddEvent(proxy_types.BigtableExecutionDoneEvent)

	if err != nil {
		return &message.ServerError{ErrorMessage: err.Error()}, err
	}

	if rawQuery.QueryType().IsDDLType() {
		q.server.HandlePostDDLEvent(query.QueryType(), query.Keyspace(), query.Table())
	}

	return selectResult, nil
}

func NewQueryRequestHandler(server IProxyServer) IProxyRequestHandler {
	return &QueryRequestHandler{server: server}
}
