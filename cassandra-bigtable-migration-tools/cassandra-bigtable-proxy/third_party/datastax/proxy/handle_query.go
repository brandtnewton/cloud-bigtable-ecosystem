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

package proxy

import (
	"context"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/parser"
	otelgo "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/otel"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"time"
)

type QueryRequestHandler struct {
}

func (q *QueryRequestHandler) Name() string {
	return "query"
}

func (q *QueryRequestHandler) OpCode() primitive.OpCode {
	return primitive.OpCodeQuery
}

func (q *QueryRequestHandler) HandleRequest(ctx context.Context, c *client, raw *frame.RawFrame, m message.Message) (message.Message, error) {
	msg := m.(*partialQuery)
	span := trace.SpanFromContext(ctx)
	startTime := time.Now()
	c.proxy.logger.Debug("handling query", zap.String("encodedQuery", msg.query), zap.Int16("stream", raw.Header.StreamId))

	var otelErr error
	qt := types.QueryTypeUnknown
	var query types.IPreparedQuery
	defer func() {
		attrs := types.Attributes{
			Method:    handleQuery,
			QueryType: qt,
			Keyspace:  c.sessionKeyspace,
			Status:    otelgoStatus(otelErr),
		}
		if query != nil {
			attrs.Table = query.Table()
		}
		otelgo.AddQueryAnnotations(span, attrs)
		c.proxy.otelInst.RecordMetrics(ctx, startTime, attrs)
	}()

	p := parser.GetParser(msg.query)
	var err error
	qt, err = parseQueryType(p)
	if err != nil {
		otelErr = err
		return &message.Invalid{ErrorMessage: err.Error()}, err
	}
	if span.IsRecording() {
		span.SetAttributes(attribute.String(QueryType, qt.String()))
	}

	rawQuery := types.NewRawQuery(raw.Header, c.sessionKeyspace, msg.query, p, qt)

	query, err = c.prepareQuery(ctx, rawQuery)
	if err != nil {
		otelErr = err
		return &message.ServerError{ErrorMessage: err.Error()}, err
	}

	values := types.NewQueryParameterValues(query.Parameters(), time.Now())
	executableQuery, err := c.proxy.translator.BindQueryParameters(query, values, raw.Header.Version)
	if err != nil {
		otelErr = err
		return &message.ConfigError{ErrorMessage: err.Error()}, err
	}

	span.AddEvent(executingBigtableSQLAPIRequestEvent)
	selectResult, err := c.proxy.executor.Execute(ctx, c, executableQuery)
	span.AddEvent(bigtableExecutionDoneEvent)

	if err != nil {
		otelErr = err
		return &message.ServerError{ErrorMessage: err.Error()}, err
	}

	if rawQuery.QueryType().IsDDLType() {
		c.handlePostDDLEvent(query.QueryType(), query.Keyspace(), query.Table())
	}

	return selectResult, nil
}

func NewQueryRequestHandler() IProxyRequestHandler {
	return &QueryRequestHandler{}
}
