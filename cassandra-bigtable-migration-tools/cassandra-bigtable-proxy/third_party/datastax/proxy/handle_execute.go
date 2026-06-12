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
	"errors"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	otelgo "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/otel"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.opentelemetry.io/otel/trace"
	"time"
)

type ExecuteRequestHandler struct {
}

func (e *ExecuteRequestHandler) Name() string {
	return "execute"
}

func (e *ExecuteRequestHandler) OpCode() primitive.OpCode {
	return primitive.OpCodeExecute
}

func (e *ExecuteRequestHandler) HandleRequest(ctx context.Context, c *client, raw *frame.RawFrame, m message.Message) (message.Message, error) {
	msg := m.(*partialExecute)
	span := trace.SpanFromContext(ctx)
	startTime := time.Now()

	var otelErr error
	var preparedStmt types.IPreparedQuery
	defer func() {
		attrs := types.Attributes{
			Method:   handleExecute,
			Keyspace: c.sessionKeyspace,
			Status:   otelgoStatus(otelErr),
		}
		if preparedStmt != nil {
			attrs.QueryType = preparedStmt.QueryType()
			attrs.Table = preparedStmt.Table()
		}
		otelgo.AddQueryAnnotations(span, attrs)
		c.proxy.otelInst.RecordMetrics(ctx, startTime, attrs)
	}()

	id := preparedIdKey(msg.queryId)

	var ok bool
	preparedStmt, ok = c.proxy.preparedQueryCache.Load(id)
	if !ok {
		otelErr = errors.New(errQueryNotPrepared)
		return &message.ServerError{ErrorMessage: errQueryNotPrepared}, otelErr
	}

	span.AddEvent("bind-query")
	boundQuery, err := c.proxy.translator.BindQuery(preparedStmt, msg.PositionalValues, msg.NamedValues, raw.Header.Version)
	if err != nil {
		otelErr = err
		return &message.ConfigError{ErrorMessage: err.Error()}, err
	}

	span.AddEvent("execute-query")
	results, err := c.proxy.executor.Execute(ctx, c, boundQuery)
	if err != nil {
		otelErr = err
		return &message.ConfigError{ErrorMessage: err.Error()}, err
	}
	if preparedStmt.QueryType().IsDDLType() {
		span.AddEvent("execute-ddl-event")
		c.handlePostDDLEvent(preparedStmt.QueryType(), preparedStmt.Keyspace(), preparedStmt.Table())
	}
	return results, nil
}

func NewExecuteRequestHandler() IProxyRequestHandler {
	return &ExecuteRequestHandler{}
}
