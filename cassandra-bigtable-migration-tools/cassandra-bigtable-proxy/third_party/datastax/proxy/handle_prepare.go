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
	"crypto/md5"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/parser"
	otelgo "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/otel"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/responsehandler"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"time"
	)

	type PrepareRequestHandler struct {
	}

	func (p *PrepareRequestHandler) Name() string {
		return "prepare"
	}

	func (p *PrepareRequestHandler) OpCode() primitive.OpCode {
		return primitive.OpCodePrepare
	}
func (p *PrepareRequestHandler) HandleRequest(ctx context.Context, c *client, raw *frame.RawFrame, m message.Message) (message.Message, error) {
	msg := m.(*message.Prepare)
	span := trace.SpanFromContext(ctx)
	startTime := time.Now()

	var otelErr error
	qt := types.QueryTypeUnknown
	defer func() {
		attrs := types.Attributes{
			Method:    handlePrepare,
			QueryType: qt,
			Keyspace:  c.sessionKeyspace,
			Status:    otelgoStatus(otelErr),
		}
		otelgo.AddQueryAnnotations(span, attrs)
		c.proxy.otelInst.RecordMetrics(ctx, startTime, attrs)
	}()

	id := c.getQueryId(msg)
	if preparedQuery, found := c.proxy.preparedQueryCache.Load(id); found {
		qt = preparedQuery.QueryType()
		return responsehandler.BuildPreparedResultResponse(id, preparedQuery), nil
	}

	c.proxy.logger.Debug("preparing query", zap.String(Query, msg.Query), zap.Int16("stream", raw.Header.StreamId))

	keyspace := c.sessionKeyspace
	if len(msg.Keyspace) != 0 {
		keyspace = types.Keyspace(msg.Keyspace)
	}

	pParser := parser.GetParser(msg.Query)
	var err error
	qt, err = parseQueryType(pParser)
	if err != nil {
		otelErr = err
		return &message.Invalid{ErrorMessage: err.Error()}, err
	}

	rawQuery := types.NewRawQuery(raw.Header, keyspace, msg.Query, pParser, qt)
	resp, _, err := c.handleServerPreparedQuery(ctx, rawQuery, id)
	otelErr = err
	return resp, err
}

func NewPrepareRequestHandler() IProxyRequestHandler {
	return &PrepareRequestHandler{}
}

func parseQueryType(p *parser.ProxyCqlParser) (types.QueryType, error) {
	tok := p.GetFirstToken()
	t := tok.GetTokenType()
	switch t {
	case cql.CqlLexerK_SELECT:
		return types.QueryTypeSelect, nil
	case cql.CqlLexerK_INSERT:
		return types.QueryTypeInsert, nil
	case cql.CqlLexerK_UPDATE:
		return types.QueryTypeUpdate, nil
	case cql.CqlLexerK_DELETE:
		return types.QueryTypeDelete, nil
	case cql.CqlLexerK_CREATE:
		return types.QueryTypeCreate, nil
	case cql.CqlLexerK_ALTER:
		return types.QueryTypeAlter, nil
	case cql.CqlLexerK_DROP:
		return types.QueryTypeDrop, nil
	case cql.CqlLexerK_TRUNCATE:
		return types.QueryTypeTruncate, nil
	case cql.CqlLexerK_USE:
		return types.QueryTypeUse, nil
	case cql.CqlLexerK_DESCRIBE, cql.CqlLexerK_DESC:
		return types.QueryTypeDescribe, nil
	default:
		return types.QueryTypeUnknown, fmt.Errorf("unsupported query type: %s", tok.String())
	}
}

func (c *client) getQueryId(msg *message.Prepare) [16]byte {
	// Generating unique prepared query_id
	return md5.Sum([]byte(msg.Query + string(c.sessionKeyspace)))
}

// handleServerPreparedQuery handle prepared query that was supposed to run on cassandra server
// This method will keep track of prepared query in a map and send hashed query_id with result
// metadata and variable column metadata to the client
//
// Parameters:
//   - raw: *frame.RawFrame
//   - msg: *message.Prepare
//
// Returns: error if any error occurs during preparation
func (c *client) handleServerPreparedQuery(ctx context.Context, query *types.RawQuery, id [16]byte) (message.Message, types.IPreparedQuery, error) {
	preparedQuery, err := c.prepareQuery(ctx, query)
	if err != nil {
		return &message.Invalid{ErrorMessage: err.Error()}, nil, err
	}

	response := responsehandler.BuildPreparedResultResponse(id, preparedQuery)

	// update cache
	c.proxy.preparedQueryCache.Store(id, preparedQuery)

	return response, preparedQuery, nil
}

func (c *client) prepareQuery(ctx context.Context, query *types.RawQuery) (types.IPreparedQuery, error) {
	preparedQuery, err := c.proxy.translator.TranslateQuery(ctx, query, c.sessionKeyspace)
	if err != nil {
		return nil, err
	}

	btPreparedQuery, err := c.proxy.bigtableClient.PrepareStatement(ctx, preparedQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare bigtable statement `%s`: %w", preparedQuery.BigtableQuery(), err)
	}
	preparedQuery.SetBigtablePreparedQuery(btPreparedQuery)

	return preparedQuery, err
}
