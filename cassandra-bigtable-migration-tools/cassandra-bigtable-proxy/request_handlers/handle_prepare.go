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
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/parser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/responsehandler"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxy/proxy_types"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.uber.org/zap"
)

type PrepareRequestHandler struct {
	server IProxyServer
}

func (p *PrepareRequestHandler) Name() string {
	return "prepare"
}

func (p *PrepareRequestHandler) OpCode() primitive.OpCode {
	return primitive.OpCodePrepare
}
func (p *PrepareRequestHandler) HandleRequest(ctx context.Context, session IProxySession, raw *frame.RawFrame, m message.Message) (message.Message, error) {
	msg := m.(*message.Prepare)

	qt := types.QueryTypeUnknown

	id := getQueryId(session, msg)
	if preparedQuery, found := p.server.PreparedQueryCache().Load(id); found {
		qt = preparedQuery.QueryType()
		return responsehandler.BuildPreparedResultResponse(id, preparedQuery), nil
	}

	p.server.Logger().Debug("preparing query", zap.String(proxy_types.Query, msg.Query), zap.Int16("stream", raw.Header.StreamId))

	keyspace := session.SessionKeyspace()
	if len(msg.Keyspace) != 0 {
		keyspace = types.Keyspace(msg.Keyspace)
	}

	pParser := parser.GetParser(msg.Query)
	var err error
	qt, err = parseQueryType(pParser)
	if err != nil {
		return &message.Invalid{ErrorMessage: err.Error()}, err
	}

	rawQuery := types.NewRawQuery(raw.Header, keyspace, msg.Query, pParser, qt)
	resp, _, err := handleServerPreparedQuery(ctx, p.server, session, rawQuery, id)
	return resp, err
}

func NewPrepareRequestHandler(server IProxyServer) IProxyRequestHandler {
	return &PrepareRequestHandler{server: server}
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
