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
	"fmt"
	bigtableModule "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/bigtable"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	otelgo "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/otel"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.opentelemetry.io/otel/trace"
	"strings"
	"time"
)

type BatchRequestHandler struct {
}

func (b *BatchRequestHandler) Name() string {
	return "batch"
}

func (b *BatchRequestHandler) OpCode() primitive.OpCode {
	return primitive.OpCodeBatch
}

func (b *BatchRequestHandler) HandleRequest(ctx context.Context, c *client, raw *frame.RawFrame, m message.Message) (message.Message, error) {
	msg := m.(*partialBatch)
	span := trace.SpanFromContext(ctx)
	startTime := time.Now()
	var otelErr error
	defer func() {
		attrs := types.Attributes{
			Method:   handleBatch,
			Keyspace: c.sessionKeyspace,
			Status:   otelgoStatus(otelErr),
		}
		otelgo.AddQueryAnnotations(span, attrs)
		c.proxy.otelInst.RecordMetrics(ctx, startTime, attrs)
	}()

	bulkMutations, keyspace, err := c.bindBulkOperations(msg, raw.Header.Version)
	if err != nil {
		otelErr = err
		return &message.ConfigError{ErrorMessage: err.Error()}, err
	}
	span.AddEvent(sendingBulkApplyMutation)
	var errs []string
	for tableName, mutations := range bulkMutations.Mutations() {
		res, err := c.proxy.bigtableClient.ApplyBulkMutation(ctx, keyspace, tableName, mutations)
		if err != nil {
			errs = append(errs, err.Error())
		} else if res.FailedRows != "" {
			err = fmt.Errorf("failed rows for table %s: %s", tableName, res.FailedRows)
			errs = append(errs, res.FailedRows)
		}
	}
	span.AddEvent(gotBulkApplyResp)
	if len(errs) > 0 {
		otelErr = errors.New(strings.Join(errs, "\n"))
		return &message.ServerError{ErrorMessage: otelErr.Error()}, otelErr
	}

	return &message.VoidResult{}, nil
}

func NewBatchRequestHandler() IProxyRequestHandler {
	return &BatchRequestHandler{}
}

func (c *client) bindBulkOperations(msg *partialBatch, pv primitive.ProtocolVersion) (*bigtableModule.BigtableBulkMutation, types.Keyspace, error) {
	var keyspace types.Keyspace
	tableMutationsMap := bigtableModule.NewBigtableBulkMutation()
	for index, queryId := range msg.queryOrIds {
		queryOrId, ok := queryId.([]byte)
		if !ok {
			return nil, "", fmt.Errorf("batch query id malformed")
		}
		id := preparedIdKey(queryOrId)
		preparedStmt, ok := c.proxy.preparedQueryCache.Load(id)
		if !ok {
			return nil, "", fmt.Errorf("prepared query not found in cache")
		}

		if preparedStmt.Keyspace() != "" {
			keyspace = preparedStmt.Keyspace()
		}

		// note: we don't support batch named queries at this time
		executableQuery, err := c.proxy.translator.BindQuery(preparedStmt, msg.BatchPositionalValues[index], nil, pv)
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
		keyspace = c.sessionKeyspace
	}
	return tableMutationsMap, keyspace, nil
}
