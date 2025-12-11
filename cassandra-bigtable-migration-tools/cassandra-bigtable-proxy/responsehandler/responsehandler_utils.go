/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package responsehandler

import (
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/google/uuid"
)

func BuildRowsResultResponse(st *types.ExecutableSelectQuery, rows []types.GoRow, pv primitive.ProtocolVersion) (*message.RowsResult, error) {
	var outputRows []message.Row
	for _, row := range rows {
		var outputRow []message.Column
		for _, c := range st.ResultColumnMetadata {
			val, ok := row[c.Name]
			if !ok {
				val = nil
			}
			encoded, err := encodeGoValueToCassandraResponse(val, c, pv)
			if err != nil {
				return nil, fmt.Errorf("error encoding column '%s' to %s in response: %w", c.Name, c.Type.String(), err)
			}
			outputRow = append(outputRow, encoded)
		}
		outputRows = append(outputRows, outputRow)
	}

	return &message.RowsResult{
		Metadata: &message.RowsMetadata{
			// todo implement pagination?
			LastContinuousPage: true,
			ColumnCount:        int32(len(st.ResultColumnMetadata)),
			Columns:            st.ResultColumnMetadata,
		},
		Data: outputRows,
	}, nil
}

func encodeGoValueToCassandraResponse(value any, md *message.ColumnMetadata, pv primitive.ProtocolVersion) ([]byte, error) {
	if md.Type == datatype.Timeuuid {
		switch v := value.(type) {
		case uuid.UUID:
			b, err := v.MarshalBinary()
			if err != nil {
				return nil, err
			}
			return proxycore.EncodeType(datatype.Timeuuid, pv, b)
		}
	}

	encoded, err := proxycore.EncodeType(md.Type, pv, value)
	if err != nil {
		return nil, fmt.Errorf("error encoding column '%s' to %s from %T in response: %w", md.Name, md.Type.String(), value, err)
	}
	return encoded, nil
}

func BuildPreparedResultResponse(id [16]byte, query types.IPreparedQuery) (*message.PreparedResult, error) {
	var variableMetadata []*message.ColumnMetadata
	params := query.Parameters()
	for i, md := range params.AllUserKeys() {
		var col = message.ColumnMetadata{
			Index: int32(i),
			Type:  md.Type.DataType(),
		}
		variableMetadata = append(variableMetadata, &col)
	}
	resultColumns := query.ResponseColumns()
	return &message.PreparedResult{
		PreparedQueryId: id[:],
		ResultMetadata: &message.RowsMetadata{
			ColumnCount: int32(len(resultColumns)),
			Columns:     resultColumns,
		},
		VariablesMetadata: &message.VariablesMetadata{
			Columns: variableMetadata,
		},
	}, nil
}
