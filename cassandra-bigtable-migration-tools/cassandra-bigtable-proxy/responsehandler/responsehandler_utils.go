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
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func selectedColumnsToMetadata(keyspace types.Keyspace, table types.TableName, columns []types.SelectedColumn) []*message.ColumnMetadata {
	var resultColumns []*message.ColumnMetadata
	for i, c := range columns {
		var col = message.ColumnMetadata{
			Keyspace: string(keyspace),
			Table:    string(table),
			Name:     string(c.ColumnName),
			Index:    int32(i),
			Type:     c.ResultType.DataType(),
		}
		resultColumns = append(resultColumns, &col)
	}
	return resultColumns
}

func BuildRowsResultResponse(st *types.BoundSelectQuery, rows []types.GoRow, pv primitive.ProtocolVersion) (*message.RowsResult, error) {
	var outputRows []message.Row
	for _, row := range rows {
		var outputRow []message.Column
		for _, c := range st.ResultColumnMetadata {
			val, ok := row[c.Name]
			if !ok {
				val = nil
			}
			encoded, err := proxycore.EncodeType(c.Type, pv, val)
			if err != nil {
				return nil, fmt.Errorf("error encoding column '%s' to %s: %w", c.Name, c.Type.String(), err)
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

func BuildPreparedResultResponse(id [16]byte, query types.IPreparedQuery) (*message.PreparedResult, error) {
	var pkIndices []uint16
	var variableMetadata []*message.ColumnMetadata

	params := query.Parameters()
	// only return the parameters we still need values for
	for i, p := range params.RemainingKeys(query.InitialValues()) {
		md := params.GetMetadata(p)

		var col = message.ColumnMetadata{
			Keyspace: string(query.Keyspace()),
			Table:    string(query.Table()),
			Index:    int32(i),
			Type:     md.Type.DataType(),
		}
		variableMetadata = append(variableMetadata, &col)

		if md.Column != nil && md.Column.IsPrimaryKey {
			pkIndices = append(pkIndices, uint16(i))
		}
	}

	resultColumns := query.ResponseColumns()
	return &message.PreparedResult{
		PreparedQueryId: id[:],
		ResultMetadata: &message.RowsMetadata{
			ColumnCount: int32(len(resultColumns)),
			Columns:     resultColumns,
		},
		VariablesMetadata: &message.VariablesMetadata{
			PkIndices: pkIndices,
			Columns:   variableMetadata,
		},
	}, nil
}

func BuildResponseForSystemQueries(rows [][]interface{}, pv primitive.ProtocolVersion) ([]message.Row, error) {
	var allRows []message.Row
	for _, row := range rows {
		var mr message.Row
		for _, val := range row {
			encodedByte, err := utilities.TypeConversion(val, pv)
			if err != nil {
				return allRows, err
			}
			mr = append(mr, encodedByte)
		}
		allRows = append(allRows, mr)
	}
	return allRows, nil
}
