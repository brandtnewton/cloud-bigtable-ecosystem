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

package bigtableclient

import (
	"cloud.google.com/go/bigtable"
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/constants"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/responsehandler"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translator"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"go.uber.org/zap"
	"strings"
	"time"
)

// SelectStatement - Executes a select statement on Bigtable and returns the result.
// It uses the SQL API (Prepare/Execute flow) to execute the statement.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - query: rh.QueryMetadata containing the query and parameters.
//
// Returns:
//   - *message.RowsResult: The result of the select statement.
//   - time.Duration: The total elapsed time for the operation.
//   - error: Error if the select statement execution fails.
func (btc *BigtableClient) SelectStatement(ctx context.Context, query *translator.BoundSelectQuery) (*message.RowsResult, error) {
	preparedStmt, err := btc.PrepareStatement(ctx, query.Query)
	query.Query.CachedBTPrepare = preparedStmt
	if err != nil {
		btc.Logger.Error("Failed to prepare statement", zap.String("query", query.Query.TranslatedQuery), zap.Error(err))
		return nil, fmt.Errorf("failed to prepare statement: %w", err)
	}
	return btc.ExecutePreparedStatement(ctx, query)
}

// ExecutePreparedStatement -  Executes a prepared statement on Bigtable and returns the result.
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - query: rh.QueryMetadata containing the query and parameters.
//   - preparedStmt: PreparedStatement object containing the prepared statement.
//
// Returns:
//   - *message.RowsResult: The result of the select statement.
//   - time.Duration: The total elapsed time for the operation.
//   - error: Error if the statement preparation or execution fails.
func (btc *BigtableClient) ExecutePreparedStatement(ctx context.Context, query *translator.BoundSelectQuery) (*message.RowsResult, error) {
	params := query.Values.AsMap()
	boundStmt, err := query.Query.CachedBTPrepare.Bind(params)
	if err != nil {
		btc.Logger.Error("Failed to bind parameters", zap.Any("params", params), zap.Error(err))
		return nil, fmt.Errorf("failed to bind parameters: %w", err)
	}

	var processingErr error
	var rows []*types.BigtableResultRow
	executeErr := boundStmt.Execute(ctx, func(resultRow bigtable.ResultRow) bool {
		r, convertErr := btc.convertResultRow(resultRow, query.Query) // Call the implemented helper
		if convertErr != nil {
			btc.Logger.Error("Failed to convert result row", zap.Error(convertErr), zap.String("btql", query.Query.TranslatedQuery))
			processingErr = convertErr // Capture the error
			return false               // Stop execution
		}
		rows = append(rows, r)
		return true // Continue processing
	})
	if executeErr != nil {
		btc.Logger.Error("Failed to execute prepared statement", zap.Error(executeErr))
		return nil, fmt.Errorf("failed to execute prepared statement: %w", executeErr)
	}
	if processingErr != nil { // Check for error during row conversion/processing
		return nil, fmt.Errorf("failed during row processing: %w", processingErr)
	}

	return responsehandler.BuildRowsResultResponse(query.Keyspace(), query.Table(), query.Query.SelectedColumns, rows)
}

// convertResultRow converts a bigtable.ResultRow into the map format expected by the ResponseHandler.
func (btc *BigtableClient) convertResultRow(resultRow bigtable.ResultRow, query *translator.PreparedSelectQuery) (*types.BigtableResultRow, error) {
	table, err := btc.SchemaMappingConfig.GetTableConfig(query.Keyspace(), query.Table())
	if err != nil {
		return nil, err
	}

	results := make(map[string]types.GoValue)

	// Iterate through the columns defined in the result row metadata
	for i, colMeta := range resultRow.Metadata.Columns {
		var resultValue any
		resultKey := colMeta.Name
		err := resultRow.GetByName(resultKey, &resultValue)
		if err != nil {
			return nil, err
		}
		// handle nil resultValue cases
		if resultValue == nil {
			// Skip columns with nil results
			continue
		}
		if strings.Contains(resultKey, "$col") {
			resultKey = query.SelectedColumns[i].Sql
		}
		switch value := resultValue.(type) {
		case string:
			dt := query.SelectedColumns[i].ResultType
			// do we need to decode base64?
			goVal, err := utilities.StringToGo(value, dt.DataType())
			if err != nil {
				return nil, err
			}
			results[resultKey] = goVal
		case []byte:
			results[resultKey] = value
		case map[string]*int64:
			// counters are always a column family with a single column with an empty qualifier
			counterValue, ok := value[""]
			if ok {
				results[resultKey] = *counterValue
			} else {
				// default the counter to 0
				results[resultKey] = 0
			}
		case map[string][]uint8: // specific case of select * column in select
			if resultKey == string(table.SystemColumnFamily) { // default column family e.g cf1
				for k, val := range value {
					key, err := decodeBase64(k)
					if err != nil {
						return nil, err
					}
					colName := types.ColumnName(key)

					// unexpected column name - continue
					if !table.HasColumn(colName) {
						continue
					}
					col, err := table.GetColumn(colName)
					if err != nil {
						return nil, fmt.Errorf("unexpected column in results: '%s'", colName)
					}
					goVal, err := proxycore.DecodeType(col.CQLType.DataType(), constants.BigtableEncodingVersion, val)
					if err != nil {
						return nil, err
					}
					results[key] = goVal
				}
			} else { // handle collections
				dt := query.SelectedColumns[i].ResultType
				if dt.Code() == types.LIST {
					lt := dt.(types.ListType)
					var listValues []types.GoValue
					for _, listElement := range value {
						goVal, err := proxycore.DecodeType(lt.ElementType().DataType(), constants.BigtableEncodingVersion, listElement)
						if err != nil {
							return nil, err
						}
						listValues = append(listValues, goVal)
					}
					results[resultKey] = listValues
				} else if dt.Code() == types.SET {
					st := dt.(types.SetType)
					var setValues []types.GoValue
					for k := range value {
						key, err := decodeBase64(k)
						if err != nil {
							return nil, err
						}
						goKey, err := utilities.StringToGo(key, st.ElementType().DataType())
						if err != nil {
							return nil, err
						}
						setValues = append(setValues, goKey)
					}
					results[resultKey] = setValues
				} else if dt.Code() == types.MAP {
					mt := dt.(types.MapType)
					mapValue := make(map[types.GoValue]types.GoValue)
					for k, val := range value {
						key, err := decodeBase64(k)
						if err != nil {
							return nil, err
						}
						goKey, err := utilities.StringToGo(key, mt.KeyType().DataType())
						if err != nil {
							return nil, err
						}
						goVal, err := proxycore.DecodeType(dt.DataType(), constants.BigtableEncodingVersion, val)
						if err != nil {
							return nil, err
						}
						mapValue[goKey] = goVal
					}
					results[resultKey] = mapValue
				} else {
					return nil, fmt.Errorf("unhandled collection response type: %s", dt.String())
				}
			}
		case [][]byte: //specific case of listType column in select
			dt := query.SelectedColumns[i].ResultType
			lt, ok := dt.(types.ListType)
			if !ok {
				return nil, fmt.Errorf("expected list result type but got %s", dt.String())
			}
			var listValues []types.GoValue
			for _, listElement := range value {
				goVal, err := proxycore.DecodeType(lt.ElementType().DataType(), constants.BigtableEncodingVersion, listElement)
				if err != nil {
					return nil, err
				}
				listValues = append(listValues, goVal)
			}
			results[resultKey] = listValues
		case int64, float64:
			results[resultKey] = value
		case float32:
			results[resultKey] = float64(value)
		case time.Time:
			results[resultKey] = value
		case nil:
			results[resultKey] = nil
		default:
			return nil, fmt.Errorf("unsupported Bigtable SQL type  %T for column %s", value, resultKey)
		}
	}
	return &types.BigtableResultRow{
		ColumnMap: results,
	}, nil
}
