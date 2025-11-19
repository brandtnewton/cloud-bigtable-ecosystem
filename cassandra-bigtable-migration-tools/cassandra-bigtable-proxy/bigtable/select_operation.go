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
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"go.uber.org/zap"
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
func (btc *BigtableClient) SelectStatement(ctx context.Context, query *types.BoundSelectQuery) (*message.RowsResult, error) {
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
func (btc *BigtableClient) ExecutePreparedStatement(ctx context.Context, query *types.BoundSelectQuery) (*message.RowsResult, error) {
	params := query.Values.AsMap()
	boundStmt, err := query.Query.CachedBTPrepare.Bind(params)
	if err != nil {
		btc.Logger.Error("Failed to bind parameters", zap.Any("params", params), zap.Error(err))
		return nil, fmt.Errorf("failed to bind parameters: %w", err)
	}

	table, err := btc.SchemaMappingConfig.GetTableConfig(query.Keyspace(), query.Table())

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

	return responsehandler.BuildRowsResultResponse(table, query.Query, rows, query.ProtocolVersion)
}

// convertResultRow converts a bigtable.ResultRow into the map format expected by the ResponseHandler.
func (btc *BigtableClient) convertResultRow(resultRow bigtable.ResultRow, query *types.PreparedSelectQuery) (*types.BigtableResultRow, error) {
	table, err := btc.SchemaMappingConfig.GetTableConfig(query.Keyspace(), query.Table())
	if err != nil {
		return nil, err
	}

	selectStarScalarsMap, err := createSelectStarScalarsMap(table, resultRow)

	var results []types.GoValue
	for i, colMeta := range query.ResultColumnMetadata {
		var col *types.Column
		var expectedType types.CqlDataType

		if query.SelectClause.IsStar {
			col, err = table.GetColumn(types.ColumnName(colMeta.Name))
			if err != nil {
				return nil, err
			}
			expectedType = col.CQLType
		} else {
			col, err = table.GetColumn(query.SelectClause.Columns[i].ColumnName)
			if err != nil {
				return nil, err
			}
			expectedType = query.SelectClause.Columns[i].ResultType
		}

		// if the query is "select *" we need to
		if query.SelectClause.IsStar && col.ColumnFamily == table.SystemColumnFamily {
			if s, ok := selectStarScalarsMap[types.ColumnName(colMeta.Name)]; ok {
				results = append(results, s)
			} else {
				// scalar value but no value has been set so default to nil
				results = append(results, nil)
			}
			continue
		}

		var resultValue any
		resultKey := colMeta.Name
		err := resultRow.GetByName(resultKey, &resultValue)
		if err != nil {
			return nil, err
		}

		switch value := resultValue.(type) {
		case string:
			// do we need to decode base64?
			goVal, err := utilities.StringToGo(value, expectedType.DataType())
			if err != nil {
				return nil, err
			}
			results = append(results, goVal)
		case []byte:
			results = append(results, value)
		case map[string]*int64:
			// counters are always a column family with a single column with an empty qualifier
			counterValue, ok := value[""]
			if ok {
				results = append(results, *counterValue)
			} else {
				// default the counter to 0
				results = append(results, int64(0))
			}
		case map[string][]uint8:
			// handle collections
			if expectedType.Code() == types.LIST {
				lt := expectedType.(*types.ListType)
				var listValues []types.GoValue
				for _, listElement := range value {
					goVal, err := proxycore.DecodeType(lt.ElementType().BigtableStorageType().DataType(), constants.BigtableEncodingVersion, listElement)
					if err != nil {
						return nil, err
					}
					listValues = append(listValues, goVal)
				}
				results = append(results, listValues)
			} else if expectedType.Code() == types.SET {
				st := expectedType.(*types.SetType)
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
				results = append(results, setValues)
			} else if expectedType.Code() == types.MAP {
				mt := expectedType.(*types.MapType)
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
					goVal, err := proxycore.DecodeType(expectedType.BigtableStorageType().DataType(), constants.BigtableEncodingVersion, val)
					if err != nil {
						return nil, err
					}
					mapValue[goKey] = goVal
				}
				results = append(results, mapValue)
			} else {
				return nil, fmt.Errorf("unhandled collection response type: %s", expectedType.String())
			}
		case [][]byte: //specific case of listType column in select
			lt, ok := expectedType.(types.ListType)
			if !ok {
				return nil, fmt.Errorf("expected list result type but got %s", expectedType.String())
			}
			var listValues []types.GoValue
			for _, listElement := range value {
				goVal, err := proxycore.DecodeType(lt.ElementType().BigtableStorageType().DataType(), constants.BigtableEncodingVersion, listElement)
				if err != nil {
					return nil, err
				}
				listValues = append(listValues, goVal)
			}
			results = append(results, listValues)
		case int64, float64:
			results = append(results, value)
		case float32:
			results = append(results, float64(value))
		case time.Time:
			results = append(results, value)
		case nil:
			results = append(results, nil)
		default:
			return nil, fmt.Errorf("unsupported Bigtable SQL type  %T for column %s", value, resultKey)
		}
	}
	return &types.BigtableResultRow{
		Values: results,
	}, nil
}

// createSelectStarScalarsMap - extracts the SystemColumnFamily result, if it exists, and loads it into a map. This map will only be populated with scalar column values when a "select *" is used because those columns are all stored in one column family.
func createSelectStarScalarsMap(table *schemaMapping.TableConfig, resultRow bigtable.ResultRow) (map[types.ColumnName]types.GoValue, error) {
	hasSysColumn := false
	for _, c := range resultRow.Metadata.Columns {
		if c.Name == string(table.SystemColumnFamily) {
			hasSysColumn = true
			break
		}
	}

	result := make(map[types.ColumnName]types.GoValue)

	if !hasSysColumn {
		return result, nil
	}

	var valueMap map[string][]uint8
	err := resultRow.GetByName(string(table.SystemColumnFamily), &valueMap)
	if err != nil {
		return nil, err
	}

	for k, val := range valueMap {
		key, err := decodeBase64(k)
		if err != nil {
			return nil, err
		}
		colName := types.ColumnName(key)

		// unexpected column name - can happen if a column was dropped
		if !table.HasColumn(colName) {
			continue
		}

		col, err := table.GetColumn(colName)
		if err != nil {
			return nil, err
		}
		goVal, err := proxycore.DecodeType(col.CQLType.BigtableStorageType().DataType(), constants.BigtableEncodingVersion, val)
		if err != nil {
			return nil, err
		}
		result[col.Name] = goVal
	}

	return result, nil
}
