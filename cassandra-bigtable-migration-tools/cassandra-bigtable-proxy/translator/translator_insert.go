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

package translator

import (
	"errors"
	"fmt"
	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/antlr4-go/antlr/v4"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// parseInsertColumns() parses columns and values from the Insert query
//
// Parameters:
//   - input: Insert Column Spec Context from antlr parser.
//   - tableName: Table Column
//   - schemaMapping: JSON Config which maintains column and its datatypes info.
//
// Returns: ColumnsResponse struct and error if any
func parseInsertColumns(input cql.IInsertColumnSpecContext, tableConfig *schemaMapping.TableConfig) ([]*types.Column, error) {

	if input == nil {
		return nil, errors.New("parseInsertColumns: No Input paramaters found for columns")
	}

	columnListObj := input.ColumnList()
	if columnListObj == nil {
		return nil, errors.New("parseInsertColumns: error while parsing columns")
	}
	columns := columnListObj.AllColumn()
	if columns == nil {
		return nil, errors.New("parseInsertColumns: error while parsing columns")
	}

	if len(columns) <= 0 {
		return nil, errors.New("parseInsertColumns: No Columns found in the Insert CqlQuery")
	}

	var foundColumns []*types.Column

	for _, val := range columns {
		columnName := types.ColumnName(val.GetText())
		if columnName == "" {
			return nil, errors.New("parseInsertColumns: No Columns found in the Insert CqlQuery")
		}
		sourceColumn, err := tableConfig.GetColumn(columnName)
		if err != nil {
			return nil, err
		}
		foundColumns = append(foundColumns, sourceColumn)
	}

	return foundColumns, nil
}

// parseAdHocValues() parses Columns from the Insert CqlQuery
//
// Parameters:
//   - input: Insert Valye Spec Context from antlr parser.
//   - columns: Array of Column Names
//
// Returns: Map Interface for param name as key and its value and error if any
func parseAdHocValues(input cql.IInsertValuesSpecContext, columns []*types.Column, protocolV primitive.ProtocolVersion) ([]types.BigtableData, map[types.ColumnName]types.GoValue, error) {
	if input == nil {
		return nil, nil, errors.New("parseAdHocValues: No ValuePlaceholder parameters found")
	}

	valuesExpressionList := input.ExpressionList()
	if valuesExpressionList == nil {
		return nil, nil, errors.New("parseAdHocValues: error while parsing values")
	}

	values := valuesExpressionList.AllExpression()
	if values == nil {
		return nil, nil, errors.New("parseAdHocValues: error while parsing values")
	}
	var data []types.BigtableData
	goValues := make(map[types.ColumnName]types.GoValue)
	for i, col := range columns {
		if i >= len(values) {
			return nil, nil, errors.New("parseAdHocValues: mismatch between columns and values")
		}
		goValue, err := parseCqlValue(values[i])
		if err != nil {
			return nil, nil, err
		}
		goValues[col.Name] = goValue

		btValues, err := encodeGoValueToBigtable(col, goValue, protocolV)
		if err != nil {
			return nil, nil, err
		}
		data = append(data, btValues...)
	}
	return data, goValues, nil
}

// PrepareInsertQuery() parses Columns from the Insert CqlQuery
//
// Parameters:
//   - queryStr: Read the query, parse its columns, values, table name, type of query and keyspaces etc.
//   - protocolV: Array of Column Names
//
// Returns: PreparedInsertQuery, build the PreparedInsertQuery and return it with nil value of error. In case of error
// PreparedInsertQuery will return as nil and error will contains the error object

func (t *Translator) PrepareInsertQuery(query string, protocolV primitive.ProtocolVersion, isPreparedQuery bool, sessionKeyspace string) (*PreparedInsertQuery, error) {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)

	insertObj := p.Insert()
	if insertObj == nil {
		return nil, errors.New("could not parse insert object")
	}
	kwInsertObj := insertObj.KwInsert()
	if kwInsertObj == nil {
		return nil, errors.New("could not parse insert object")
	}
	insertObj.KwInto()
	keyspace := insertObj.Keyspace()
	table := insertObj.Table()

	var keyspaceName string
	if keyspace != nil && keyspace.OBJECT_NAME() != nil && keyspace.OBJECT_NAME().GetText() != "" {
		keyspaceName = keyspace.OBJECT_NAME().GetText()
	} else if sessionKeyspace != "" {
		keyspaceName = sessionKeyspace
	} else {
		return nil, fmt.Errorf("invalid input parameters found for keyspace")
	}

	var tableName string
	if table != nil && table.OBJECT_NAME() != nil && table.OBJECT_NAME().GetText() != "" {
		tableName = table.OBJECT_NAME().GetText()
	} else {
		return nil, fmt.Errorf("invalid input paramaters found for table")
	}

	tableConfig, err := t.SchemaMappingConfig.GetTableConfig(keyspaceName, tableName)
	if err != nil {
		return nil, err
	}

	ifNotExists := insertObj.IfNotExist() != nil

	columns, err := parseInsertColumns(insertObj.InsertColumnSpec(), tableConfig)
	if err != nil {
		return nil, err
	}

	var columnNames []types.ColumnName
	for _, col := range columns {
		columnNames = append(columnNames, col.Name)
	}

	timestampInfo, err := GetTimestampInfo(insertObj, int32(len(columns)))
	if err != nil {
		return nil, err
	}

	err = ValidateRequiredPrimaryKeys(tableConfig, columnNames)
	if err != nil {
		return nil, err
	}

	var rowKey types.RowKey
	var data []types.BigtableData

	if !isPreparedQuery {
		var goValues map[types.ColumnName]types.GoValue
		data, goValues, err = parseAdHocValues(insertObj.InsertValuesSpec(), columns, protocolV)
		if err != nil {
			return nil, err
		}

		rowKey, err = createOrderedCodeKey(tableConfig, goValues)
		if err != nil {
			return nil, err
		}
	}

	insertQueryData := &PreparedInsertQuery{
		CqlQuery:      query,
		Table:         tableName,
		Keyspace:      keyspaceName,
		Data:          data,
		RowKey:        rowKey,
		TimestampInfo: timestampInfo,
		IfNotExists:   ifNotExists,
	}
	return insertQueryData, nil
}

func (t *Translator) BindInsert(cassandraValues []*primitive.Value, preparedQuery *PreparedInsertQuery, pv primitive.ProtocolVersion) (*PreparedInsertQuery, error) {
	tableConfig, err := t.SchemaMappingConfig.GetTableConfig(preparedQuery.Keyspace, preparedQuery.Table)
	if err != nil {
		return nil, err
	}

	timestampInfo, err := getTimestampInfo(preparedQuery, cassandraValues)
	if err != nil {
		return nil, err
	}

	values, err := decodePreparedValues(tableConfig, preparedQuery.Columns, nil, cassandraValues, pv)
	if err != nil {
		fmt.Println("Error processing prepared collection columns:", err)
		return nil, err
	}

	rowKey, err := createOrderedCodeKey(tableConfig, values.GoValues)
	if err != nil {
		return nil, err
	}

	insertQueryData := &PreparedInsertQuery{
		Columns:       preparedQuery.Columns,
		Data:          values.Data,
		RowKey:        rowKey,
		TimestampInfo: timestampInfo,
		CqlQuery:      preparedQuery.CqlQuery,
		Table:         preparedQuery.Table,
		Keyspace:      preparedQuery.Keyspace,
		ParamKeys:     preparedQuery.ParamKeys,
		IfNotExists:   preparedQuery.IfNotExists,
	}

	return insertQueryData, nil
}
