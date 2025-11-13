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
	"strings"

	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/antlr4-go/antlr/v4"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// parseTableFromDelete() extracts the table and keyspace information from the input context of a DELETE query.
//
// Parameters:
//   - input: A context interface representing the FROM specification of a CQL DELETE query.
//
// Returns:
//   - A pointer to a TableObj containing the extracted table and keyspace names.
//   - An error if the input is nil, or if there are issues in parsing the table or keyspace names.
func parseTableFromDelete(input cql.IFromSpecContext) (*TableObj, error) {
	if input == nil {
		return nil, errors.New("no input parameters found for table and keyspace")
	}

	fromSpec, err := getFromSpecElement(input)
	if err != nil {
		return nil, err
	}

	allObj, err := getAllObjectNames(fromSpec)
	if err != nil {
		return nil, err
	}

	keyspaceName, tableName, err := getTableAndKeyspaceObjects(allObj)
	if err != nil {
		return nil, err
	}

	response := TableObj{
		TableName:    tableName,
		KeyspaceName: keyspaceName,
	}

	return &response, nil
}

// TranslateDelete() translate the CQL Delete CqlQuery into bigtable mutation api equivalent.
//
// Parameters:
//   - queryStr: CQL delete query with condition
//
// Returns: WhereClause and an error if any.
func (t *Translator) TranslateDelete(query string, isPreparedQuery bool, sessionKeyspace string) (*DeleteQueryMapping, error) {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)

	deleteObj := p.Delete_()
	if deleteObj == nil || deleteObj.KwDelete() == nil {
		return nil, errors.New("error while parsing delete object")
	}

	tableSpec, err := parseTableFromDelete(deleteObj.FromSpec())
	if err != nil {
		return nil, err
	}
	keyspaceName := tableSpec.KeyspaceName
	tableName := tableSpec.TableName

	if keyspaceName == "" {
		if sessionKeyspace != "" {
			keyspaceName = sessionKeyspace
		} else {
			return nil, fmt.Errorf("invalid input parameters found for keyspace")
		}
	}

	tableConfig, err := t.SchemaMappingConfig.GetTableConfig(keyspaceName, tableName)
	if err != nil {
		return nil, err
	}

	selectedColumns, err := parseDeleteColumns(deleteObj.DeleteColumnList(), tableConfig)
	if err != nil {
		return nil, err
	}
	timestampInfo, err := GetTimestampInfoForRawDelete(deleteObj)
	if err != nil {
		return nil, err
	}
	ifExistObj := deleteObj.IfExist()
	var ifExist = false
	if ifExistObj != nil {
		val := strings.ToLower(ifExistObj.GetText())
		if val == ifExists {
			ifExist = true
		}
	}

	var QueryClauses WhereClause

	if deleteObj.WhereSpec() != nil {
		resp, err := parseWhereByClause(deleteObj.WhereSpec(), tableConfig)
		if err != nil {
			return nil, errors.New("TranslateDeletetQuerytoBigtable: Invalid Where clause condition")
		}
		QueryClauses = *resp
	}

	pkValues := make(map[types.ColumnName]types.GoValue)
	var pkNames []types.ColumnName
	for _, condition := range QueryClauses.Conditions {
		if !condition.Column.IsPrimaryKey {
			return nil, fmt.Errorf("non PRIMARY KEY columns found in where condition: %s", condition.Column.Name)
		}
		if condition.Operator != "=" {
			return nil, fmt.Errorf("primary key conditions can only be equals")
		}
		pkValues[condition.Column.Name] = condition.Value
		pkNames = append(pkNames, condition.Column.Name)
	}

	err = ValidateRequiredPrimaryKeysOnly(tableConfig, pkNames)
	if err != nil {
		return nil, err
	}

	var rowKey types.RowKey
	if !isPreparedQuery {
		rowKey, err = createOrderedCodeKey(tableConfig, pkValues)
		if err != nil {
			return nil, fmt.Errorf("key encoding failed. %w", err)
		}
	}

	deleteQueryData := &DeleteQueryMapping{
		Query:           query,
		Table:           tableName,
		Keyspace:        keyspaceName,
		Conditions:      QueryClauses.Conditions,
		Params:          QueryClauses.Params,
		ParamKeys:       QueryClauses.ParamKeys,
		RowKey:          rowKey,
		TimestampInfo:   timestampInfo,
		IfExists:        ifExist,
		SelectedColumns: selectedColumns,
	}
	return deleteQueryData, nil
}

func (t *Translator) BindDelete(tableConfig *schemaMapping.TableConfig, st *DeleteQueryMapping, columns []*types.Column, values []*primitive.Value, protocolV primitive.ProtocolVersion) (types.RowKey, TimestampInfo, error) {
	timestamp, values, err := ProcessTimestampByDelete(st, values)
	if err != nil {
		return "", TimestampInfo{}, fmt.Errorf("error while getting timestamp value")
	}

	preparedValues, err := decodePreparedValues(tableConfig, columns, nil, values, protocolV)
	if err != nil {
		return "", timestamp, err
	}
	rowKey, err := createOrderedCodeKey(tableConfig, preparedValues.GoValues)
	if err != nil {
		return "", timestamp, fmt.Errorf("key encoding failed. %w", err)
	}
	return rowKey, timestamp, nil
}

// Parses the delete columns from a CQL DELETE statement and returns the selected columns with their associated map keys or list indices.
func parseDeleteColumns(deleteColumns cql.IDeleteColumnListContext, tableConfig *schemaMapping.TableConfig) ([]types.SelectedColumn, error) {
	if deleteColumns == nil {
		return nil, nil
	}
	cols := deleteColumns.AllDeleteColumnItem()
	var Columns []types.SelectedColumn
	var decimalLiteral, stringLiteral string
	for _, v := range cols {
		var Column types.SelectedColumn
		Column.Name = types.ColumnName(v.OBJECT_NAME().GetText())
		if v.LS_BRACKET() != nil {
			if v.DecimalLiteral() != nil { // for list index
				decimalLiteral = v.DecimalLiteral().GetText()
				Column.ListIndex = decimalLiteral
			}
			if v.StringLiteral() != nil { //for map Key
				stringLiteral = v.StringLiteral().GetText()
				stringLiteral = strings.Trim(stringLiteral, "'")
				Column.MapKey = stringLiteral
			}
		}
		_, err := tableConfig.GetColumnType(Column.Name)
		if err != nil {
			return nil, err
		}
		Columns = append(Columns, Column)
	}
	return Columns, nil
}
