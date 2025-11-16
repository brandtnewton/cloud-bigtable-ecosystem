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
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/bindings"
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

func (t *Translator) TranslateDelete(query string, isPreparedQuery bool, sessionKeyspace string) (*DeleteQueryMapping, *types.BoundDeleteQuery, error) {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)

	deleteObj := p.Delete_()
	if deleteObj == nil || deleteObj.KwDelete() == nil {
		return nil, nil, errors.New("error while parsing delete object")
	}

	tableSpec, err := parseTableFromDelete(deleteObj.FromSpec())
	if err != nil {
		return nil, nil, err
	}
	keyspaceName := tableSpec.KeyspaceName
	tableName := tableSpec.TableName

	if keyspaceName == "" {
		if sessionKeyspace != "" {
			keyspaceName = sessionKeyspace
		} else {
			return nil, nil, fmt.Errorf("invalid input parameters found for keyspace")
		}
	}

	tableConfig, err := t.SchemaMappingConfig.GetTableConfig(keyspaceName, tableName)
	if err != nil {
		return nil, nil, err
	}

	selectedColumns, err := parseDeleteColumns(deleteObj.DeleteColumnList(), tableConfig)
	if err != nil {
		return nil, nil, err
	}
	timestampInfo, err := GetTimestampInfoForRawDelete(deleteObj)
	if err != nil {
		return nil, nil, err
	}
	ifExistObj := deleteObj.IfExist()
	var ifExist = false
	if ifExistObj != nil {
		val := strings.ToLower(ifExistObj.GetText())
		if val == ifExists {
			ifExist = true
		}
	}

	params := types.NewQueryParameters()
	values := types.NewQueryParameterValues(params)

	whereClause, err := parseWhereByClause(deleteObj.WhereSpec(), tableConfig, params, values)
	if err != nil {
		return nil, nil, err
	}

	for _, condition := range whereClause.Conditions {
		if condition.Operator != "=" {
			return nil, nil, fmt.Errorf("primary key conditions can only be equals")
		}
	}

	err = ValidateRequiredPrimaryKeysOnly(tableConfig, params)
	if err != nil {
		return nil, nil, err
	}

	if params.Has(types.UsingTimePlaceholder) {
		return nil, nil, fmt.Errorf("delete USING TIMESTAMP not supported")
	}

	st := &DeleteQueryMapping{
		Query:           query,
		Table:           tableName,
		Keyspace:        keyspaceName,
		Conditions:      whereClause.Conditions,
		Params:          whereClause.Params,
		IfExists:        ifExist,
		SelectedColumns: selectedColumns,
	}

	var bound *types.BoundDeleteQuery
	if !isPreparedQuery {
		bound, err = t.doBindDelete(st, values)
		if err != nil {
			return nil, nil, err
		}
	} else {
		bound = nil
	}

	return st, bound, nil
}

func (t *Translator) BindDelete(st *DeleteQueryMapping, cassandraValues []*primitive.Value, protocolV primitive.ProtocolVersion) (*types.BoundDeleteQuery, error) {
	values, err := bindings.BindQueryParams(st.Params, cassandraValues, protocolV)
	if err != nil {
		return nil, err
	}

	return t.doBindDelete(st, values)
}

func (t *Translator) doBindDelete(st *DeleteQueryMapping, values *types.QueryParameterValues) (*types.BoundDeleteQuery, error) {
	tableConfig, err := t.SchemaMappingConfig.GetTableConfig(st.Keyspace, st.Table)
	if err != nil {
		return nil, err
	}

	rowKey, err := bindings.BindRowKey(tableConfig, values)
	if err != nil {
		return nil, fmt.Errorf("key encoding failed: %w", err)
	}

	cols, err := bindings.BindSelectColumns(tableConfig, st.SelectedColumns, values)
	if err != nil {
		return nil, err
	}
	return &types.BoundDeleteQuery{
		RowKey:  rowKey,
		Columns: cols,
	}, nil
}

// Parses the delete columns from a CQL DELETE statement and returns the selected columns with their associated map keys or list indices.
func parseDeleteColumns(deleteColumns cql.IDeleteColumnListContext, tableConfig *schemaMapping.TableConfig) ([]types.SelectedColumn, error) {
	if deleteColumns == nil {
		return nil, nil
	}
	cols := deleteColumns.AllDeleteColumnItem()
	var columns []types.SelectedColumn
	var decimalLiteral, stringLiteral string
	for _, v := range cols {
		var col types.SelectedColumn
		col.Name = v.OBJECT_NAME().GetText()
		if v.LS_BRACKET() != nil {
			if v.DecimalLiteral() != nil { // for list index
				decimalLiteral = v.DecimalLiteral().GetText()
				col.ListIndex = decimalLiteral
			}
			if v.StringLiteral() != nil { //for map Key
				stringLiteral = v.StringLiteral().GetText()
				stringLiteral = strings.Trim(stringLiteral, "'")
				col.MapKey = stringLiteral
			}
		}
		if !tableConfig.HasColumn(types.ColumnName(col.Name)) {
			return nil, fmt.Errorf("unknown column `%s`", col.Name)
		}
		columns = append(columns, col)
	}
	return columns, nil
}
