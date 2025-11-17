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

func (t *Translator) TranslateDelete(query string, sessionKeyspace types.Keyspace, isPreparedQuery bool) (*PreparedDeleteQuery, *BoundDeleteQuery, error) {
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

	ifExistObj := deleteObj.IfExist()
	var ifExist = false
	if ifExistObj != nil {
		val := strings.ToLower(ifExistObj.GetText())
		if val == ifExists {
			ifExist = true
		}
	}

	params := NewQueryParameters()
	values := NewQueryParameterValues(params)

	selectedColumns, err := parseDeleteColumns(deleteObj.DeleteColumnList(), tableConfig, params, values)
	if err != nil {
		return nil, nil, err
	}

	conditions, err := parseWhereClause(deleteObj.WhereSpec(), tableConfig, params, values)
	if err != nil {
		return nil, nil, err
	}

	for _, condition := range conditions {
		if condition.Operator != "=" {
			return nil, nil, fmt.Errorf("primary key conditions can only be equals")
		}
	}

	err = ValidateRequiredPrimaryKeysOnly(tableConfig, params)
	if err != nil {
		return nil, nil, err
	}

	if params.Has(UsingTimePlaceholder) {
		return nil, nil, fmt.Errorf("delete USING TIMESTAMP not supported")
	}

	st := &PreparedDeleteQuery{
		cqlQuery:        query,
		table:           tableName,
		keyspace:        keyspaceName,
		Conditions:      conditions,
		Params:          params,
		IfExists:        ifExist,
		SelectedColumns: selectedColumns,
	}

	var bound *BoundDeleteQuery
	if !isPreparedQuery {
		bound, err = t.doBindDelete(st, values)
		if err != nil {
			return nil, nil, err
		}
	} else {
		bound = nil
	}

	if isPreparedQuery {
		err = ValidateZeroParamsSet(values)
		if err != nil {
			return nil, nil, err
		}
	}

	return st, bound, nil
}

func (t *Translator) BindDelete(st *PreparedDeleteQuery, cassandraValues []*primitive.Value, protocolV primitive.ProtocolVersion) (*BoundDeleteQuery, error) {
	values, err := BindQueryParams(st.Params, cassandraValues, protocolV)
	if err != nil {
		return nil, err
	}

	return t.doBindDelete(st, values)
}

func (t *Translator) doBindDelete(st *PreparedDeleteQuery, values *QueryParameterValues) (*BoundDeleteQuery, error) {
	tableConfig, err := t.SchemaMappingConfig.GetTableConfig(st.Keyspace(), st.Table())
	if err != nil {
		return nil, err
	}

	rowKey, err := BindRowKey(tableConfig, values)
	if err != nil {
		return nil, fmt.Errorf("key encoding failed: %w", err)
	}

	cols, err := BindSelectColumns(tableConfig, st.SelectedColumns, values)
	if err != nil {
		return nil, err
	}
	return &BoundDeleteQuery{
		keyspace: st.Keyspace(),
		table:    st.Table(),
		IfExists: st.IfExists,
		rowKey:   rowKey,
		Columns:  cols,
	}, nil
}

// Parses the delete columns from a CQL DELETE statement and returns the selected columns with their associated map keys or list indices.
func parseDeleteColumns(deleteColumns cql.IDeleteColumnListContext, tableConfig *schemaMapping.TableConfig, params *QueryParameters, values *QueryParameterValues) ([]SelectedColumn, error) {
	if deleteColumns == nil {
		return nil, nil
	}
	cols := deleteColumns.AllDeleteColumnItem()
	var columns []SelectedColumn
	for _, v := range cols {
		var col SelectedColumn
		col.Name = v.OBJECT_NAME().GetText()
		if v.LS_BRACKET() != nil {
			if v.DecimalLiteral() != nil { // for list index
				p, err := parseDecimalLiteral(v.DecimalLiteral(), types.TypeInt, params, values)
				if err != nil {
					return nil, err
				}
				col.ListIndex = p
			} else if v.StringLiteral() != nil { //for map Key
				p, err := parseStringLiteral(v.StringLiteral(), params, values)
				if err != nil {
					return nil, err
				}
				col.MapKey = p
			} else {
				return nil, errors.New("unhandled delete column clause")
			}
		}
		if !tableConfig.HasColumn(types.ColumnName(col.Name)) {
			return nil, fmt.Errorf("unknown column `%s`", col.Name)
		}
		columns = append(columns, col)
	}
	return columns, nil
}
