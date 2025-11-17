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
func parseInsertColumns(input cql.IInsertColumnSpecContext, tableConfig *schemaMapping.TableConfig, params *QueryParameters) ([]Assignment, error) {

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

	var assignments []Assignment
	for _, val := range columns {
		columnName := types.ColumnName(val.GetText())
		col, err := tableConfig.GetColumn(columnName)
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, &ComplexAssignmentSet{
			column: col,
			Value:  params.PushParameter(col.Name, col.CQLType),
		})
	}

	return assignments, nil
}

func parseInsertValues(input cql.IInsertValuesSpecContext, columns []Assignment, params *QueryParameters, values *QueryParameterValues) error {
	if input == nil {
		return errors.New("insert values clause missing or malformed")
	}

	valuesExpressionList := input.ExpressionList()
	if valuesExpressionList == nil {
		return errors.New("setParamsFromValues: error while parsing values")
	}

	allValues := valuesExpressionList.AllExpression()
	if allValues == nil {
		return errors.New("setParamsFromValues: error while parsing values")
	}

	if len(allValues) != len(columns) {
		return fmt.Errorf("found mismatch between column count (%d) value count (%d)", len(columns), len(allValues))

	}

	for i, value := range allValues {
		if value.GetText() == "?" {
			continue
		}

		column := columns[i].Column()
		placeholder, ok := params.GetPlaceholderForColumn(column.Name)
		if !ok {
			return fmt.Errorf("unhandled error: missing parameter for column '%s'", column.Name)
		}

		// todo this function doesn't handle collections
		val, err := stringToGo(trimQuotes(value.GetText()), column.CQLType.DataType())

		err = values.SetValue(placeholder, val)
		if err != nil {
			return err
		}
	}

	return nil
}

// TranslateInsertQuery() parses Columns from the Insert CqlQuery
//
// Parameters:
//   - queryStr: Read the query, parse its columns, values, table name, type of query and keyspaces etc.
//   - protocolV: Array of Column Names
//
// Returns: PreparedInsertQuery, build the PreparedInsertQuery and return it with nil value of error. In case of error
// PreparedInsertQuery will return as nil and error will contains the error object

func (t *Translator) TranslateInsertQuery(query string, isPreparedQuery bool, sessionKeyspace string) (*PreparedInsertQuery, *BigtableWriteMutation, error) {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)

	insertObj := p.Insert()
	if insertObj == nil {
		return nil, nil, errors.New("could not parse insert object")
	}
	kwInsertObj := insertObj.KwInsert()
	if kwInsertObj == nil {
		return nil, nil, errors.New("could not parse insert object")
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
		return nil, nil, fmt.Errorf("invalid input parameters found for keyspace")
	}

	var tableName string
	if table != nil && table.OBJECT_NAME() != nil && table.OBJECT_NAME().GetText() != "" {
		tableName = table.OBJECT_NAME().GetText()
	} else {
		return nil, nil, fmt.Errorf("invalid input paramaters found for table")
	}

	tableConfig, err := t.SchemaMappingConfig.GetTableConfig(keyspaceName, tableName)
	if err != nil {
		return nil, nil, err
	}

	ifNotExists := insertObj.IfNotExist() != nil

	params := NewQueryParameters()
	values := NewQueryParameterValues(params)

	assignments, err := parseInsertColumns(insertObj.InsertColumnSpec(), tableConfig, params)
	if err != nil {
		return nil, nil, err
	}

	err = parseInsertValues(insertObj.InsertValuesSpec(), assignments, params, values)
	if err != nil {
		return nil, nil, err
	}

	err = GetTimestampInfo(insertObj.UsingTtlTimestamp(), params, values)
	if err != nil {
		return nil, nil, err
	}

	err = ValidateRequiredPrimaryKeys(tableConfig, params)
	if err != nil {
		return nil, nil, err
	}

	st := &PreparedInsertQuery{
		CqlQuery:    query,
		Keyspace:    keyspaceName,
		Table:       tableName,
		Params:      params,
		Assignments: assignments,
		IfNotExists: ifNotExists,
	}
	var bound *BigtableWriteMutation
	if !isPreparedQuery {
		bound, err = t.doBindInsert(st, values)
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

func (t *Translator) BindInsert(st *PreparedInsertQuery, cassandraValues []*primitive.Value, pv primitive.ProtocolVersion) (*BigtableWriteMutation, error) {
	values, err := BindQueryParams(st.Params, cassandraValues, pv)
	if err != nil {
		return nil, err
	}
	return t.doBindInsert(st, values)
}

func (t *Translator) doBindInsert(st *PreparedInsertQuery, values *QueryParameterValues) (*BigtableWriteMutation, error) {
	tableConfig, err := t.SchemaMappingConfig.GetTableConfig(st.Keyspace, st.Table)
	if err != nil {
		return nil, err
	}

	rowKey, err := BindRowKey(tableConfig, values)
	if err != nil {
		return nil, fmt.Errorf("key encoding failed: %w", err)
	}

	mutations := BigtableWriteMutation{
		RowKey: rowKey,
	}
	err = BindMutations(st.Assignments, values, &mutations)
	if err != nil {
		return nil, err
	}

	return &mutations, nil
}
