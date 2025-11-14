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
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"slices"
	"strconv"
	"strings"

	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/antlr4-go/antlr/v4"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// Helper function to check if a node is a column reference
func isColumn(node antlr.Tree, colName types.ColumnName) bool {
	if tn, ok := node.(antlr.TerminalNode); ok {
		return types.ColumnName(tn.GetText()) == colName
	}
	return false
}

// Helper function to check if a node is a collection type
func isCollection(node antlr.Tree) bool {
	switch node.(type) {
	case cql.IAssignmentListContext, cql.IAssignmentSetContext, cql.IAssignmentMapContext:
		return true
	}
	return false
}

// Helper function to check if a node is a collection type
func isCounter(node antlr.Tree) bool {
	switch node.(type) {
	case cql.IDecimalLiteralContext:
		return true
	default:
		return false
	}
}

// Helper function to get text from a node
func getNodeText(node antlr.Tree, parser antlr.Parser) string {
	if tn, ok := node.(antlr.TerminalNode); ok {
		return tn.GetText()
	}
	return antlr.TreesGetNodeText(node, nil, parser)
}

// Helper function to get value from a node
func getNodeValue(node antlr.Tree, parent antlr.ParserRuleContext) interface{} {
	switch ctx := node.(type) {
	case cql.IAssignmentListContext:
		val, _ := parseCqlValue(ctx)
		return val
	case cql.IAssignmentSetContext:
		val, _ := parseCqlValue(ctx)
		return val
	case cql.IAssignmentMapContext:
		val, _ := parseCqlValue(ctx)
		return val
	case cql.IDecimalLiteralContext:
		return ctx.GetText()
	case antlr.TerminalNode:
		return ctx.GetText()
	default:
		if parser, ok := parent.(interface{ GetParser() antlr.Parser }); ok {
			return antlr.TreesGetNodeText(node, nil, parser.GetParser())
		}
		return antlr.TreesGetNodeText(node, nil, nil)
	}

}

// parseAssignments() processes a list of assignment elements from a CQL update statement,
// generating a structured response that includes the assignments' details and their corresponding
// placeholder values for parameterized queries.
//
// Parameters:
//   - assignments: A slice of IAssignmentElementContext, each representing an assignment in the CQL query.
//   - tableName: The name of the table being updated.
//   - schemaMapping: A pointer to SchemaMappingConfig holding schema information for the table.
//   - keyspace: The name of the keyspace containing the table.
//
// Returns:
//   - A pointer to an UpdateSetResponse which contains structured information about the assignments,
//     including updated values, parameter keys, and parameters map.
//   - An error if invalid input is detected, such as an empty assignment list or issues with assignment syntax,
//     or if a column type cannot be retrieved from the schema mapping table, or if an attempt is made to assign
//     a value to a primary key.
func parseAssignments(assignments []cql.IAssignmentElementContext, tableConfig *schemaMapping.TableConfig) (*UpdateSetResponse, error) {
	if len(assignments) == 0 {
		return nil, errors.New("invalid input")
	}
	var setResp []UpdateSetValue
	var paramKeys []string
	params := make(map[string]interface{})

	for i, setVal := range assignments {
		colObj := setVal.OBJECT_NAME(0)
		if colObj == nil {
			return nil, errors.New("error parsing column for assignments")
		}
		columnName := types.ColumnName(colObj.GetText())
		if columnName == "" {
			return nil, errors.New("no columnName found for assignments")
		}

		col, err := tableConfig.GetColumn(columnName)
		if err != nil {
			return nil, err
		}
		var complexAssignment ComplexAssignment
		var goValue types.GoValue
		var bv types.BigtableValue

		// Handle append, prepend, remove as ComplexAssignment
		if setVal.PLUS() != nil || setVal.MINUS() != nil {
			var op = "+"
			if setVal.MINUS() != nil {
				op = "-"
			}
			// Find the operator index
			var opIndex int
			for i := 0; i < setVal.GetChildCount(); i++ {
				child := setVal.GetChild(i)
				if token, ok := child.(antlr.TerminalNode); ok && token.GetText() == op {
					opIndex = i
					break
				}
			}
			left := setVal.GetChild(opIndex - 1)
			right := setVal.GetChild(opIndex + 1)
			var leftVal, rightVal interface{}
			isPrepend := false
			if isColumn(left, columnName) && (isCollection(right) || isCounter(right)) && op == "+" {
				// Append: col = col + [values]
				leftVal = getNodeText(left, setVal.GetParser())
				rightVal = getNodeValue(right, setVal)
			} else if (isCollection(left) || isCounter(left)) && isColumn(right, col.Name) && op == "+" {
				// Prepend: col = [values] + col
				leftVal = getNodeValue(left, setVal)
				rightVal = getNodeText(right, setVal.GetParser())
				isPrepend = true
			} else {
				// fallback: handle error or other cases
				leftVal = getNodeText(left, setVal.GetParser())
				rightVal = getNodeValue(right, setVal)
			}
			complexAssignment = ComplexAssignment{
				Column:    columnName,
				Operation: op,
				IsPrepend: isPrepend,
				Left:      leftVal,
				Right:     rightVal,
			}
		} else if setVal.SyntaxBracketLs() != nil && setVal.DecimalLiteral() != nil && setVal.SyntaxBracketRs() != nil && setVal.Constant() != nil {
			// i.e. marks[1] = 99 (index update)
			index := strings.Trim(setVal.DecimalLiteral().GetText(), "'")
			valueRaw, err := parseCqlConstant(setVal.Constant())
			if err != nil {
				return nil, err
			}
			complexAssignment = ComplexAssignment{
				Column:    columnName,
				Operation: "update_index",
				Left:      index,
				Right:     valueRaw,
			}
		} else if setVal.Constant() != nil {
			goValue, err = parseCqlConstant(setVal.Constant())
		} else if setVal.AssignmentMap() != nil {
			goValue, err = parseCqlMapAssignment(setVal.AssignmentMap())
		} else if setVal.AssignmentSet() != nil {
			goValue, err = parseCqlSetAssignment(setVal.AssignmentSet())
		} else if setVal.AssignmentList() != nil {
			goValue, err = parseCqlListAssignment(setVal.AssignmentList())
		} else if setVal.QUESTION_MARK() != nil {
			goValue = questionMark
		} else {
			goValue = setVal.GetText()
		}
		if err != nil {
			return nil, err
		}

		column, err := tableConfig.GetColumn(columnName)
		if err != nil {
			return nil, err
		}
		if column.IsPrimaryKey {
			return nil, fmt.Errorf("primary key not allowed to assignments")
		}

		paramKeys = append(paramKeys, "set"+strconv.Itoa(i+1))
		setResp = append(setResp, UpdateSetValue{
			Column:            columnName,
			Value:             "@set" + strconv.Itoa(i+1),
			BigtableValue:     bv,
			GoValue:           goValue,
			ComplexAssignment: complexAssignment,
			CQLType:           column.CQLType,
		})
	}
	return &UpdateSetResponse{
		UpdateSetValues: setResp,
		Params:          params,
	}, nil
}

// PrepareUpdateQuery() frames UpdateQueryMapping which translates the update to bigtable mutation
//
// Parameters:
//   - query: CQL Update query
//
// Returns: UpdateQueryMapping struct and error if any
func (t *Translator) PrepareUpdateQuery(query string, isPreparedQuery bool, sessionKeyspace string) (*UpdateQueryMapping, error) {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)
	updateObj := p.Update()

	if updateObj == nil || updateObj.KwUpdate() == nil {
		return nil, errors.New("error parsing the update object")
	}

	keyspace := updateObj.Keyspace()
	updateObj.DOT()
	table := updateObj.Table()

	var keyspaceName, tableName string

	if table != nil && table.OBJECT_NAME() != nil && table.OBJECT_NAME().GetText() != "" {
		tableName = table.OBJECT_NAME().GetText()
	} else {
		return nil, fmt.Errorf("invalid input paramaters found for table")
	}

	if keyspace != nil && keyspace.OBJECT_NAME() != nil && keyspace.OBJECT_NAME().GetText() != "" {
		keyspaceName = keyspace.OBJECT_NAME().GetText()
	} else if sessionKeyspace != "" {
		keyspaceName = sessionKeyspace
	} else {
		return nil, fmt.Errorf("invalid input parameters found for keyspace")
	}

	tableConfig, err := t.SchemaMappingConfig.GetTableConfig(keyspaceName, tableName)
	if err != nil {
		return nil, err
	}

	timestampInfo, err := GetTimestampInfoByUpdate(updateObj)
	if err != nil {
		return nil, err
	}
	updateObj.KwSet()
	if updateObj.Assignments() == nil || updateObj.Assignments().AllAssignmentElement() == nil {
		return nil, errors.New("error parsing the assignment object")
	}

	assignmentObj := updateObj.Assignments()
	allAssignmentObj := assignmentObj.AllAssignmentElement()
	if allAssignmentObj == nil {
		return nil, errors.New("error parsing all the assignment object")
	}
	setValues, err := parseAssignments(allAssignmentObj, tableConfig)
	if err != nil {
		return nil, err
	}

	var clauses WhereClause
	if updateObj.WhereSpec() != nil {
		resp, err := parseWhereByClause(updateObj.WhereSpec(), tableConfig)
		if err != nil {
			return nil, err
		}
		clauses = *resp
		for k, v := range clauses.Params {
			setValues.Params[k] = v
		}
	}
	ifExistObj := updateObj.IfExist()
	var ifExist bool = false
	if ifExistObj != nil {
		val := strings.ToLower(ifExistObj.GetText())
		if val == ifExists {
			ifExist = true
		}
	}

	var actualPrimaryKeys []types.ColumnName
	for _, val := range clauses.Conditions {
		actualPrimaryKeys = append(actualPrimaryKeys, val.Column.Name)
	}
	err = ValidateRequiredPrimaryKeysOnly(tableConfig, actualPrimaryKeys)
	if err != nil {
		return nil, err
	}

	var rowKey types.RowKey
	var columns []*types.Column
	var values []types.BigtableData
	for _, val := range setValues.UpdateSetValues {
		c := &types.Column{Name: val.Column, ColumnFamily: t.SchemaMappingConfig.SystemColumnFamily, CQLType: val.CQLType}
		columns = append(columns, c)
		values = append(values, types.BigtableData{
			Family: val.ColumnFamily,
			Column: types.ColumnQualifier(val.Column),
			Bytes:  val.BigtableValue,
		})
	}
	var delColumns []*types.Column
	var delColumnFamily []types.ColumnFamily
	var complexMeta map[types.ColumnFamily]*ComplexOperation
	var rawOutput *AdHocQueryValues // Declare rawOutput here

	if !isPreparedQuery {
		pkValues := make(map[types.ColumnName]types.GoValue)
		for _, key := range primaryKeys {
			for _, clause := range clauses.Conditions {
				if !clause.Column.IsPrimaryKey {
					return nil, errors.New("any value other then primary key is not accepted in where clause")
				}
				if clause.Column.IsPrimaryKey && clause.Operator == "=" && key == clause.Column {
					pv := clause.Value
					if strings.HasPrefix(clause.Value, "@") {
						pv = clause.Value[1:]
					}
					pkValues[clause.Column.Name] = clauses.Params[pv]
				}
			}
		}

		rowKey, err = createOrderedCodeKey(tableConfig, pkValues)
		if err != nil {
			return nil, err
		}

		rawOutput, err = parseComplexOperations(tableConfig, complexMeta)
		if err != nil {
			return nil, fmt.Errorf("error processing raw collection columns: %w", err)
		}
		values = rawOutput.NewColumns
		delColumnFamily = rawOutput.DelColumnFamily
		delColumns = rawOutput.DelColumns
		complexMeta = rawOutput.ComplexOps // Assign complexMeta from output

		for _, val := range clauses.Conditions {
			c, err := tableConfig.GetColumn(val.Column)
			if err != nil {
				return nil, err
			}

			pv := val.Value
			if strings.HasPrefix(val.Value, "@") {
				pv = val.Value[1:]
			}
			value := fmt.Sprintf("%v", clauses.Params[pv])
			encodedValue, err := encodeValueForBigtable(value, c.CQLType.DataType(), 4)
			if err != nil {
				return nil, err
			}
			values = append(values, &ColumnAndValue{Column: c, Value: encodedValue})
		}
	} else {
		complexMeta, err = t.ProcessComplexUpdate(columns, values)
		if err != nil {
			return nil, err
		}
	}

	updateQueryData := &UpdateQueryMapping{
		Query:                 query,
		Table:                 tableName,
		RowKey:                rowKey,
		Columns:               columns,
		Data:                  values,
		DeleteColumnFamilies:  delColumnFamily,
		DeleteColumQualifiers: delColumns,
		IfExists:              ifExist,
		Keyspace:              keyspaceName,
		Clauses:               clauses.Conditions,
		Params:                setValues.Params,
		UpdateSetValues:       setValues.UpdateSetValues,
		TimestampInfo:         timestampInfo,
		ComplexOperations:     complexMeta,
	}

	return updateQueryData, nil
}

// BindUpdate() constructs an UpdateQueryMapping for an update operation, preparing the necessary
// components such as columns, values, row keys, and primary keys. It also processes any collection types
// and manages timestamp information for the update query.
//
// Parameters:
//   - columnsResponse: A slice of Column structs representing metadata of columns involved in the update.
//   - values: A slice of pointers to primitive.Value, representing the values for the update operation.
//   - st: A pointer to an UpdateQueryMapping that contains existing query data and metadata.
//   - protocolV: The Cassandra protocol version used for encoding and decoding operations.
//
// Returns:
//   - A pointer to an UpdateQueryMapping populated with the new query components required for the update.
//   - An error if any issues arise during processing, such as failure to fetch primary keys or errors
//     handling column data or timestamp.
func (t *Translator) BindUpdate(columns []*types.Column, values []*primitive.Value, st *UpdateQueryMapping, pv primitive.ProtocolVersion) (*UpdateQueryMapping, error) {
	var newValues []*ColumnAndValue
	var primaryKeys []string = st.PrimaryKeys
	var err error
	var unencrypted map[string]interface{}
	var delColumnFamily []string
	var delColumns []*types.Column    // Added missing declaration
	var prepareOutput *PreparedValues // Declare prepareOutput

	tableConfig, err := t.SchemaMappingConfig.GetTableConfig(st.Keyspace, st.Table)
	if err != nil {
		return nil, err
	}

	if len(primaryKeys) == 0 {
		primaryKeys = tableConfig.GetPrimaryKeys()
	}
	timestampInfo, values, variableMetadata, err := ProcessTimestampByUpdate(st, values)

	if err != nil {
		return nil, err
	}

	prepareInput := ProcessPrepareCollectionsInput{
		ColumnsResponse: columns,
		Values:          values,
		TableName:       st.Table,
		ProtocolV:       pv,
		PrimaryKeys:     primaryKeys,
		Translator:      t,
		KeySpace:        st.Keyspace,
		ComplexMeta:     st.ComplexOperations,
	}

	prepareOutput, err = decodePreparedValues(tableConfig, st.Columns, st.ComplexOperations, values, pv)
	if err != nil {
		return nil, err
	}
	newValues = prepareOutput.Values
	unencrypted = prepareOutput.GoValues
	indexEnd := prepareOutput.IndexEnd
	delColumnFamily = prepareOutput.DelColumnFamily
	delColumns = prepareOutput.DelColumns

	for i, clause := range st.Clauses {
		var column types.Column
		if columns, exists := tableConfig.Columns[clause.Column]; exists {
			column = types.Column{Name: columns.Name}
		}

		if slices.Contains(primaryKeys, column.Name) {

			val, _ := utilities.DecodeBytesToCassandraColumnType(values[i+indexEnd+1].Contents, variableMetadata[i+indexEnd+1].Type, pv)
			unencrypted[column.Name] = val
		}
	}

	rowKeyBytes, err := createOrderedCodeKey(tableConfig, unencrypted)
	if err != nil {
		return nil, err
	}
	rowKey := string(rowKeyBytes)

	UpdateQueryData := &UpdateQueryMapping{
		Query:                 st.Query,
		QueryType:             st.QueryType,
		Keyspace:              st.Keyspace,
		Columns:               newValues,
		PrimaryKeys:           primaryKeys,
		RowKey:                rowKey,
		Table:                 st.Table,
		DeleteColumnFamilies:  delColumnFamily,
		DeleteColumQualifiers: delColumns,
		Clauses:               st.Clauses,
		TimestampInfo:         timestampInfo,
		IfExists:              st.IfExists,
		ComplexOperations:     st.ComplexOperations,
	}

	return UpdateQueryData, nil
}
