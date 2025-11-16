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
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"strconv"
	"strings"

	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
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

// Helper function to get value from a node
func getNodeValue(node antlr.Tree, parent antlr.ParserRuleContext) (types.GoValue, error) {
	switch v := node.(type) {
	case cql.IAssignmentListContext:
		val, err := parseCqlValue(v)
		if err != nil {
			return nil, err
		}
		return val, nil
	case cql.IAssignmentSetContext:
		val, err := parseCqlValue(v)
		if err != nil {
			return nil, err
		}
		return val, nil
	case cql.IAssignmentMapContext:
		val, err := parseCqlValue(v)
		if err != nil {
			return nil, err
		}
		return val, nil
	case cql.IDecimalLiteralContext:
		return v.GetText(), nil
	case antlr.TerminalNode:
		return v.GetText(), nil
	default:
		if parser, ok := parent.(interface{ GetParser() antlr.Parser }); ok {
			return antlr.TreesGetNodeText(node, nil, parser.GetParser()), nil
		}
		return antlr.TreesGetNodeText(node, nil, nil), nil
	}

}

// parseUpdateValues() processes a list of assignment elements from a CQL update statement,
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
func parseUpdateValues(assignments []cql.IAssignmentElementContext, tableConfig *schemaMapping.TableConfig, params *types.QueryParameters, values *types.QueryParameterValues) ([]Assignment, error) {
	if len(assignments) == 0 {
		return nil, errors.New("invalid input")
	}
	var parsed []Assignment

	for _, setVal := range assignments {
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
		if col.IsPrimaryKey {
			return nil, fmt.Errorf("primary key not allowed to assignments")
		}
		var assignment Assignment
		hasValue := setVal.QUESTION_MARK() == nil

		// Handle append, prepend, remove as Assignment
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
			if isColumn(left, columnName) && (isCollection(right) || isCounter(right)) && op == "+" {
				// Append: col = col + [values]
				// todo handle prepared vs. adhoc
				p := params.PushParameter(col.Name, col.CQLType)
				if hasValue {
					val, err := getNodeValue(right, setVal)
					if err != nil {
						return nil, err
					}
					err = values.SetValue(p, val)
					if err != nil {
						return nil, err
					}
				}
				assignment = NewComplexAssignmentAdd(col, false, p)
			} else if (isCollection(left) || isCounter(left)) && isColumn(right, col.Name) && op == "+" {
				// Prepend: col = [values] + col
				// todo handle prepared vs. adhoc
				p := params.PushParameter(col.Name, col.CQLType)
				if hasValue {
					val, err := getNodeValue(left, setVal)
					if err != nil {
						return nil, err
					}
					err = values.SetValue(p, val)
					if err != nil {
						return nil, err
					}
				}
				assignment = NewComplexAssignmentAdd(col, true, p)
			} else if op == "-" {
				p := params.PushParameter(col.Name, col.CQLType)
				if hasValue {
					val, err := getNodeValue(right, setVal)
					if err != nil {
						return nil, err
					}
					err = values.SetValue(p, val)
					if err != nil {
						return nil, err
					}
				}

				assignment = NewComplexAssignmentRemove(col, p)
			} else {
				return nil, fmt.Errorf("unhandled operation type %s", op)
			}
		} else if setVal.SyntaxBracketLs() != nil && setVal.DecimalLiteral() != nil && setVal.SyntaxBracketRs() != nil && setVal.Constant() != nil {
			// i.e. marks[1] = 99 (index update)
			indexStr := trimQuotes(setVal.DecimalLiteral().GetText())
			index, err := strconv.ParseInt(indexStr, 10, 64)
			if err != nil {
				return nil, err
			}

			p := params.PushParameter(col.Name, col.CQLType)

			if hasValue {
				valueRaw, err := parseCqlConstant(setVal.Constant())
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, valueRaw)
				if err != nil {
					return nil, err
				}
			}

			assignment = NewComplexAssignmentUpdateIndex(col, index, p)
		} else if setVal.Constant() != nil { // set a scalar value
			goValue, err := parseCqlConstant(setVal.Constant())
			if err != nil {
				return nil, err
			}
			p := params.PushParameter(col.Name, col.CQLType)
			assignment = NewComplexAssignmentSet(col, p)
			err = values.SetValue(p, goValue)
			if err != nil {
				return nil, err
			}
		} else if setVal.AssignmentMap() != nil {
			p := params.PushParameter(col.Name, col.CQLType)
			assignment = NewComplexAssignmentSet(col, p)

			if hasValue {
				goValue, err := parseCqlMapAssignment(setVal.AssignmentMap())
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, goValue)
				if err != nil {
					return nil, err
				}
			}
		} else if setVal.AssignmentSet() != nil {
			p := params.PushParameter(col.Name, col.CQLType)
			assignment = NewComplexAssignmentSet(col, p)
			if hasValue {
				goValue, err := parseCqlSetAssignment(setVal.AssignmentSet())
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, goValue)
				if err != nil {
					return nil, err
				}
			}
		} else if setVal.AssignmentList() != nil {
			p := params.PushParameter(col.Name, col.CQLType)
			assignment = NewComplexAssignmentSet(col, p)
			if hasValue {
				goValue, err := parseCqlListAssignment(setVal.AssignmentList())
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, goValue)
				if err != nil {
					return nil, err
				}
			}
		} else if setVal.QUESTION_MARK() != nil {
			if err != nil {
				return nil, err
			}
			p := params.PushParameter(col.Name, col.CQLType)
			assignment = NewComplexAssignmentSet(col, p)
		} else {
			return nil, fmt.Errorf("unhandled update set value operation: '%s'", setVal.GetText())
		}

		parsed = append(parsed, assignment)
	}
	return parsed, nil
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

	params := types.NewQueryParameters()

	assignments, err := parseUpdateValues(allAssignmentObj, tableConfig, params)
	if err != nil {
		return nil, err
	}

	if updateObj.WhereSpec() != nil {
		return nil, errors.New("error parsing update where clause")
	}

	whereClause, err := parseWhereByClause(updateObj.WhereSpec(), tableConfig, params)
	if err != nil {
		return nil, err
	}
	for k, v := range whereClause.Params {
		setValues.Params[k] = v
	}

	var ifExist = updateObj.IfExist() != nil

	var actualPrimaryKeys []types.ColumnName
	for _, val := range whereClause.Conditions {
		actualPrimaryKeys = append(actualPrimaryKeys, val.Column.Name)
	}
	err = ValidateRequiredPrimaryKeysOnly(tableConfig, actualPrimaryKeys)
	if err != nil {
		return nil, err
	}

	var rowKey types.RowKey
	var columns []*types.Column
	var values []types.BigtableData
	for _, val := range setValues.Assignments {
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
	var rawOutput *BigtableMutations // Declare rawOutput here

	if !isPreparedQuery {
		pkValues := extractValuesFromWhereClause(whereClause)

		rowKey, err = createOrderedCodeKey(tableConfig, pkValues)
		if err != nil {
			return nil, err
		}

		var assignments []*Assignment
		for _, v := range setValues.Assignments {
			if v.ComplexAssignment != nil {
				assignments = append(assignments, v.ComplexAssignment)
			}
		}
		rawOutput, err = bindings.bindMutations(tableConfig, assignments)
		if err != nil {
			return nil, fmt.Errorf("error processing raw collection columns: %w", err)
		}
		values = rawOutput.NewColumns
		delColumnFamily = rawOutput.DelColumnFamily
		delColumns = rawOutput.DelColumns
		complexMeta = rawOutput.ComplexOps // Assign complexMeta from output

		for _, val := range whereClause.Conditions {
			c, err := tableConfig.GetColumn(val.Column)
			if err != nil {
				return nil, err
			}

			pv := val.ValuePlaceholder
			if strings.HasPrefix(val.ValuePlaceholder, "@") {
				pv = val.ValuePlaceholder[1:]
			}
			value := fmt.Sprintf("%v", whereClause.Params[pv])
			encodedValue, err := bindings.encodeScalarForBigtable(value, c.CQLType.DataType(), 4)
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
		DeleteColumnFamilies:  delColumnFamily,
		DeleteColumQualifiers: delColumns,
		IfExists:              ifExist,
		Keyspace:              keyspaceName,
		Clauses:               whereClause.Conditions,
		Params:                params,
		UpdateSetValues:       assignments,
		TimestampInfo:         timestampInfo,
	}

	return updateQueryData, nil
}

func (t *Translator) BindUpdate(st *UpdateQueryMapping, cassandraValues []*primitive.Value, pv primitive.ProtocolVersion) (*types.BigtableMutations, error) {
	tableConfig, err := t.SchemaMappingConfig.GetTableConfig(st.Keyspace, st.Table)
	if err != nil {
		return nil, err
	}

	values, err := bindings.BindQueryParams(st.Params, cassandraValues, pv)
	if err != nil {
		return nil, err
	}

	rowKey, err := bindings.BindRowKey(tableConfig, values)
	if err != nil {
		return nil, err
	}

	mutations := &types.BigtableMutations{RowKey: rowKey}
	err = bindings.BindMutations(tableConfig, st.UpdateSetValues, values, mutations)
	if err != nil {
		return nil, err
	}

	mutations.UsingTimestamp, err = bindings.BindUsingTimestamp(values)
	if err != nil {
		return nil, err
	}

	return mutations, err
}
