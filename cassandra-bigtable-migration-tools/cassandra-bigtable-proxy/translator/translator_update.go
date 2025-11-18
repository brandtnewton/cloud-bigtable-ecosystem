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
	"strconv"
)

func (t *Translator) TranslateUpdate(query string, sessionKeyspace types.Keyspace, isPreparedQuery bool) (*PreparedUpdateQuery, *BigtableWriteMutation, error) {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)
	updateObj := p.Update()

	if updateObj == nil || updateObj.KwUpdate() == nil {
		return nil, nil, errors.New("error parsing the update object")
	}

	keyspaceName, tableName, err := parseTarget(updateObj, sessionKeyspace, t.SchemaMappingConfig)
	if err != nil {
		return nil, nil, err
	}

	tableConfig, err := t.SchemaMappingConfig.GetTableConfig(keyspaceName, tableName)
	if err != nil {
		return nil, nil, err
	}

	updateObj.KwSet()
	if updateObj.Assignments() == nil || updateObj.Assignments().AllAssignmentElement() == nil {
		return nil, nil, errors.New("error parsing the assignment object")
	}

	assignmentObj := updateObj.Assignments()
	allAssignmentObj := assignmentObj.AllAssignmentElement()
	if allAssignmentObj == nil {
		return nil, nil, errors.New("error parsing all the assignment object")
	}

	params := NewQueryParameters()
	values := NewQueryParameterValues(params)

	assignments, err := parseUpdateValues(allAssignmentObj, tableConfig, params, values)
	if err != nil {
		return nil, nil, err
	}

	if updateObj.WhereSpec() != nil {
		return nil, nil, errors.New("error parsing update where clause")
	}

	whereClause, err := parseWhereClause(updateObj.WhereSpec(), tableConfig, params, values)
	if err != nil {
		return nil, nil, err
	}

	err = GetTimestampInfo(updateObj.UsingTtlTimestamp(), params, values)
	if err != nil {
		return nil, nil, err
	}

	var ifExist = updateObj.IfExist() != nil

	err = ValidateRequiredPrimaryKeysOnly(tableConfig, params)
	if err != nil {
		return nil, nil, err
	}

	var columns []*types.Column
	for _, assignment := range assignments {
		columns = append(columns, assignment.Column())
	}

	st := &PreparedUpdateQuery{
		cqlQuery: query,
		table:    tableName,
		Columns:  columns,
		IfExists: ifExist,
		keyspace: keyspaceName,
		Clauses:  whereClause,
		Params:   params,
		Values:   assignments,
	}

	var bound *BigtableWriteMutation
	if !isPreparedQuery {
		bound, err = t.doBindUpdate(st, values)
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

func (t *Translator) BindUpdate(st *PreparedUpdateQuery, cassandraValues []*primitive.Value, pv primitive.ProtocolVersion) (*BigtableWriteMutation, error) {
	values, err := BindQueryParams(st.Params, cassandraValues, pv)
	if err != nil {
		return nil, err
	}
	return t.doBindUpdate(st, values)
}

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
func getNodeValue(node antlr.Tree, parent antlr.ParserRuleContext, expectedType types.CqlDataType) (types.GoValue, error) {
	switch v := node.(type) {
	case cql.IAssignmentListContext:
		val, err := parseCqlValue(v, expectedType)
		if err != nil {
			return nil, err
		}
		return val, nil
	case cql.IAssignmentSetContext:
		val, err := parseCqlValue(v, expectedType)
		if err != nil {
			return nil, err
		}
		return val, nil
	case cql.IAssignmentMapContext:
		val, err := parseCqlValue(v, expectedType)
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

func getAppendType(dt types.CqlDataType) (types.CqlDataType, error) {
	if dt.Code() == types.MAP {
		return dt, nil
	} else if dt.Code() == types.LIST {
		st := dt.(types.ListType)
		return types.NewListType(st.ElementType()), nil
	} else if dt.Code() == types.SET {
		st := dt.(types.SetType)
		return types.NewListType(st.ElementType()), nil
	} else if dt.Code() == types.COUNTER {
		return types.TypeBigint, nil
	} else {
		return nil, fmt.Errorf("cannot append to column of type %s", dt.String())
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
func parseUpdateValues(assignments []cql.IAssignmentElementContext, tableConfig *schemaMapping.TableConfig, params *QueryParameters, values *QueryParameterValues) ([]Assignment, error) {
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
				at, err := getAppendType(col.CQLType)
				if err != nil {
					return nil, err
				}
				p := params.PushParameter(col, at)
				if hasValue {
					at, err := getAppendType(col.CQLType)
					if err != nil {
						return nil, err
					}
					val, err := getNodeValue(right, setVal, at)
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
				at, err := getAppendType(col.CQLType)
				if err != nil {
					return nil, err
				}
				p := params.PushParameter(col, at)
				if hasValue {
					val, err := getNodeValue(left, setVal, at)
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
				p := params.PushParameter(col, col.CQLType)
				if hasValue {
					val, err := getNodeValue(right, setVal, col.CQLType)
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
			lt, ok := col.CQLType.(types.ListType)
			if !ok {
				return nil, fmt.Errorf("expected index operation to be on a list but got %s", lt.String())
			}
			// i.e. marks[1] = 99 (index update)
			indexStr := setVal.DecimalLiteral().GetText()
			index, err := strconv.ParseInt(indexStr, 10, 64)
			if err != nil {
				return nil, err
			}

			p := params.PushParameter(col, col.CQLType)

			if hasValue {
				valueRaw, err := getCqlConstant(setVal.Constant(), lt.ElementType())
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
			goValue, err := getCqlConstant(setVal.Constant(), col.CQLType)
			if err != nil {
				return nil, err
			}
			p := params.PushParameter(col, col.CQLType)
			assignment = NewComplexAssignmentSet(col, p)
			err = values.SetValue(p, goValue)
			if err != nil {
				return nil, err
			}
		} else if setVal.AssignmentMap() != nil {
			p := params.PushParameter(col, col.CQLType)
			assignment = NewComplexAssignmentSet(col, p)

			if hasValue {
				goValue, err := parseCqlMapAssignment(setVal.AssignmentMap(), col.CQLType)
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, goValue)
				if err != nil {
					return nil, err
				}
			}
		} else if setVal.AssignmentSet() != nil {
			p := params.PushParameter(col, col.CQLType)
			assignment = NewComplexAssignmentSet(col, p)
			if hasValue {
				goValue, err := parseCqlSetAssignment(setVal.AssignmentSet(), col.CQLType)
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, goValue)
				if err != nil {
					return nil, err
				}
			}
		} else if setVal.AssignmentList() != nil {
			p := params.PushParameter(col, col.CQLType)
			assignment = NewComplexAssignmentSet(col, p)
			if hasValue {
				goValue, err := parseCqlListAssignment(setVal.AssignmentList(), col.CQLType)
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
			p := params.PushParameter(col, col.CQLType)
			assignment = NewComplexAssignmentSet(col, p)
		} else {
			return nil, fmt.Errorf("unhandled update set value operation: '%s'", setVal.GetText())
		}

		parsed = append(parsed, assignment)
	}
	return parsed, nil
}

func (t *Translator) doBindUpdate(st *PreparedUpdateQuery, values *QueryParameterValues) (*BigtableWriteMutation, error) {
	tableConfig, err := t.SchemaMappingConfig.GetTableConfig(st.Keyspace(), st.Table())
	if err != nil {
		return nil, err
	}

	rowKey, err := BindRowKey(tableConfig, values)
	if err != nil {
		return nil, err
	}

	mutations := NewBigtableWriteMutation(st.keyspace, st.table, rowKey)
	err = BindMutations(st.Values, values, mutations)
	if err != nil {
		return nil, err
	}

	return mutations, err
}
