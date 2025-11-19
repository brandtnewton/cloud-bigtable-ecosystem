package update_translator

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators"
	"github.com/antlr4-go/antlr/v4"
	"strconv"
)

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
func parseUpdateValues(assignments []cql.IAssignmentElementContext, tableConfig *schemaMapping.TableConfig, params *types.QueryParameters, values *types.QueryParameterValues) ([]translators.Assignment, error) {
	if len(assignments) == 0 {
		return nil, errors.New("invalid input")
	}
	var parsed []translators.Assignment

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
		var assignment translators.Assignment
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
				assignment = translators.NewComplexAssignmentAdd(col, false, p)
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
				assignment = translators.NewComplexAssignmentAdd(col, true, p)
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
				assignment = translators.NewComplexAssignmentRemove(col, p)
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
				valueRaw, err := translators.GetCqlConstant(setVal.Constant(), lt.ElementType())
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, valueRaw)
				if err != nil {
					return nil, err
				}
			}

			assignment = translators.NewComplexAssignmentUpdateIndex(col, index, p)
		} else if setVal.Constant() != nil { // set a scalar value
			goValue, err := translators.GetCqlConstant(setVal.Constant(), col.CQLType)
			if err != nil {
				return nil, err
			}
			p := params.PushParameter(col, col.CQLType)
			assignment = translators.NewComplexAssignmentSet(col, p)
			err = values.SetValue(p, goValue)
			if err != nil {
				return nil, err
			}
		} else if setVal.AssignmentMap() != nil {
			p := params.PushParameter(col, col.CQLType)
			assignment = translators.NewComplexAssignmentSet(col, p)

			if hasValue {
				goValue, err := translators.ParseCqlMapAssignment(setVal.AssignmentMap(), col.CQLType)
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
			assignment = translators.NewComplexAssignmentSet(col, p)
			if hasValue {
				goValue, err := translators.ParseCqlSetAssignment(setVal.AssignmentSet(), col.CQLType)
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
			assignment = translators.NewComplexAssignmentSet(col, p)
			if hasValue {
				goValue, err := translators.ParseCqlListAssignment(setVal.AssignmentList(), col.CQLType)
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
			assignment = translators.NewComplexAssignmentSet(col, p)
		} else {
			return nil, fmt.Errorf("unhandled update set value operation: '%s'", setVal.GetText())
		}

		parsed = append(parsed, assignment)
	}
	return parsed, nil
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
		val, err := translators.ParseCqlValue(v, expectedType)
		if err != nil {
			return nil, err
		}
		return val, nil
	case cql.IAssignmentSetContext:
		val, err := translators.ParseCqlValue(v, expectedType)
		if err != nil {
			return nil, err
		}
		return val, nil
	case cql.IAssignmentMapContext:
		val, err := translators.ParseCqlValue(v, expectedType)
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
