package update_translator

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/common"
)

func parseUpdateValues(assignments []cql.IAssignmentElementContext, tableConfig *schemaMapping.TableConfig, params *types.QueryParameters, values *types.QueryParameterValues) ([]types.Assignment, error) {
	if len(assignments) == 0 {
		return nil, errors.New("invalid input")
	}
	var parsed []types.Assignment

	for _, a := range assignments {
		if a.AssignmentEquals() != nil {
			assignment := a.AssignmentEquals()
			col, err := common.ParseColumnContext(tableConfig, assignment.Column())
			if err != nil {
				return nil, err
			}
			p := params.PushParameter(col, col.CQLType, col.CQLType.IsCollection())
			parsed = append(parsed, types.NewComplexAssignmentSet(col, p))
			err = common.ExtractValueAny(assignment.ValueAny(), col.CQLType, p, values)
			if err != nil {
				return nil, err
			}
		} else if a.AssignmentAppend() != nil {
			assignment := a.AssignmentAppend()
			append_, err := ParseAppend(assignment.Column(), assignment.ArithmeticOperator(), assignment.ValueAny(), false, tableConfig, params, values)
			if err != nil {
				return nil, err
			}
			parsed = append(parsed, append_)
		} else if a.AssignmentPrepend() != nil {
			assignment := a.AssignmentPrepend()
			prepend, err := ParseAppend(assignment.Column(), assignment.ArithmeticOperator(), assignment.ValueAny(), true, tableConfig, params, values)
			if err != nil {
				return nil, err
			}
			parsed = append(parsed, prepend)
		} else if a.AssignmentIndex() != nil {
			assignment := a.AssignmentIndex()
			col, err := common.ParseColumnContext(tableConfig, assignment.Column())
			if err != nil {
				return nil, err
			}
			if col.CQLType.Code() == types.MAP {
				mt := col.CQLType.(*types.MapType)
				keyValue, err := common.ParseCqlConstant(assignment.IndexOrKeyAccess().Constant(), mt.KeyType())
				if err != nil {
					return nil, fmt.Errorf("failed to parse map key: %w", err)
				}
				p := params.PushParameter(col, mt.ValueType(), false)
				parsed = append(parsed, types.NewComplexAssignmentUpdateMapValue(col, keyValue, p))
				err = common.ExtractConstantValue(assignment.Constant(), mt.ValueType(), p, values)
				if err != nil {
					return nil, err
				}
			} else if col.CQLType.Code() == types.LIST {
				lt := col.CQLType.(*types.ListType)
				// cql doesn't allow parameterized index access so no need to handle that here
				index, err := common.ParseBigInt(assignment.IndexOrKeyAccess().Constant().DecimalLiteral())
				if err != nil {
					return nil, fmt.Errorf("failed to parse list collection index: %w", err)
				}
				p := params.PushParameter(col, lt.ElementType(), false)
				parsed = append(parsed, types.NewComplexAssignmentUpdateListIndex(col, index, p))
				err = common.ExtractConstantValue(assignment.Constant(), lt.ElementType(), p, values)
				if err != nil {
					return nil, err
				}
			} else {
				return nil, fmt.Errorf("cannot do key or index based access on type %s", col.CQLType.String())
			}
		}
	}
	return parsed, nil
}

func ParseAppend(columnContext cql.IColumnContext, op cql.IArithmeticOperatorContext, valueAny cql.IValueAnyContext, isPrepend bool, tableConfig *schemaMapping.TableConfig, params *types.QueryParameters, values *types.QueryParameterValues) (types.Assignment, error) {
	col, err := common.ParseColumnContext(tableConfig, columnContext)
	if err != nil {
		return nil, err
	}

	operator, err := common.ParseArithmeticOperator(op)

	// append value type might be different from the column type, depending on the operator and column type.
	var valueType types.CqlDataType
	if col.CQLType.Code() == types.MAP {
		if operator == types.PLUS {
			valueType = col.CQLType
		} else if operator == types.MINUS {
			// removing elements can only be keys for maps
			mt := col.CQLType.(*types.MapType)
			valueType = types.NewListType(mt.KeyType())
		} else {
			return nil, fmt.Errorf("unsupported map append operator: %s", operator)
		}
	} else if col.CQLType.Code() == types.LIST {
		valueType = col.CQLType
	} else if col.CQLType.Code() == types.SET {
		st := col.CQLType.(*types.SetType)
		valueType = types.NewListType(st.ElementType())
	} else if col.CQLType.Code() == types.COUNTER {
		valueType = col.CQLType
	} else {
		return nil, fmt.Errorf("cannot append on column type: %s", col.CQLType.String())
	}

	p := params.PushParameter(col, valueType, false)
	err = common.ExtractValueAny(valueAny, valueType, p, values)
	if err != nil {
		return nil, err
	}
	return types.NewComplexAssignmentAppend(col, operator, p, isPrepend), nil
}
