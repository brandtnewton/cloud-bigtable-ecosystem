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
			err = common.ParseValueAny(assignment.ValueAny(), col, p, values)
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
			p := params.PushParameter(col, col.CQLType, col.CQLType.IsCollection())
			assignment.DecimalLiteral()
			index, err := common.ParseBigInt(assignment.DecimalLiteral())
			if err != nil {
				return nil, fmt.Errorf("failed to parse list collection index: %w", err)
			}
			parsed = append(parsed, types.NewComplexAssignmentUpdateIndex(col, index, p))
			err = common.ParseConstantContext(assignment.Constant(), col, p, values)
			if err != nil {
				return nil, err
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
	p := params.PushParameter(col, col.CQLType, col.CQLType.IsCollection())
	operator, err := common.ParseArithmeticOperator(op)
	err = common.ParseValueAny(valueAny, col, p, values)
	if err != nil {
		return nil, err
	}
	return types.NewComplexAssignmentAppend(col, operator, p, isPrepend), nil
}
