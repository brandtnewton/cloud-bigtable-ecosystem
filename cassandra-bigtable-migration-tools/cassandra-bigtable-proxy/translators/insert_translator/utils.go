package insert_translator

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
)

// parseInsertColumns() parses columns and values from the Insert query
//
// Parameters:
//   - input: Insert Columns Spec Context from antlr parser.
//   - tableName: Table Columns
//   - schemaMapping: JSON Config which maintains column and its datatypes info.
//
// Returns: ColumnsResponse struct and error if any
func parseInsertColumns(input cql.IInsertColumnSpecContext, tableConfig *schemaMapping.TableConfig, params *types.QueryParameters) ([]translators.Assignment, error) {

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

	var assignments []translators.Assignment
	for _, val := range columns {
		columnName := types.ColumnName(val.GetText())
		col, err := tableConfig.GetColumn(columnName)
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, translators.NewComplexAssignmentSet(col, params.PushParameter(col, col.CQLType)))
	}

	return assignments, nil
}

func parseInsertValues(input cql.IInsertValuesSpecContext, columns []translators.Assignment, params *types.QueryParameters, values *types.QueryParameterValues, isPrepared bool) error {
	if input == nil {
		return errors.New("insert values clause missing or malformed")
	}

	if isPrepared {
		return nil
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
		column := columns[i].Column()
		placeholder, ok := params.GetPlaceholderForColumn(column.Name)
		if !ok {
			return fmt.Errorf("unhandled error: missing parameter for column '%s'", column.Name)
		}

		val, err := utilities.StringToGo(translators.TrimQuotes(value.GetText()), column.CQLType.DataType())
		err = values.SetValue(placeholder, val)
		if err != nil {
			return err
		}
	}

	return nil
}
