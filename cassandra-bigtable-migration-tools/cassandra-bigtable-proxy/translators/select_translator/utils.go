package select_translator

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	sm "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/common"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"strconv"
	"strings"
)

// parseSelectClause() parse Columns from the Select CqlQuery
//
// Parameters:
//   - input: The Select Element context from the antlr Parser.
//
// Returns: Columns Meta and an error if any.
func parseSelectClause(input cql.ISelectElementsContext, table *sm.TableConfig) (*types.SelectClause, error) {
	if input == nil {
		return nil, errors.New("select clause empty")
	}

	if input.STAR() != nil {
		return &types.SelectClause{
			IsStar: true,
		}, nil
	}

	if len(input.AllSelectElement()) == 0 {
		return nil, errors.New("select clause empty")
	}

	var selectedColumns []types.SelectedColumn
	for _, val := range input.AllSelectElement() {
		alias, err := common.ParseAs(val.AsSpec())
		if err != nil {
			return nil, err
		}
		var selected types.SelectedColumn
		if val.SelectFunction() != nil {
			selected, err = common.ParseSelectFunction(val.SelectFunction(), alias, table)
		} else if val.SelectColumn() != nil {
			selected, err = common.ParseSelectColumn(val.SelectColumn(), alias, table)
		} else if val.SelectIndex() != nil {
			selected, err = common.ParseSelectIndex(val.SelectIndex(), alias, table)
		} else {
			return nil, fmt.Errorf("unhandled select column `%s`", val.GetText())
		}
		if err != nil {
			return nil, err
		}
		if selected.ResultType == nil {
			return nil, fmt.Errorf("unhandled result type")
		}
		selectedColumns = append(selectedColumns, selected)
	}

	return &types.SelectClause{Columns: selectedColumns}, nil
}

// parseOrderByFromSelect() parse Order By from the Select CqlQuery
//
// Parameters:
//   - input: The Order Spec context from the antlr Parser.
//
// Returns: OrderBy struct
func parseOrderByFromSelect(input cql.IOrderSpecContext) (types.OrderBy, error) {
	var response types.OrderBy

	if input == nil {
		response.IsOrderBy = false
		return response, nil
	}

	orderSpecElements := input.AllOrderSpecElement()
	if len(orderSpecElements) == 0 {
		return types.OrderBy{}, fmt.Errorf("order_by section not have proper values")
	}

	response.IsOrderBy = true
	response.Columns = make([]types.OrderByColumn, 0, len(orderSpecElements))

	for _, element := range orderSpecElements {
		object := element.OBJECT_NAME()
		if object == nil {
			return types.OrderBy{}, fmt.Errorf("order_by section not have proper values")
		}

		colName := strings.TrimSpace(object.GetText())
		if colName == "" {
			return types.OrderBy{}, fmt.Errorf("order_by section has empty column name")
		}

		orderByCol := types.OrderByColumn{
			Column:    colName,
			Operation: types.Asc,
		}

		if element.KwDesc() != nil {
			orderByCol.Operation = types.Desc
		}

		response.Columns = append(response.Columns, orderByCol)
	}

	return response, nil
}

func parseLimitClause(input cql.ILimitSpecContext, params *types.QueryParameters, values *types.QueryParameterValues) error {
	if input == nil {
		return nil
	}

	params.AddParameterWithoutColumn(types.LimitPlaceholder, types.TypeInt)

	if input.DecimalLiteral().DECIMAL_LITERAL() != nil {
		text := input.DecimalLiteral().DECIMAL_LITERAL().GetText()
		limitValue, err := strconv.Atoi(text)
		if err != nil {
			return errors.New("failed to parse limit")
		} else if limitValue < 0 {
			return errors.New("limit must be positive")
		}
		err = values.SetValue(types.LimitPlaceholder, int32(limitValue))
		if err != nil {
			return err
		}
	}

	return nil
}

func parseGroupByColumn(input cql.IGroupSpecContext) []string {
	if input == nil {
		return nil
	}

	groupSpecElements := input.AllGroupSpecElement()
	if len(groupSpecElements) == 0 {
		return nil
	}

	var columns []string
	for _, element := range groupSpecElements {
		object := element.OBJECT_NAME()
		if object == nil {
			// If any group by element is missing, treat as malformed and return nil
			return nil
		}

		colName := object.GetText()
		columns = append(columns, colName)
	}

	return columns
}

// createBtqlSelectClause() processes the selected columns, formats them, and returns a map of aliases to metadata and a slice of formatted columns.
// Parameters:
//   - t : Translator instance
//   - selectedColumns: []types
//   - tableName : table name on which query is being executes
//   - keySpace : keyspace name on which query is being executed
//
// Returns:
//   - []string column containing formatted selected columns for bigtable query
//   - error if any
func createBtqlSelectClause(tableConfig *sm.TableConfig, s *types.SelectClause, isGroupBy bool) (string, error) {
	if s.IsStar {
		return "*", nil
	}
	var columns []string
	for _, col := range s.Columns {
		if col.Func != types.FuncCodeUnknown {
			c, err := createBtqlFunc(col, tableConfig)
			if err != nil {
				return "", err
			}
			columns = append(columns, c)
		} else {
			c, err := createBtqlSelectCol(tableConfig, col, isGroupBy)
			if err != nil {
				return "", err
			}
			columns = append(columns, c)
		}
	}

	return strings.Join(columns, ", "), nil
}

// createBtqlFunc processes columns that have aggregate functions applied to them in a SELECT query.
// It handles special cases like COUNT(*) and validates if the column and function types are allowed in aggregates.
//
// Parameters:
//   - t: Translator instance containing schema mapping configuration
//   - columnMetadata: Contains information about the selected column including function name and aliases
//   - tableName: Columns of the table being queried
//   - keySpace: Keyspace name where the table exists
//   - columns: Slice of strings containing the processed column expressions
//
// Returns:
//   - []string: Updated slice of columns with the processed function column
//   - string: The data type of the column after function application
//   - error: Error if column metadata is not found, or if column/function type is not supported for aggregation
//
// The function performs the following operations:
//  1. Handles COUNT(*) as a special case
//  2. Validates column existence in schema mapping
//  3. Checks if column data type is allowed in aggregates
//  4. Validates if the aggregate function is supported
//  5. Applies any necessary type casting (converting cql select columns to respective bigtable columns)
//  6. Formats the column expression with function and alias if specified
func createBtqlFunc(col types.SelectedColumn, tableConfig *sm.TableConfig) (string, error) {
	if col.Func == types.FuncCodeCount && col.ColumnName == "*" {
		if col.Alias != "" {
			return "count(*) as " + col.Alias, nil
		}
		return "count(*)", nil
	}

	colMeta, found := tableConfig.Columns[col.ColumnName]
	if !found {
		// Check if the column is an alias
		if aliasMeta, aliasFound := tableConfig.Columns[types.ColumnName(col.Alias)]; aliasFound {
			colMeta = aliasMeta
		} else {
			return "", fmt.Errorf("column metadata not found for column '%s' in table '%s' and keyspace '%s'", col.ColumnName, tableConfig.Name, tableConfig.Keyspace)
		}
	}

	if !funcAllowedInAggregate(col.Func) {
		return "", fmt.Errorf("unknown function '%s'", col.Func.String())
	}
	if !isTypeAllowedInAggregate(colMeta.CQLType.DataType()) {
		return "", fmt.Errorf("column not supported for aggregate")
	}
	castValue, castErr := CastScalarColumn(colMeta)
	if castErr != nil {
		return "", castErr
	}
	column := fmt.Sprintf("%s(%s)", col.Func.String(), castValue)

	if col.Alias != "" {
		column = column + " as " + col.Alias
	}

	return column, nil
}

// isTypeAllowedInAggregate checks whether the provided data type is allowed in aggregate functions.
// It returns true if dataType is one of the supported numeric types (i.e., "int", "bigint", "float", or "double"),
// ensuring that only appropriate types are used for aggregate operations.
func isTypeAllowedInAggregate(dt datatype.DataType) bool {
	return dt == datatype.Int ||
		dt == datatype.Bigint ||
		dt == datatype.Float ||
		dt == datatype.Double ||
		dt == datatype.Counter
}

// funcAllowedInAggregate checks if a given function name is allowed within an aggregate function.
func funcAllowedInAggregate(f types.BtqlFuncCode) bool {
	return f == types.FuncCodeAvg || f == types.FuncCodeSum || f == types.FuncCodeMin || f == types.FuncCodeMax || f == types.FuncCodeCount
}

func createBtqlSelectCol(tableConfig *sm.TableConfig, selectedColumn types.SelectedColumn, isGroupBy bool) (string, error) {
	colName := selectedColumn.Sql
	if selectedColumn.MapKey != "" {
		colName = string(selectedColumn.ColumnName)
	}
	col, err := tableConfig.GetColumn(types.ColumnName(colName))
	if err != nil {
		return "", err
	}
	if selectedColumn.Alias != "" {
		c, err := processAsColumn(selectedColumn, col, isGroupBy)
		if err != nil {
			return "", err
		}
		return c, nil
	}
	c, err := processRegularColumn(selectedColumn, col)
	if err != nil {
		return "", err
	}
	return c, nil
}

func processAsColumn(selectedColumn types.SelectedColumn, column *types.Column, isGroupBy bool) (string, error) {
	var columnFamily types.ColumnFamily
	if !column.CQLType.IsCollection() {
		var columnName = selectedColumn.Sql
		if column.CQLType == types.TypeCounter {
			// counters are stored as counter_col['']
			columnFamily = column.ColumnFamily
			columnName = ""
		}
		if isGroupBy {
			castedCol, err := CastScalarColumn(column)
			if err != nil {
				return "", err
			}
			return castedCol + " as " + selectedColumn.Alias, nil
		} else if column.IsPrimaryKey {
			return fmt.Sprintf("%s as %s", columnName, selectedColumn.Alias), nil
		} else {
			return fmt.Sprintf("%s['%s'] as %s", columnFamily, columnName, selectedColumn.Alias), nil
		}
	} else {
		if column.CQLType.DataType().GetDataTypeCode() == primitive.DataTypeCodeList {
			return fmt.Sprintf("MAP_VALUES(%s) as %s", selectedColumn.Sql, selectedColumn.Alias), nil
		} else {
			if selectedColumn.MapKey != "" {
				return fmt.Sprintf("%s['%s'] as %s", selectedColumn.ColumnName, selectedColumn.MapKey, selectedColumn.Alias), nil
			}
			return fmt.Sprintf("`%s` as %s", selectedColumn.Sql, selectedColumn.Alias), nil
		}
	}
}

/*
processRegularColumn processes the given column based on its metadata and table context.

It formats the column name differently depending on whether the current table name matches the column metadata's name.
If there is a match, the function qualifies the column name using the table name; otherwise, it formats the column name as standalone.
Additionally, if the column is not a collection, it builds a formatted reference that includes accessing the column within the specified column family;
if the column is a collection, it uses the preformatted column name from the metadata.

Parameters:
  - columnMetadata: Specifies metadata for the selected column, including its name and a field for storing the formatted version.
  - tableName: The name of the current table to determine if table-qualified formatting is needed.
  - columnFamily: The column family identifier used when constructing the column reference for non-collection types.
  - colMeta: A pointer to column configuration containing additional details such as whether the column is a collection.
  - columns: A slice of strings that accumulates the formatted column references.

Returns:

	An updated slice of strings with the new formatted column reference appended.
*/
func processRegularColumn(selectedColumn types.SelectedColumn, col *types.Column) (string, error) {
	if !col.CQLType.IsCollection() {
		return CastScalarColumn(col)
	} else {
		if col.CQLType.DataType().GetDataTypeCode() == primitive.DataTypeCodeList {
			return fmt.Sprintf("MAP_VALUES(%s)", selectedColumn.Sql), nil
		} else {
			if selectedColumn.MapKey != "" {
				return fmt.Sprintf("%s['%s']", selectedColumn.ColumnName, selectedColumn.MapKey), nil
			}
			return fmt.Sprintf("`%s`", selectedColumn.Sql), nil
		}
	}
}

// createBigtableSql() Returns Bigtable Select query using Parsed information.
//
// Parameters:
//   - data: PreparedSelectQuery struct with all select query info from CQL query
func createBigtableSql(t *SelectTranslator, st *types.PreparedSelectQuery) (string, error) {
	column := ""

	tableConfig, err := t.schemaMappingConfig.GetTableConfig(st.Keyspace(), st.Table())
	if err != nil {
		return "", err
	}

	isGroupBy := false
	if len(st.GroupByColumns) > 0 {
		isGroupBy = true
	}
	column, err = createBtqlSelectClause(tableConfig, st.SelectClause, isGroupBy)
	if err != nil {
		return "", err
	}

	btQuery := fmt.Sprintf("SELECT %s FROM %s", column, st.Table())
	whereCondition, err := createBtqlWhereClause(st.Conditions, tableConfig)
	if err != nil {
		return "nil", err
	}
	if whereCondition != "" {
		btQuery += whereCondition
	}

	// Build alias-to-column map
	aliasToColumn := make(map[string]string)
	for _, col := range st.SelectClause.Columns {
		if col.Alias != "" {
			aliasToColumn[col.Alias] = string(col.ColumnName)
		}
	}

	if len(st.GroupByColumns) > 0 {
		btQuery = btQuery + " GROUP BY "
		var groupByKeys []string
		for _, col := range st.GroupByColumns {
			lookupCol := col
			if _, ok := aliasToColumn[col]; ok {
				groupByKeys = append(groupByKeys, col)
			} else {
				if colMeta, ok := tableConfig.Columns[types.ColumnName(lookupCol)]; ok {
					if !colMeta.CQLType.IsCollection() {
						col, err := CastScalarColumn(colMeta)
						if err != nil {
							return "", err
						}
						groupByKeys = append(groupByKeys, col)
					} else {
						return "", errors.New("group by on collection st type is not supported")
					}
				}
			}
		}
		btQuery = btQuery + strings.Join(groupByKeys, ",")
	}

	if st.OrderBy.IsOrderBy {
		orderByClauses := make([]string, 0, len(st.OrderBy.Columns))
		for _, orderByCol := range st.OrderBy.Columns {
			lookupCol := orderByCol.Column
			if _, ok := aliasToColumn[orderByCol.Column]; ok {
				orderByClauses = append(orderByClauses, orderByCol.Column+" "+string(orderByCol.Operation))
			} else {
				if colMeta, ok := tableConfig.Columns[types.ColumnName(lookupCol)]; ok {
					if colMeta.IsPrimaryKey {
						orderByClauses = append(orderByClauses, orderByCol.Column+" "+string(orderByCol.Operation))
					} else if !colMeta.CQLType.IsCollection() {
						orderByKey, err := CastScalarColumn(colMeta)
						if err != nil {
							return "", err
						}
						orderByClauses = append(orderByClauses, orderByKey+" "+string(orderByCol.Operation))
					} else {
						return "", errors.New("order by on collection st type is not supported")
					}
				} else {
					return "", fmt.Errorf("unknown column name '%s' in table %s.%s", orderByCol.Column, st.Keyspace(), st.Table())
				}
			}
		}
		btQuery = btQuery + " ORDER BY " + strings.Join(orderByClauses, ", ")
	}

	if st.Params.Has(types.LimitPlaceholder) {
		btQuery = btQuery + " LIMIT " + string(types.LimitPlaceholder)
	}
	btQuery += ";"
	return btQuery, nil
}

// createBtqlWhereClause(): takes a slice of Condition structs and returns a string representing the WHERE clause of a bigtable SQL query.
// It iterates over the clauses and constructs the WHERE clause by combining the column name, operator, and value of each clause.
// If the operator is "IN", the value is wrapped with the UNNEST function.
// The constructed WHERE clause is returned as a string.
func createBtqlWhereClause(conditions []types.Condition, tableConfig *sm.TableConfig) (string, error) {
	var btqlConditions []string
	for _, condition := range conditions {
		column := "`" + string(condition.Column.Name) + "`"
		if col, ok := tableConfig.Columns[condition.Column.Name]; ok {
			// Check if the column is a primitive type and prepend the column family
			if !col.CQLType.IsCollection() {
				var castErr error
				column, castErr = CastScalarColumn(col)
				if castErr != nil {
					return "", castErr
				}
			}
		}

		var btql string
		if condition.Operator == types.BETWEEN {
			btql = fmt.Sprintf("%s BETWEEN %s AND %s", column, condition.ValuePlaceholder, condition.ValuePlaceholder2)
		} else if condition.Operator == types.IN {
			btql = fmt.Sprintf("%s IN UNNEST(%s)", column, condition.ValuePlaceholder)
		} else if condition.Operator == types.MAP_CONTAINS_KEY {
			btql = fmt.Sprintf("MAP_CONTAINS_KEY(%s, %s)", column, condition.ValuePlaceholder)
		} else if condition.Operator == types.ARRAY_INCLUDES {
			btql = fmt.Sprintf("ARRAY_INCLUDES(MAP_VALUES(%s), %s)", column, condition.ValuePlaceholder)
		} else {
			btql = fmt.Sprintf("%s %s %s", column, condition.Operator, condition.ValuePlaceholder)
		}

		btqlConditions = append(btqlConditions, btql)
	}

	if len(btqlConditions) == 0 {
		return "", nil
	}

	return " WHERE " + strings.Join(btqlConditions, " AND "), nil
}

func selectedColumnsToMetadata(table *sm.TableConfig, selectClause *types.SelectClause) []*message.ColumnMetadata {
	if selectClause.IsStar {
		return table.GetMetadata()
	}

	var resultColumns []*message.ColumnMetadata
	for i, c := range selectClause.Columns {
		var col = message.ColumnMetadata{
			Keyspace: string(table.Keyspace),
			Table:    string(table.Name),
			Name:     string(c.ColumnName),
			Index:    int32(i),
			Type:     c.ResultType.DataType(),
		}
		resultColumns = append(resultColumns, &col)
	}
	return resultColumns
}

// CastScalarColumn handles column type casting in queries.
// Manages type conversion for column values with validation.
// Returns error if column type is invalid or conversion fails.
func CastScalarColumn(colMeta *types.Column) (string, error) {
	if colMeta.CQLType.IsCollection() {
		return "", fmt.Errorf("cannot cast collection type column '%s'", colMeta.Name)
	}
	if colMeta.IsPrimaryKey {
		// primary keys are stored in structured row keys, not column families, and have type information already, so no need to case
		return string(colMeta.Name), nil
	}

	switch colMeta.CQLType.DataType() {
	case datatype.Int:
		return fmt.Sprintf("TO_INT64(%s['%s'])", colMeta.ColumnFamily, colMeta.Name), nil
	case datatype.Bigint:
		return fmt.Sprintf("TO_INT64(%s['%s'])", colMeta.ColumnFamily, colMeta.Name), nil
	case datatype.Float:
		return fmt.Sprintf("TO_FLOAT32(%s['%s'])", colMeta.ColumnFamily, colMeta.Name), nil
	case datatype.Double:
		return fmt.Sprintf("TO_FLOAT64(%s['%s'])", colMeta.ColumnFamily, colMeta.Name), nil
	case datatype.Boolean:
		return fmt.Sprintf("TO_INT64(%s['%s'])", colMeta.ColumnFamily, colMeta.Name), nil
	case datatype.Timestamp:
		return fmt.Sprintf("TO_TIME(%s['%s'])", colMeta.ColumnFamily, colMeta.Name), nil
	case datatype.Counter:
		return fmt.Sprintf("%s['']", colMeta.Name), nil
	case datatype.Blob:
		return fmt.Sprintf("TO_BLOB(%s['%s'])", colMeta.ColumnFamily, colMeta.Name), nil
	case datatype.Varchar:
		return fmt.Sprintf("%s['%s']", colMeta.ColumnFamily, colMeta.Name), nil
	default:
		return "", fmt.Errorf("unsupported CQL type: %s", colMeta.CQLType.DataType())
	}
}
