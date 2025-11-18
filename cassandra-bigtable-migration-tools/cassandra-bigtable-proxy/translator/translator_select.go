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
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/constants"
	"strconv"
	"strings"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

const (
	customWriteTime = "writetime_"
)

// TranslateSelect() Translates Cassandra select statement into a compatible Cloud Bigtable select query.
//
// Parameters:
//   - query: CQL Select statement
//
// Returns: PreparedSelectQuery struct and error if any
func (t *Translator) TranslateSelect(query string, sessionKeyspace types.Keyspace, isPreparedQuery bool) (*PreparedSelectQuery, *BoundSelectQuery, error) {
	p, err := NewCqlParser(query, false)
	if err != nil {
		return nil, nil, err
	}
	selectObj := p.Select_()
	if selectObj == nil || selectObj.KwSelect() == nil {
		return nil, nil, errors.New("ToBigtableSelect: Could not parse select object")
	}

	columns, err := parseSelectClause(selectObj.SelectElements())
	if err != nil {
		return nil, nil, err
	}

	keyspaceName, tableName, err := parseTarget(selectObj.FromSpec(), sessionKeyspace, t.SchemaMappingConfig)
	if err != nil {
		return nil, nil, err
	}

	tableConfig, err := t.SchemaMappingConfig.GetTableConfig(keyspaceName, tableName)
	if err != nil {
		return nil, nil, err
	}

	params := NewQueryParameters()
	values := NewQueryParameterValues(params)

	conditions, err := parseWhereClause(selectObj.WhereSpec(), tableConfig, params, values)
	if err != nil {
		return nil, nil, err
	}

	var groupBy []string
	if selectObj.GroupSpec() != nil {
		groupBy = parseGroupByColumn(selectObj.GroupSpec())
	}
	var orderBy OrderBy
	if selectObj.OrderSpec() != nil {
		orderBy, err = parseOrderByFromSelect(selectObj.OrderSpec())
		if err != nil {
			// pass the original error to provide proper root cause of error.
			return nil, nil, err
		}
	} else {
		orderBy.IsOrderBy = false
	}

	err = parseLimitClause(selectObj.LimitSpec(), params, values)
	if err != nil {
		return nil, nil, err
	}

	st := &PreparedSelectQuery{
		cqlQuery:        query,
		TranslatedQuery: "", // created later
		table:           tableName,
		keyspace:        keyspaceName,
		SelectClause:    *columns,
		Conditions:      conditions,
		OrderBy:         orderBy,
		GroupByColumns:  groupBy,
		Params:          params,
	}

	translatedResult, err := getBigtableSelectQuery(t, st)
	if err != nil {
		return nil, nil, err
	}
	st.TranslatedQuery = translatedResult

	var bound *BoundSelectQuery
	if !isPreparedQuery {
		bound, err = t.doBindSelect(st, values, primitive.ProtocolVersion4)
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

func (t *Translator) BindSelect(st *PreparedSelectQuery, cassandraValues []*primitive.Value, pv primitive.ProtocolVersion) (*BoundSelectQuery, error) {
	values, err := BindQueryParams(st.Params, cassandraValues, pv)
	if err != nil {
		return nil, err
	}
	return t.doBindSelect(st, values, pv)
}

func (t *Translator) doBindSelect(st *PreparedSelectQuery, values *QueryParameterValues, pv primitive.ProtocolVersion) (*BoundSelectQuery, error) {
	query := &BoundSelectQuery{
		Query:           st,
		ProtocolVersion: pv,
		Values:          values,
	}
	return query, nil
}

// parseSelectClause() parse Columns from the Select CqlQuery
//
// Parameters:
//   - input: The Select Element context from the antlr Parser.
//
// Returns: Columns Meta and an error if any.
func parseSelectClause(input cql.ISelectElementsContext) (*ColumnMeta, error) {
	if input == nil || len(input.AllSelectElement()) == 0 {
		return nil, errors.New("select clause empty")
	}

	if input.STAR() != nil {
		return &ColumnMeta{
			IsStar: true,
		}, nil
	}

	var selectedSolumns []SelectedColumn
	for _, val := range input.AllSelectElement() {
		selected := SelectedColumn{
			Sql: val.GetText(),
		}
		alias, err := parseAs(val.AsSpec())
		if err != nil {
			return nil, err
		}
		selected.Alias = alias
		funcCall := val.FunctionCall()
		if funcCall == nil {
			selected.Sql = val.GetText()
			selected.ColumnName = val.GetText()
			// Handle map access
			mapAccess := val.MapAccess()

			if mapAccess != nil {
				objectName := mapAccess.OBJECT_NAME().GetText()
				mapKey := mapAccess.Constant().GetText()

				// Remove surrounding quotes from mapKey
				mapKey = trimQuotes(mapKey)

				// Populate the selected column
				selected.Sql = fmt.Sprintf("%s['%s']", objectName, mapKey)
				selected.ColumnName = objectName
			}
		} else {
			if funcCall.OBJECT_NAME() == nil {
				return nil, errors.New("function call object is nil")
			}
			f := ParseCqlFunc(funcCall.OBJECT_NAME().GetText())
			if f == FuncUnknown {
				return nil, fmt.Errorf("unhandled cql function type: '%s'", funcCall.OBJECT_NAME().GetText())
			}

			var argument string
			if funcCall.STAR() != nil {
				argument = funcCall.STAR().GetText()
			} else {
				if funcCall.FunctionArgs() == nil {
					return nil, errors.New("function call argument object is nil")
				}
				argument = funcCall.FunctionArgs().GetText()
			}
			selected.Func = f
			selected.ColumnName = argument
		}

		selectedSolumns = append(selectedSolumns, selected)
	}

	return &ColumnMeta{Columns: selectedSolumns}, nil
}

func parseAs(a cql.IAsSpecContext) (string, error) {
	if a == nil || a.OBJECT_NAME() == nil {
		return "", nil
	}

	alias := a.OBJECT_NAME().GetText()
	if utilities.IsReservedCqlKeyword(alias) {
		return "", fmt.Errorf("cannot use reserved word as alias: '%s'", alias)
	}

	return alias, nil
}

// parseOrderByFromSelect() parse Order By from the Select CqlQuery
//
// Parameters:
//   - input: The Order Spec context from the antlr Parser.
//
// Returns: OrderBy struct
func parseOrderByFromSelect(input cql.IOrderSpecContext) (OrderBy, error) {
	var response OrderBy

	if input == nil {
		response.IsOrderBy = false
		return response, nil
	}

	orderSpecElements := input.AllOrderSpecElement()
	if len(orderSpecElements) == 0 {
		return OrderBy{}, fmt.Errorf("Order_by section not have proper values")
	}

	response.IsOrderBy = true
	response.Columns = make([]OrderByColumn, 0, len(orderSpecElements))

	for _, element := range orderSpecElements {
		object := element.OBJECT_NAME()
		if object == nil {
			return OrderBy{}, fmt.Errorf("Order_by section not have proper values")
		}

		colName := strings.TrimSpace(object.GetText())
		if colName == "" {
			return OrderBy{}, fmt.Errorf("Order_by section has empty column name")
		}
		if strings.Contains(colName, missingUndefined) {
			return OrderBy{}, fmt.Errorf("In order by, column name not provided correctly")
		}

		orderByCol := OrderByColumn{
			Column:    colName,
			Operation: Asc,
		}

		if element.KwDesc() != nil {
			orderByCol.Operation = Desc
		}

		response.Columns = append(response.Columns, orderByCol)
	}

	return response, nil
}

func parseLimitClause(input cql.ILimitSpecContext, params *QueryParameters, values *QueryParameterValues) error {
	if input == nil {
		return nil
	}

	params.AddParameterWithoutColumn(limitPlaceholder, types.TypeInt)

	if input.DecimalLiteral().DECIMAL_LITERAL() != nil {
		text := input.DecimalLiteral().DECIMAL_LITERAL().GetText()
		limitValue, err := strconv.Atoi(text)
		if err != nil {
			return errors.New("failed to parse limit")
		} else if limitValue < 0 {
			return errors.New("limit must be positive")
		}
		err = values.SetValue(limitPlaceholder, limitValue)
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
		if strings.Contains(colName, missingUndefined) || strings.Contains(colName, missing) {
			// If any group by element is malformed, treat as malformed and return nil
			return nil
		}

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
func createBtqlSelectClause(tableConfig *schemaMapping.TableConfig, s ColumnMeta, isGroupBy bool) (string, error) {
	if s.IsStar {
		return STAR, nil
	}
	var columns []string
	for _, col := range s.Columns {
		if col.Func != FuncUnknown {
			//todo: implement genereralized handling of writetime with rest of the aggregate functions
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
func createBtqlFunc(col SelectedColumn, tableConfig *schemaMapping.TableConfig) (string, error) {
	if col.Func == FuncCount && col.ColumnName == STAR {
		if col.Alias != "" {
			return "count(*) as " + col.Alias, nil
		}
		return "count(*)", nil
	}

	colMeta, found := tableConfig.Columns[types.ColumnName(col.ColumnName)]
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
	if col.Func != FuncCount {
		if !isTypeAllowedInAggregate(colMeta.CQLType.DataType()) {
			return "", fmt.Errorf("column not supported for aggregate")
		}
	}
	castValue, castErr := castScalarColumn(colMeta)
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
// It converts the input string to lowercase and checks if it exists as a key in the allowedFunctions map.
// The allowed functions are "avg", "sum", "min", "max", and "count".
// It returns true if the function is allowed, and false otherwise.
func funcAllowedInAggregate(f BtqlFunc) bool {
	return f == FuncAvg || f == FuncSum || f == FuncMin || f == FuncMax || f == FuncCount
}

func createBtqlSelectCol(tableConfig *schemaMapping.TableConfig, selectedColumn SelectedColumn, isGroupBy bool) (string, error) {
	colName := selectedColumn.Sql
	if selectedColumn.MapKey != "" {
		colName = selectedColumn.ColumnName
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

func processAsColumn(selectedColumn SelectedColumn, column *types.Column, isGroupBy bool) (string, error) {
	var columnFamily types.ColumnFamily
	if !column.CQLType.IsCollection() {
		var columnName = selectedColumn.Sql
		if column.CQLType == types.TypeCounter {
			// counters are stored as counter_col['']
			columnFamily = column.ColumnFamily
			columnName = ""
		}
		if isGroupBy {
			castedCol, err := castScalarColumn(column)
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
func processRegularColumn(selectedColumn SelectedColumn, col *types.Column) (string, error) {
	if !col.CQLType.IsCollection() {
		return castScalarColumn(col)
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

// inferDataType() returns the data type based on the name of a function.
//
// Parameters:
//   - methodName: Columns of aggregate function
//
// Returns: Returns datatype of aggregate function.
func inferDataType(methodName string) (string, error) {
	switch methodName {
	case "count":
		return "bigint", nil
	case "round":
		return "float", nil
	default:
		return "", fmt.Errorf("unknown function '%s'", methodName)
	}
}

// getBigtableSelectQuery() Returns Bigtable Select query using Parsed information.
//
// Parameters:
//   - data: PreparedSelectQuery struct with all select query info from CQL query
func getBigtableSelectQuery(t *Translator, st *PreparedSelectQuery) (string, error) {
	column := ""

	tableConfig, err := t.SchemaMappingConfig.GetTableConfig(st.Keyspace(), st.Table())
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
			aliasToColumn[col.Alias] = col.ColumnName
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
						col, err := castScalarColumn(colMeta)
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
						orderByKey, err := castScalarColumn(colMeta)
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

	if st.Params.Has(limitPlaceholder) {
		btQuery = btQuery + " LIMIT " + string(limitPlaceholder)
	}
	btQuery += ";"
	return btQuery, nil
}

// createBtqlWhereClause(): takes a slice of Condition structs and returns a string representing the WHERE clause of a bigtable SQL query.
// It iterates over the clauses and constructs the WHERE clause by combining the column name, operator, and value of each clause.
// If the operator is "IN", the value is wrapped with the UNNEST function.
// The constructed WHERE clause is returned as a string.
func createBtqlWhereClause(clauses []Condition, tableConfig *schemaMapping.TableConfig) (string, error) {
	whereClause := ""
	for _, val := range clauses {
		column := "`" + string(val.Column.Name) + "`"
		value := val.ValuePlaceholder
		if col, ok := tableConfig.Columns[val.Column.Name]; ok {
			// Check if the column is a primitive type and prepend the column family
			if !col.CQLType.IsCollection() {
				var castErr error
				column, castErr = castScalarColumn(col)
				if castErr != nil {
					return "", castErr
				}
			}
		}
		if whereClause != "" && val.Operator != constants.BETWEEN_AND {
			whereClause += " AND "
		}
		if val.Operator == constants.BETWEEN {
			whereClause += fmt.Sprintf("%s BETWEEN %s", column, val.ValuePlaceholder)
		} else if val.Operator == constants.BETWEEN_AND {
			whereClause += fmt.Sprintf(" AND %s", val.ValuePlaceholder)
		} else if val.Operator == constants.IN {
			whereClause += fmt.Sprintf("%s IN UNNEST(%s)", column, val.ValuePlaceholder)
		} else if val.Operator == constants.MAP_CONTAINS_KEY {
			whereClause += fmt.Sprintf("MAP_CONTAINS_KEY(%s, %s)", column, value)
		} else if val.Operator == constants.ARRAY_INCLUDES {
			whereClause += fmt.Sprintf("ARRAY_INCLUDES(MAP_VALUES(%s), %s)", column, value)
		} else {
			whereClause += fmt.Sprintf("%s %s %s", column, val.Operator, value)
		}
	}

	if whereClause != "" {
		whereClause = " WHERE " + whereClause
	}
	return whereClause, nil
}
