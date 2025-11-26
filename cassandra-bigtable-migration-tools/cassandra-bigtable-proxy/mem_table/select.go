package mem_table

import (
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
)

func (e *InMemEngine) Execute(query *types.ExecutableSelectQuery) ([]types.GoRow, error) {
	data, err := e.GetTable(query.Keyspace(), query.Table())
	if err != nil {
		return nil, err
	}

	// filter with where clause
	var filtered []types.GoRow
	for _, row := range data {
		matches, err := matchesConditions(row, query.Conditions, query.Values)
		if err != nil {
			return nil, err
		}
		if matches {
			filtered = append(filtered, row)
		}
	}

	// handle aggregates if present
	for _, col := range query.SelectClause.Columns {
		if col.Func == types.FuncCodeCount {
			key := "count"
			if col.Alias != "" {
				key = col.Alias
			}
			return []types.GoRow{
				{key: len(filtered)},
			}, nil
		}
	}

	// projection (SELECT specific columns)
	if query.SelectClause.IsStar {
		return filtered, nil
	}

	projectedRows := make([]types.GoRow, len(filtered))
	for i, row := range filtered {
		projectedRows[i] = projectRow(row, query.SelectClause.Columns)
	}

	return projectedRows, nil
}

func matchesConditions(row types.GoRow, conditions []types.Condition, parameterValues *types.QueryParameterValues) (bool, error) {
	for _, cond := range conditions {
		col := string(cond.Column.Name)
		rowValue, ok := row[col]
		if !ok {
			return false, fmt.Errorf("unknown column '%s' on table", col)
		}

		paramValue, err := parameterValues.GetValue(cond.ValuePlaceholder)
		if err != nil {
			return false, err
		}

		if cond.Operator == types.EQ {
			if rowValue != paramValue {
				return false, nil
			}
		} else {
			return false, fmt.Errorf("unhandled where clause operator: '%s'", cond.Operator)
		}
	}
	return true, nil
}

func projectRow(source types.GoRow, columns []types.SelectedColumn) types.GoRow {
	newRow := make(types.GoRow)
	for _, col := range columns {
		val, exists := source[string(col.ColumnName)]
		if exists {
			// Use Alias if present, otherwise use original Family Name
			targetKey := col.Alias
			if targetKey == "" {
				targetKey = string(col.ColumnName)
			}
			newRow[targetKey] = val
		}
	}
	return newRow
}
