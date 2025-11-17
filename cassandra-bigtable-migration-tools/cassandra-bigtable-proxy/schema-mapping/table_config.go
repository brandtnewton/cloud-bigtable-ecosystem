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

package schemaMapping

import (
	"fmt"
	"slices"
	"strings"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"golang.org/x/exp/maps"
)

// TableConfig contains all schema information about a single table
type TableConfig struct {
	Keyspace           types.Keyspace
	Name               types.TableName
	Columns            map[types.ColumnName]*types.Column
	PrimaryKeys        []*types.Column
	SystemColumnFamily types.ColumnFamily
	IntRowKeyEncoding  types.IntRowKeyEncodingType
}

// NewTableConfig is a constructor for TableConfig. Please use this instead of direct initialization.
func NewTableConfig(
	keyspace types.Keyspace, table types.TableName,
	systemColumnFamily types.ColumnFamily,
	intRowKeyEncoding types.IntRowKeyEncodingType,
	columns []*types.Column,
) *TableConfig {
	columnMap := make(map[types.ColumnName]*types.Column)
	var pmks []*types.Column = nil

	for i, column := range columns {
		column.Metadata = message.ColumnMetadata{
			Keyspace: string(keyspace),
			Table:    string(table),
			Name:     string(column.Name),
			Index:    int32(i),
			Type:     column.CQLType.DataType(),
		}
		columnMap[column.Name] = column
		if column.KeyType != utilities.KEY_TYPE_REGULAR {
			// this field is redundant - make sure it's in sync with other fields
			column.IsPrimaryKey = true
			pmks = append(pmks, column)
		}
	}
	sortPrimaryKeys(pmks)

	return &TableConfig{
		Keyspace:           keyspace,
		Name:               table,
		Columns:            columnMap,
		PrimaryKeys:        pmks,
		SystemColumnFamily: systemColumnFamily,
		IntRowKeyEncoding:  intRowKeyEncoding,
	}
}

func (tableConfig *TableConfig) GetPkByTableNameWithFilter(filterPrimaryKeys []types.ColumnName) []*types.Column {
	var result []*types.Column
	for _, pmk := range tableConfig.PrimaryKeys {
		if slices.Contains(filterPrimaryKeys, pmk.Name) {
			result = append(result, pmk)
		}
	}
	return result
}

func (tableConfig *TableConfig) GetCassandraPositionForColumn(column types.ColumnName) int {
	col, err := tableConfig.GetColumn(column)
	if err != nil {
		return -1
	}
	// regular columns all have a position of -1 in cassandra - position is only used to track primary key position/index.
	if !col.IsPrimaryKey {
		return -1
	}
	// we need to return the position/index of the column _within_ it's key type.
	// Example: given PRIMARY KEY((org, user), email, name) org=0, user=1, email=0 and name=1
	result := 0
	for _, key := range tableConfig.PrimaryKeys {
		if key.KeyType != col.KeyType {
			continue
		}
		if key.Name == col.Name {
			return result
		}
		result++
	}
	// this shouldn't happen
	return 0
}

// GetColumnFamily retrieves the column family for a given column.
// Returns the column family name from the schema mapping with validation.
// Returns default column family for primitive types and column name for collections.
func (tableConfig *TableConfig) GetColumnFamily(columnName types.ColumnName) types.ColumnFamily {
	colType, err := tableConfig.GetColumnType(columnName)
	if err == nil && colType.IsCollection() {
		return types.ColumnFamily(columnName)
	}
	return tableConfig.SystemColumnFamily
}

func (tableConfig *TableConfig) HasColumn(columnName types.ColumnName) bool {
	_, ok := tableConfig.Columns[columnName]
	return ok
}

func (tableConfig *TableConfig) Describe() string {
	cols := maps.Values(tableConfig.Columns)
	// sort by metadata index for consistent output
	slices.SortFunc(cols, func(a, b *types.Column) int {
		if a.IsPrimaryKey && b.IsPrimaryKey {
			return a.PkPrecedence - b.PkPrecedence
		} else if a.IsPrimaryKey {
			return -1
		} else if b.IsPrimaryKey {
			return 1
		} else {
			return int(a.Metadata.Index - b.Metadata.Index)
		}
	})
	var colNames []types.ColumnName = nil
	for _, col := range cols {
		colNames = append(colNames, col.Name)
	}

	// First collect all column definitions with their data types
	var colDefs []string
	for _, colName := range colNames {
		col := tableConfig.Columns[colName]
		colDefs = append(colDefs, fmt.Sprintf("%s %s", colName, strings.ToUpper(col.CQLType.String())))
	}

	var pkCols []string = nil
	var clusteringCols []string = nil
	for _, key := range tableConfig.PrimaryKeys {
		if key.KeyType == utilities.KEY_TYPE_PARTITION {
			pkCols = append(pkCols, string(key.Name))
		} else {
			clusteringCols = append(clusteringCols, string(key.Name))
		}
	}

	// Build primary key clause
	pkClause := ""
	if len(pkCols) == 1 && len(clusteringCols) == 0 {
		pkClause = fmt.Sprintf("PRIMARY KEY (%s)", pkCols[0])
	} else if len(pkCols) == 1 && len(clusteringCols) > 0 {
		pkClause = fmt.Sprintf("PRIMARY KEY (%s, %s)", pkCols[0], strings.Join(clusteringCols, ", "))
	} else {
		pkClause = fmt.Sprintf("PRIMARY KEY ((%s), %s)",
			strings.Join(pkCols, ", "),
			strings.Join(clusteringCols, ", "))
	}

	createTableStmt := fmt.Sprintf("CREATE TABLE %s.%s (\n    %s,\n    %s\n);",
		tableConfig.Keyspace, tableConfig.Name,
		strings.Join(colDefs, ",\n    "),
		pkClause)
	return createTableStmt
}

func (tableConfig *TableConfig) GetColumn(columnName types.ColumnName) (*types.Column, error) {
	col, ok := tableConfig.Columns[columnName]
	if !ok {
		return nil, fmt.Errorf("unknown column '%s' in table %s.%s", columnName, tableConfig.Keyspace, tableConfig.Name)
	}
	return col, nil
}

func (tableConfig *TableConfig) GetPrimaryKeys() []types.ColumnName {
	var primaryKeys []types.ColumnName
	for _, pk := range tableConfig.PrimaryKeys {
		primaryKeys = append(primaryKeys, pk.Name)
	}
	return primaryKeys
}

func (tableConfig *TableConfig) GetColumnDataType(columnName types.ColumnName) (datatype.DataType, error) {
	col, err := tableConfig.GetColumnType(columnName)
	if err != nil {
		return nil, err
	}
	return col.DataType(), nil
}
func (tableConfig *TableConfig) GetColumnType(columnName types.ColumnName) (types.CqlDataType, error) {
	col, ok := tableConfig.Columns[columnName]
	if !ok {
		return nil, fmt.Errorf("undefined column name %s in table %s.%s", columnName, tableConfig.Keyspace, tableConfig.Name)
	}

	if col.CQLType == nil {
		return nil, fmt.Errorf("undefined column name %s in table %s.%s", columnName, tableConfig.Keyspace, tableConfig.Name)
	}
	return col.CQLType, nil
}
