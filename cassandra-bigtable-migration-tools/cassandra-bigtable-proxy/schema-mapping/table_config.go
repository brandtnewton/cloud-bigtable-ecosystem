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
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"golang.org/x/exp/maps"
)

// TableConfig contains all schema information about a single table
type TableConfig struct {
	Keyspace           string
	Name               string
	Columns            map[string]*types.Column
	PrimaryKeys        []*types.Column
	SystemColumnFamily string
	IntRowKeyEncoding  types.IntRowKeyEncodingType
}

// NewTableConfig is a constructor for TableConfig. Please use this instead of direct initialization.
func NewTableConfig(
	keyspace string,
	name string,
	systemColumnFamily string,
	intRowKeyEncoding types.IntRowKeyEncodingType,
	columns []*types.Column,
) *TableConfig {
	columnMap := make(map[string]*types.Column)
	var pmks []*types.Column = nil

	for i, column := range columns {
		column.Metadata = message.ColumnMetadata{
			Keyspace: keyspace,
			Table:    name,
			Name:     column.Name,
			Index:    int32(i),
			Type:     column.CQLType,
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
		Name:               name,
		Columns:            columnMap,
		PrimaryKeys:        pmks,
		SystemColumnFamily: systemColumnFamily,
		IntRowKeyEncoding:  intRowKeyEncoding,
	}
}

func (tableConfig *TableConfig) GetPkByTableNameWithFilter(filterPrimaryKeys []string) []*types.Column {
	var result []*types.Column
	for _, pmk := range tableConfig.PrimaryKeys {
		if slices.Contains(filterPrimaryKeys, pmk.Name) {
			result = append(result, pmk)
		}
	}
	return result
}

func (tableConfig *TableConfig) GetCassandraPositionForColumn(column string) int {
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
func (tableConfig *TableConfig) GetColumnFamily(columnName string) string {
	colType, err := tableConfig.GetColumnType(columnName)
	if err == nil && utilities.IsCollection(colType) {
		return columnName
	}
	return tableConfig.SystemColumnFamily
}

func (tableConfig *TableConfig) HasColumn(columnName string) bool {
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
	var colNames []string = nil
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
			pkCols = append(pkCols, key.Name)
		} else {
			clusteringCols = append(clusteringCols, key.Name)
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

func (tableConfig *TableConfig) GetColumn(columnName string) (*types.Column, error) {
	col, ok := tableConfig.Columns[columnName]
	if !ok {
		return nil, fmt.Errorf("unknown column '%s' in table %s.%s", columnName, tableConfig.Keyspace, tableConfig.Name)
	}
	return col, nil
}

func (tableConfig *TableConfig) GetPrimaryKeys() []string {
	var primaryKeys []string
	for _, pk := range tableConfig.PrimaryKeys {
		primaryKeys = append(primaryKeys, pk.Name)
	}
	return primaryKeys
}

func (tableConfig *TableConfig) GetColumnType(columnName string) (datatype.DataType, error) {
	col, ok := tableConfig.Columns[columnName]
	if !ok {
		return nil, fmt.Errorf("undefined column name %s in table %s.%s", columnName, tableConfig.Keyspace, tableConfig.Name)
	}

	if col.CQLType == nil {
		return nil, fmt.Errorf("undefined column name %s in table %s.%s", columnName, tableConfig.Keyspace, tableConfig.Name)
	}
	return col.CQLType, nil
}

// GetMetadataForColumns retrieves metadata for specific columns in a given table.
// This method is a part of the SchemaMappingConfig struct.
//
// Parameters:
//   - tableName: The name of the table for which column metadata is being requested.
//   - columnNames(optional):Accepts nil if no columnNames provided or else A slice of strings containing the names of the columns for which
//     metadata is required. If this slice is empty, metadata for all
//     columns in the table will be returned.
//
// Returns:
// - A slice of pointers to ColumnMetadata structs containing metadata for each requested column.
// - An error if the specified table is not found in the Columns.
func (tableConfig *TableConfig) GetMetadataForColumns(columnNames []string) ([]*message.ColumnMetadata, error) {
	if len(columnNames) == 0 {
		return getAllColumnsMetadata(tableConfig.Columns), nil
	}
	ff, err := tableConfig.getSpecificColumnsMetadata(columnNames)
	return ff, err
}

// GetMetadataForSelectedColumns retrieves metadata for specified columns in a given table.
// This method fetches metadata for the selected columns from the schema mapping configuration.
// If no columns are specified, metadata for all columns in the table is returned.
//
// Parameters:
//   - tableName: The name of the table for which column metadata is being requested.
//   - keySpace: The keyspace where the table resides.
//   - columnNames: A slice of SelectedColumn specifying the columns for which metadata is required.
//     If nil or empty, metadata for all columns in the table is returned.
//
// Returns:
//   - []*message.ColumnMetadata: A slice of pointers to ColumnMetadata structs containing metadata
//     for each requested column.
//   - error: Returns an error if the specified table is not found in Columns.
func (tableConfig *TableConfig) GetMetadataForSelectedColumns(columnNames []types.SelectedColumn) ([]*message.ColumnMetadata, error) {
	if len(columnNames) == 0 {
		return getAllColumnsMetadata(tableConfig.Columns), nil
	}
	return tableConfig.getSpecificColumnsMetadataForSelectedColumns(tableConfig.Columns, columnNames)
}

// getSpecificColumnsMetadataForSelectedColumns() generates column metadata for specifically selected columns.
// It handles regular columns, writetime columns, and special columns, and logs an error for invalid configurations.
//
// Parameters:
//   - columnsMap: A map where the keys are column names and the values are pointers to types.Column containing metadata.
//   - selectedColumns: A slice of SelectedColumn representing columns that have been selected for query.
//   - tableName: The name of the table from which columns are being selected.
//
// Returns:
//   - A slice of pointers to ColumnMetadata, representing the metadata for each selected column.
//   - An error if a column cannot be found in the map, or if there's an issue handling special columns.
func (tableConfig *TableConfig) getSpecificColumnsMetadataForSelectedColumns(columnsMap map[string]*types.Column, selectedColumns []types.SelectedColumn) ([]*message.ColumnMetadata, error) {
	var columnMetadataList []*message.ColumnMetadata
	var columnName string
	for i, columnMeta := range selectedColumns {
		columnName = columnMeta.Name

		if column, ok := columnsMap[columnName]; ok {
			columnMetadataList = append(columnMetadataList, cloneColumnMetadata(&column.Metadata, int32(i)))
		} else if columnMeta.IsWriteTimeColumn {
			metadata, err := handleSpecialColumn(columnsMap, getTimestampColumnName(columnMeta.Alias, columnMeta.ColumnName), int32(i), true)
			if err != nil {
				return nil, err
			}
			columnMetadataList = append(columnMetadataList, metadata)
		} else if isSpecialColumn(columnName) {
			metadata, err := handleSpecialColumn(columnsMap, columnName, int32(i), false)
			if err != nil {
				return nil, err
			}
			columnMetadataList = append(columnMetadataList, metadata)
		} else if columnMeta.IsFunc || columnMeta.MapKey != "" || columnMeta.IsWriteTimeColumn {
			metadata, err := tableConfig.handleSpecialSelectedColumn(columnsMap, columnMeta, int32(i))
			if err != nil {
				return nil, err
			}
			columnMetadataList = append(columnMetadataList, metadata)
		} else {
			return nil, fmt.Errorf("metadata not found for column `%s` in table `%s`", columnName, tableConfig.Name)
		}
		if columnMeta.IsAs {
			columnMetadataList[i].Name = columnMeta.Alias
		}
	}
	return columnMetadataList, nil
}

// getSpecificColumnsMetadata() retrieves metadata for specific columns in a given table.
//
// Parameters:
//   - columnsMap: column info for a given table.
//   - columnNames: column names for which the metadata is required.
//   - tableName: name of the table
//
// Returns:
// - A slice of pointers to ColumnMetadata structs containing metadata for each requested column.
// - An error
func (tableConfig *TableConfig) getSpecificColumnsMetadata(columnNames []string) ([]*message.ColumnMetadata, error) {
	var columnMetadataList []*message.ColumnMetadata
	for i, columnName := range columnNames {
		if column, ok := tableConfig.Columns[columnName]; ok {
			columnMetadataList = append(columnMetadataList, cloneColumnMetadata(&column.Metadata, int32(i)))
		} else if isSpecialColumn(columnName) {
			metadata, err := handleSpecialColumn(tableConfig.Columns, columnName, int32(i), false)
			if err != nil {
				return nil, err
			}
			columnMetadataList = append(columnMetadataList, metadata)
		} else {
			return nil, fmt.Errorf("metadata not found for column `%s` in table `%s`", columnName, tableConfig.Name)
		}
	}
	return columnMetadataList, nil
}

// handleSpecialSelectedColumn processes special column types and returns corresponding ColumnMetadata.
// It handles count functions, write time columns, aliased columns, function columns, and map columns.
//
// Parameters:
//   - columnsMap: A map of column names to their types.Column definitions
//   - columnSelected: Selected column configuration including special attributes
//   - index: The position index of the column
//   - tableName: Name of the table containing the column
//   - keySpace: Keyspace name containing the table
//
// Returns:
//   - *message.ColumnMetadata: Metadata for the processed column
//   - error: If the column is not found in the provided metadata
func (tableConfig *TableConfig) handleSpecialSelectedColumn(columnsMap map[string]*types.Column, columnSelected types.SelectedColumn, index int32) (*message.ColumnMetadata, error) {
	var cqlType datatype.DataType
	if columnSelected.FuncName == "count" || columnSelected.IsWriteTimeColumn {
		// For count function, the type is always bigint
		return &message.ColumnMetadata{
			Keyspace: tableConfig.Keyspace,
			Table:    tableConfig.Name,
			Type:     datatype.Bigint,
			Index:    index,
			Name:     columnSelected.Name,
		}, nil
	}
	lookupColumn := columnSelected.Name
	if columnSelected.Alias != "" || columnSelected.IsFunc || columnSelected.MapKey != "" {
		lookupColumn = columnSelected.ColumnName
	}
	column, ok := columnsMap[lookupColumn]
	if !ok {
		return nil, fmt.Errorf("special column %s not found in provided metadata", lookupColumn)
	}
	cqlType = column.Metadata.Type
	if columnSelected.MapKey != "" && cqlType.GetDataTypeCode() == primitive.DataTypeCodeMap {
		cqlType = cqlType.(datatype.MapType).GetValueType() // this gets the type of map value e.g map<varchar,int> -> datatype(int)
	}
	columnMd := &message.ColumnMetadata{
		Keyspace: tableConfig.Keyspace,
		Table:    tableConfig.Name,
		Type:     cqlType,
		Index:    index,
		Name:     columnSelected.Name,
	}

	return columnMd, nil
}
