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
	"cloud.google.com/go/bigtable"
	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"go.uber.org/zap"
	"time"
)

type Translator struct {
	Logger              *zap.Logger
	SchemaMappingConfig *schemaMapping.SchemaMappingConfig
	// determines the encoding for int row keys in all new tables
	DefaultIntRowKeyEncoding types.IntRowKeyEncodingType
}

// SelectQueryMap represents the mapping of a select query along with its translation details.
type SelectQueryMap struct {
	Query            string     // Original query string
	TranslatedQuery  string     // btql
	Table            string     // Table involved in the query
	Keyspace         string     // Keyspace to which the table belongs
	ColumnMeta       ColumnMeta // Translator generated Metadata about the columns involved
	SelectedColumns  []types.SelectedColumn
	Conditions       []types.Condition
	Params           *types.QueryParameters      // Parameters for the query
	Columns          []*types.Column             //all columns mentioned in query
	ReturnMetadata   []*message.ColumnMetadata   // Metadata of selected columns in Cassandra format
	VariableMetadata []*message.ColumnMetadata   // Metadata of variable columns for prepared queries in Cassandra format
	CachedBTPrepare  *bigtable.PreparedStatement // prepared statement object for bigtable
	OrderBy          OrderBy                     // Order by clause details
	GroupByColumns   []string                    // Group by Columns - could be a column name or a column index
}

type SelectQueryAndData struct {
	Query  *SelectQueryMap
	Values *types.QueryParameterValues
}

type OrderOperation string

const (
	Asc  OrderOperation = "asc"
	Desc OrderOperation = "desc"
)

type OrderBy struct {
	IsOrderBy bool
	Columns   []OrderByColumn
}

type OrderByColumn struct {
	Column    string
	Operation OrderOperation
}

type ColumnMeta struct {
	Star   bool
	Column []types.SelectedColumn
}

type IfSpec struct {
	IfExists    bool
	IfNotExists bool
}

type IncrementOperationType int

const (
	None IncrementOperationType = iota
	Increment
	Decrement
)

// PreparedInsertQuery represents the mapping of an insert query along with its translation details.
type PreparedInsertQuery struct {
	CqlQuery         string
	Keyspace         string
	Table            string
	Params           *types.QueryParameters
	Assignments      []Assignment
	ReturnMetadata   []*message.ColumnMetadata // Metadata of all columns of that table in Cassandra format
	VariableMetadata []*message.ColumnMetadata // Metadata of variable columns for prepared queries in Cassandra format
	IfNotExists      bool
}

// DeleteQueryMapping represents the mapping of a delete query along with its translation details.
type DeleteQueryMapping struct {
	Query             string            // Original query string
	Table             string            // Table involved in the query
	Keyspace          string            // Keyspace to which the table belongs
	Conditions        []types.Condition // List of clauses in the delete query
	Params            *types.QueryParameters
	ExecuteByMutation bool                      // Flag to indicate if the delete should be executed by mutation
	VariableMetadata  []*message.ColumnMetadata // Metadata of variable columns for prepared queries in Cassandra format
	ReturnMetadata    []*message.ColumnMetadata // Metadata of all columns of that table in Cassandra format
	IfExists          bool
	SelectedColumns   []types.SelectedColumn
}

type CreateTableStatementMap struct {
	QueryType         string
	Keyspace          string
	Table             string
	IfNotExists       bool
	Columns           []types.CreateColumn
	PrimaryKeys       []CreateTablePrimaryKeyConfig
	IntRowKeyEncoding types.IntRowKeyEncodingType
}

type CreateTablePrimaryKeyConfig struct {
	Name    types.ColumnName
	KeyType string
}

type AlterTableStatementMap struct {
	QueryType   string
	Keyspace    string
	Table       string
	IfNotExists bool
	AddColumns  []types.CreateColumn
	DropColumns []string
}

type DropTableStatementMap struct {
	QueryType string
	Keyspace  string
	Table     string
	IfExists  bool
}

type TruncateTableStatementMap struct {
	QueryType string
	Keyspace  string
	Table     string
}

// UpdateQueryMapping represents the mapping of an update query along with its translation details.
type UpdateQueryMapping struct {
	Query            string                    // Original query string
	Table            string                    // Table involved in the query
	Keyspace         string                    // Keyspace to which the table belongs
	Values           []Assignment              // Columns to be updated
	Clauses          []types.Condition         // List of clauses in the update query
	Params           *types.QueryParameters    // Parameters for the query
	Columns          []*types.Column           // Cassandra columns updated
	ReturnMetadata   []*message.ColumnMetadata // Metadata of all columns of that table in Cassandra format
	VariableMetadata []*message.ColumnMetadata // Metadata of variable columns for prepared queries in Cassandra format
	IfExists         bool
}

type UpdateSetResponse struct {
	Assignments []Assignment
}

type TableObj struct {
	TableName    string
	KeyspaceName string
}

type DescribeStatementMap struct {
	Keyspace string
	Table    string
}

type BoundSelectColumn interface {
	Column() *types.Column
}

type BoundIndexColumn struct {
	column *types.Column
	Index  int
}

func NewBoundIndexColumn(column *types.Column, index int) *BoundIndexColumn {
	return &BoundIndexColumn{column: column, Index: index}
}

func (b *BoundIndexColumn) Column() *types.Column {
	return b.column
}

type BoundKeyColumn struct {
	column *types.Column
	Key    types.ColumnQualifier
}

func NewBoundKeyColumn(column *types.Column, key types.ColumnQualifier) *BoundKeyColumn {
	return &BoundKeyColumn{column: column, Key: key}
}

func (b *BoundKeyColumn) Column() *types.Column {
	return b.column
}

type BoundTimestampInfo struct {
	Timestamp         time.Time
	HasUsingTimestamp bool
}

type BoundDeleteQuery struct {
	Keyspace string
	Table    string
	RowKey   types.RowKey
	IfExists bool
	Columns  []BoundSelectColumn
}

// BigtableMutations holds the results from parseComplexOperations.
type BigtableMutations struct {
	RowKey                types.RowKey
	IfSpec                IfSpec
	UsingTimestamp        *BoundTimestampInfo
	Data                  []*types.BigtableData
	DelColumnFamily       []types.ColumnFamily
	DelColumns            []*types.BigtableColumn
	Counters              []BigtableCounterOp
	SetIndexOps           []BigtableSetIndexOp
	DeleteListElementsOps []BigtableDeleteListElementsOp
}

type BigtableCounterOp struct {
	Family types.ColumnFamily
	Value  int64
}
type BigtableSetIndexOp struct {
	Family types.ColumnFamily
	Index  int
	Value  types.BigtableValue
}
type BigtableDeleteListElementsOp struct {
	Family types.ColumnFamily
	Values []types.BigtableValue
}
