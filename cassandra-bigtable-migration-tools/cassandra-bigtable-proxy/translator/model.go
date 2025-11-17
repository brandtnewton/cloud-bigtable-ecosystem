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
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/constants"
	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"go.uber.org/zap"
	"time"
)

type Translator struct {
	Logger              *zap.Logger
	SchemaMappingConfig *schemaMapping.SchemaMappingConfig
	// determines the encoding for int row keys in all new tables
	DefaultIntRowKeyEncoding types.IntRowKeyEncodingType
}

// PreparedSelectQuery represents the mapping of a select query along with its translation details.
type PreparedSelectQuery struct {
	keyspace        types.Keyspace  // Keyspace to which the table belongs
	table           types.TableName // Table involved in the query
	cqlQuery        string          // Original query string
	TranslatedQuery string          // btql
	ColumnMeta      ColumnMeta      // Translator generated Metadata about the columns involved
	SelectedColumns []SelectedColumn
	Conditions      []Condition
	Params          *QueryParameters            // Parameters for the query
	Columns         []*types.Column             //all columns mentioned in query
	CachedBTPrepare *bigtable.PreparedStatement // prepared statement object for bigtable
	OrderBy         OrderBy                     // Order by clause details
	GroupByColumns  []string                    // Group by Columns - could be a column name or a column index
}

func (p PreparedSelectQuery) Keyspace() types.Keyspace {
	return p.keyspace
}

func (p PreparedSelectQuery) Table() types.TableName {
	return p.table
}

func (p PreparedSelectQuery) CqlQuery() string {
	return p.cqlQuery
}

type BoundSelectQuery struct {
	Query           *PreparedSelectQuery
	ProtocolVersion primitive.ProtocolVersion
	Values          *QueryParameterValues
}

func (b BoundSelectQuery) SelectedColumns() []SelectedColumn {
	return b.Query.SelectedColumns
}

func (b BoundSelectQuery) Keyspace() types.Keyspace {
	return b.Query.Keyspace()
}

func (b BoundSelectQuery) Table() types.TableName {
	return b.Query.Table()
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
	IsStar bool
	Column []SelectedColumn
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

type IBoundQuery interface {
	Keyspace() types.Keyspace
	Table() types.TableName
	SelectedColumns() []SelectedColumn
}
type IPreparedQuery interface {
	Keyspace() types.Keyspace
	Table() types.TableName
	CqlQuery() string
}

// PreparedInsertQuery represents the mapping of an insert query along with its translation details.
type PreparedInsertQuery struct {
	cqlQuery    string
	keyspace    types.Keyspace
	table       types.TableName
	Params      *QueryParameters
	Assignments []Assignment
	IfNotExists bool
}

func (p PreparedInsertQuery) Keyspace() types.Keyspace {
	return p.keyspace
}

func (p PreparedInsertQuery) Table() types.TableName {
	return p.table
}

func (p PreparedInsertQuery) CqlQuery() string {
	return p.cqlQuery
}

// PreparedDeleteQuery represents the mapping of a delete query along with its translation details.
type PreparedDeleteQuery struct {
	keyspace          types.Keyspace  // Keyspace to which the table belongs
	table             types.TableName // Table involved in the query
	cqlQuery          string          // Original query string
	Conditions        []Condition     // List of clauses in the delete query
	Params            *QueryParameters
	ExecuteByMutation bool // Flag to indicate if the delete should be executed by mutation
	IfExists          bool
	SelectedColumns   []SelectedColumn
}

func (p PreparedDeleteQuery) Keyspace() types.Keyspace {
	return p.keyspace
}

func (p PreparedDeleteQuery) Table() types.TableName {
	return p.table
}

func (p PreparedDeleteQuery) CqlQuery() string {
	return p.cqlQuery
}

type CreateTableStatementMap struct {
	Keyspace          types.Keyspace
	Table             types.TableName
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
	Keyspace    types.Keyspace
	Table       types.TableName
	IfNotExists bool
	AddColumns  []types.CreateColumn
	DropColumns []string
}

type DropTableStatementMap struct {
	Keyspace types.Keyspace
	Table    types.TableName
	IfExists bool
}

type TruncateTableStatementMap struct {
	Keyspace types.Keyspace
	Table    types.TableName
}

type PreparedUpdateQuery struct {
	keyspace     types.Keyspace   // Keyspace to which the table belongs
	table        types.TableName  // Table involved in the query
	cqlQuery     string           // Original query string
	Values       []Assignment     // Columns to be updated
	Clauses      []Condition      // List of clauses in the update query
	Params       *QueryParameters // Parameters for the query
	Columns      []*types.Column  // Cassandra columns updated
	IfExists     bool
	ExpectedType types.CqlDataType
}

func (p PreparedUpdateQuery) Keyspace() types.Keyspace {
	return p.keyspace
}

func (p PreparedUpdateQuery) Table() types.TableName {
	return p.table
}

func (p PreparedUpdateQuery) CqlQuery() string {
	return p.cqlQuery
}

type UpdateSetResponse struct {
	Assignments []Assignment
}

type TableObj struct {
	TableName    types.TableName
	KeyspaceName types.Keyspace
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

type IBigtableMutation interface {
	Keyspace() types.Keyspace
	Table() types.TableName
	RowKey() types.RowKey
}

type BoundDeleteQuery struct {
	keyspace types.Keyspace
	table    types.TableName
	rowKey   types.RowKey
	IfExists bool
	Columns  []BoundSelectColumn
}

func (b BoundDeleteQuery) SelectedColumns() []SelectedColumn {
	return nil
}

func (b BoundDeleteQuery) Keyspace() types.Keyspace {
	return b.keyspace
}

func (b BoundDeleteQuery) Table() types.TableName {
	return b.table
}

func (b BoundDeleteQuery) RowKey() types.RowKey {
	return b.rowKey
}

// BigtableWriteMutation holds the results from parseComplexOperations.
type BigtableWriteMutation struct {
	keyspace              types.Keyspace
	table                 types.TableName
	rowKey                types.RowKey
	IfSpec                IfSpec
	UsingTimestamp        *BoundTimestampInfo
	Data                  []*types.BigtableData
	DelColumnFamily       []types.ColumnFamily
	DelColumns            []*types.BigtableColumn
	CounterOps            []BigtableCounterOp
	SetIndexOps           []BigtableSetIndexOp
	DeleteListElementsOps []BigtableDeleteListElementsOp
}

func (b BigtableWriteMutation) SelectedColumns() []SelectedColumn {
	return nil
}

func (b BigtableWriteMutation) Keyspace() types.Keyspace {
	return b.keyspace
}

func (b BigtableWriteMutation) Table() types.TableName {
	return b.table
}

func (b BigtableWriteMutation) RowKey() types.RowKey {
	return b.rowKey
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

type Condition struct {
	Column   *types.Column
	Operator constants.Operator
	// points to a placeholder
	ValuePlaceholder Placeholder
}

// SelectedColumn describes a column that was selected as part of a query. It's
// an output of query translating, and is also used for response construction.
type SelectedColumn struct {
	// Name is the original value of the selected column, including functions. It
	// does not include the alias. e.g. "region" or "count(*)"
	Name   string
	IsFunc bool
	// IsAs is true if an alias is used
	IsAs      bool
	FuncName  string
	Alias     string
	MapKey    Placeholder
	ListIndex Placeholder
	// ColumnName is the name of the underlying column in a function, or map key
	// access. e.g. the column name of "max(price)" is "price"
	ColumnName        string
	KeyType           string
	IsWriteTimeColumn bool
	ResultType        types.CqlDataType
}
