package types

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/constants"
	"time"
)

type OrderOperation string

const (
	Asc  OrderOperation = "asc"
	Desc OrderOperation = "desc"
)

type AssignmentOperation string

const (
	AssignAdd    AssignmentOperation = "+"
	AssignRemove AssignmentOperation = "-"
	AssignIndex  AssignmentOperation = "update_index"
)

type OrderBy struct {
	IsOrderBy bool
	Columns   []OrderByColumn
}

type OrderByColumn struct {
	Column    string
	Operation OrderOperation
}

type IfSpec struct {
	IfExists    bool
	IfNotExists bool
}

type UpdateSetResponse struct {
	Assignments []Assignment
}

type BoundSelectColumn interface {
	Column() *Column
}

type BoundIndexColumn struct {
	column *Column
	Index  int
}

func NewBoundIndexColumn(column *Column, index int) *BoundIndexColumn {
	return &BoundIndexColumn{column: column, Index: index}
}

func (b *BoundIndexColumn) Column() *Column {
	return b.column
}

type BoundKeyColumn struct {
	column *Column
	Key    ColumnQualifier
}

func NewBoundKeyColumn(column *Column, key ColumnQualifier) *BoundKeyColumn {
	return &BoundKeyColumn{column: column, Key: key}
}

func (b *BoundKeyColumn) Column() *Column {
	return b.column
}

type BoundTimestampInfo struct {
	Timestamp         time.Time
	HasUsingTimestamp bool
}

type IBigtableMutation interface {
	Keyspace() Keyspace
	Table() TableName
	RowKey() RowKey
}

// BigtableWriteMutation holds the results from parseComplexOperations.
type BigtableWriteMutation struct {
	keyspace              Keyspace
	queryType             QueryType
	table                 TableName
	rowKey                RowKey
	IfSpec                IfSpec
	UsingTimestamp        *BoundTimestampInfo
	Data                  []*BigtableData
	DelColumnFamily       []ColumnFamily
	DelColumns            []*BigtableColumn
	CounterOps            []BigtableCounterOp
	SetIndexOps           []BigtableSetIndexOp
	DeleteListElementsOps []BigtableDeleteListElementsOp
}

func NewBigtableWriteMutation(keyspace Keyspace, table TableName, qt QueryType, rowKey RowKey) *BigtableWriteMutation {
	return &BigtableWriteMutation{keyspace: keyspace, table: table, queryType: qt, rowKey: rowKey}
}

func (b BigtableWriteMutation) QueryType() QueryType {
	return b.queryType
}

func (b BigtableWriteMutation) Keyspace() Keyspace {
	return b.keyspace
}

func (b BigtableWriteMutation) Table() TableName {
	return b.table
}

func (b BigtableWriteMutation) RowKey() RowKey {
	return b.rowKey
}

type BigtableCounterOp struct {
	Family ColumnFamily
	Value  int64
}
type BigtableSetIndexOp struct {
	Family ColumnFamily
	Index  int
	Value  BigtableValue
}
type BigtableDeleteListElementsOp struct {
	Family ColumnFamily
	Values []BigtableValue
}

type Condition struct {
	Column   *Column
	Operator constants.Operator
	// points to a placeholder
	ValuePlaceholder Placeholder
}

type Assignment interface {
	Column() *Column
}

type ComplexAssignmentAdd struct {
	column    *Column
	IsPrepend bool
	Value     Placeholder
}

func NewComplexAssignmentAdd(column *Column, isPrepend bool, value Placeholder) *ComplexAssignmentAdd {
	return &ComplexAssignmentAdd{column: column, IsPrepend: isPrepend, Value: value}
}

func (c ComplexAssignmentAdd) Column() *Column {
	return c.column
}

type IncrementOperationType int

const (
	None IncrementOperationType = iota
	Increment
	Decrement
)

type AssignmentCounterIncrement struct {
	column *Column
	Op     IncrementOperationType
	Value  Placeholder
}

func NewAssignmentCounterIncrement(column *Column, op IncrementOperationType, value Placeholder) *AssignmentCounterIncrement {
	return &AssignmentCounterIncrement{column: column, Op: op, Value: value}
}

func (c AssignmentCounterIncrement) Column() *Column {
	return c.column
}

type ComplexAssignmentRemove struct {
	column *Column
	Value  Placeholder
}

func NewComplexAssignmentRemove(column *Column, value Placeholder) *ComplexAssignmentRemove {
	return &ComplexAssignmentRemove{column: column, Value: value}
}

func (c ComplexAssignmentRemove) Column() *Column {
	return c.column
}

type ComplexAssignmentUpdateIndex struct {
	column *Column
	Index  int64
	Value  Placeholder
}

func NewComplexAssignmentUpdateIndex(column *Column, index int64, value Placeholder) *ComplexAssignmentUpdateIndex {
	return &ComplexAssignmentUpdateIndex{column: column, Index: index, Value: value}
}

func (c ComplexAssignmentUpdateIndex) Column() *Column {
	return c.column
}

type ComplexAssignmentSet struct {
	column *Column
	Value  Placeholder
}

func NewComplexAssignmentSet(column *Column, value Placeholder) *ComplexAssignmentSet {
	return &ComplexAssignmentSet{column: column, Value: value}
}

func (c ComplexAssignmentSet) Column() *Column {
	return c.column
}

// PreparedDeleteQuery represents the mapping of a deleted query along with its translation details.
type PreparedDeleteQuery struct {
	keyspace        Keyspace    // Keyspace to which the table belongs
	table           TableName   // Table involved in the query
	cqlQuery        string      // Original query string
	Conditions      []Condition // List of clauses in the delete query
	Params          *QueryParameters
	IfExists        bool
	SelectedColumns []SelectedColumn
}

func (p PreparedDeleteQuery) Keyspace() Keyspace {
	return p.keyspace
}

func (p PreparedDeleteQuery) Table() TableName {
	return p.table
}

func (p PreparedDeleteQuery) CqlQuery() string {
	return p.cqlQuery
}

type BoundDeleteQuery struct {
	keyspace Keyspace
	table    TableName
	rowKey   RowKey
	IfExists bool
	Columns  []BoundSelectColumn
}

func (b BoundDeleteQuery) QueryType() QueryType {
	return QueryTypeDelete
}

func (b BoundDeleteQuery) Keyspace() Keyspace {
	return b.keyspace
}

func (b BoundDeleteQuery) Table() TableName {
	return b.table
}

func (b BoundDeleteQuery) RowKey() RowKey {
	return b.rowKey
}
