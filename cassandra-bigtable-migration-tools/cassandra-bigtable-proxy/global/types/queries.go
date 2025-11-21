package types

import (
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
	cqlQuery              string
	queryType             QueryType
	keyspace              Keyspace
	table                 TableName
	IfSpec                IfSpec
	rowKey                RowKey
	UsingTimestamp        *BoundTimestampInfo
	Data                  []*BigtableData
	DelColumnFamily       []ColumnFamily
	DelColumns            []*BigtableColumn
	CounterOps            []BigtableCounterOp
	SetIndexOps           []BigtableSetIndexOp
	DeleteListElementsOps []BigtableDeleteListElementsOp
}

func NewBigtableWriteMutation(keyspace Keyspace, table TableName, cqlQuery string, ifSpec IfSpec, qt QueryType, rowKey RowKey) *BigtableWriteMutation {
	return &BigtableWriteMutation{keyspace: keyspace, table: table, cqlQuery: cqlQuery, IfSpec: ifSpec, queryType: qt, rowKey: rowKey}
}

func (b BigtableWriteMutation) CqlQuery() string {
	return b.cqlQuery
}

func (b BigtableWriteMutation) BigtableQuery() string {
	return ""
}

func (b BigtableWriteMutation) AsBulkMutation() (IBigtableMutation, bool) {
	return b, true
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

type Operator string

// TODO: we need to move all the constants here.
// Just to keep the code clean, we have defined the constants here.
// It will help in reducing the number of magic strings in the code also to avoid the circular dependency.
const (
	EQ               Operator = "="
	GT               Operator = ">"
	LT               Operator = "<"
	GTE              Operator = ">="
	LTE              Operator = "<="
	BETWEEN          Operator = "BETWEEN"
	LIKE             Operator = "LIKE"
	BETWEEN_AND      Operator = "BETWEEN-AND"
	IN               Operator = "IN"
	ARRAY_INCLUDES   Operator = "ARRAY_INCLUDES"
	MAP_CONTAINS_KEY Operator = "MAP_CONTAINS_KEY"
	CONTAINS_KEY     Operator = "CONTAINS KEY"
	CONTAINS         Operator = "CONTAINS"
)

type Condition struct {
	Column   *Column
	Operator Operator
	// points to a placeholder
	ValuePlaceholder Placeholder
}

type Assignment interface {
	Column() *Column
	IsIdempotentAssignment() bool
}

type ComplexAssignmentAdd struct {
	column    *Column
	IsPrepend bool
	Value     Placeholder
}

func (c ComplexAssignmentAdd) IsIdempotentAssignment() bool {
	return false
}

func NewComplexAssignmentAdd(column *Column, isPrepend bool, value Placeholder) Assignment {
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

func (c AssignmentCounterIncrement) IsIdempotentAssignment() bool {
	return false
}

func NewAssignmentCounterIncrement(column *Column, op IncrementOperationType, value Placeholder) Assignment {
	return &AssignmentCounterIncrement{column: column, Op: op, Value: value}
}

func (c AssignmentCounterIncrement) Column() *Column {
	return c.column
}

type ComplexAssignmentRemove struct {
	column *Column
	Value  Placeholder
}

func (c ComplexAssignmentRemove) IsIdempotentAssignment() bool {
	return false
}

func NewComplexAssignmentRemove(column *Column, value Placeholder) Assignment {
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

func (c ComplexAssignmentUpdateIndex) IsIdempotentAssignment() bool {
	return true
}

func NewComplexAssignmentUpdateIndex(column *Column, index int64, value Placeholder) Assignment {
	return &ComplexAssignmentUpdateIndex{column: column, Index: index, Value: value}
}

func (c ComplexAssignmentUpdateIndex) Column() *Column {
	return c.column
}

type ComplexAssignmentSet struct {
	column *Column
	Value  Placeholder
}

func (c ComplexAssignmentSet) IsIdempotentAssignment() bool {
	return true
}

func NewComplexAssignmentSet(column *Column, value Placeholder) Assignment {
	return &ComplexAssignmentSet{column: column, Value: value}
}

func (c ComplexAssignmentSet) Column() *Column {
	return c.column
}
