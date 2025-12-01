package types

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

type IBigtableMutation interface {
	Keyspace() Keyspace
	Table() TableName
	RowKey() RowKey
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
	IN               Operator = "IN"
	ARRAY_INCLUDES   Operator = "ARRAY_INCLUDES"
	MAP_CONTAINS_KEY Operator = "MAP_CONTAINS_KEY"
	CONTAINS_KEY     Operator = "CONTAINS KEY"
	CONTAINS         Operator = "CONTAINS"
)

type ArithmeticOperator string

const (
	PLUS  ArithmeticOperator = "+"
	MINUS ArithmeticOperator = "-"
)

type Condition struct {
	Column   *Column
	Operator Operator
	// points to a placeholder
	Value DynamicValue
	// used for BETWEEN operator
	Value2 DynamicValue
}

type Assignment interface {
	Column() *Column
	IsIdempotentAssignment() bool
	Value() DynamicValue
}

type ComplexAssignmentAppend struct {
	column    *Column
	Operator  ArithmeticOperator
	Value     DynamicValue
	IsPrepend bool
}

func (c ComplexAssignmentAppend) IsIdempotentAssignment() bool {
	return false
}

func NewComplexAssignmentAppend(column *Column, op ArithmeticOperator, value DynamicValue, isPrepend bool) Assignment {
	return &ComplexAssignmentAppend{column: column, Operator: op, Value: value, IsPrepend: isPrepend}
}

func (c ComplexAssignmentAppend) Column() *Column {
	return c.column
}

type AssignmentCounterIncrement struct {
	column *Column
	Op     ArithmeticOperator
	Value  DynamicValue
}

func (c AssignmentCounterIncrement) IsIdempotentAssignment() bool {
	return false
}

func NewAssignmentCounterIncrement(column *Column, op ArithmeticOperator, value DynamicValue) Assignment {
	return &AssignmentCounterIncrement{column: column, Op: op, Value: value}
}

func (c AssignmentCounterIncrement) Column() *Column {
	return c.column
}

type ComplexAssignmentUpdateListIndex struct {
	column *Column
	// cassandra requires a literal, so no need to handle a parameter for the index
	Index int64
	Value DynamicValue
}

func (c ComplexAssignmentUpdateListIndex) IsIdempotentAssignment() bool {
	return true
}

func NewComplexAssignmentUpdateListIndex(column *Column, index int64, value DynamicValue) Assignment {
	return &ComplexAssignmentUpdateListIndex{column: column, Index: index, Value: value}
}

func (c ComplexAssignmentUpdateListIndex) Column() *Column {
	return c.column
}

type ComplexAssignmentUpdateMapValue struct {
	column *Column
	// cassandra requires a literal, so no need to handle a parameter for the key
	Key   GoValue
	Value DynamicValue
}

func (c ComplexAssignmentUpdateMapValue) IsIdempotentAssignment() bool {
	return true
}

func NewComplexAssignmentUpdateMapValue(column *Column, index GoValue, value DynamicValue) Assignment {
	return &ComplexAssignmentUpdateMapValue{column: column, Key: index, Value: value}
}

func (c ComplexAssignmentUpdateMapValue) Column() *Column {
	return c.column
}

type ComplexAssignmentSet struct {
	column *Column
	value  DynamicValue
}

func (c ComplexAssignmentSet) Value() DynamicValue {
	return c.value
}

func (c ComplexAssignmentSet) IsIdempotentAssignment() bool {
	return true
}

func NewComplexAssignmentSet(column *Column, value DynamicValue) Assignment {
	return &ComplexAssignmentSet{column: column, value: value}
}

func (c ComplexAssignmentSet) Column() *Column {
	return c.column
}
