package types

import "time"

// BigtableWriteMutation holds the results from parseComplexOperations.
type BigtableWriteMutation struct {
	cqlQuery       string
	queryType      QueryType
	keyspace       Keyspace
	table          TableName
	IfSpec         IfSpec
	rowKey         RowKey
	UsingTimestamp *BoundTimestampInfo
	mutations      []IBigtableMutationOp
}

type IBigtableMutationOp interface {
	_x()
}

type WriteCellOp struct {
	Family ColumnFamily
	Column ColumnQualifier
	Bytes  BigtableValue
}

func (w WriteCellOp) _x() {}

func NewWriteCellOp(family ColumnFamily, column ColumnQualifier, bytes BigtableValue) IBigtableMutationOp {
	return &WriteCellOp{Family: family, Column: column, Bytes: bytes}
}

type DeleteCellsOp struct {
	Family ColumnFamily
}

func (d DeleteCellsOp) _x() {}

func NewDeleteCellsOp(family ColumnFamily) IBigtableMutationOp {
	return &DeleteCellsOp{Family: family}
}

type DeleteColumnOp struct {
	Column BigtableColumn
}

func (d *DeleteColumnOp) _x() {}

func NewDeleteColumnOp(column BigtableColumn) IBigtableMutationOp {
	return &DeleteColumnOp{Column: column}
}

type BigtableCounterOp struct {
	Family ColumnFamily
	Value  int64
}

func (b BigtableCounterOp) _x() {}

func NewBigtableCounterOp(family ColumnFamily, value int64) IBigtableMutationOp {
	return &BigtableCounterOp{Family: family, Value: value}
}

type BigtableSetIndexOp struct {
	Family ColumnFamily
	Index  int
	Value  BigtableValue
}

func (b BigtableSetIndexOp) _x() {}

func NewBigtableSetIndexOp(family ColumnFamily, index int, value BigtableValue) IBigtableMutationOp {
	return &BigtableSetIndexOp{Family: family, Index: index, Value: value}
}

type BigtableDeleteListElementsOp struct {
	Family ColumnFamily
	Values []BigtableValue
}

func (b BigtableDeleteListElementsOp) _x() {}

func NewBigtableDeleteListElementsOp(family ColumnFamily, values []BigtableValue) IBigtableMutationOp {
	return &BigtableDeleteListElementsOp{Family: family, Values: values}
}

type BoundTimestampInfo struct {
	Timestamp         time.Time
	HasUsingTimestamp bool
}

func NewBigtableWriteMutation(keyspace Keyspace, table TableName, cqlQuery string, ifSpec IfSpec, qt QueryType, rowKey RowKey) *BigtableWriteMutation {
	return &BigtableWriteMutation{keyspace: keyspace, table: table, cqlQuery: cqlQuery, IfSpec: ifSpec, queryType: qt, rowKey: rowKey}
}

func (b *BigtableWriteMutation) CqlQuery() string {
	return b.cqlQuery
}

func (b *BigtableWriteMutation) BigtableQuery() string {
	return ""
}

func (b *BigtableWriteMutation) AsBulkMutation() (IBigtableMutation, bool) {
	return b, true
}

func (b *BigtableWriteMutation) QueryType() QueryType {
	return b.queryType
}

func (b *BigtableWriteMutation) Keyspace() Keyspace {
	return b.keyspace
}

func (b *BigtableWriteMutation) Table() TableName {
	return b.table
}

func (b *BigtableWriteMutation) RowKey() RowKey {
	return b.rowKey
}

func (b *BigtableWriteMutation) Mutations() []IBigtableMutationOp {
	return b.mutations
}

func (b *BigtableWriteMutation) AddMutations(m ...IBigtableMutationOp) {
	b.mutations = append(b.mutations, m...)
}
