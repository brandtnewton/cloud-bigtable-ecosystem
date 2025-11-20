package types

type AlterTableStatementMap struct {
	keyspace    Keyspace
	table       TableName
	IfNotExists bool
	AddColumns  []CreateColumn
	DropColumns []ColumnName
}

func NewAlterTableStatementMap(keyspace Keyspace, table TableName, ifNotExists bool, addColumns []CreateColumn, dropColumns []ColumnName) *AlterTableStatementMap {
	return &AlterTableStatementMap{keyspace: keyspace, table: table, IfNotExists: ifNotExists, AddColumns: addColumns, DropColumns: dropColumns}
}

func (a AlterTableStatementMap) Keyspace() Keyspace {
	return a.keyspace
}

func (a AlterTableStatementMap) Table() TableName {
	return a.table
}

func (a AlterTableStatementMap) QueryType() QueryType {
	return QueryTypeAlter
}
