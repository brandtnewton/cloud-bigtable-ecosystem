package types

type AlterTableStatementMap struct {
	keyspace    Keyspace
	table       TableName
	IfNotExists bool
	AddColumns  []CreateColumn
	DropColumns []ColumnName
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
