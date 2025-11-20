package types

type AlterTableStatementMap struct {
	cqlQuery    string
	keyspace    Keyspace
	table       TableName
	IfNotExists bool
	AddColumns  []CreateColumn
	DropColumns []ColumnName
}

func (a AlterTableStatementMap) CqlQuery() string {
	return a.cqlQuery
}

func (a AlterTableStatementMap) BigtableQuery() string {
	return ""
}

func NewAlterTableStatementMap(keyspace Keyspace, table TableName, cqlQuery string, ifNotExists bool, addColumns []CreateColumn, dropColumns []ColumnName) *AlterTableStatementMap {
	return &AlterTableStatementMap{
		keyspace:    keyspace,
		table:       table,
		cqlQuery:    cqlQuery,
		IfNotExists: ifNotExists,
		AddColumns:  addColumns,
		DropColumns: dropColumns,
	}
}

func (a AlterTableStatementMap) AsBulkMutation() (IBigtableMutation, bool) {
	return nil, false
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
