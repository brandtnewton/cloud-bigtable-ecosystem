package types

type TruncateTableStatementMap struct {
	cqlQuery string
	keyspace Keyspace
	table    TableName
}

func (c TruncateTableStatementMap) CqlQuery() string {
	return c.cqlQuery
}

func (c TruncateTableStatementMap) BigtableQuery() string {
	return ""
}

func NewTruncateTableStatementMap(keyspace Keyspace, table TableName, cqlQuery string) *TruncateTableStatementMap {
	return &TruncateTableStatementMap{
		keyspace: keyspace,
		table:    table,
		cqlQuery: cqlQuery,
	}
}

func (c TruncateTableStatementMap) AsBulkMutation() (IBigtableMutation, bool) {
	return nil, false
}

func (c TruncateTableStatementMap) Keyspace() Keyspace {
	return c.keyspace
}

func (c TruncateTableStatementMap) Table() TableName {
	return c.table
}

func (c TruncateTableStatementMap) QueryType() QueryType {
	return QueryTypeTruncate
}

type CreateTablePrimaryKeyConfig struct {
	Name    ColumnName
	KeyType KeyType
}
