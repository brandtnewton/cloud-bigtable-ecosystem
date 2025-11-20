package types

type TruncateTableStatementMap struct {
	keyspace Keyspace
	table    TableName
}

func NewTruncateTableStatementMap(keyspace Keyspace, table TableName) *TruncateTableStatementMap {
	return &TruncateTableStatementMap{keyspace: keyspace, table: table}
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
	KeyType string
}
