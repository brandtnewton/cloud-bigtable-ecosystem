package types

type CreateTableStatementMap struct {
	keyspace          Keyspace
	table             TableName
	IfNotExists       bool
	Columns           []CreateColumn
	PrimaryKeys       []CreateTablePrimaryKeyConfig
	IntRowKeyEncoding IntRowKeyEncodingType
}

func (c CreateTableStatementMap) Keyspace() Keyspace {
	return c.keyspace
}

func (c CreateTableStatementMap) Table() TableName {
	return c.table
}

func (c CreateTableStatementMap) QueryType() QueryType {
	return QueryTypeCreate
}
