package types

type CreateTableStatementMap struct {
	keyspace          Keyspace
	table             TableName
	IfNotExists       bool
	Columns           []CreateColumn
	PrimaryKeys       []CreateTablePrimaryKeyConfig
	IntRowKeyEncoding IntRowKeyEncodingType
}

func NewCreateTableStatementMap(keyspace Keyspace, table TableName, ifNotExists bool, columns []CreateColumn, primaryKeys []CreateTablePrimaryKeyConfig, intRowKeyEncoding IntRowKeyEncodingType) *CreateTableStatementMap {
	return &CreateTableStatementMap{keyspace: keyspace, table: table, IfNotExists: ifNotExists, Columns: columns, PrimaryKeys: primaryKeys, IntRowKeyEncoding: intRowKeyEncoding}
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
