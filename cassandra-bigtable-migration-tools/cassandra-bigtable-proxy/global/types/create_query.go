package types

type CreateTableStatementMap struct {
	cqlQuery          string
	keyspace          Keyspace
	table             TableName
	IfNotExists       bool
	Columns           []CreateColumn
	PrimaryKeys       []CreateTablePrimaryKeyConfig
	IntRowKeyEncoding IntRowKeyEncodingType
}

func (c CreateTableStatementMap) CqlQuery() string {
	return c.cqlQuery
}

func (c CreateTableStatementMap) BigtableQuery() string {
	return ""
}

func NewCreateTableStatementMap(keyspace Keyspace, table TableName, cqlQuery string, ifNotExists bool, columns []CreateColumn, primaryKeys []CreateTablePrimaryKeyConfig, intRowKeyEncoding IntRowKeyEncodingType) *CreateTableStatementMap {
	return &CreateTableStatementMap{
		keyspace:          keyspace,
		table:             table,
		cqlQuery:          cqlQuery,
		IfNotExists:       ifNotExists,
		Columns:           columns,
		PrimaryKeys:       primaryKeys,
		IntRowKeyEncoding: intRowKeyEncoding,
	}
}

func (c CreateTableStatementMap) AsBulkMutation() (IBigtableMutation, bool) {
	return nil, false
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
