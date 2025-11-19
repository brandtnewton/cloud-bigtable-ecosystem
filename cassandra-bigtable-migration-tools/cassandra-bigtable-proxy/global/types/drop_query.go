package types

type DropTableQuery struct {
	keyspace Keyspace
	table    TableName
	IfExists bool
}

func (c DropTableQuery) Keyspace() Keyspace {
	return c.keyspace
}

func (c DropTableQuery) Table() TableName {
	return c.table
}

func (c DropTableQuery) QueryType() QueryType {
	return QueryTypeDrop
}
