package types

type DropTableQuery struct {
	cqlQuery string
	keyspace Keyspace
	table    TableName
	IfExists bool
}

func NewDropTableQuery(keyspace Keyspace, table TableName, ifExists bool) *DropTableQuery {
	return &DropTableQuery{keyspace: keyspace, table: table, IfExists: ifExists}
}

func (c DropTableQuery) CqlQuery() string {
	return c.cqlQuery
}

func (c DropTableQuery) BigtableQuery() string {
	return ""
}

func (c DropTableQuery) AsBulkMutation() (IBigtableMutation, bool) {
	return nil, false
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
