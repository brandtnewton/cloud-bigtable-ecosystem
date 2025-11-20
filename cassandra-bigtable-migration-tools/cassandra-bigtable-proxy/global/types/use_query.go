package types

type UseTableStatementMap struct {
	cqlQuery string
	keyspace Keyspace
}

func (u UseTableStatementMap) Keyspace() Keyspace {
	return u.keyspace
}

func (u UseTableStatementMap) Table() TableName {
	return ""
}

func (u UseTableStatementMap) AsBulkMutation() (IBigtableMutation, bool) {
	return nil, false
}

func (u UseTableStatementMap) CqlQuery() string {
	return u.cqlQuery
}

func (u UseTableStatementMap) BigtableQuery() string {
	return ""
}

func (u UseTableStatementMap) QueryType() QueryType {
	return QueryTypeUse
}

func NewUseTableStatementMap(keyspace Keyspace, cqlQuery string) *UseTableStatementMap {
	return &UseTableStatementMap{
		keyspace: keyspace,
		cqlQuery: cqlQuery,
	}
}
