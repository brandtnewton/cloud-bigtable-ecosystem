package types

type Attributes struct {
	Method    string
	Err       error
	QueryType QueryType
	Keyspace  Keyspace
	Table     TableName
	Status    string
}

func (a *Attributes) SetQueryInfo(q IQuery) {
	a.Keyspace = q.Keyspace()
	a.Table = q.Table()
	a.QueryType = q.QueryType()
}
