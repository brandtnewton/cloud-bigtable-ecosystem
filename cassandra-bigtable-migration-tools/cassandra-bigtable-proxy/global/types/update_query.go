package types

import (
	"cloud.google.com/go/bigtable"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

type PreparedUpdateQuery struct {
	keyspace     Keyspace         // Keyspace to which the table belongs
	table        TableName        // Table involved in the query
	cqlQuery     string           // Original query string
	Values       []Assignment     // Columns to be updated
	Clauses      []Condition      // List of clauses in the update query
	params       *QueryParameters // Parameters for the query
	IfExists     bool
	ExpectedType CqlDataType
}

func (p PreparedUpdateQuery) Parameters() *QueryParameters {
	return p.params
}

func (p PreparedUpdateQuery) ResponseColumns() []*message.ColumnMetadata {
	return nil
}

func (p PreparedUpdateQuery) SetBigtablePreparedQuery(_ *bigtable.PreparedStatement) {
	// nothing to store
}

func (p PreparedUpdateQuery) QueryType() QueryType {
	return QueryTypeUpdate
}

func (p PreparedUpdateQuery) Keyspace() Keyspace {
	return p.keyspace
}

func (p PreparedUpdateQuery) Table() TableName {
	return p.table
}

func (p PreparedUpdateQuery) CqlQuery() string {
	return p.cqlQuery
}
