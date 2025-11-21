package types

import (
	"cloud.google.com/go/bigtable"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

// PreparedInsertQuery represents the mapping of an insert query along with its translation details.
type PreparedInsertQuery struct {
	keyspace    Keyspace
	table       TableName
	IfNotExists bool
	cqlQuery    string
	params      *QueryParameters
	Assignments []Assignment
}

func (p *PreparedInsertQuery) IsIdempotent() bool {
	return p.IfNotExists
}

func (p *PreparedInsertQuery) QueryType() QueryType {
	return QueryTypeInsert
}

func (p *PreparedInsertQuery) Parameters() *QueryParameters {
	return p.params
}

func (p *PreparedInsertQuery) ResponseColumns() []*message.ColumnMetadata {
	return nil
}

func (p *PreparedInsertQuery) SetBigtablePreparedQuery(s *bigtable.PreparedStatement) {
}

func (p *PreparedInsertQuery) BigtableQuery() string {
	return ""
}

func NewPreparedInsertQuery(keyspace Keyspace, table TableName, ifNotExists bool, cqlQuery string, params *QueryParameters, assignments []Assignment) *PreparedInsertQuery {
	return &PreparedInsertQuery{keyspace: keyspace, table: table, IfNotExists: ifNotExists, cqlQuery: cqlQuery, params: params, Assignments: assignments}
}

func (p *PreparedInsertQuery) Keyspace() Keyspace {
	return p.keyspace
}

func (p *PreparedInsertQuery) Table() TableName {
	return p.table
}

func (p *PreparedInsertQuery) CqlQuery() string {
	return p.cqlQuery
}
