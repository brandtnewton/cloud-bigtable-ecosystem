package types

import (
	"cloud.google.com/go/bigtable"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

// DescribeQuery represents a parsed DESCRIBE/DESC statement
// Only one of the fields will be set to true/non-empty depending on the type of DESCRIBE
//   - Keyspaces: true for DESCRIBE KEYSPACES
//   - Tables: true for DESCRIBE TABLES
//   - table: set for DESCRIBE TABLE <keyspace.table>
//   - keyspace: set for DESCRIBE KEYSPACE <keyspace> or DESCRIBE <keyspace>
type DescribeQuery struct {
	cqlQuery  string
	Keyspaces bool
	Tables    bool
	table     TableName
	keyspace  Keyspace
}

func NewDescribeKeyspacesQuery(cqlQuery string) *DescribeQuery {
	return &DescribeQuery{cqlQuery: cqlQuery, Keyspaces: true}
}
func NewDescribeTablesQuery(cqlQuery string, keyspace Keyspace) *DescribeQuery {
	return &DescribeQuery{cqlQuery: cqlQuery, Tables: true, keyspace: keyspace}
}

func NewDescribeTable(cqlQuery string, keyspace Keyspace, table TableName) *DescribeQuery {
	return &DescribeQuery{cqlQuery: cqlQuery, keyspace: keyspace, table: table}
}

func (a *DescribeQuery) CqlQuery() string {
	return a.cqlQuery
}

func (a *DescribeQuery) BigtableQuery() string {
	return ""
}

func (a *DescribeQuery) AsBulkMutation() (IBigtableMutation, bool) {
	return nil, false
}

func (a *DescribeQuery) Keyspace() Keyspace {
	return a.keyspace
}

func (a *DescribeQuery) Table() TableName {
	return a.table
}

func (a *DescribeQuery) QueryType() QueryType {
	return QueryTypeDescribe
}

func (a *DescribeQuery) Parameters() *QueryParameters {
	return nil
}

func (a *DescribeQuery) ResponseColumns() []*message.ColumnMetadata {
	return nil
}

func (a *DescribeQuery) SetBigtablePreparedQuery(s *bigtable.PreparedStatement) {

}

func (a *DescribeQuery) IsIdempotent() bool {
	return true
}
