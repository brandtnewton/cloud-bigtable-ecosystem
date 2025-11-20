package types

import (
	"cloud.google.com/go/bigtable"
	"github.com/datastax/go-cassandra-native-protocol/message"
)

// PreparedDeleteQuery represents the mapping of a deleted query along with its translation details.
type PreparedDeleteQuery struct {
	keyspace        Keyspace  // Keyspace to which the table belongs
	table           TableName // Table involved in the query
	IfExists        bool
	cqlQuery        string      // Original query string
	Conditions      []Condition // List of clauses in the delete query
	params          *QueryParameters
	SelectedColumns []SelectedColumn
}

func (p PreparedDeleteQuery) QueryType() QueryType {
	return QueryTypeDelete
}

func (p PreparedDeleteQuery) Parameters() *QueryParameters {
	return p.params
}

func (p PreparedDeleteQuery) ResponseColumns() []*message.ColumnMetadata {
	return nil
}

func (p PreparedDeleteQuery) SetBigtablePreparedQuery(s *bigtable.PreparedStatement) {
}

func (p PreparedDeleteQuery) BigtableQuery() string {
	return ""
}

func NewPreparedDeleteQuery(keyspace Keyspace, table TableName, ifExists bool, cqlQuery string, conditions []Condition, params *QueryParameters, selectedColumns []SelectedColumn) *PreparedDeleteQuery {
	return &PreparedDeleteQuery{keyspace: keyspace, table: table, cqlQuery: cqlQuery, Conditions: conditions, params: params, IfExists: ifExists, SelectedColumns: selectedColumns}
}

func (p PreparedDeleteQuery) Keyspace() Keyspace {
	return p.keyspace
}

func (p PreparedDeleteQuery) Table() TableName {
	return p.table
}

func (p PreparedDeleteQuery) CqlQuery() string {
	return p.cqlQuery
}

type BoundDeleteQuery struct {
	keyspace Keyspace
	table    TableName
	rowKey   RowKey
	IfExists bool
	Columns  []BoundSelectColumn
}

func NewBoundDeleteQuery(keyspace Keyspace, table TableName, rowKey RowKey, ifExists bool, columns []BoundSelectColumn) *BoundDeleteQuery {
	return &BoundDeleteQuery{keyspace: keyspace, table: table, rowKey: rowKey, IfExists: ifExists, Columns: columns}
}

func (b BoundDeleteQuery) AsBulkMutation() (IBigtableMutation, bool) {
	return b, true
}

func (b BoundDeleteQuery) QueryType() QueryType {
	return QueryTypeDelete
}

func (b BoundDeleteQuery) Keyspace() Keyspace {
	return b.keyspace
}

func (b BoundDeleteQuery) Table() TableName {
	return b.table
}

func (b BoundDeleteQuery) RowKey() RowKey {
	return b.rowKey
}
