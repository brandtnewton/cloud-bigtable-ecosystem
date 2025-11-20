package types

import (
	"cloud.google.com/go/bigtable"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type SelectClause struct {
	IsStar  bool
	Columns []SelectedColumn
}

type BtqlFuncCode int

const (
	FuncCodeUnknown BtqlFuncCode = iota
	FuncCodeWriteTime
	FuncCodeCount
	FuncCodeAvg
	FuncCodeSum
	FuncCodeMin
	FuncCodeMax
)

func (c BtqlFuncCode) String() string {
	switch c {
	case FuncCodeWriteTime:
		return "writetime"
	case FuncCodeCount:
		return "count"
	case FuncCodeAvg:
		return "avg"
	case FuncCodeSum:
		return "sum"
	case FuncCodeMin:
		return "min"
	case FuncCodeMax:
		return "max"
	default:
		return "unknown"
	}
}

// SelectedColumn describes a column that was selected as part of a query. It's
// an output of query translating, and is also used for response construction.
type SelectedColumn struct {
	// Sql is the original value of the selected column, including functions, not including alias. e.g. "region" or "count(*)"
	Sql       string
	Func      BtqlFuncCode
	Alias     string
	MapKey    Placeholder
	ListIndex Placeholder
	// ColumnName is the name of the underlying column in a function, or map key
	// access. e.g. the column name of "max(price)" is "price"
	ColumnName ColumnName
	ResultType CqlDataType
}

// PreparedSelectQuery represents the mapping of a select query along with its translation details.
type PreparedSelectQuery struct {
	keyspace             Keyspace      // Keyspace to which the table belongs
	table                TableName     // Table involved in the query
	cqlQuery             string        // Original query string
	TranslatedQuery      string        // btql
	SelectClause         *SelectClause // Translator generated Metadata about the columns involved
	Conditions           []Condition
	Params               *QueryParameters            // Parameters for the query
	CachedBTPrepare      *bigtable.PreparedStatement // prepared statement object for bigtable
	OrderBy              OrderBy                     // Order by clause details
	GroupByColumns       []string                    // Group by Columns - could be a column name or a column index
	ResultColumnMetadata []*message.ColumnMetadata
}

func (p PreparedSelectQuery) IsIdempotent() bool {
	return true
}

func NewPreparedSelectQuery(keyspace Keyspace, table TableName, cqlQuery string, translatedQuery string, selectClause *SelectClause, conditions []Condition, params *QueryParameters, orderBy OrderBy, groupByColumns []string, resultColumnMetadata []*message.ColumnMetadata) *PreparedSelectQuery {
	return &PreparedSelectQuery{keyspace: keyspace, table: table, cqlQuery: cqlQuery, TranslatedQuery: translatedQuery, SelectClause: selectClause, Conditions: conditions, Params: params, OrderBy: orderBy, GroupByColumns: groupByColumns, ResultColumnMetadata: resultColumnMetadata}
}

func (p PreparedSelectQuery) Parameters() *QueryParameters {
	return p.Params
}

func (p PreparedSelectQuery) ResponseColumns() []*message.ColumnMetadata {
	return p.ResultColumnMetadata
}

func (p PreparedSelectQuery) SetBigtablePreparedQuery(s *bigtable.PreparedStatement) {
	p.CachedBTPrepare = s
}

func (p PreparedSelectQuery) BigtableQuery() string {
	return p.TranslatedQuery
}

func (p PreparedSelectQuery) QueryType() QueryType {
	return QueryTypeSelect
}

func (p PreparedSelectQuery) Keyspace() Keyspace {
	return p.keyspace
}

func (p PreparedSelectQuery) Table() TableName {
	return p.table
}

func (p PreparedSelectQuery) CqlQuery() string {
	return p.cqlQuery
}

type BoundSelectQuery struct {
	Query           *PreparedSelectQuery
	ProtocolVersion primitive.ProtocolVersion
	Values          *QueryParameterValues
}

func (b BoundSelectQuery) CqlQuery() string {
	return b.Query.cqlQuery
}

func (b BoundSelectQuery) BigtableQuery() string {
	return b.Query.TranslatedQuery
}

func (b BoundSelectQuery) AsBulkMutation() (IBigtableMutation, bool) {
	return nil, false
}

func (b BoundSelectQuery) QueryType() QueryType {
	return QueryTypeSelect
}

func (b BoundSelectQuery) Keyspace() Keyspace {
	return b.Query.Keyspace()
}

func (b BoundSelectQuery) Table() TableName {
	return b.Query.Table()
}
