/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package translator

import (
	"cloud.google.com/go/bigtable"
	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"go.uber.org/zap"
)

type Translator struct {
	Logger              *zap.Logger
	SchemaMappingConfig *schemaMapping.SchemaMappingConfig
	// determines the encoding for int row keys in all new tables
	DefaultIntRowKeyEncoding types.IntRowKeyEncodingType
}

// SelectQueryMap represents the mapping of a select query along with its translation details.
type SelectQueryMap struct {
	Query            string // Original query string
	TranslatedQuery  string
	QueryType        string                      // Type of the query (e.g., SELECT)
	Table            string                      // Table involved in the query
	Keyspace         string                      // Keyspace to which the table belongs
	ColumnMeta       ColumnMeta                  // Translator generated Metadata about the columns involved
	Clauses          []types.Condition           // List of clauses in the query
	Limit            Limit                       // Limit clause details
	OrderBy          OrderBy                     // Order by clause details
	GroupByColumns   []string                    // Group by Columns
	Params           map[string]interface{}      // Parameters for the query
	ParamKeys        []types.ColumnName          // column_name of the parameters
	Columns          []*types.Column             //all columns mentioned in query
	Conditions       map[string]string           // List of conditions in the query
	ReturnMetadata   []*message.ColumnMetadata   // Metadata of selected columns in Cassandra format
	VariableMetadata []*message.ColumnMetadata   // Metadata of variable columns for prepared queries in Cassandra format
	CachedBTPrepare  *bigtable.PreparedStatement // prepared statement object for bigtable
	ParamTypes       map[string]datatype.DataType
}

type OrderOperation string

const (
	Asc  OrderOperation = "asc"
	Desc OrderOperation = "desc"
)

type OrderBy struct {
	IsOrderBy bool
	Columns   []OrderByColumn
}

type OrderByColumn struct {
	Column    string
	Operation OrderOperation
}

type Limit struct {
	IsLimit bool
	Count   string
}

type ColumnMeta struct {
	Star   bool
	Column []types.SelectedColumn
}

type IfSpec struct {
	IfExists    bool
	IfNotExists bool
}

// This struct will be useful in combining all the clauses into one.
type WhereClause struct {
	Conditions []types.Condition
	Params     map[string]interface{}
	ParamKeys  []types.ColumnName
}

type TimestampInfo struct {
	Timestamp         bigtable.Timestamp
	HasUsingTimestamp bool
	Index             int32
}

type IncrementOperationType int

const (
	None IncrementOperationType = iota
	Increment
	Decrement
)

type ComplexOperation struct {
	Append           bool                   // this is for map/set/list
	PrependList      bool                   // this is for list
	Delete           bool                   // this is for map/set/list
	IncrementType    IncrementOperationType // for incrementing a counter
	IncrementValue   int64                  // how much to increment a counter by
	UpdateListIndex  string                 // this is for List index
	ExpectedDatatype datatype.DataType      // this datatype has to be provided in case of change in wantNewColumns datatype.
	mapKey           interface{}            // this key is for map key
	Value            []byte                 // this is value for setting at index for list
	ListDelete       bool                   // this is for list = list - {value1, value2}
	ListDeleteValues [][]byte               // this stores the values to be deleted from list
}

// PreparedInsertQuery represents the mapping of an insert query along with its translation details.
type PreparedInsertQuery struct {
	CqlQuery         string
	Keyspace         string
	Table            string
	Columns          []*types.Column
	ParamKeys        []types.ColumnName        // Column names of the parameters
	ReturnMetadata   []*message.ColumnMetadata // Metadata of all columns of that table in Cassandra format
	VariableMetadata []*message.ColumnMetadata // Metadata of variable columns for prepared queries in Cassandra format
	IfNotExists      bool
	// =====
	// == Bound values
	// =====
	RowKey        types.RowKey
	TimestampInfo TimestampInfo
	Data          []types.BigtableData
}

// DeleteQueryMapping represents the mapping of a delete query along with its translation details.
type DeleteQueryMapping struct {
	Query             string                    // Original query string
	Table             string                    // Table involved in the query
	Keyspace          string                    // Keyspace to which the table belongs
	Conditions        []types.Condition         // List of clauses in the delete query
	Params            map[string]interface{}    // Parameters for the query
	ParamKeys         []types.ColumnName        // Column names of the parameters
	RowKey            types.RowKey              // Unique rowkey which is required for delete operation
	ExecuteByMutation bool                      // Flag to indicate if the delete should be executed by mutation
	VariableMetadata  []*message.ColumnMetadata // Metadata of variable columns for prepared queries in Cassandra format
	ReturnMetadata    []*message.ColumnMetadata // Metadata of all columns of that table in Cassandra format
	TimestampInfo     TimestampInfo
	IfExists          bool
	SelectedColumns   []types.SelectedColumn
}

type CreateTableStatementMap struct {
	QueryType         string
	Keyspace          string
	Table             string
	IfNotExists       bool
	Columns           []types.CreateColumn
	PrimaryKeys       []CreateTablePrimaryKeyConfig
	IntRowKeyEncoding types.IntRowKeyEncodingType
}

type CreateTablePrimaryKeyConfig struct {
	Name    types.ColumnName
	KeyType string
}

type AlterTableStatementMap struct {
	QueryType   string
	Keyspace    string
	Table       string
	IfNotExists bool
	AddColumns  []types.CreateColumn
	DropColumns []string
}

type DropTableStatementMap struct {
	QueryType string
	Keyspace  string
	Table     string
	IfExists  bool
}

type TruncateTableStatementMap struct {
	QueryType string
	Keyspace  string
	Table     string
}

// UpdateQueryMapping represents the mapping of an update query along with its translation details.
type UpdateQueryMapping struct {
	Query                 string // Original query string
	TranslatedQuery       string
	Table                 string                    // Table involved in the query
	Keyspace              string                    // Keyspace to which the table belongs
	UpdateSetValues       []UpdateSetValue          // Columns to be updated
	Clauses               []types.Condition         // List of clauses in the update query
	Params                map[string]interface{}    // Parameters for the query
	PrimaryKeys           []string                  // Primary keys of the table
	Columns               []*types.Column           // Cassandra columns updated
	Data                  []types.BigtableData      // values - only defined for adhoc queries
	RowKey                types.RowKey              // Unique rowkey which is required for update operation
	DeleteColumnFamilies  []types.ColumnFamily      // List of all collection type of columns
	DeleteColumQualifiers []*types.Column           // List of all map key deletion in complex update
	ReturnMetadata        []*message.ColumnMetadata // Metadata of all columns of that table in Cassandra format
	VariableMetadata      []*message.ColumnMetadata // Metadata of variable columns for prepared queries in Cassandra format
	TimestampInfo         TimestampInfo
	IfExists              bool
	ComplexOperations     map[types.ColumnFamily]*ComplexOperation
}

type UpdateSetValue struct {
	Column       types.ColumnName
	ColumnFamily types.ColumnFamily
	CQLType      types.CqlDataType
	Value        string
	GoValue      types.GoValue
	//
	ComplexAssignment ComplexAssignment
	BigtableValue     types.BigtableValue
}

type UpdateSetResponse struct {
	UpdateSetValues []UpdateSetValue
	Params          map[string]interface{}
}

type TableObj struct {
	TableName    string
	KeyspaceName string
}

// AdHocQueryValues holds the results from parseComplexOperations.
type AdHocQueryValues struct {
	Data            []*types.BigtableData
	DelColumnFamily []types.ColumnFamily
	DelColumns      []*types.BigtableColumn
	ComplexOps      map[types.ColumnFamily]*ComplexOperation
}

// PreparedValues holds the results from decodePreparedValues.
type PreparedValues struct {
	Data            []types.BigtableData
	GoValues        map[types.ColumnName]types.GoValue
	IndexEnd        int
	DelColumnFamily []types.ColumnFamily
	DelColumns      []*types.BigtableColumng
	ComplexMeta     map[types.ColumnFamily]*ComplexOperation
}

type DescribeStatementMap struct {
	Keyspace string
	Table    string
}
