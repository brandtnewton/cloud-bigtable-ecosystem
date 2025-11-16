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
	Query           string     // Original query string
	TranslatedQuery string     // btql
	Table           string     // Table involved in the query
	Keyspace        string     // Keyspace to which the table belongs
	ColumnMeta      ColumnMeta // Translator generated Metadata about the columns involved
	SelectedColumns []types.SelectedColumn
	Clauses         []types.Condition // List of clauses in the query

	Params           *types.QueryParameters      // Parameters for the query
	Columns          []*types.Column             //all columns mentioned in query
	Conditions       map[string]string           // List of conditions in the query
	ReturnMetadata   []*message.ColumnMetadata   // Metadata of selected columns in Cassandra format
	VariableMetadata []*message.ColumnMetadata   // Metadata of variable columns for prepared queries in Cassandra format
	CachedBTPrepare  *bigtable.PreparedStatement // prepared statement object for bigtable
	OrderBy          OrderBy                     // Order by clause details
	GroupByColumns   []string                    // Group by Columns - could be a column name or a column index
	Limit            Limit                       // Limit clause details
}

type SelectQueryAndData struct {
	Query  *SelectQueryMap
	Values *types.QueryParameterValues
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

type IncrementOperationType int

const (
	None IncrementOperationType = iota
	Increment
	Decrement
)

// PreparedInsertQuery represents the mapping of an insert query along with its translation details.
type PreparedInsertQuery struct {
	CqlQuery         string
	Keyspace         string
	Table            string
	Params           *types.QueryParameters
	Assignments      []Assignment
	ReturnMetadata   []*message.ColumnMetadata // Metadata of all columns of that table in Cassandra format
	VariableMetadata []*message.ColumnMetadata // Metadata of variable columns for prepared queries in Cassandra format
	IfNotExists      bool
}

// DeleteQueryMapping represents the mapping of a delete query along with its translation details.
type DeleteQueryMapping struct {
	Query             string            // Original query string
	Table             string            // Table involved in the query
	Keyspace          string            // Keyspace to which the table belongs
	Conditions        []types.Condition // List of clauses in the delete query
	Params            *types.QueryParameters
	ExecuteByMutation bool                      // Flag to indicate if the delete should be executed by mutation
	VariableMetadata  []*message.ColumnMetadata // Metadata of variable columns for prepared queries in Cassandra format
	ReturnMetadata    []*message.ColumnMetadata // Metadata of all columns of that table in Cassandra format
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
	Query                 string                    // Original query string
	Table                 string                    // Table involved in the query
	Keyspace              string                    // Keyspace to which the table belongs
	UpdateSetValues       []Assignment              // Columns to be updated
	Clauses               []types.Condition         // List of clauses in the update query
	Params                *types.QueryParameters    // Parameters for the query
	Columns               []*types.Column           // Cassandra columns updated
	RowKey                types.RowKey              // Unique rowkey which is required for update operation
	DeleteColumnFamilies  []types.ColumnFamily      // List of all collection type of columns
	DeleteColumQualifiers []*types.Column           // List of all map key deletion in complex update
	ReturnMetadata        []*message.ColumnMetadata // Metadata of all columns of that table in Cassandra format
	VariableMetadata      []*message.ColumnMetadata // Metadata of variable columns for prepared queries in Cassandra format
	IfExists              bool
}

type UpdateSetResponse struct {
	Assignments []Assignment
}

type TableObj struct {
	TableName    string
	KeyspaceName string
}

type DescribeStatementMap struct {
	Keyspace string
	Table    string
}
