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

package proxy

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translator"
	"slices"
	"strings"
	"time"

	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/responsehandler"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/parser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxy/config"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type TimeTrackInfo struct {
	// bigtableStart and bigtableEnd represent time taken by whole function including data encoding to cassandra
	start, bigtableStart, bigtableEnd time.Time
	ResponseProcessingTime            time.Duration
}

type SystemQueryMetadataCache struct {
	KeyspaceSystemQueryMetadataCache map[primitive.ProtocolVersion][]message.Row
	TableSystemQueryMetadataCache    map[primitive.ProtocolVersion][]message.Row
	ColumnsSystemQueryMetadataCache  map[primitive.ProtocolVersion][]message.Row
}

// addSecondsToCurrentTimestamp takes a number of seconds as input
// and returns the current Unix timestamp plus the input time in seconds.
func addSecondsToCurrentTimestamp(seconds int64) string {
	// Get the current time
	currentTime := time.Now()

	// Add the input seconds to the current time
	futureTime := currentTime.Add(time.Second * time.Duration(seconds))

	// Return the future time as a Unix timestamp (in seconds)
	return unixToISO(futureTime.Unix())
}

// unixToISO converts a Unix timestamp (in seconds) to an ISO 8601 formatted string.
func unixToISO(unixTimestamp int64) string {
	// Convert the Unix timestamp to a time.timestamp object
	t := time.Unix(unixTimestamp, 0).UTC()

	// Format the time as an ISO 8601 string
	return t.Format(time.RFC3339)
}

// GetSystemQueryMetadataCache converts structured metadata rows into a SystemQueryMetadataCache.
// It encodes sessionKeyspace, table, and column metadata into a format compatible with Cassandra system queries.
//
// Parameters:
// - keyspaceMetadataRows: Slice of sessionKeyspace metadata inany format.
// - tableMetadataRows: Slice of table metadata inany format.
// - columnsMetadataRows: Slice of column metadata inany format.
//
// Returns:
// - *SystemQueryMetadataCache: A pointer to a structured metadata cache containing keyspaces, tables, and metaDataColumns.
// - error: Returns an error if metadata conversion fails at any step.
func getSystemQueryMetadataCache(keyspaceMetadataRows, tableMetadataRows, columnsMetadataRows [][]any) (*SystemQueryMetadataCache, error) {
	var err error
	protocolIV := primitive.ProtocolVersion4

	systemQueryMetadataCache := &SystemQueryMetadataCache{
		KeyspaceSystemQueryMetadataCache: make(map[primitive.ProtocolVersion][]message.Row),
		TableSystemQueryMetadataCache:    make(map[primitive.ProtocolVersion][]message.Row),
		ColumnsSystemQueryMetadataCache:  make(map[primitive.ProtocolVersion][]message.Row),
	}

	systemQueryMetadataCache.KeyspaceSystemQueryMetadataCache[protocolIV], err = responsehandler.BuildResponseForSystemQueries(keyspaceMetadataRows, protocolIV)
	if err != nil {
		return systemQueryMetadataCache, err
	}
	systemQueryMetadataCache.TableSystemQueryMetadataCache[protocolIV], err = responsehandler.BuildResponseForSystemQueries(tableMetadataRows, protocolIV)
	if err != nil {
		return systemQueryMetadataCache, err
	}
	systemQueryMetadataCache.ColumnsSystemQueryMetadataCache[protocolIV], err = responsehandler.BuildResponseForSystemQueries(columnsMetadataRows, protocolIV)
	if err != nil {
		return systemQueryMetadataCache, err
	}

	return systemQueryMetadataCache, nil
}

// getKeyspaceMetadata converts table metadata into sessionKeyspace metadata rows
func getKeyspaceMetadata(tableMetadata map[types.Keyspace]map[types.TableName]*schemaMapping.TableConfig) [][]any {
	// Replication settings for system and example keyspaces, matching Cassandra output
	replicationMap := map[types.Keyspace]map[string]string{
		"system":                {"class": "org.apache.cassandra.locator.LocalStrategy"},
		"system_schema":         {"class": "org.apache.cassandra.locator.LocalStrategy"},
		"system_auth":           {"class": "org.apache.cassandra.locator.SimpleStrategy", "replication_factor": "1"},
		"system_distributed":    {"class": "org.apache.cassandra.locator.SimpleStrategy", "replication_factor": "3"},
		"system_traces":         {"class": "org.apache.cassandra.locator.SimpleStrategy", "replication_factor": "2"},
		"system_views":          {"class": "org.apache.cassandra.locator.SimpleStrategy", "replication_factor": "1"},
		"system_virtual_schema": {"class": "org.apache.cassandra.locator.LocalStrategy"},
	}

	var keyspaceMetadataRows [][]any
	for keyspace := range tableMetadata {
		repl := map[string]string{"class": "org.apache.cassandra.locator.SimpleStrategy", "replication_factor": "1"}
		if val, ok := replicationMap[keyspace]; ok {
			repl = val
		}
		// Add sessionKeyspace metadata
		keyspaceMetadataRows = append(keyspaceMetadataRows, []any{
			string(keyspace), true, repl,
		})
	}
	return keyspaceMetadataRows
}

// getTableMetadata converts table metadata into table metadata rows
func getTableMetadata(tableMetadata map[types.Keyspace]map[types.TableName]*schemaMapping.TableConfig) [][]any {
	tableMetadataRows := [][]any{}
	for keyspace, tables := range tableMetadata {
		for tableName := range tables {
			// Add table metadata
			tableMetadataRows = append(tableMetadataRows, []any{
				string(keyspace), string(tableName), "99p", 0.01, map[string]string{
					"keys":               "ALL",
					"rows_per_partition": "NONE",
				},
				[]string{"compound"},
			})
		}
	}
	return tableMetadataRows
}

// getColumnMetadata converts table metadata into column metadata rows
func getColumnMetadata(tableMetadata map[types.Keyspace]map[types.TableName]*schemaMapping.TableConfig) [][]any {
	var columnsMetadataRows [][]any
	for keyspace, tables := range tableMetadata {
		for tableName, table := range tables {
			for columnName, column := range table.Columns {

				position := table.GetCassandraPositionForColumn(columnName)

				clusteringOrder := "none"
				if column.KeyType == utilities.KEY_TYPE_CLUSTERING {
					clusteringOrder = "asc"
				}

				// Add column metadata
				columnsMetadataRows = append(columnsMetadataRows, []any{
					string(keyspace), string(tableName), string(columnName), clusteringOrder, column.KeyType, position, column.CQLType.String(),
				})
			}
		}
	}
	// sort by sessionKeyspace, table name and column name for deterministic output
	slices.SortFunc(columnsMetadataRows, func(a, b []any) int {
		if res := strings.Compare(a[0].(string), b[0].(string)); res != 0 {
			return res
		}
		if res := strings.Compare(a[1].(string), b[1].(string)); res != 0 {
			return res
		}
		return strings.Compare(a[2].(string), b[2].(string))
	})
	return columnsMetadataRows
}

// ConstructSystemMetadataRows constructs system metadata rows for keyspaces, tables, and metaDataColumns.
// It iterates through the provided table metadata and formats the data into a Cassandra-compatible structure.
// The resulting metadata is used for system queries in the Bigtable proxy.
//
// Parameters:
//   - tableMetadata: A nested map where the first level represents keyspaces, the second level represents tables,
//     and the third level represents metaDataColumns within each table.
//
// Returns:
// - A pointer to a SystemQueryMetadataCache, which contains structured metadata for keyspaces, tables, and metaDataColumns.
// - An error if any issue occurs while building the metadata cache.
func ConstructSystemMetadataRows(tableMetadata map[types.Keyspace]map[types.TableName]*schemaMapping.TableConfig) (*SystemQueryMetadataCache, error) {
	// Initialize the metadata map if it's nil
	if tableMetadata == nil {
		tableMetadata = make(map[types.Keyspace]map[types.TableName]*schemaMapping.TableConfig)
	}

	// Add system keyspaces to metadata
	err := addSystemKeyspacesToMetadata(tableMetadata)
	if err != nil {
		return nil, err
	}

	// Get metadata for system keyspaces
	keyspaceMetadata := getKeyspaceMetadata(tableMetadata)
	tableMetadataRows := getTableMetadata(tableMetadata)
	columnMetadata := getColumnMetadata(tableMetadata)

	// Get system query metadata cache
	systemQueryMetadataCache := &SystemQueryMetadataCache{
		KeyspaceSystemQueryMetadataCache: make(map[primitive.ProtocolVersion][]message.Row),
		TableSystemQueryMetadataCache:    make(map[primitive.ProtocolVersion][]message.Row),
		ColumnsSystemQueryMetadataCache:  make(map[primitive.ProtocolVersion][]message.Row),
	}

	protocolIV := primitive.ProtocolVersion4

	systemQueryMetadataCache.KeyspaceSystemQueryMetadataCache[protocolIV], err = responsehandler.BuildResponseForSystemQueries(keyspaceMetadata, protocolIV)
	if err != nil {
		return nil, err
	}

	systemQueryMetadataCache.TableSystemQueryMetadataCache[protocolIV], err = responsehandler.BuildResponseForSystemQueries(tableMetadataRows, protocolIV)
	if err != nil {
		return nil, err
	}

	systemQueryMetadataCache.ColumnsSystemQueryMetadataCache[protocolIV], err = responsehandler.BuildResponseForSystemQueries(columnMetadata, protocolIV)
	if err != nil {
		return nil, err
	}

	return systemQueryMetadataCache, nil
}

// Add system keyspaces, tables, and metaDataColumns to the schema mapping before system cache construction
func addSystemKeyspacesToMetadata(tableMetadata map[types.Keyspace]map[types.TableName]*schemaMapping.TableConfig) error {
	// Replication settings for system and example keyspaces, matching Cassandra output
	replicationMap := map[types.Keyspace]map[string]string{
		types.Keyspace("system"):                {"class": "org.apache.cassandra.locator.LocalStrategy"},
		types.Keyspace("system_schema"):         {"class": "org.apache.cassandra.locator.LocalStrategy"},
		types.Keyspace("system_auth"):           {"class": "org.apache.cassandra.locator.SimpleStrategy", "replication_factor": "1"},
		types.Keyspace("system_distributed"):    {"class": "org.apache.cassandra.locator.SimpleStrategy", "replication_factor": "3"},
		types.Keyspace("system_traces"):         {"class": "org.apache.cassandra.locator.SimpleStrategy", "replication_factor": "2"},
		types.Keyspace("system_views"):          {"class": "org.apache.cassandra.locator.SimpleStrategy", "replication_factor": "1"},
		types.Keyspace("system_virtual_schema"): {"class": "org.apache.cassandra.locator.LocalStrategy"},
	}

	for ks := range replicationMap {
		if _, exists := tableMetadata[ks]; !exists {
			tableMetadata[ks] = make(map[types.TableName]*schemaMapping.TableConfig)
		}
		// Add system_schema tables
		if ks == "system_schema" {
			// Add tables table
			if _, exists := tableMetadata[ks]["tables"]; !exists {
				tableMetadata[ks]["tables"] = &schemaMapping.TableConfig{
					Keyspace: ks,
					Name:     "tables",
					Columns:  make(map[types.ColumnName]*types.Column),
				}
			}
			for _, col := range parser.SystemSchemaTablesColumns {
				cqlDataType, err := utilities.FromDataCode(col.Type)
				if err != nil {
					return err
				}
				tableMetadata[ks]["tables"].Columns[types.ColumnName(col.Name)] = &types.Column{
					Name:         types.ColumnName(col.Name),
					CQLType:      cqlDataType,
					IsPrimaryKey: col.Name == "keyspace_name" || col.Name == "table_name",
					KeyType: func() string {
						if col.Name == "keyspace_name" || col.Name == "table_name" {
							return utilities.KEY_TYPE_PARTITION
						}
						return utilities.KEY_TYPE_REGULAR
					}(),
				}
			}

			// Add metaDataColumns table
			if _, exists := tableMetadata[ks]["metaDataColumns"]; !exists {
				tableMetadata[ks]["metaDataColumns"] = &schemaMapping.TableConfig{
					Keyspace: ks,
					Name:     "metaDataColumns",
					Columns:  make(map[types.ColumnName]*types.Column),
				}
			}
			for _, col := range parser.SystemSchemaColumnsColumns {
				cqlDataType, err := utilities.FromDataCode(col.Type)
				if err != nil {
					return err
				}
				tableMetadata[ks]["metaDataColumns"].Columns[types.ColumnName(col.Name)] = &types.Column{
					Name:         types.ColumnName(col.Name),
					CQLType:      cqlDataType,
					IsPrimaryKey: col.Name == "keyspace_name" || col.Name == "table_name" || col.Name == "column_name",
					KeyType: func() string {
						if col.Name == "keyspace_name" || col.Name == "table_name" || col.Name == "column_name" {
							return utilities.KEY_TYPE_PARTITION
						}
						return utilities.KEY_TYPE_REGULAR
					}(),
				}
			}

			// Add keyspaces table
			if _, exists := tableMetadata[ks]["keyspaces"]; !exists {
				tableMetadata[ks]["keyspaces"] = &schemaMapping.TableConfig{
					Keyspace: ks,
					Name:     "keyspaces",
					Columns:  make(map[types.ColumnName]*types.Column),
				}
			}
			for _, col := range parser.SystemSchemaKeyspacesColumns {
				cqlDataType, err := utilities.FromDataCode(col.Type)
				if err != nil {
					return err
				}
				tableMetadata[ks]["keyspaces"].Columns[types.ColumnName(col.Name)] = &types.Column{
					Name:         types.ColumnName(col.Name),
					CQLType:      cqlDataType,
					IsPrimaryKey: col.Name == "keyspace_name",
					KeyType: func() string {
						if col.Name == "keyspace_name" {
							return utilities.KEY_TYPE_PARTITION
						}
						return utilities.KEY_TYPE_REGULAR
					}(),
				}
			}
		}
		// Add system tables
		if ks == "system" {
			// Add local table
			if _, exists := tableMetadata[ks]["local"]; !exists {
				tableMetadata[ks]["local"] = &schemaMapping.TableConfig{
					Keyspace: ks,
					Name:     "local",
					Columns:  make(map[types.ColumnName]*types.Column),
				}
			}
			// Add metaDataColumns for system.local
			columns := []struct {
				Name string
				Type datatype.DataType
			}{
				{"key", datatype.Varchar},
				{"bootstrapped", datatype.Varchar},
				{"broadcast_address", datatype.Inet},
				{"cluster_name", datatype.Varchar},
				{"cql_version", datatype.Varchar},
				{"data_center", datatype.Varchar},
				{"gossip_generation", datatype.Int},
				{"host_id", datatype.Uuid},
				{"listen_address", datatype.Inet},
				{"native_protocol_version", datatype.Varchar},
				{"partitioner", datatype.Varchar},
				{"rack", datatype.Varchar},
				{"release_version", datatype.Varchar},
				{"rpc_address", datatype.Inet},
				{"schema_version", datatype.Uuid},
				{"thrift_version", datatype.Varchar},
				{"tokens", datatype.NewSetType(datatype.Varchar)},
			}
			for _, col := range columns {
				cqlDataType, err := utilities.FromDataCode(col.Type)
				if err != nil {
					return err
				}
				tableMetadata[ks]["local"].Columns[types.ColumnName(col.Name)] = &types.Column{
					Name:         types.ColumnName(col.Name),
					CQLType:      cqlDataType,
					IsPrimaryKey: col.Name == "key",
					KeyType: func() string {
						if col.Name == "key" {
							return utilities.KEY_TYPE_PARTITION
						}
						return utilities.KEY_TYPE_REGULAR
					}(),
				}
			}

			// Add peers table
			if _, exists := tableMetadata[ks]["peers"]; !exists {
				tableMetadata[ks]["peers"] = &schemaMapping.TableConfig{
					Keyspace: ks,
					Name:     "peers",
					Columns:  make(map[types.ColumnName]*types.Column),
				}
			}
			// Add metaDataColumns for system.peers
			peerColumns := []struct {
				Name string
				Type datatype.DataType
			}{
				{"peer", datatype.Inet},
				{"data_center", datatype.Varchar},
				{"host_id", datatype.Uuid},
				{"preferred_ip", datatype.Inet},
				{"rack", datatype.Varchar},
				{"release_version", datatype.Varchar},
				{"rpc_address", datatype.Inet},
				{"schema_version", datatype.Uuid},
				{"tokens", datatype.NewSetType(datatype.Varchar)},
			}
			for _, col := range peerColumns {
				cqlDataType, err := utilities.FromDataCode(col.Type)
				if err != nil {
					return err
				}
				tableMetadata[ks]["peers"].Columns[types.ColumnName(col.Name)] = &types.Column{
					Name:         types.ColumnName(col.Name),
					CQLType:      cqlDataType,
					IsPrimaryKey: col.Name == "peer",
					KeyType: func() string {
						if col.Name == "peer" {
							return utilities.KEY_TYPE_PARTITION
						}
						return utilities.KEY_TYPE_REGULAR
					}(),
				}
			}

			// Add peers_v2 table
			if _, exists := tableMetadata[ks]["peers_v2"]; !exists {
				tableMetadata[ks]["peers_v2"] = &schemaMapping.TableConfig{
					Keyspace: ks,
					Name:     "peers_v2",
					Columns:  make(map[types.ColumnName]*types.Column),
				}
			}
			// Add metaDataColumns for system.peers_v2
			peerV2Columns := []struct {
				Name string
				Type datatype.DataType
			}{
				{"peer", datatype.Inet},
				{"data_center", datatype.Varchar},
				{"host_id", datatype.Uuid},
				{"native_address", datatype.Inet},
				{"native_port", datatype.Int},
				{"preferred_ip", datatype.Inet},
				{"rack", datatype.Varchar},
				{"release_version", datatype.Varchar},
				{"rpc_address", datatype.Inet},
				{"schema_version", datatype.Uuid},
				{"tokens", datatype.NewSetType(datatype.Varchar)},
			}
			for _, col := range peerV2Columns {
				cqlDataType, err := utilities.FromDataCode(col.Type)
				if err != nil {
					return err
				}
				tableMetadata[ks]["peers_v2"].Columns[types.ColumnName(col.Name)] = &types.Column{
					Name:         types.ColumnName(col.Name),
					CQLType:      cqlDataType,
					IsPrimaryKey: col.Name == "peer",
					KeyType: func() string {
						if col.Name == "peer" {
							return utilities.KEY_TYPE_PARTITION
						}
						return utilities.KEY_TYPE_REGULAR
					}(),
				}
			}
		}
	}

	// Add system_virtual_schema tables and metaDataColumns
	if _, exists := tableMetadata["system_virtual_schema"]; !exists {
		tableMetadata["system_virtual_schema"] = make(map[types.TableName]*schemaMapping.TableConfig)
	}
	// keyspaces
	tableMetadata["system_virtual_schema"]["keyspaces"] = &schemaMapping.TableConfig{
		Keyspace: "system_virtual_schema",
		Name:     "keyspaces",
		Columns:  make(map[types.ColumnName]*types.Column),
	}
	for _, col := range parser.SystemVirtualSchemaKeyspaces {
		cqlDataType, err := utilities.FromDataCode(col.Type)
		if err != nil {
			return err
		}
		tableMetadata["system_virtual_schema"]["keyspaces"].Columns[types.ColumnName(col.Name)] = &types.Column{
			Name:         types.ColumnName(col.Name),
			CQLType:      cqlDataType,
			IsPrimaryKey: false,
			KeyType:      utilities.KEY_TYPE_REGULAR,
		}
	}
	// tables
	tableMetadata["system_virtual_schema"]["tables"] = &schemaMapping.TableConfig{
		Keyspace: "system_virtual_schema",
		Name:     "tables",
		Columns:  make(map[types.ColumnName]*types.Column),
	}
	for _, col := range parser.SystemVirtualSchemaTables {
		cqlDataType, err := utilities.FromDataCode(col.Type)
		if err != nil {
			return err
		}
		tableMetadata["system_virtual_schema"]["tables"].Columns[types.ColumnName(col.Name)] = &types.Column{
			Name:         types.ColumnName(col.Name),
			CQLType:      cqlDataType,
			IsPrimaryKey: false,
			KeyType:      utilities.KEY_TYPE_REGULAR,
		}
	}
	// metaDataColumns
	tableMetadata["system_virtual_schema"]["metaDataColumns"] = &schemaMapping.TableConfig{
		Keyspace: "system_virtual_schema",
		Name:     "metaDataColumns",
		Columns:  make(map[types.ColumnName]*types.Column),
	}
	for _, col := range parser.SystemVirtualSchemaColumns {
		cqlDataType, err := utilities.FromDataCode(col.Type)
		if err != nil {
			return err
		}
		tableMetadata["system_virtual_schema"]["metaDataColumns"].Columns[types.ColumnName(col.Name)] = &types.Column{
			Name:         types.ColumnName(col.Name),
			CQLType:      cqlDataType,
			IsPrimaryKey: false,
			KeyType:      utilities.KEY_TYPE_REGULAR,
		}
	}
	return nil
}

func getTimestampMetadata(table *schemaMapping.TableConfig, params *translator.QueryParameters) []*message.ColumnMetadata {
	index := params.Index(translator.UsingTimePlaceholder)
	if index != -1 {
		return []*message.ColumnMetadata{{
			Keyspace: string(table.Keyspace),
			Table:    string(table.Name),
			Name:     config.TimestampColumnName,
			Index:    int32(index),
			Type:     datatype.Bigint,
		}}
	}
	return nil
}
