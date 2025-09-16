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

package schemaMapping

import (
	"fmt"
	"sort"
	"sync"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"go.uber.org/zap"
)

const (
	LimitValue = "limitValue"
)

// SchemaMappingConfig contains the schema information for all tables, across
// all Bigtable instances, managed by this proxy.
type SchemaMappingConfig struct {
	Logger                 *zap.Logger
	mu                     sync.RWMutex
	tables                 map[string]map[string]*TableConfig
	SystemColumnFamily     string
	SchemaMappingTableName string
}

// NewSchemaMappingConfig is a constructor for SchemaMappingConfig. Please use this instead of direct initialization.
func NewSchemaMappingConfig(schemaMappingTableName, systemColumnFamily string, logger *zap.Logger, tableConfigs []*TableConfig) *SchemaMappingConfig {
	tablesMap := make(map[string]map[string]*TableConfig)
	for _, tableConfig := range tableConfigs {
		if keyspace, exists := tablesMap[tableConfig.Keyspace]; !exists {
			keyspace = make(map[string]*TableConfig)
			tablesMap[tableConfig.Keyspace] = keyspace
		}
		tablesMap[tableConfig.Keyspace][tableConfig.Name] = tableConfig
	}
	return &SchemaMappingConfig{
		Logger:                 logger,
		SchemaMappingTableName: schemaMappingTableName,
		SystemColumnFamily:     systemColumnFamily,
		tables:                 tablesMap,
	}
}

func GetSystemTableConfigs() []*TableConfig {
	// note these most of these types aren't correct
	st := NewTableConfig("system", "tables", "na", types.OrderedCodeEncoding, []*types.Column{
		{
			Name:         "keyspace_name",
			CQLType:      datatype.Varchar,
			IsPrimaryKey: true,
			PkPrecedence: 1,
			KeyType:      utilities.KEY_TYPE_PARTITION,
		},
		{
			Name:         "table_name",
			CQLType:      datatype.Varchar,
			IsPrimaryKey: true,
			PkPrecedence: 2,
			KeyType:      utilities.KEY_TYPE_CLUSTERING,
		},
		{
			Name:    "additional_write_policy",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "bloom_filter_fp_chance",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "caching",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "cdc",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "comment",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "compaction",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "compression",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "crc_check_chance",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "dclocal_read_repair_chance",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "default_time_to_live",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "extensions",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "flags",
			CQLType: datatype.NewSetType(datatype.Varchar),
		},
		{
			Name:    "gc_grace_seconds",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "id",
			CQLType: datatype.Uuid,
		},
		{
			Name:    "max_index_interval",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "memtable",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "memtable_flush_period_in_ms",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "min_index_interval",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "read_repair",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "read_repair_chance",
			CQLType: datatype.Varchar,
		},
		{
			Name:    "speculative_retry",
			CQLType: datatype.Varchar,
		},
	})

	sc := NewTableConfig("system", "columns", "na", types.OrderedCodeEncoding, []*types.Column{
		{
			Name:         "keyspace_name",
			CQLType:      datatype.Varchar,
			IsPrimaryKey: true,
			PkPrecedence: 1,
			KeyType:      utilities.KEY_TYPE_PARTITION,
		},
		{
			Name:         "table_name",
			CQLType:      datatype.Varchar,
			IsPrimaryKey: true,
			PkPrecedence: 2,
			KeyType:      utilities.KEY_TYPE_CLUSTERING,
		},
		{
			Name:         "column_name",
			CQLType:      datatype.Varchar,
			PkPrecedence: -1,
			KeyType:      utilities.KEY_TYPE_REGULAR,
		},
		{
			Name:         "clustering_order",
			CQLType:      datatype.Int,
			PkPrecedence: -1,
			KeyType:      utilities.KEY_TYPE_REGULAR,
		},
		{
			Name:         "column_name_bytes",
			CQLType:      datatype.Blob,
			PkPrecedence: -1,
			KeyType:      utilities.KEY_TYPE_REGULAR,
		},
		{
			Name:         "kind",
			CQLType:      datatype.Varchar,
			PkPrecedence: -1,
			KeyType:      utilities.KEY_TYPE_REGULAR,
		},
		{
			Name:         "position",
			CQLType:      datatype.Int,
			PkPrecedence: -1,
			KeyType:      utilities.KEY_TYPE_REGULAR,
		},
		{
			Name:         "type",
			CQLType:      datatype.Varchar,
			PkPrecedence: -1,
			KeyType:      utilities.KEY_TYPE_REGULAR,
		},
	})
	return []*TableConfig{st, sc}
}

// GetAllTables DEPRECATED - will be removed in the future
func (c *SchemaMappingConfig) GetAllTables() map[string]map[string]*TableConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// make a new shallow copy to ensure that read is thread safe
	tablesCopy := make(map[string]map[string]*TableConfig, len(c.tables))

	for keyspace, tables := range c.tables {
		innerCopy := make(map[string]*TableConfig, len(tables))
		for name, config := range tables {
			innerCopy[name] = config
		}
		tablesCopy[keyspace] = innerCopy
	}

	return tablesCopy
}

func (c *SchemaMappingConfig) GetKeyspace(keyspace string) ([]*TableConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tables, ok := c.tables[keyspace]
	if !ok {
		return nil, fmt.Errorf("keyspace %s does not exist", keyspace)
	}
	var results []*TableConfig = nil
	for _, table := range tables {
		results = append(results, table)
	}
	return results, nil
}

func (c *SchemaMappingConfig) ReplaceTables(tableConfigs []*TableConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// clear the existing tables
	c.tables = make(map[string]map[string]*TableConfig)

	for _, tableConfig := range tableConfigs {
		if _, exists := c.tables[tableConfig.Keyspace]; !exists {
			c.tables[tableConfig.Keyspace] = make(map[string]*TableConfig)
		}
		c.tables[tableConfig.Keyspace][tableConfig.Name] = tableConfig
	}
}

func (c *SchemaMappingConfig) UpdateTables(tableConfigs []*TableConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tables == nil {
		c.tables = make(map[string]map[string]*TableConfig)
	}
	for _, tableConfig := range tableConfigs {
		if _, exists := c.tables[tableConfig.Keyspace]; !exists {
			c.tables[tableConfig.Keyspace] = make(map[string]*TableConfig)
		}
		c.tables[tableConfig.Keyspace][tableConfig.Name] = tableConfig
	}
}

// GetTableConfig finds the primary key columns of a specified table in a given keyspace.
//
// This method looks up the cached primary key metadata and returns the relevant columns.
//
// Parameters:
//   - keySpace: The name of the keyspace where the table resides.
//   - tableName: The name of the table for which primary key metadata is requested.
//
// Returns:
//   - []types.Column: A slice of types.Column structs representing the primary keys of the table.
//   - error: Returns an error if the primary key metadata is not found.
func (c *SchemaMappingConfig) GetTableConfig(keySpace string, tableName string) (*TableConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keyspace, ok := c.tables[keySpace]
	if !ok {
		return nil, fmt.Errorf("keyspace %s does not exist", keySpace)
	}
	tableConfig, ok := keyspace[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}
	return tableConfig, nil
}

func (c *SchemaMappingConfig) CountTables() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var result = 0
	for _, keyspaceTables := range c.tables {
		result += len(keyspaceTables)
	}
	return result
}

// ListKeyspaces returns a sorted list of all keyspace names in the schema mapping.
func (c *SchemaMappingConfig) ListKeyspaces() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keyspaces := make([]string, 0, len(c.tables))
	for ks := range c.tables {
		keyspaces = append(keyspaces, ks)
	}
	sort.Strings(keyspaces)
	return keyspaces
}
