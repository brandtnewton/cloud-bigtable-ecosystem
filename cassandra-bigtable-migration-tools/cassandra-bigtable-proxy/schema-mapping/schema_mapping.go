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
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"slices"
	"strings"
	"sync"

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
	tables                 map[types.Keyspace]map[types.TableName]*TableConfig
	SystemColumnFamily     types.ColumnFamily
	SchemaMappingTableName types.TableName
}

// NewSchemaMappingConfig is a constructor for SchemaMappingConfig. Please use this instead of direct initialization.
func NewSchemaMappingConfig(schemaMappingTableName types.TableName, systemColumnFamily types.ColumnFamily, logger *zap.Logger, tableConfigs []*TableConfig) *SchemaMappingConfig {
	tablesMap := make(map[types.Keyspace]map[types.TableName]*TableConfig)

	tableConfigs = append(tableConfigs, getSystemTableConfigs()...)

	for _, tableConfig := range tableConfigs {
		if keyspace, exists := tablesMap[tableConfig.Keyspace]; !exists {
			keyspace = make(map[types.TableName]*TableConfig)
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

// GetAllTables DEPRECATED - will be removed in the future
func (c *SchemaMappingConfig) GetAllTables() map[types.Keyspace]map[types.TableName]*TableConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// make a new shallow copy to ensure that read is thread safe
	tablesCopy := make(map[types.Keyspace]map[types.TableName]*TableConfig, len(c.tables))

	for keyspace, tables := range c.tables {
		innerCopy := make(map[types.TableName]*TableConfig, len(tables))
		for name, config := range tables {
			innerCopy[name] = config
		}
		tablesCopy[keyspace] = innerCopy
	}

	return tablesCopy
}

func (c *SchemaMappingConfig) GetKeyspace(keyspace types.Keyspace) ([]*TableConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tables, ok := c.tables[keyspace]
	if !ok {
		return nil, fmt.Errorf("keyspace '%s' does not exist", keyspace)
	}
	var results []*TableConfig = nil
	for _, table := range tables {
		results = append(results, table)
	}
	return results, nil
}

func (c *SchemaMappingConfig) ReplaceTables(keyspace types.Keyspace, tableConfigs []*TableConfig) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// clear the keyspace
	c.tables[keyspace] = make(map[types.TableName]*TableConfig)
	for _, tableConfig := range tableConfigs {
		if tableConfig.Keyspace != keyspace {
			return fmt.Errorf("cannot replace table with keyspace '%s' because we're only updating keyspace '%s'", tableConfig.Keyspace, keyspace)
		}
		c.tables[keyspace][tableConfig.Name] = tableConfig
	}
	return nil
}

func (c *SchemaMappingConfig) UpdateTables(tableConfigs []*TableConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, tableConfig := range tableConfigs {
		if _, exists := c.tables[tableConfig.Keyspace]; !exists {
			c.tables[tableConfig.Keyspace] = make(map[types.TableName]*TableConfig)
		}
		c.tables[tableConfig.Keyspace][tableConfig.Name] = tableConfig
	}
}

// GetTableConfig finds the primary key columns of a specified table in a given keyspace.
//
// This method looks up the cached primary key metadata and returns the relevant columns.
//
// Parameters:
//   - keyspace: The name of the keyspace where the table resides.
//   - tableName: The name of the table for which primary key metadata is requested.
//
// Returns:
//   - []types.Column: A slice of types.Column structs representing the primary keys of the table.
//   - error: Returns an error if the primary key metadata is not found.
func (c *SchemaMappingConfig) GetTableConfig(k types.Keyspace, t types.TableName) (*TableConfig, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keyspace, ok := c.tables[k]
	if !ok {
		return nil, fmt.Errorf("keyspace '%s' does not exist", k)
	}
	tableConfig, ok := keyspace[t]
	if !ok {
		return nil, fmt.Errorf("table %s does not exist", t)
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
func (c *SchemaMappingConfig) ListKeyspaces() []types.Keyspace {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keyspaces := make([]types.Keyspace, 0, len(c.tables))
	for ks := range c.tables {
		keyspaces = append(keyspaces, ks)
	}
	slices.SortFunc(keyspaces, func(a, b types.Keyspace) int {
		return strings.Compare(string(a), string(b))
	})
	return keyspaces
}
