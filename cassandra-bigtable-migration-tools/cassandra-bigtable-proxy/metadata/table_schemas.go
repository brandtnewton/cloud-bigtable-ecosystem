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

package metadata

import (
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"slices"
	"strings"
	"sync"
)

const (
	LimitValue = "limitValue"
)

// SchemaMetadata contains the schema information for all tables, across
// all Bigtable instances, managed by this proxy.
type SchemaMetadata struct {
	mu                 sync.RWMutex
	tables             map[types.Keyspace]map[types.TableName]*TableSchema
	SystemColumnFamily types.ColumnFamily
}

// NewSchemaMetadata is a constructor for SchemaMetadata. Please use this instead of direct initialization.
func NewSchemaMetadata(systemColumnFamily types.ColumnFamily, tableConfigs []*TableSchema) *SchemaMetadata {
	tablesMap := make(map[types.Keyspace]map[types.TableName]*TableSchema)

	tableConfigs = append(tableConfigs, getSystemTableConfigs()...)

	for _, tableConfig := range tableConfigs {
		if keyspace, exists := tablesMap[tableConfig.Keyspace]; !exists {
			keyspace = make(map[types.TableName]*TableSchema)
			tablesMap[tableConfig.Keyspace] = keyspace
		}
		tablesMap[tableConfig.Keyspace][tableConfig.Name] = tableConfig
	}
	return &SchemaMetadata{
		SystemColumnFamily: systemColumnFamily,
		tables:             tablesMap,
	}
}

func (c *SchemaMetadata) Keyspaces() []types.Keyspace {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var results []types.Keyspace
	for keyspace := range c.tables {
		results = append(results, keyspace)
	}
	return results
}

func (c *SchemaMetadata) Tables() []*TableSchema {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var tables []*TableSchema
	for _, keyspace := range c.tables {
		for _, t := range keyspace {
			tables = append(tables, t)
		}
	}
	return tables
}

func (c *SchemaMetadata) ValidateKeyspace(keyspace types.Keyspace) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.tables[keyspace]
	if !ok {
		return fmt.Errorf("keyspace '%s' does not exist", keyspace)
	}
	return nil
}

func (c *SchemaMetadata) GetKeyspace(keyspace types.Keyspace) ([]*TableSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tables, ok := c.tables[keyspace]
	if !ok {
		return nil, fmt.Errorf("keyspace '%s' does not exist", keyspace)
	}
	var results []*TableSchema = nil
	for _, table := range tables {
		results = append(results, table)
	}
	return results, nil
}

func (c *SchemaMetadata) ReplaceTables(keyspace types.Keyspace, tableConfigs []*TableSchema) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// clear the keyspace
	c.tables[keyspace] = make(map[types.TableName]*TableSchema)
	for _, tableConfig := range tableConfigs {
		if tableConfig.Keyspace != keyspace {
			return fmt.Errorf("cannot replace table with keyspace '%s' because we're only updating keyspace '%s'", tableConfig.Keyspace, keyspace)
		}
		c.tables[keyspace][tableConfig.Name] = tableConfig
	}
	return nil
}

func (c *SchemaMetadata) UpdateTables(keyspace types.Keyspace, tableConfigs []*TableSchema) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.tables[keyspace]; !exists {
		c.tables[keyspace] = make(map[types.TableName]*TableSchema)
	}

	for _, tableConfig := range tableConfigs {
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
func (c *SchemaMetadata) GetTableConfig(k types.Keyspace, t types.TableName) (*TableSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	keyspace, ok := c.tables[k]
	if !ok {
		return nil, fmt.Errorf("keyspace '%s' does not exist", k)
	}
	tableConfig, ok := keyspace[t]
	if !ok {
		return nil, fmt.Errorf("table '%s' does not exist", t)
	}
	return tableConfig, nil
}

func (c *SchemaMetadata) CountTables() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var result = 0
	for _, keyspaceTables := range c.tables {
		result += len(keyspaceTables)
	}
	return result
}

// ListKeyspaces returns a sorted list of all keyspace names in the schema mapping.
func (c *SchemaMetadata) ListKeyspaces() []types.Keyspace {
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
