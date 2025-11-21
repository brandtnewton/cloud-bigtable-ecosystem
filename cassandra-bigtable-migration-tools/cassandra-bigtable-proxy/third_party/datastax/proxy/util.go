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
	"crypto"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/constants"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/mem_table"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"io"
	"net"
)

func InitializeSystemTables(schemas *schemaMapping.SchemaMappingConfig, engine *mem_table.InMemEngine, proxy *Proxy) error {

	// add all system tables with empty data, so they're at least queryable
	for _, table := range schemas.Tables() {
		if !table.Keyspace.IsSystemKeyspace() {
			continue
		}
		_ = engine.SetData(table, nil)
	}

	err := ReloadSystemTables(schemas, engine, proxy)
	if err != nil {
		return err
	}

	return nil
}

func ReloadSystemTables(schemas *schemaMapping.SchemaMappingConfig, engine *mem_table.InMemEngine, proxy *Proxy) error {

	// add all system tables with empty data, so they're at least queryable
	for _, table := range schemas.Tables() {
		if !table.Keyspace.IsSystemKeyspace() {
			continue
		}
		_ = engine.SetData(table, nil)
	}

	var err error
	err = engine.SetData(schemaMapping.SystemSchemaTableKeyspace, getKeyspaceMetadata(schemas))
	if err != nil {
		return err
	}

	err = engine.SetData(schemaMapping.SystemSchemaTableTables, getTableMetadata(schemas))
	if err != nil {
		return err
	}

	err = engine.SetData(schemaMapping.SystemSchemaTableColumns, getColumnMetadata(schemas))
	if err != nil {
		return err
	}

	err = engine.SetData(schemaMapping.SystemTableLocal, getLocalMetadata(proxy))
	if err != nil {
		return err
	}

	err = engine.SetData(schemaMapping.SystemTablePeers, getPeerMetadata(proxy))
	if err != nil {
		return err
	}

	err = engine.SetData(schemaMapping.SystemTablePeersV2, getPeerMetadata(proxy))
	if err != nil {
		return err
	}

	return nil
}

func getKeyspaceMetadata(schemas *schemaMapping.SchemaMappingConfig) []types.GoRow {
	var rows []types.GoRow
	for _, keyspace := range schemas.Keyspaces() {
		md := keyspace.GetMetadata()
		row := types.GoRow{
			"keyspace_name":  md.KeyspaceName,
			"durable_writes": md.DurableWrites,
			"replication":    md.Replication,
		}
		rows = append(rows, row)
	}
	return rows
}

// getTableMetadata converts table metadata into table metadata rows
func getTableMetadata(schemas *schemaMapping.SchemaMappingConfig) []types.GoRow {
	var rows []types.GoRow
	for _, t := range schemas.Tables() {
		md := t.CreateTableMetadata()
		rows = append(rows, types.GoRow{
			"keyspace_name":           md.KeyspaceName,
			"table_name":              md.TableName,
			"additional_write_policy": md.AdditionalWritePolicy,
			"bloom_filter_fp_chance":  md.BloomFilterFpChance,
			"caching":                 md.Caching,
			"flags":                   md.Flags,
		})
	}
	return rows
}

// getColumnMetadata converts table metadata into column metadata rows
func getColumnMetadata(schemas *schemaMapping.SchemaMappingConfig) []types.GoRow {
	var rows []types.GoRow
	for _, table := range schemas.Tables() {
		for columnName, column := range table.Columns {
			position := table.GetCassandraPositionForColumn(columnName)
			md := column.SystemMetadata(position)
			rows = append(rows, types.GoRow{
				"keyspace_name":    md.KeyspaceName,
				"table_name":       md.TableName,
				"column_name":      md.ColumnName,
				"clustering_order": md.ClusteringOrder,
				"kind":             md.Kind,
				"position":         md.Position,
				"type":             md.Type,
			})
		}
	}
	return rows
}

func getLocalMetadata(proxy *Proxy) []types.GoRow {
	return []types.GoRow{
		{
			"rpc_address":             localIP(proxy),
			"host_id":                 nameBasedUUID(localIP(proxy).String()),
			"key":                     "local",
			"data_center":             proxy.localNode.dc,
			"rack":                    "rack1",
			"tokens":                  []string{"-9223372036854775808"},
			"release_version":         proxy.config.Options.ReleaseVersion,
			"partitioner":             proxy.config.Options.Partitioner,
			"cluster_name":            fmt.Sprintf("cassandra-bigtable-proxy-%s", constants.ProxyReleaseVersion),
			"cql_version":             proxy.config.Options.CQLVersion,
			"schema_version":          *schemaVersion,
			"native_protocol_version": proxy.config.Options.ProtocolVersion.String(),
			"dse_version":             proxy.cluster.Info.DSEVersion,
		},
	}
}
func getPeerMetadata(proxy *Proxy) []types.GoRow {
	var rows []types.GoRow
	for _, peer := range proxy.nodes {
		rows = append(rows, types.GoRow{
			"peer":            peer.addr.IP,
			"rpc_address":     peer.addr.IP,
			"data_center":     peer.dc,
			"dse_version":     proxy.cluster.Info.DSEVersion,
			"rack":            "rack1",
			"tokens":          peer.tokens,
			"release_version": proxy.config.Options.ReleaseVersion,
			"schema_version":  *schemaVersion,
			"host_id":         nameBasedUUID(peer.addr.String()),
		})
	}
	return rows
}

func localIP(proxy *Proxy) net.IP {
	if proxy.config.RPCAddr != "" {
		return net.ParseIP(proxy.config.RPCAddr)
	}
	return net.ParseIP("127.0.0.1")
}

func nameBasedUUID(name string) primitive.UUID {
	var uuid primitive.UUID
	m := crypto.MD5.New()
	_, _ = io.WriteString(m, name)
	hash := m.Sum(nil)
	for i := 0; i < len(uuid); i++ {
		uuid[i] = hash[i]
	}
	uuid[6] &= 0x0F
	uuid[6] |= 0x30
	uuid[8] &= 0x3F
	uuid[8] |= 0x80
	return uuid
}
