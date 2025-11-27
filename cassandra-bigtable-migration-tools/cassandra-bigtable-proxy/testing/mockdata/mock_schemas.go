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

package mockdata

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"log"
)

func GetSchemaMappingConfig() *schemaMapping.SchemaMappingConfig {
	var (
		testTableColumns = []*types.Column{
			{Name: "pk1", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, IsPrimaryKey: true, PkPrecedence: 1},
			{Name: "pk2", CQLType: types.TypeVarchar, KeyType: types.KeyTypeClustering, IsPrimaryKey: true, PkPrecedence: 2},
			// Regular columns
			{Name: "col_blob", CQLType: types.TypeBlob},
			{Name: "col_bool", CQLType: types.TypeBoolean},
			{Name: "list_text", CQLType: types.NewListType(types.TypeText)},
			{Name: "col_ts", CQLType: types.TypeTimestamp},
			{Name: "col_int", CQLType: types.TypeInt},
			{Name: "col_bigint", CQLType: types.TypeBigint},
			{Name: "set_text", CQLType: types.NewSetType(types.TypeText)},
			{Name: "map_varchar_bool", CQLType: types.NewMapType(types.TypeVarchar, types.TypeBoolean)},
			{Name: "map_text_text", CQLType: types.NewMapType(types.TypeText, types.TypeText)},
		}

		userInfoColumns = []*types.Column{
			{Name: "name", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, IsPrimaryKey: true, PkPrecedence: 0},
			{Name: "age", CQLType: types.TypeBigint, KeyType: types.KeyTypeClustering, IsPrimaryKey: true, PkPrecedence: 1},
			{Name: "email", CQLType: types.TypeText},
			{Name: "username", CQLType: types.TypeText},
		}
	)

	var allTableConfigs = []*schemaMapping.TableConfig{
		schemaMapping.NewTableConfig(
			"test_keyspace",
			"test_table",
			"cf1",
			types.OrderedCodeEncoding,
			testTableColumns,
		),
		schemaMapping.NewTableConfig(
			"test_keyspace",
			"user_info",
			"cf1",
			types.OrderedCodeEncoding,
			userInfoColumns,
		),
		schemaMapping.NewTableConfig(
			"test_keyspace",
			"user_info_big_endian",
			"cf1",
			types.BigEndianEncoding,
			userInfoColumns,
		),
	}
	return schemaMapping.NewSchemaMappingConfig(
		"schema_mapping",
		"cf1",
		allTableConfigs,
	)
}

func GetColumnOrDie(k types.Keyspace, t types.TableName, c types.ColumnName) *types.Column {
	config, err := GetSchemaMappingConfig().GetTableConfig(k, t)
	if err != nil {
		log.Fatalf("no such table or keyspace: %s", err.Error())
	}
	column, err := config.GetColumn(c)
	if err != nil {
		log.Fatalf("no such column: %s", err.Error())
	}
	return column
}
