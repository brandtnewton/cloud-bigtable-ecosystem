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
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"go.uber.org/zap"
	"log"
)

func GetSchemaMappingConfig() *schemaMapping.SchemaMappingConfig {
	var (
		testTableColumns = []*types.Column{
			{Name: "column1", CQLType: utilities.ParseCqlTypeOrDie("varchar"), KeyType: types.KeyTypePartition, IsPrimaryKey: true, PkPrecedence: 1},
			{Name: "column10", CQLType: utilities.ParseCqlTypeOrDie("varchar"), KeyType: types.KeyTypeClustering, IsPrimaryKey: true, PkPrecedence: 2},
			// Regular columns
			{Name: "column2", CQLType: utilities.ParseCqlTypeOrDie("blob"), KeyType: types.KeyTypeRegular},
			{Name: "column3", CQLType: utilities.ParseCqlTypeOrDie("boolean"), KeyType: types.KeyTypeRegular},
			{Name: "column4", CQLType: utilities.ParseCqlTypeOrDie("list<text>"), KeyType: types.KeyTypeRegular},
			{Name: "column5", CQLType: utilities.ParseCqlTypeOrDie("timestamp"), KeyType: types.KeyTypeRegular},
			{Name: "column6", CQLType: utilities.ParseCqlTypeOrDie("int"), KeyType: types.KeyTypeRegular},
			{Name: "column7", CQLType: utilities.ParseCqlTypeOrDie("set<text>"), KeyType: types.KeyTypeRegular},
			{Name: "column8", CQLType: utilities.ParseCqlTypeOrDie("map<varchar,boolean>"), KeyType: types.KeyTypeRegular},
			{Name: "column9", CQLType: utilities.ParseCqlTypeOrDie("bigint"), KeyType: types.KeyTypeRegular},
			{Name: "column11", CQLType: utilities.ParseCqlTypeOrDie("set<text>"), KeyType: types.KeyTypeRegular},
		}

		userInfoColumns = []*types.Column{
			{Name: "name", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, IsPrimaryKey: true, PkPrecedence: 0},
			{Name: "age", CQLType: types.TypeBigint, KeyType: types.KeyTypeClustering, IsPrimaryKey: true, PkPrecedence: 1},
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
		zap.NewNop(),
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
