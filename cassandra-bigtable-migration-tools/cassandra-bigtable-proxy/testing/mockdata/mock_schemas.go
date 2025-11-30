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
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
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
			{Name: "col_float", CQLType: types.TypeFloat},
			{Name: "col_double", CQLType: types.TypeDouble},
			{Name: "col_bigint", CQLType: types.TypeBigInt},
			{Name: "col_counter", CQLType: types.TypeCounter},
			{Name: "set_text", CQLType: types.NewSetType(types.TypeText)},
			{Name: "map_varchar_bool", CQLType: types.NewMapType(types.TypeVarchar, types.TypeBoolean)},
			{Name: "map_text_text", CQLType: types.NewMapType(types.TypeText, types.TypeText)},
			{Name: "map_text_bool", CQLType: types.NewMapType(types.TypeText, types.TypeBoolean)},
		}

		userInfoColumns = []*types.Column{
			{Name: "name", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, IsPrimaryKey: true, PkPrecedence: 1},
			{Name: "age", CQLType: types.TypeBigInt, KeyType: types.KeyTypeClustering, IsPrimaryKey: true, PkPrecedence: 2},
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

func CreateQueryParameterValuesFromMap2(table *schemaMapping.TableConfig, values map[types.ColumnName]types.GoValue) *types.QueryParameterValues {
	params := types.NewQueryParameters()
	result := types.NewQueryParameterValues(params)
	for colName, val := range values {
		col, err := table.GetColumn(colName)
		if err != nil {
			log.Fatalf(err.Error())
		}
		p := params.PushParameter(col, col.CQLType, false)
		err = result.SetValue(p, val)
		if err != nil {
			log.Fatalf("failed to set value: %s", err.Error())
		}
	}
	return result
}
func CreateQueryParameterValuesFromMap(values map[*types.Column]types.GoValue) *types.QueryParameterValues {
	params := types.NewQueryParameters()
	result := types.NewQueryParameterValues(params)
	for col, val := range values {
		p := params.PushParameter(col, col.CQLType, false)
		err := result.SetValue(p, val)
		if err != nil {
			log.Fatalf("failed to set value: %s", err.Error())
		}
	}
	return result
}

func EncodePrimitiveValueOrDie(v any, dt types.CqlDataType, pv primitive.ProtocolVersion) *primitive.Value {
	bytes, err := proxycore.EncodeType(dt.DataType(), pv, v)
	if err != nil {
		log.Fatalf("failed to encode primitive value: %s", err.Error())
	}
	return primitive.NewValue(bytes)
}

func GetTableOrDie(k types.Keyspace, t types.TableName) *schemaMapping.TableConfig {
	config, err := GetSchemaMappingConfig().GetTableConfig(k, t)
	if err != nil {
		log.Fatalf("no such table or keyspace: %s", err.Error())
	}
	return config
}

func GetColumnOrDie(k types.Keyspace, t types.TableName, c types.ColumnName) *types.Column {
	config, err := GetSchemaMappingConfig().GetTableConfig(k, t)
	if err != nil {
		log.Fatalf("no such table or keyspace: %s", err.Error())
	}
	column, err := config.GetColumn(c)
	if err != nil {
		log.Fatalf("no such column `%s` in table %s.%s", err.Error(), k, t)
	}
	return column
}
