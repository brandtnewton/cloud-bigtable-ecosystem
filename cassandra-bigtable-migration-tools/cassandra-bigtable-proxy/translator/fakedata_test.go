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
	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"go.uber.org/zap"
)

// GetSchemaMappingConfig builds and returns the mock schema configuration using constructors.
func GetSchemaMappingConfig(intRowKeyEncoding types.IntRowKeyEncodingType) *schemaMapping.SchemaMappingConfig {
	const systemColumnFamily = "cf1"

	testTableColumns := []*types.Column{
		{Name: "column1", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Varchar), KeyType: utilities.KEY_TYPE_PARTITION, PkPrecedence: 1},
		{Name: "column10", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Varchar), KeyType: utilities.KEY_TYPE_CLUSTERING, PkPrecedence: 2},
		{Name: "column2", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Blob), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column3", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Boolean), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column5", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Timestamp), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column6", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Int), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column7", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewSetType(datatype.Varchar)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column8", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Varchar, datatype.Boolean)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "column9", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Bigint), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "blob_col", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Blob), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "text_col", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Varchar), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "bool_col", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Boolean), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "timestamp_col", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Timestamp), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "int_col", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Int), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "set_text_col", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewSetType(datatype.Varchar)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "map_text_bool_col", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Varchar, datatype.Boolean)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "bigint_col", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Bigint), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "float_col", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Float), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "double_col", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Double), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "map_text_text", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Varchar, datatype.Varchar)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "list_text", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewListType(datatype.Varchar)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "counter_col", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Counter), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "frozen_list_text", TypeInfo: types.NewCqlTypeInfo("frozen<list<text>>", datatype.NewListType(datatype.Varchar), true), KeyType: utilities.KEY_TYPE_REGULAR},
	}

	intTableColumns := []*types.Column{
		{Name: "num", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Int), KeyType: utilities.KEY_TYPE_PARTITION, PkPrecedence: 1},
		{Name: "big_num", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Bigint), KeyType: utilities.KEY_TYPE_CLUSTERING, PkPrecedence: 2},
		{Name: "name", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Varchar), KeyType: utilities.KEY_TYPE_REGULAR},
	}

	userInfoColumns := []*types.Column{
		{Name: "name", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Varchar), KeyType: utilities.KEY_TYPE_PARTITION, PkPrecedence: 1},
		{Name: "age", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Int), KeyType: utilities.KEY_TYPE_CLUSTERING, PkPrecedence: 2},
	}

	nonPrimitiveTableColumns := []*types.Column{
		{Name: "pk_1_text", TypeInfo: types.NewCqlTypeInfoFromType(datatype.Varchar), KeyType: utilities.KEY_TYPE_PARTITION, PkPrecedence: 1},
		{Name: "map_text_text", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Varchar, datatype.Varchar)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "map_text_int", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Varchar, datatype.Int)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "map_text_float", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Varchar, datatype.Float)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "map_text_double", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Varchar, datatype.Double)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "map_text_timestamp", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Varchar, datatype.Timestamp)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "map_timestamp_text", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Timestamp, datatype.Varchar)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "map_timestamp_int", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Timestamp, datatype.Int)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "map_timestamp_boolean", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Timestamp, datatype.Boolean)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "map_timestamp_timestamp", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Timestamp, datatype.Timestamp)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "map_timestamp_bigint", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Timestamp, datatype.Bigint)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "map_timestamp_float", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Timestamp, datatype.Float)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "map_timestamp_double", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Timestamp, datatype.Double)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "set_text", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewSetType(datatype.Varchar)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "set_boolean", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewSetType(datatype.Boolean)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "set_int", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewSetType(datatype.Int)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "set_float", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewSetType(datatype.Float)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "set_double", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewSetType(datatype.Double)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "set_bigint", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewSetType(datatype.Bigint)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "set_timestamp", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewSetType(datatype.Timestamp)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "map_text_boolean", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Varchar, datatype.Boolean)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "map_text_bigint", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewMapType(datatype.Varchar, datatype.Bigint)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "list_text", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewListType(datatype.Varchar)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "list_int", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewListType(datatype.Int)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "list_float", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewListType(datatype.Float)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "list_double", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewListType(datatype.Double)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "list_boolean", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewListType(datatype.Boolean)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "list_timestamp", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewListType(datatype.Timestamp)), KeyType: utilities.KEY_TYPE_REGULAR},
		{Name: "list_bigint", TypeInfo: types.NewCqlTypeInfoFromType(datatype.NewListType(datatype.Bigint)), KeyType: utilities.KEY_TYPE_REGULAR},
	}

	allTableConfigs := []*schemaMapping.TableConfig{
		schemaMapping.NewTableConfig("test_keyspace", "test_table", systemColumnFamily, intRowKeyEncoding, testTableColumns),
		schemaMapping.NewTableConfig("test_keyspace", "int_table", systemColumnFamily, intRowKeyEncoding, intTableColumns),
		schemaMapping.NewTableConfig("test_keyspace", "user_info", systemColumnFamily, intRowKeyEncoding, userInfoColumns),
		schemaMapping.NewTableConfig("test_keyspace", "non_primitive_table", systemColumnFamily, intRowKeyEncoding, nonPrimitiveTableColumns),
	}

	return schemaMapping.NewSchemaMappingConfig("schema_mapping", systemColumnFamily, zap.NewNop(), allTableConfigs)
}
