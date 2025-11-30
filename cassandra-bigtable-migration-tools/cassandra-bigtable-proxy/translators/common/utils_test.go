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

package common

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/testing/mockdata"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"math"
	"reflect"
	"testing"
)

func Test_formatValues(t *testing.T) {
	type args struct {
		value     string
		cqlType   types.CqlDataType
		protocolV primitive.ProtocolVersion
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name:    "Invalid int",
			args:    args{"abc", types.TypeInt, primitive.ProtocolVersion4},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Invalid bigint",
			args:    args{"abc", types.TypeBigInt, primitive.ProtocolVersion4},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Invalid float",
			args:    args{"abc", types.TypeFloat, primitive.ProtocolVersion4},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Invalid double",
			args:    args{"abc", types.TypeDouble, primitive.ProtocolVersion4},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Invalid boolean",
			args:    args{"abc", types.TypeBoolean, primitive.ProtocolVersion4},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "Invalid timestamp",
			args:    args{"abc", types.TypeTimestamp, primitive.ProtocolVersion4},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EncodeScalarForBigtable(tt.args.value, tt.args.cqlType)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_bindValues(t *testing.T) {
	tests := []struct {
		name          string
		params        *types.QueryParameters
		initialValues map[types.Placeholder]types.GoValue
		values        []*primitive.Value
		pv            primitive.ProtocolVersion
		want          map[types.Placeholder]types.GoValue
		err           string
	}{
		{
			name: "success",
			params: types.NewQueryParameters().
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"), types.TypeVarchar, false).
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"), types.TypeVarchar, false),
			initialValues: make(map[types.Placeholder]types.GoValue),
			values: []*primitive.Value{
				mockdata.EncodePrimitiveValueOrDie("abc", types.TypeText, primitive.ProtocolVersion4),
				mockdata.EncodePrimitiveValueOrDie("def", types.TypeText, primitive.ProtocolVersion4),
			},
			want: map[types.Placeholder]types.GoValue{
				"@value0": "abc",
				"@value1": "def",
			},
			err: "",
		},
		{
			name: "too many input values",
			params: types.NewQueryParameters().
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"), types.TypeVarchar, false).
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"), types.TypeVarchar, false),
			initialValues: make(map[types.Placeholder]types.GoValue),
			values: []*primitive.Value{
				mockdata.EncodePrimitiveValueOrDie("abc", types.TypeText, primitive.ProtocolVersion4),
				mockdata.EncodePrimitiveValueOrDie("def", types.TypeText, primitive.ProtocolVersion4),
				// unexpected value
				mockdata.EncodePrimitiveValueOrDie("xyz", types.TypeText, primitive.ProtocolVersion4),
			},
			want: nil,
			err:  "expected 2 prepared values but got 3",
		},
		{
			name: "too few input values",
			params: types.NewQueryParameters().
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"), types.TypeVarchar, false).
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"), types.TypeVarchar, false),
			initialValues: make(map[types.Placeholder]types.GoValue),
			values: []*primitive.Value{
				mockdata.EncodePrimitiveValueOrDie("abc", types.TypeText, primitive.ProtocolVersion4),
			},
			want: nil,
			err:  "expected 2 prepared values but got 1",
		},
		{
			name: "wrong input type",
			params: types.NewQueryParameters().
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "user_info", "age"), types.NewListType(types.TypeBigInt), false),
			initialValues: make(map[types.Placeholder]types.GoValue),
			values: []*primitive.Value{
				mockdata.EncodePrimitiveValueOrDie("abcdefgh", types.TypeText, primitive.ProtocolVersion4),
			},
			want: nil,
			err:  "cannot decode CQL list<bigint>",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values, err := BindQueryParams(tt.params, tt.initialValues, tt.values, tt.pv)
			if tt.err != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, values.AsMap())
		})
	}
}

func Test_validateRequiredPrimaryKeys(t *testing.T) {
	tests := []struct {
		name  string
		table *schemaMapping.TableConfig
		keys  *types.QueryParameters
		err   string
	}{
		{
			name:  "success",
			table: mockdata.GetTableOrDie("test_keyspace", "test_table"),
			keys: types.NewQueryParameters().
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"), types.TypeVarchar, false).
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"), types.TypeVarchar, false),
			err: "",
		},
		{
			name:  "missing pk2",
			table: mockdata.GetTableOrDie("test_keyspace", "test_table"),
			keys: types.NewQueryParameters().
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"), types.TypeVarchar, false),
			err: "todo",
		},
		{
			name:  "extra columns ok",
			table: mockdata.GetTableOrDie("test_keyspace", "test_table"),
			keys: types.NewQueryParameters().
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk1"), types.TypeVarchar, false).
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "test_table", "col_text"), types.TypeVarchar, false).
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "test_table", "pk2"), types.TypeVarchar, false),
			err: "",
		},
		{
			name:  "mixed types",
			table: mockdata.GetTableOrDie("test_keyspace", "user_info"),
			keys: types.NewQueryParameters().
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "user_info", "name"), types.TypeVarchar, false).
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "user_info", "age"), types.TypeBigInt, false).
				BuildParameter(mockdata.GetColumnOrDie("test_keyspace", "user_info", "username"), types.TypeVarchar, false),
			err: "",
		},

		{
			name:  "no params",
			table: mockdata.GetTableOrDie("test_keyspace", "test_table"),
			keys:  types.NewQueryParameters(),
			err:   "todo",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRequiredPrimaryKeys(tt.table, tt.keys)
			if tt.err != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCreateOrderedCodeKey(t *testing.T) {
	tests := []struct {
		name        string
		tableConfig *schemaMapping.TableConfig
		values      map[types.ColumnName]types.GoValue
		want        []byte
		wantErr     bool
	}{
		{
			name: "simple string",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values:  map[types.ColumnName]types.GoValue{"user_id": "user1"},
			want:    []byte("user1"),
			wantErr: false,
		},
		{
			name: "int nonzero",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeBigInt, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values:  map[types.ColumnName]types.GoValue{"user_id": int64(1)},
			want:    []byte("\x81"),
			wantErr: false,
		},
		{
			name: "int32 nonzero",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeInt, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values:  map[types.ColumnName]types.GoValue{"user_id": int32(1)},
			want:    []byte("\x81"),
			wantErr: false,
		},
		{
			name: "int32 nonzero big endian",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.BigEndianEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeInt, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values:  map[types.ColumnName]types.GoValue{"user_id": int32(1)},
			want:    []byte("\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x01"),
			wantErr: false,
		},
		{
			name: "int32 max",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.BigEndianEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeInt, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values:  map[types.ColumnName]types.GoValue{"user_id": int32(2147483647)},
			want:    []byte("\x00\xff\x00\xff\x00\xff\x00\xff\x7f\xff\xff\xff"),
			wantErr: false,
		},
		{
			name: "int64 max",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.BigEndianEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeBigInt, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values:  map[types.ColumnName]types.GoValue{"user_id": int64(9223372036854775807)},
			want:    []byte("\x7f\xff\xff\xff\xff\xff\xff\xff"),
			wantErr: false,
		},
		{
			name: "negative int",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeBigInt, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values:  map[types.ColumnName]types.GoValue{"user_id": int64(-1)},
			want:    []byte("\x7f"),
			wantErr: false,
		},
		{
			name: "negative int big endian fails",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.BigEndianEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeBigInt, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values:  map[types.ColumnName]types.GoValue{"user_id": int64(-1)},
			want:    nil,
			wantErr: true,
		},
		{
			name: "int zero",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeBigInt, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values:  map[types.ColumnName]types.GoValue{"user_id": int64(0)},
			want:    []byte("\x80"),
			wantErr: false,
		},
		{
			name: "int64 minvalue",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeBigInt, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values:  map[types.ColumnName]types.GoValue{"user_id": int64(math.MinInt64)},
			want:    []byte("\x00\xff\x3f\x80\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff"),
			wantErr: false,
		},
		{
			name: "int64 negative value with leading null byte",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeBigInt, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values:  map[types.ColumnName]types.GoValue{"user_id": int64(-922337203685473)},
			want:    []byte("\x00\xff\xfc\xb9\x23\xa2\x9c\x77\x9f"),
			wantErr: false,
		},
		{
			name: "int32 minvalue",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeInt, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values:  map[types.ColumnName]types.GoValue{"user_id": int64(math.MinInt32)},
			want:    []byte("\x07\x80\x00\xff\x00\xff\x00\xff"),
			wantErr: false,
		},
		{
			name: "int minvalue combined",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeBigInt, KeyType: types.KeyTypePartition, PkPrecedence: 1},
				{Name: "other_id", CQLType: types.TypeInt, KeyType: types.KeyTypePartition, PkPrecedence: 2},
				{Name: "yet_another_id", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, PkPrecedence: 3},
			}),
			values:  map[types.ColumnName]types.GoValue{"user_id": int64(math.MinInt64), "other_id": int64(math.MinInt32), "yet_another_id": "id123"},
			want:    []byte("\x00\xff\x3f\x80\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\x01\x07\x80\x00\xff\x00\xff\x00\xff\x00\x01\x69\x64\x31\x32\x33"),
			wantErr: false,
		},
		{
			name: "int mixed",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeBigInt, KeyType: types.KeyTypePartition, PkPrecedence: 1},
				{Name: "other_id", CQLType: types.TypeInt, KeyType: types.KeyTypePartition, PkPrecedence: 2},
			}),
			values:  map[types.ColumnName]types.GoValue{"user_id": int64(-43232545), "other_id": int64(-12451)},
			want:    []byte("\x0d\x6c\x52\xdf\x00\x01\x1f\xcf\x5d"),
			wantErr: false,
		},
		{
			name: "int zero big endian",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.BigEndianEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeBigInt, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values:  map[types.ColumnName]types.GoValue{"user_id": int64(0)},
			want:    []byte("\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff"),
			wantErr: false,
		},
		{
			name: "compound key",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, PkPrecedence: 1},
				{Name: "team_num", CQLType: types.TypeBigInt, KeyType: types.KeyTypeClustering, PkPrecedence: 2},
				{Name: "city", CQLType: types.TypeVarchar, KeyType: types.KeyTypeClustering, PkPrecedence: 3},
			}),
			values: map[types.ColumnName]types.GoValue{
				"user_id":  "user1",
				"team_num": int64(1),
				"city":     "new york",
			},
			want:    []byte("user1\x00\x01\x81\x00\x01new york"),
			wantErr: false,
		},
		{
			name: "compound key big endian",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.BigEndianEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, PkPrecedence: 1},
				{Name: "team_num", CQLType: types.TypeBigInt, KeyType: types.KeyTypeClustering, PkPrecedence: 2},
				{Name: "city", CQLType: types.TypeVarchar, KeyType: types.KeyTypeClustering, PkPrecedence: 3},
			}),
			values: map[types.ColumnName]types.GoValue{
				"user_id":  "user1",
				"team_num": int64(1),
				"city":     "new york",
			},
			want:    []byte("user1\x00\x01\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x01\x00\x01new york"),
			wantErr: false,
		},
		{
			name: "unhandled int row key encoding type",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", 4 /*unhandled type*/, []*types.Column{
				{Name: "user_id", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, PkPrecedence: 1},
				{Name: "team_num", CQLType: types.TypeBigInt, KeyType: types.KeyTypeClustering, PkPrecedence: 2},
				{Name: "city", CQLType: types.TypeVarchar, KeyType: types.KeyTypeClustering, PkPrecedence: 3},
			}),
			values: map[types.ColumnName]types.GoValue{
				"user_id":  "user1",
				"team_num": int64(1),
				"city":     "new york",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "compound key with trailing empty",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, PkPrecedence: 1},
				{Name: "team_num", CQLType: types.TypeBigInt, KeyType: types.KeyTypeClustering, PkPrecedence: 2},
				{Name: "city", CQLType: types.TypeVarchar, KeyType: types.KeyTypeClustering, PkPrecedence: 3},
				{Name: "borough", CQLType: types.TypeVarchar, KeyType: types.KeyTypeClustering, PkPrecedence: 4},
			}),
			values: map[types.ColumnName]types.GoValue{
				"user_id":  "user3",
				"team_num": int64(3),
				"city":     "",
				"borough":  "",
			},
			want:    []byte("user3\x00\x01\x83"),
			wantErr: false,
		},
		{
			name: "compound key with trailing empty big endian",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.BigEndianEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, PkPrecedence: 1},
				{Name: "team_num", CQLType: types.TypeBigInt, KeyType: types.KeyTypeClustering, PkPrecedence: 2},
				{Name: "city", CQLType: types.TypeVarchar, KeyType: types.KeyTypeClustering, PkPrecedence: 3},
				{Name: "borough", CQLType: types.TypeVarchar, KeyType: types.KeyTypeClustering, PkPrecedence: 4},
			}),
			values: map[types.ColumnName]types.GoValue{
				"user_id":  "user3",
				"team_num": int64(3),
				"city":     "",
				"borough":  "",
			},
			want:    []byte("user3\x00\x01\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x03"),
			wantErr: false,
		},
		{
			name: "compound key with empty middle",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeBlob, KeyType: types.KeyTypePartition, PkPrecedence: 1},
				{Name: "team_id", CQLType: types.TypeBlob, KeyType: types.KeyTypeClustering, PkPrecedence: 2},
				{Name: "city", CQLType: types.TypeBlob, KeyType: types.KeyTypeClustering, PkPrecedence: 3},
			}),
			values: map[types.ColumnName]types.GoValue{
				"user_id": "\xa2",
				"team_id": "",
				"city":    "\xb7",
			},
			want:    []byte("\xa2\x00\x01\x00\x00\x00\x01\xb7"),
			wantErr: false,
		},
		{
			name: "bytes with delimiter",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeBlob, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values: map[types.ColumnName]types.GoValue{
				"user_id": "\x80\x00\x01\x81",
			},
			want:    []byte("\x80\x00\xff\x01\x81"),
			wantErr: false,
		},
		{
			name: "compound key with 2 empty middle fields",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeBlob, KeyType: types.KeyTypePartition, PkPrecedence: 1},
				{Name: "team_num", CQLType: types.TypeBlob, KeyType: types.KeyTypeClustering, PkPrecedence: 2},
				{Name: "city", CQLType: types.TypeBlob, KeyType: types.KeyTypeClustering, PkPrecedence: 3},
				{Name: "borough", CQLType: types.TypeBlob, KeyType: types.KeyTypeClustering, PkPrecedence: 4},
			}),
			values: map[types.ColumnName]types.GoValue{
				"user_id":  "\xa2",
				"team_num": "",
				"city":     "",
				"borough":  "\xb7",
			},
			want:    []byte("\xa2\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01\xb7"),
			wantErr: false,
		},
		{
			name: "byte strings",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeBlob, KeyType: types.KeyTypePartition, PkPrecedence: 1},
				{Name: "city", CQLType: types.TypeBlob, KeyType: types.KeyTypeClustering, PkPrecedence: 2},
			}),
			values: map[types.ColumnName]types.GoValue{
				"user_id": "\xa5",
				"city":    "\x90",
			},
			want:    []byte("\xa5\x00\x01\x90"),
			wantErr: false,
		},
		{
			name: "empty first value",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, PkPrecedence: 1},
				{Name: "city", CQLType: types.TypeBlob, KeyType: types.KeyTypeClustering, PkPrecedence: 2},
			}),
			values: map[types.ColumnName]types.GoValue{
				"user_id": "",
				"city":    "\xaa",
			},
			want:    []byte("\x00\x00\x00\x01\xaa"),
			wantErr: false,
		},
		{
			name: "null escaped",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, PkPrecedence: 1},
				{Name: "city", CQLType: types.TypeVarchar, KeyType: types.KeyTypeClustering, PkPrecedence: 2},
				{Name: "borough", CQLType: types.TypeVarchar, KeyType: types.KeyTypeClustering, PkPrecedence: 3},
			}),
			values: map[types.ColumnName]types.GoValue{
				"user_id": "nn",
				"city":    "t\x00t",
				"borough": "end",
			},
			want:    []byte("nn\x00\x01t\x00\xfft\x00\x01end"),
			wantErr: false,
		},
		{
			name: "null escaped (big endian)",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.BigEndianEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, PkPrecedence: 1},
				{Name: "team_num", CQLType: types.TypeBigInt, KeyType: types.KeyTypeClustering, PkPrecedence: 2},
				{Name: "city", CQLType: types.TypeVarchar, KeyType: types.KeyTypeClustering, PkPrecedence: 3},
			}),
			values: map[types.ColumnName]types.GoValue{
				"user_id":  "abcd",
				"team_num": int64(45),
				"city":     "name",
			},
			want:    []byte("abcd\x00\x01\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x00\xff\x2d\x00\x01name"),
			wantErr: false,
		},
		{
			name: "invalid utf8 varchar returns error",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values: map[types.ColumnName]types.GoValue{
				"user_id": string([]uint8{182}),
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "null char",
			tableConfig: schemaMapping.NewTableConfig("keyspace", "table", "cf1", types.OrderedCodeEncoding, []*types.Column{
				{Name: "user_id", CQLType: types.TypeVarchar, KeyType: types.KeyTypePartition, PkPrecedence: 1},
			}),
			values: map[types.ColumnName]types.GoValue{
				"user_id": "\x00\x01",
			},
			want:    []byte("\x00\xff\x01"),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := mockdata.CreateQueryParameterValuesFromMap2(tt.tableConfig, tt.values)
			got, err := BindRowKey(tt.tableConfig, values)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEncodeBool(t *testing.T) {
	tests := []struct {
		name string
		args struct {
			value interface{}
			pv    primitive.ProtocolVersion
		}
		want    []byte
		wantErr bool
	}{
		{
			name: "Valid string 'true'",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: "true",
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 1},
			wantErr: false,
		},
		{
			name: "Valid string 'false'",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: "false",
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 0},
			wantErr: false,
		},
		{
			name: "String parsing error",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: "notabool",
				pv:    primitive.ProtocolVersion4,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Valid bool true",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: true,
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 1},
			wantErr: false,
		},
		{
			name: "Valid bool false",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: false,
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 0},
			wantErr: false,
		},
		{
			name: "Valid []byte input for true",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: []byte{1},
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 1},
			wantErr: false,
		},
		{
			name: "Valid []byte input for false",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: []byte{0},
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 0},
			wantErr: false,
		},
		{
			name: "Unsupported type",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: 123,
				pv:    primitive.ProtocolVersion4,
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encodeBoolForBigtable(tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("encodeBoolForBigtable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("encodeBoolForBigtable() = %v, wantNewColumns %v", got, tt.want)
			}
		})
	}
}

func TestEncodeInt(t *testing.T) {
	tests := []struct {
		name string
		args struct {
			value interface{}
			pv    primitive.ProtocolVersion
		}
		want    []byte
		wantErr bool
	}{
		{
			name: "Valid string input",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: "12",
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 12}, // Replace with the bytes you expect
			wantErr: false,
		},
		{
			name: "String parsing error",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: "abc",
				pv:    primitive.ProtocolVersion4,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Valid int32 input",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: int32(12),
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 12}, // Replace with the bytes you expect
			wantErr: false,
		},
		{
			name: "Valid []byte input",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: []byte{0, 0, 0, 12}, // Replace with actual bytes representing an int32 value
				pv:    primitive.ProtocolVersion4,
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 12}, // Replace with the bytes you expect
			wantErr: false,
		},
		{
			name: "Unsupported type",
			args: struct {
				value interface{}
				pv    primitive.ProtocolVersion
			}{
				value: 12.34, // Unsupported float64 type.
				pv:    primitive.ProtocolVersion4,
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := encodeBigIntForBigtable(tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("encodeBigIntForBigtable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("encodeBigIntForBigtable() = %v, wantNewColumns %v", got, tt.want)
			}
		})
	}
}

func TestTrimQuotes(t *testing.T) {
	tests := []struct {
		value string
		want  string
	}{
		{
			value: `foo`,
			want:  `foo`,
		},
		{
			value: `f`,
			want:  `f`,
		},
		{
			value: `fo`,
			want:  `fo`,
		},
		{
			value: ``,
			want:  ``,
		},
		{
			value: `''`,
			want:  ``,
		},
		{
			value: `'foo'`,
			want:  `foo`,
		},
		{
			value: `'''foo'`,
			want:  `'foo`,
		},
		{
			value: `'foo'''`,
			want:  `foo'`,
		},
		// only trim the outermost quotes
		{
			value: `'sister''s'`,
			want:  `sister's`,
		},
		// should keep inner quotes
		{
			value: `'"foo"'`,
			want:  `"foo"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			got := TrimQuotes(tt.value)
			assert.Equal(t, tt.want, got)
		})
	}
}
