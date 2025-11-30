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
package utilities

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsCollectionDataType(t *testing.T) {
	testCases := []struct {
		input datatype.DataType
		want  bool
	}{
		{datatype.Varchar, false},
		{datatype.Blob, false},
		{datatype.Bigint, false},
		{datatype.Boolean, false},
		{datatype.Date, false},
		{datatype.NewMapType(datatype.Varchar, datatype.Boolean), true},
		{datatype.NewListType(datatype.Int), true},
		{datatype.NewSetType(datatype.Varchar), true},
	}

	for _, tt := range testCases {
		t.Run(tt.input.String(), func(t *testing.T) {
			got := IsCollection(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseCqlTimestamp(t *testing.T) {
	// Define a helper to create times safely without error checking in the struct
	mustParse := func(layout, value string) time.Time {
		t, err := time.Parse(layout, value)
		if err != nil {
			panic(err)
		}
		return t
	}

	tests := []struct {
		name    string
		input   string
		want    time.Time
		wantErr bool
	}{
		{
			name:    "Standard RFC3339",
			input:   "2023-11-27T10:30:00Z",
			want:    mustParse(time.RFC3339, "2023-11-27T10:30:00Z"),
			wantErr: false,
		},
		{
			name:  "Simple Date (YYYY-MM-DD)",
			input: "2023-11-27",
			// Assuming simple date defaults to midnight UTC
			want:    mustParse("2006-01-02", "2023-11-27"),
			wantErr: false,
		},
		{
			name:    "Date with Space and Time",
			input:   "2023-11-27 10:30:00",
			want:    mustParse("2006-01-02 15:04:05", "2023-11-27 10:30:00"),
			wantErr: false,
		},
		{
			name:    "Date with Milliseconds",
			input:   "2023-11-27 10:30:00.555",
			want:    mustParse("2006-01-02 15:04:05.000", "2023-11-27 10:30:00.555"),
			wantErr: false,
		},
		{
			name:    "Invalid Format - Nonsense String",
			input:   "not-a-timestamp",
			want:    time.Time{},
			wantErr: true,
		},
		{
			name:    "Invalid Date - Month out of range",
			input:   "2023-13-01 10:00:00",
			want:    time.Time{},
			wantErr: true,
		},
		{
			name:    "Empty String",
			input:   "",
			want:    time.Time{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCqlTimestamp(tt.input)

			// 1. Check Error State
			if (err != nil) != tt.wantErr {
				t.Errorf("parseCqlTimestamp() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// 2. Check Time Equality
			// We use .Equal() rather than == to handle monotonic clock differences
			// and timezone pointer differences.
			if !tt.wantErr && !got.Equal(tt.want) {
				t.Errorf("parseCqlTimestamp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStringToPrimitives(t *testing.T) {
	tests := []struct {
		value    string
		cqlType  types.CqlDataType
		expected interface{}
		hasError bool
	}{
		{"123", types.TypeInt, int32(123), false},
		{"not_an_int", types.TypeInt, nil, true},
		{"123456789", types.TypeBigInt, int64(123456789), false},
		{"not_a_bigint", types.TypeBigInt, nil, true},
		{"3.14", types.TypeFloat, float32(3.14), false},
		{"not_a_float", types.TypeFloat, nil, true},
		{"3.1415926535", types.TypeDouble, float64(3.1415926535), false},
		{"not_a_double", types.TypeDouble, nil, true},
		{"true", types.TypeBoolean, true, false},
		{"false", types.TypeBoolean, false, false},
		{"not_a_boolean", types.TypeBoolean, nil, true},
		{"blob_data", types.TypeBlob, "blob_data", false},
		{"hello", types.TypeVarchar, "hello", false},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s_%s", tt.cqlType, tt.value), func(t *testing.T) {
			result, err := StringToGo(tt.value, tt.cqlType)
			if tt.hasError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
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
			got, err := EncodeInt(tt.args.value, tt.args.pv)
			if (err != nil) != tt.wantErr {
				t.Errorf("EncodeInt() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EncodeInt() = %v, want %v", got, tt.want)
			}
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
			got, err := EncodeBool(tt.args.value, tt.args.pv)
			if (err != nil) != tt.wantErr {
				t.Errorf("EncodeBool() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("EncodeBool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataConversionInInsertionIfRequired(t *testing.T) {
	tests := []struct {
		name string
		args struct {
			value        interface{}
			pv           primitive.ProtocolVersion
			cqlType      string
			responseType string
		}
		want    interface{}
		wantErr bool
	}{
		{
			name: "Boolean to string true",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        "true",
				pv:           primitive.ProtocolVersion4,
				cqlType:      "boolean",
				responseType: "string",
			},
			want:    "1",
			wantErr: false,
		},
		{
			name: "Boolean to string false",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        "false",
				pv:           primitive.ProtocolVersion4,
				cqlType:      "boolean",
				responseType: "string",
			},
			want:    "0",
			wantErr: false,
		},
		{
			name: "Invalid boolean string",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				// value is not a valid boolean string
				value:        "notabool",
				pv:           primitive.ProtocolVersion4,
				cqlType:      "boolean",
				responseType: "string",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Boolean to EncodeBool",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        true,
				pv:           primitive.ProtocolVersion4,
				cqlType:      "boolean",
				responseType: "default",
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 1}, // Expecting boolean encoding, replace as needed
			wantErr: false,
		},
		{
			name: "Int to string",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        "123",
				pv:           primitive.ProtocolVersion4,
				cqlType:      "int",
				responseType: "string",
			},
			want:    "123",
			wantErr: false,
		},
		{
			name: "Invalid int string",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				// value is not a valid int string
				value:        "notanint",
				pv:           primitive.ProtocolVersion4,
				cqlType:      "int",
				responseType: "string",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Int to EncodeInt",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        int32(12),
				pv:           primitive.ProtocolVersion4,
				cqlType:      "int",
				responseType: "default",
			},
			want:    []byte{0, 0, 0, 0, 0, 0, 0, 12}, // Expecting int encoding, replace as needed
			wantErr: false,
		},
		{
			name: "Unsupported cqlType",
			args: struct {
				value        interface{}
				pv           primitive.ProtocolVersion
				cqlType      string
				responseType string
			}{
				value:        "anything",
				pv:           primitive.ProtocolVersion4,
				cqlType:      "unsupported",
				responseType: "default",
			},
			want:    "anything",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DataConversionInInsertionIfRequired(tt.args.value, tt.args.pv, tt.args.cqlType, tt.args.responseType)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataConversionInInsertionIfRequired() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataConversionInInsertionIfRequired() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsSupportedPrimaryKeyType(t *testing.T) {
	testCases := []struct {
		name     string
		input    types.CqlDataType
		expected bool
	}{
		{"Supported CQLType - Int", ParseCqlTypeOrDie("int"), true},
		{"Supported CQLType - Bigint", ParseCqlTypeOrDie("bigint"), true},
		{"Supported CQLType - Varchar", ParseCqlTypeOrDie("varchar"), true},
		{"Unsupported CQLType - Boolean", ParseCqlTypeOrDie("boolean"), false},
		{"Unsupported CQLType - Float", ParseCqlTypeOrDie("float"), false},
		{"Unsupported CQLType - Blob", ParseCqlTypeOrDie("blob"), false},
		{"Unsupported CQLType - List", ParseCqlTypeOrDie("list<int>"), false},
		{"Unsupported CQLType - Frozen", ParseCqlTypeOrDie("frozen<list<int>>"), false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsSupportedPrimaryKeyType(tc.input)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestIsSupportedCollectionElementType(t *testing.T) {
	testCases := []struct {
		name     string
		input    datatype.DataType
		expected bool
	}{
		{"Supported CQLType - Int", datatype.Int, true},
		{"Supported CQLType - Bigint", datatype.Bigint, true},
		{"Supported CQLType - Varchar", datatype.Varchar, true},
		{"Supported CQLType - Float", datatype.Float, true},
		{"Supported CQLType - Double", datatype.Double, true},
		{"Supported CQLType - Timestamp", datatype.Timestamp, true},
		{"Supported CQLType - Boolean", datatype.Boolean, true},
		{"Unsupported CQLType - Blob", datatype.Blob, false},
		{"Unsupported CQLType - UUID", datatype.Uuid, false},
		{"Unsupported CQLType - Map", datatype.NewMapType(datatype.Varchar, datatype.Int), false},
		{"Unsupported CQLType - Set", datatype.NewSetType(datatype.Varchar), false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := isSupportedCollectionElementType(tc.input)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestIsSupportedColumnType(t *testing.T) {
	testCases := []struct {
		name     string
		input    types.CqlDataType
		expected bool
	}{
		// --- Positive Cases: Primitive Types ---
		{"Supported Primitive - Int", ParseCqlTypeOrDie("int"), true},
		{"Supported Primitive - Bigint", ParseCqlTypeOrDie("bigint"), true},
		{"Supported Primitive - Blob", ParseCqlTypeOrDie("blob"), true},
		{"Supported Primitive - Boolean", ParseCqlTypeOrDie("boolean"), true},
		{"Supported Primitive - Double", ParseCqlTypeOrDie("double"), true},
		{"Supported Primitive - Float", ParseCqlTypeOrDie("float"), true},
		{"Supported Primitive - Timestamp", ParseCqlTypeOrDie("timestamp"), true},
		{"Supported Primitive - Varchar", ParseCqlTypeOrDie("varchar"), true},

		// --- Positive Cases: Collection Types ---
		{"Supported List", ParseCqlTypeOrDie("list<int>"), true},
		{"Supported Set", ParseCqlTypeOrDie("set<varchar>"), true},
		{"Supported Map", ParseCqlTypeOrDie("map<timestamp,float>"), true},
		{"Supported Map with Text Key", ParseCqlTypeOrDie("map<text,bigint>"), true},

		// --- Negative Cases: Primitive Types ---
		{"Unsupported Primitive - UUID", ParseCqlTypeOrDie("uuid"), false},
		{"Unsupported Primitive - TimeUUID", ParseCqlTypeOrDie("timeuuid"), false},

		// --- Negative Cases: Collection Types ---
		{"Unsupported List Element", ParseCqlTypeOrDie("list<uuid>"), false},
		{"Unsupported List Element", types.NewListType(types.NewListType(types.TypeInt)), false},
		{"Unsupported Set Element", ParseCqlTypeOrDie("set<blob>"), false},
		{"Unsupported Map Key", ParseCqlTypeOrDie("map<blob,text>"), false},
		{"Unsupported Map ValuePlaceholder", ParseCqlTypeOrDie("map<text,uuid>"), false},
		{"Nested Collection - List of Maps", types.NewListType(types.NewMapType(types.TypeVarchar, types.TypeInt)), false},
		// --- Negative Cases: Frozen Types ---
		{"Frozen List", ParseCqlTypeOrDie("frozen<list<int>>"), false},
		{"Frozen Set", ParseCqlTypeOrDie("frozen<set<text>>"), false},
		{"Frozen Map", ParseCqlTypeOrDie("frozen<map<timestamp, float>>"), false},
		{"Frozen Map with Text Key", ParseCqlTypeOrDie("frozen<map<text, bigint>>"), false},
		{"List of frozen lists", ParseCqlTypeOrDie("list<frozen<list<bigint>>>"), false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := IsSupportedColumnType(tc.input)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestGetCassandraColumnType(t *testing.T) {
	const NO_ERROR_EXPECTED = ""
	testCases := []struct {
		input    string
		wantType types.CqlDataType
		wantErr  string
	}{
		{"text", types.TypeText, NO_ERROR_EXPECTED},
		{"blob", types.TypeBlob, NO_ERROR_EXPECTED},
		{"timestamp", types.TypeTimestamp, NO_ERROR_EXPECTED},
		{"TimeSTAmp", types.TypeTimestamp, NO_ERROR_EXPECTED},
		{"int", types.TypeInt, NO_ERROR_EXPECTED},
		{"float", types.TypeFloat, NO_ERROR_EXPECTED},
		{"double", types.TypeDouble, NO_ERROR_EXPECTED},
		{"bigint", types.TypeBigInt, NO_ERROR_EXPECTED},
		{"boolean", types.TypeBoolean, NO_ERROR_EXPECTED},
		{"uuid", types.TypeUuid, NO_ERROR_EXPECTED},
		{"map<text, boolean>", types.NewMapType(types.TypeText, types.TypeBoolean), NO_ERROR_EXPECTED},
		{"map<varchar, boolean>", types.NewMapType(types.TypeVarchar, types.TypeBoolean), NO_ERROR_EXPECTED},
		{"map<text, text>", types.NewMapType(types.TypeText, types.TypeText), NO_ERROR_EXPECTED},
		{"map<text, varchar>", types.NewMapType(types.TypeText, types.TypeVarchar), NO_ERROR_EXPECTED},
		{"map<varchar,text>", types.NewMapType(types.TypeVarchar, types.TypeText), NO_ERROR_EXPECTED},
		{"map<varchar, varchar>", types.NewMapType(types.TypeVarchar, types.TypeVarchar), NO_ERROR_EXPECTED},
		{"maP<vARCHar, varcHAr>", types.NewMapType(types.TypeVarchar, types.TypeVarchar), NO_ERROR_EXPECTED},
		{"map<varchar>", nil, "expected exactly 2 types but found 1 in: 'map<varchar>'"},
		{"map<varchar,varchar,varchar>", nil, "expected exactly 2 types but found 3 in: 'map<varchar,varchar,varchar>'"},
		{"map<>", nil, "empty type definition in 'map<>'"},
		{"int<>", nil, "unexpected data type definition: 'int<>'"},
		{"list<text>", types.NewListType(types.TypeText), NO_ERROR_EXPECTED},
		{"list<varchar>", types.NewListType(types.TypeVarchar), NO_ERROR_EXPECTED},
		{"frozen<list<text>>", types.NewFrozenType(types.NewListType(types.TypeText)), NO_ERROR_EXPECTED},
		{"frozen<list<varchar>>", types.NewFrozenType(types.NewListType(types.TypeVarchar)), NO_ERROR_EXPECTED},
		{"set<text>", types.NewSetType(types.TypeText), NO_ERROR_EXPECTED},
		{"set<text", nil, "missing closing type bracket in: 'set<text'"},
		{"set", nil, "data type definition missing in: 'set'"},
		{"frozen", nil, "data type definition missing in: 'frozen'"},
		{"map", nil, "data type definition missing in: 'map'"},
		{"list", nil, "data type definition missing in: 'list'"},
		{"set<", nil, "failed"}, // we don't get a good parsing error from here but that's ok
		{"set<varchar>", types.NewSetType(types.TypeVarchar), NO_ERROR_EXPECTED},
		{"frozen<set<text>>", types.NewFrozenType(types.NewSetType(types.TypeText)), NO_ERROR_EXPECTED},
		{"frozen<set<varchar>>", types.NewFrozenType(types.NewSetType(types.TypeVarchar)), NO_ERROR_EXPECTED},
		{"unknown", nil, "unknown data type name: 'unknown'"},
		{"", nil, "unknown data type name: ''"},
		{"<>list", nil, "unknown data type name: ''"},
		{"<int>", nil, "unknown data type name: '<int'"},
		{"map<map<text,int>", nil, "expected exactly 2 types but found 1 in: 'map<map<text,int>'"},
		{"map<map<text,int>,text>", nil, "map key types must be scalar"},
		{"map<text, frozen<map<text,int>>>", types.NewMapType(types.TypeText, types.NewFrozenType(types.NewMapType(types.TypeText, types.TypeInt))), NO_ERROR_EXPECTED},
		// Future scope items below:
		{"map<text, int>", types.NewMapType(types.TypeText, types.TypeInt), NO_ERROR_EXPECTED},
		{"map<varchar, int>", types.NewMapType(types.TypeVarchar, types.TypeInt), NO_ERROR_EXPECTED},
		{"map<text, bigint>", types.NewMapType(types.TypeText, types.TypeBigInt), NO_ERROR_EXPECTED},
		{"map<varchar, bigint>", types.NewMapType(types.TypeVarchar, types.TypeBigInt), NO_ERROR_EXPECTED},
		{"map<text, float>", types.NewMapType(types.TypeText, types.TypeFloat), NO_ERROR_EXPECTED},
		{"map<varchar, float>", types.NewMapType(types.TypeVarchar, types.TypeFloat), NO_ERROR_EXPECTED},
		{"map<text, double>", types.NewMapType(types.TypeText, types.TypeDouble), NO_ERROR_EXPECTED},
		{"map<varchar, double>", types.NewMapType(types.TypeVarchar, types.TypeDouble), NO_ERROR_EXPECTED},
		{"map<text, timestamp>", types.NewMapType(types.TypeText, types.TypeTimestamp), NO_ERROR_EXPECTED},
		{"map<varchar, timestamp>", types.NewMapType(types.TypeVarchar, types.TypeTimestamp), NO_ERROR_EXPECTED},
		{"map<timestamp, text>", types.NewMapType(types.TypeTimestamp, types.TypeText), NO_ERROR_EXPECTED},
		{"map<timestamp, varchar>", types.NewMapType(types.TypeTimestamp, types.TypeVarchar), NO_ERROR_EXPECTED},
		{"map<timestamp, boolean>", types.NewMapType(types.TypeTimestamp, types.TypeBoolean), NO_ERROR_EXPECTED},
		{"map<timestamp, int>", types.NewMapType(types.TypeTimestamp, types.TypeInt), NO_ERROR_EXPECTED},
		{"map<timestamp, bigint>", types.NewMapType(types.TypeTimestamp, types.TypeBigInt), NO_ERROR_EXPECTED},
		{"map<timestamp, float>", types.NewMapType(types.TypeTimestamp, types.TypeFloat), NO_ERROR_EXPECTED},
		{"map<timestamp, double>", types.NewMapType(types.TypeTimestamp, types.TypeDouble), NO_ERROR_EXPECTED},
		{"map<timestamp, timestamp>", types.NewMapType(types.TypeTimestamp, types.TypeTimestamp), NO_ERROR_EXPECTED},
		{"map<timestamp, sdfs>", nil, "failed"}, // we don't get a good error because the lexer doesn't recognize the invalid type
		{"map<intt, int>", nil, "failed"},       // we don't get a good error because the lexer doesn't recognize the invalid type
		{"set<asdfasdf>", nil, "failed"},        // we don't get a good error because the lexer doesn't recognize the invalid type
		{"list<asdfasdf>", nil, "failed"},       // we don't get a good error because the lexer doesn't recognize the invalid type
		{"frozen<asdfasdf>", nil, "failed"},     // we don't get a good error because the lexer doesn't recognize the invalid type
		{"set<int>", types.NewSetType(types.TypeInt), NO_ERROR_EXPECTED},
		{"set<bigint>", types.NewSetType(types.TypeBigInt), NO_ERROR_EXPECTED},
		{"set<float>", types.NewSetType(types.TypeFloat), NO_ERROR_EXPECTED},
		{"set<double>", types.NewSetType(types.TypeDouble), NO_ERROR_EXPECTED},
		{"set<boolean>", types.NewSetType(types.TypeBoolean), NO_ERROR_EXPECTED},
		{"set<timestamp>", types.NewSetType(types.TypeTimestamp), NO_ERROR_EXPECTED},
		{"set<text>", types.NewSetType(types.TypeText), NO_ERROR_EXPECTED},
		{"set<varchar>", types.NewSetType(types.TypeVarchar), NO_ERROR_EXPECTED},
		{"set<frozen<list<varchar>>>", types.NewSetType(types.NewFrozenType(types.NewListType(types.TypeVarchar))), NO_ERROR_EXPECTED},
		{"set<frozen<map<text,int>>>", types.NewSetType(types.NewFrozenType(types.NewMapType(types.TypeText, types.TypeInt))), NO_ERROR_EXPECTED},
		{"list<frozen<map<text,int>>>", types.NewListType(types.NewFrozenType(types.NewMapType(types.TypeText, types.TypeInt))), NO_ERROR_EXPECTED},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			got, err := ParseCqlTypeString(tc.input)
			if tc.wantErr != "" {
				assert.Nil(t, got)
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantType, got)
		})
	}
}

func TestFromDataCode(t *testing.T) {

	// ## Test Cases for Primitive Types ##
	// These tests verify that each primitive type is correctly converted.
	primitiveTests := []struct {
		name     string
		input    datatype.DataType
		expected types.CqlDataType
	}{
		{"Ascii", datatype.Ascii, types.TypeAscii},
		{"Bigint", datatype.Bigint, types.TypeBigInt},
		{"Blob", datatype.Blob, types.TypeBlob},
		{"Boolean", datatype.Boolean, types.TypeBoolean},
		{"Counter", datatype.Counter, types.TypeCounter},
		{"Decimal", datatype.Decimal, types.TypeDecimal},
		{"Double", datatype.Double, types.TypeDouble},
		{"Float", datatype.Float, types.TypeFloat},
		{"Int", datatype.Int, types.TypeInt},
		{"Text", datatype.Varchar, types.TypeVarchar},
		{"Timestamp", datatype.Timestamp, types.TypeTimestamp},
		{"Uuid", datatype.Uuid, types.TypeUuid},
		{"Varchar", datatype.Varchar, types.TypeVarchar},
		{"Varint", datatype.Varint, types.TypeVarint},
		{"Timeuuid", datatype.Timeuuid, types.TypeTimeuuid},
		{"Inet", datatype.Inet, types.TypeInet},
		{"Date", datatype.Date, types.TypeDate},
		{"timestamp", datatype.Time, types.TypeTime},
		{"Smallint", datatype.Smallint, types.TypeSmallint},
		{"Tinyint", datatype.Tinyint, types.TypeTinyint},
	}

	for _, tc := range primitiveTests {
		t.Run(fmt.Sprintf("Primitive_%s", tc.name), func(t *testing.T) {
			got, err := FromDataCode(tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}

	// ## Test Cases for Collection Types ##
	// These tests verify simple and nested collections.
	collectionTests := []struct {
		name     string
		input    datatype.DataType
		expected types.CqlDataType
	}{
		{
			name:     "Simple List",
			input:    datatype.NewListType(datatype.Int),
			expected: types.NewListType(types.TypeInt),
		},
		{
			name:     "Simple Set",
			input:    datatype.NewSetType(datatype.Varchar),
			expected: types.NewSetType(types.TypeVarchar),
		},
		{
			name:     "Simple Map",
			input:    datatype.NewMapType(datatype.Uuid, datatype.Boolean),
			expected: types.NewMapType(types.TypeUuid, types.TypeBoolean),
		},
		{
			name:     "Nested Collection List<Map<Int, Text>>",
			input:    datatype.NewListType(datatype.NewMapType(datatype.Int, datatype.Varchar)),
			expected: types.NewListType(types.NewMapType(types.TypeInt, types.TypeVarchar)),
		},
	}

	for _, tc := range collectionTests {
		t.Run(fmt.Sprintf("Collection_%s", tc.name), func(t *testing.T) {
			got, err := FromDataCode(tc.input)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}

	// ## Test Cases for errs ##
	// These tests ensure the function fails gracefully.
	errorTests := []struct {
		name        string
		input       datatype.DataType
		expectedErr string
	}{
		{
			name:        "Unhandled Primitive Type",
			input:       datatype.NewCustomType("foobar"), // An invalid/unhandled code
			expectedErr: "unhandled type: custom(foobar)",
		},
		{
			name:  "List with unhandled element type",
			input: datatype.NewListType(datatype.NewCustomType("foobar")), // An invalid/unhandled code

			expectedErr: "unhandled type: custom(foobar)",
		},
		{
			name:        "Map with unhandled key type",
			input:       datatype.NewMapType(datatype.Varchar, datatype.NewCustomType("foobar")),
			expectedErr: "unhandled type: custom(foobar)",
		},
		{
			name:        "Map with unhandled key type",
			input:       datatype.NewMapType(datatype.NewCustomType("foobar"), datatype.Varchar),
			expectedErr: "unhandled type: custom(foobar)",
		},
	}

	for _, tc := range errorTests {
		t.Run(fmt.Sprintf("Error_%s", tc.name), func(t *testing.T) {
			_, err := FromDataCode(tc.input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErr)
		})
	}
}
