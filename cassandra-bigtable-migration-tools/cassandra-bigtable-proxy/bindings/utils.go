package bindings

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"strconv"
	"time"
)

const (
	bigtableEncodingVersion = primitive.ProtocolVersion4
	referenceTime           = int64(1262304000000)
	maxNanos                = int32(9999)
)

func encodeGoValueToBigtable(column *types.Column, value types.GoValue) ([]*types.BigtableData, error) {
	if column.CQLType.Code() == types.MAP {
		mt := column.CQLType.(types.MapType)
		mv, ok := value.(map[interface{}]interface{})
		if !ok {
			return nil, errors.New("failed to parse map")
		}

		var results []*types.BigtableData
		for k, v := range mv {
			// todo use key specific encode
			keyEncoded, err := encodeScalarForBigtable(k, mt.KeyType().DataType())
			if err != nil {
				return nil, err
			}
			valueBytes, err := encodeScalarForBigtable(v, mt.ValueType().DataType())
			if err != nil {
				return nil, err
			}
			results = append(results, &types.BigtableData{Family: column.ColumnFamily, Column: types.ColumnQualifier(keyEncoded), Bytes: valueBytes})
		}
		return results, nil
	} else if column.CQLType.Code() == types.LIST {
		lt := column.CQLType.(types.ListType)
		lv, ok := value.([]any)
		if !ok {
			return nil, errors.New("failed to parse list")
		}

		var results []*types.BigtableData
		for i, v := range lv {
			valueBytes, err := encodeScalarForBigtable(v, lt.ElementType().DataType())
			if err != nil {
				return nil, err
			}
			// todo use list index encoder
			results = append(results, &types.BigtableData{Family: column.ColumnFamily, Column: types.ColumnQualifier(fmt.Sprintf("%d", i)), Bytes: valueBytes})
		}
		return results, nil
	} else if column.CQLType.Code() == types.SET {
		st := column.CQLType.(types.ListType)
		lv, ok := value.([]any)
		if !ok {
			return nil, errors.New("failed to parse list")
		}

		var results []*types.BigtableData
		for _, v := range lv {
			// todo use key specific encode
			valueBytes, err := encodeScalarForBigtable(v, st.ElementType().DataType())
			if err != nil {
				return nil, err
			}
			// todo use correct column value
			results = append(results, &types.BigtableData{Family: column.ColumnFamily, Column: types.ColumnQualifier(fmt.Sprintf("%v", valueBytes)), Bytes: []byte("")})
		}
		return results, nil
	}

	v, err := encodeScalarForBigtable(value, column.CQLType.DataType())
	if err != nil {
		return nil, err
	}
	return []*types.BigtableData{{Family: column.ColumnFamily, Column: types.ColumnQualifier(column.Name), Bytes: v}}, nil
}

// encodeScalarForBigtable converts a value to its byte representation based on CQL type.
// Handles type conversion and encoding according to the protocol version.
// Returns error if value type is invalid or encoding fails.
func encodeScalarForBigtable(value types.GoValue, cqlType datatype.DataType) (types.BigtableValue, error) {
	if value == nil {
		return nil, nil
	}

	var iv interface{}
	var dt datatype.DataType
	switch cqlType {
	case datatype.Int, datatype.Bigint:
		return encodeBigIntForBigtable(value)
	case datatype.Float, datatype.Double:
		return encodeFloat64ForBigtable(value)
	case datatype.Boolean:
		return encodeBoolForBigtable(value)
	case datatype.Timestamp:
		return encodeTimestampForBigtable(value)
	case datatype.Blob:
		iv = value
		dt = datatype.Blob
	case datatype.Varchar:
		iv = value
		dt = datatype.Varchar
	default:
		return nil, fmt.Errorf("unsupported CQL type: %s", cqlType)
	}

	bd, err := proxycore.EncodeType(dt, bigtableEncodingVersion, iv)
	if err != nil {
		return nil, fmt.Errorf("error encoding value: %w", err)
	}

	return bd, nil
}

func cassandraValueToGoValue(dt types.CqlDataType, value *primitive.Value, pv primitive.ProtocolVersion) (types.GoValue, error) {
	goValue, err := proxycore.DecodeType(dt.DataType(), pv, value.Contents)
	if err != nil {
		return nil, err
	}
	return goValue, nil
}

// encodeBigIntForBigtable encodes bigint values to bytes.
// Converts bigint values to byte representation with validation.
// Returns error if value is invalid or encoding fails.
func encodeBigIntForBigtable(value interface{}) ([]byte, error) {
	intVal, err := parseCassandraValueToInt64(value, bigtableEncodingVersion)
	if err != nil {
		return nil, err
	}
	result, err := proxycore.EncodeType(datatype.Bigint, bigtableEncodingVersion, intVal)
	if err != nil {
		return nil, fmt.Errorf("failed to encode bigint: %w", err)
	}
	return result, err
}

// encodeBigIntForBigtable encodes bigint values to bytes.
// Converts bigint values to byte representation with validation.
// Returns error if value is invalid or encoding fails.
func encodeFloat64ForBigtable(value interface{}) ([]byte, error) {
	var floatVal float64
	var err error
	switch v := value.(type) {
	case string:
		floatVal, err = strconv.ParseFloat(v, 64)
	case float32:
		floatVal = float64(v)
	case float64:
		floatVal = v
	case []byte:
		floatAny, err := proxycore.DecodeType(datatype.Double, bigtableEncodingVersion, v)
		if err != nil {
			return nil, err
		}
		var ok bool
		floatVal, ok = floatAny.(float64)
		if !ok {
			return nil, errors.New("failed to convert float")
		}
	default:
		return nil, fmt.Errorf("unsupported type for bigint conversion: %v", value)
	}
	if err != nil {
		return nil, err
	}
	result, err := proxycore.EncodeType(datatype.Double, bigtableEncodingVersion, floatVal)
	if err != nil {
		return nil, fmt.Errorf("failed to encode double: %w", err)
	}
	return result, err
}

// encodeBoolForBigtable encodes boolean values to bytes.
// Converts boolean values to byte representation with validation.
// Returns error if value is invalid or encoding fails.
func encodeBoolForBigtable(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case string:
		val, err := strconv.ParseBool(v)
		if err != nil {
			return nil, err
		}
		strVal := "0"
		if val {
			strVal = "1"
		}
		intVal, err := strconv.ParseInt(strVal, 10, 64)
		if err != nil {
			return nil, err
		}
		bd, err := proxycore.EncodeType(datatype.Bigint, bigtableEncodingVersion, intVal)
		if err != nil {
			return nil, err
		}
		return bd, nil
	case bool:
		var valInBigint int64
		if v {
			valInBigint = 1
		} else {
			valInBigint = 0
		}
		bd, err := proxycore.EncodeType(datatype.Bigint, bigtableEncodingVersion, valInBigint)
		if err != nil {
			return nil, err
		}
		return bd, nil
	case []byte:
		vaInInterface, err := proxycore.DecodeType(datatype.Boolean, bigtableEncodingVersion, v)
		if err != nil {
			return nil, err
		}
		if vaInInterface == nil {
			return nil, nil
		}
		if vaInInterface.(bool) {
			return proxycore.EncodeType(datatype.Bigint, bigtableEncodingVersion, 1)
		} else {
			return proxycore.EncodeType(datatype.Bigint, bigtableEncodingVersion, 0)
		}
	default:
		return nil, fmt.Errorf("unsupported type: %v", value)
	}
}

func parseCassandraValueToInt64(value interface{}, clientPv primitive.ProtocolVersion) (int64, error) {
	switch v := value.(type) {
	case string:
		return strconv.ParseInt(v, 10, 64)
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case []byte:
		if len(v) == 4 {
			decoded, err := proxycore.DecodeType(datatype.Int, clientPv, v)
			if err != nil {
				return 0, err
			}
			return int64(decoded.(int32)), nil
		} else {
			decoded, err := proxycore.DecodeType(datatype.Bigint, clientPv, v)
			if err != nil {
				return 0, err
			}
			if decoded == nil {
				return 0, nil
			}
			return decoded.(int64), nil
		}
	default:
		return 0, fmt.Errorf("unsupported type for bigint conversion: %v", value)
	}
}

func encodeTimestampForBigtable(value interface{}) (types.BigtableValue, error) {
	var t time.Time
	switch v := value.(type) {
	case int64:
		t = time.UnixMilli(v)
	default:
		return nil, fmt.Errorf("unsupported timestamp type: %T", value)
	}
	return proxycore.EncodeType(datatype.Timestamp, bigtableEncodingVersion, t)
}

// scalarToString converts a primitive value to its string representation.
// Handles various data types and returns a formatted string.
// Returns error if value type is invalid or conversion fails.
func scalarToString(val interface{}) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case int32:
		return strconv.Itoa(int(v)), nil
	case int:
		return strconv.Itoa(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(v), nil
	default:
		return "", fmt.Errorf("unsupported type: %T", v)
	}
}
