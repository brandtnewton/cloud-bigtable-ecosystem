package placeholders

import (
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"reflect"
)

func validateGoType(v any, dt types.CqlDataType) error {
	expected, err := getGoType(dt)
	if err != nil {
		return fmt.Errorf("failed to determine expected type: %w", err)
	}

	if expected != reflect.TypeOf(v) {
		return fmt.Errorf("got %T, expected %s (%s)", v, dt.String(), expected.String())
	}

	return nil
}

func getGoType(dt types.CqlDataType) (reflect.Type, error) {
	switch dt.Code() {
	case types.INT:
		return reflect.TypeOf(int32(0)), nil
	case types.BIGINT:
		return reflect.TypeOf(int64(0)), nil
	case types.VARCHAR, types.TEXT, types.ASCII:
		return reflect.TypeOf(""), nil
	case types.BOOLEAN:
		return reflect.TypeOf(false), nil
	case types.LIST:
		lt := dt.(types.ListType)
		inner, err := GetGoType(lt.ElementType())
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(inner), nil
	case types.SET:
		lt := dt.(types.SetType)
		inner, err := GetGoType(lt.ElementType())
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(inner), nil
	case types.MAP:
		lt := dt.(types.MapType)
		key, err := GetGoType(lt.KeyType())
		if err != nil {
			return nil, err
		}
		value, err := GetGoType(lt.ValueType())
		if err != nil {
			return nil, err
		}
		return reflect.MapOf(key, value), nil
	default:
		return nil, fmt.Errorf("unhandled data type: %s", dt.String())
	}
}
