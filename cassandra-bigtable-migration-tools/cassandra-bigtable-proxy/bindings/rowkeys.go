package bindings

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
)

var kOrderedCodeEmptyField = []byte("\x00\x00")
var kOrderedCodeDelimiter = []byte("\x00\x01")

// BindRowKey creates an ordered row key.
// Generates a byte-encoded row key from primary key values with validation.
// Returns error if key type is invalid or encoding fails.
func BindRowKey(tableConfig *schemaMapping.TableConfig, values *types.QueryParameterValues) (types.RowKey, error) {
	var result []byte
	var trailingEmptyFields []byte
	for i, pmk := range tableConfig.PrimaryKeys {
		if i != pmk.PkPrecedence-1 {
			return "", fmt.Errorf("wrong order for primary keys")
		}
		value, err := values.GetValueByColumn(pmk.Name)
		if err != nil {
			return "", fmt.Errorf("missing primary key `%s`", pmk.Name)
		}

		var orderEncodedField []byte
		switch v := value.(type) {
		case int64:
			orderEncodedField, err = encodeInt64Key(v, tableConfig.IntRowKeyEncoding)
			if err != nil {
				return "", err
			}
		case string:
			orderEncodedField, err = Append(nil, v)
			if err != nil {
				return "", err
			}
			// the ordered code library always appends a delimiter to strings, but we have custom delimiter logic so remove it
			orderEncodedField = orderEncodedField[:len(orderEncodedField)-2]
		default:
			return "", fmt.Errorf("unsupported row key type %T", value)
		}

		// Omit trailing empty fields from the encoding. We achieve this by holding
		// them back in a separate buffer until we hit a non-empty field.
		if len(orderEncodedField) == 0 {
			trailingEmptyFields = append(trailingEmptyFields, kOrderedCodeEmptyField...)
			trailingEmptyFields = append(trailingEmptyFields, kOrderedCodeDelimiter...)
			continue
		}

		if len(result) != 0 {
			result = append(result, kOrderedCodeDelimiter...)
		}

		// Since this field is non-empty, any empty fields we held back are not
		// trailing and should not be omitted. Add them in before appending the
		// latest field. Note that they will correctly end with a delimiter.
		result = append(result, trailingEmptyFields...)
		trailingEmptyFields = nil

		// Finally, append the non-empty field
		result = append(result, orderEncodedField...)
	}

	keyStr := string(result)
	return types.RowKey(keyStr), nil
}

// encodeInt64Key encodes an int64 value for row keys.
// Converts int64 values to byte representation with validation.
// Returns error if value is invalid or encoding fails.
func encodeInt64Key(value int64, intRowKeyEncoding types.IntRowKeyEncodingType) ([]byte, error) {
	switch intRowKeyEncoding {
	case types.BigEndianEncoding:
		return encodeIntRowKeysWithBigEndian(value)
	case types.OrderedCodeEncoding:
		return Append(nil, value)
	}
	return nil, fmt.Errorf("unhandled int encoding type: %v", intRowKeyEncoding)
}

func encodeIntRowKeysWithBigEndian(value int64) ([]byte, error) {
	if value < 0 {
		return nil, errors.New("row keys with big endian encoding cannot contain negative integer values")
	}

	var b bytes.Buffer
	err := binary.Write(&b, binary.BigEndian, value)
	if err != nil {
		return nil, err
	}

	result, err := Append(nil, b.String())
	if err != nil {
		return nil, err
	}

	return result[:len(result)-2], nil
}
