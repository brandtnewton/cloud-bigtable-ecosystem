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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"cloud.google.com/go/bigtable"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/collectiondecoder"
	constants "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/constants"
	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/antlr4-go/antlr/v4"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

const (
	limitPlaceholder = "limitValue"
	missingUndefined = "<missing undefined>"
	missing          = "<missing"
	questionMark     = "?"
	STAR             = "*"
	maxNanos         = int32(9999)
	referenceTime    = int64(1262304000000)
	ifExists         = "ifexists"
)

var (
	validTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

var (
	ErrEmptyTable           = errors.New("could not find table name to create spanner query")
	ErrParsingDelObj        = errors.New("error while parsing delete object")
	ErrParsingTs            = errors.New("timestamp could not be parsed")
	ErrTsNoValue            = errors.New("no value found for Timestamp")
	ErrEmptyTableOrKeyspace = errors.New("ToBigtableDelete: No table or keyspace name found in the query")
)

// ComplexAssignment represents a complex update operation on a column.
// It contains the column name, operation type, and left/right operands for the operation.
type ComplexAssignment struct {
	Column    types.ColumnName
	Operation string
	IsPrepend bool
	Left      interface{}
	Right     interface{}
}

// parseTimestamp(): Parse a timestamp string in various formats.
// Supported formats
// "2024-02-05T14:00:00Z",
// "2024-02-05 14:00:00",
// "2024/02/05 14:00:00",
// "1672522562000",          // Unix timestamp (milliseconds)
func parseTimestamp(timestampStr string) (time.Time, error) {
	// Define multiple layouts to try
	layouts := []string{
		time.RFC3339,          // ISO 8601 format
		"2006-01-02 15:04:05", // Common date-time format
		"2006/01/02 15:04:05", // Common date-time format with slashes
	}

	var parsedTime time.Time
	var err error

	// Try to parse the timestamp using each layout
	for _, layout := range layouts {
		if timestampStr == "totimestamp(now())" {
			timestampStr = time.Now().Format(time.RFC3339)
		}
		parsedTime, err = time.Parse(layout, timestampStr)
		if err == nil {
			return parsedTime, nil
		}
	}
	// Try to parse as Unix timestamp (in seconds, milliseconds, or microseconds)
	if unixTime, err := strconv.ParseInt(timestampStr, 10, 64); err == nil {
		timestrlen := timestampStr
		if len(timestampStr) > 0 && timestampStr[0] == '-' {
			timestrlen = timestampStr[1:]
		}
		if len(timestrlen) <= 19 {
			// Handle timestamps in milliseconds (Cassandra supports millisecond epoch time)
			secs := unixTime / 1000
			nanos := (unixTime % 1000) * int64(time.Millisecond)
			return time.Unix(secs, nanos).UTC(), nil
		} else {
			return time.Time{}, fmt.Errorf("invalid unix timestamp: %s", timestampStr)
		}
		// checking if value is float
	} else if floatTime, err := strconv.ParseFloat(timestampStr, 64); err == nil {
		unixTime := int64(floatTime)
		unixStr := strconv.FormatInt(unixTime, 10)
		timestrlen := unixStr
		if len(unixStr) > 0 && unixStr[0] == '-' {
			timestrlen = timestampStr[1:]
		}
		if len(timestrlen) <= 18 {
			// Handle timestamps in milliseconds (Cassandra supports millisecond epoch time)
			secs := unixTime / 1000
			nanos := (unixTime % 1000) * int64(time.Millisecond)
			return time.Unix(secs, nanos).UTC(), nil
		} else {
			return time.Time{}, fmt.Errorf("invalid unix timestamp: %s", timestampStr)
		}
	}

	// If all formats fail, return the last error
	return time.Time{}, err
}

func encodeGoValueToBigtable(column *types.Column, value types.GoValue, clientVersion primitive.ProtocolVersion) ([]types.BigtableData, error) {
	if column.CQLType.Code() == types.MAP {
		mt := column.CQLType.(types.MapType)
		mv, ok := value.(map[interface{}]interface{})
		if !ok {
			return nil, errors.New("failed to parse map")
		}

		var results []types.BigtableData
		for k, v := range mv {
			// todo use key specific encode
			keyEncoded, err := encodeValueForBigtable(k, mt.KeyType().DataType(), clientVersion)
			if err != nil {
				return nil, err
			}
			valueBytes, err := encodeValueForBigtable(v, mt.ValueType().DataType(), clientVersion)
			if err != nil {
				return nil, err
			}
			results = append(results, types.BigtableData{Family: column.ColumnFamily, Column: types.ColumnQualifier(keyEncoded), Bytes: valueBytes})
		}
		return results, nil
	} else if column.CQLType.Code() == types.LIST {
		lt := column.CQLType.(types.ListType)
		lv, ok := value.([]any)
		if !ok {
			return nil, errors.New("failed to parse list")
		}

		var results []types.BigtableData
		for i, v := range lv {
			valueBytes, err := encodeValueForBigtable(v, lt.ElementType().DataType(), clientVersion)
			if err != nil {
				return nil, err
			}
			// todo use list index encoder
			results = append(results, types.BigtableData{Family: column.ColumnFamily, Column: types.ColumnQualifier(fmt.Sprintf("%d", i)), Bytes: valueBytes})
		}
		return results, nil
	} else if column.CQLType.Code() == types.SET {
		st := column.CQLType.(types.ListType)
		lv, ok := value.([]any)
		if !ok {
			return nil, errors.New("failed to parse list")
		}

		var results []types.BigtableData
		for _, v := range lv {
			// todo use key specific encode
			valueBytes, err := encodeValueForBigtable(v, st.ElementType().DataType(), clientVersion)
			if err != nil {
				return nil, err
			}
			// todo use correct column value
			results = append(results, types.BigtableData{Family: column.ColumnFamily, Column: types.ColumnQualifier(fmt.Sprintf("%v", valueBytes)), Bytes: []byte("")})
		}
		return results, nil
	}

	v, err := encodeValueForBigtable(value, column.CQLType.DataType(), clientVersion)
	if err != nil {
		return nil, err
	}
	return []types.BigtableData{{Family: column.ColumnFamily, Column: types.ColumnQualifier(column.Name), Bytes: v}}, nil
}

// encodeValueForBigtable converts a value to its byte representation based on CQL type.
// Handles type conversion and encoding according to the protocol version.
// Returns error if value type is invalid or encoding fails.
func encodeValueForBigtable(value any, cqlType datatype.DataType, clientPv primitive.ProtocolVersion) (types.BigtableValue, error) {
	if value == nil {
		return nil, nil
	}

	var iv interface{}
	var dt datatype.DataType
	switch cqlType {
	case datatype.Int, datatype.Bigint:
		return encodeBigIntForBigtable(value, clientPv)
	case datatype.Float, datatype.Double:
		return encodeFloat64ForBigtable(value, clientPv)
	case datatype.Boolean:
		return encodeBoolForBigtable(value, clientPv)
	case datatype.Timestamp:
		return encodeTimestampForBigtable(value, clientPv)
	case datatype.Blob:
		iv = value
		dt = datatype.Blob
	case datatype.Varchar:
		iv = value
		dt = datatype.Varchar
	default:
		return nil, fmt.Errorf("unsupported CQL type: %s", cqlType)
	}

	bd, err := proxycore.EncodeType(dt, primitive.ProtocolVersion4, iv)
	if err != nil {
		return nil, fmt.Errorf("error encoding value: %w", err)
	}

	return bd, nil
}

// primitivesToString converts a primitive value to its string representation.
// Handles various data types and returns a formatted string.
// Returns error if value type is invalid or conversion fails.
func primitivesToString(val interface{}) (string, error) {
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

// stringToPrimitives converts a string value to its primitive type based on CQL type.
// Performs type conversion according to the specified CQL data type.
// Returns error if value type is invalid or conversion fails.
func stringToPrimitives(value string, cqlType datatype.DataType) (interface{}, error) {
	var iv interface{}

	switch cqlType {
	case datatype.Int:
		val, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("error converting string to int32: %w", err)
		}
		iv = int32(val)

	case datatype.Bigint, datatype.Counter:
		val, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error converting string to int64: %w", err)
		}
		iv = val

	case datatype.Float:
		val, err := strconv.ParseFloat(value, 32)
		if err != nil {
			return nil, fmt.Errorf("error converting string to float32: %w", err)
		}
		iv = float32(val)

	case datatype.Double:
		val, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, fmt.Errorf("error converting string to float64: %w", err)
		}
		iv = val
	case datatype.Boolean:
		val, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf("error converting string to bool: %w", err)
		}
		if val {
			iv = int64(1)
		} else {
			iv = int64(0)
		}

	case datatype.Timestamp:
		val, err := parseTimestamp(value)

		if err != nil {
			return nil, fmt.Errorf("error converting string to timestamp: %w", err)
		}
		iv = val
	case datatype.Blob:
		iv = value
	case datatype.Varchar:
		iv = value

	default:
		return nil, fmt.Errorf("unsupported CQL type: %s", cqlType)

	}
	return iv, nil
}

// cqlTypeToEmptyPrimitive returns a keeping type value for a given CQL type.
func cqlTypeToEmptyPrimitive(cqlType datatype.DataType, isPrimaryKey bool) interface{} {
	switch cqlType {
	case datatype.Int:
		return int32(0)
	case datatype.Bigint:
		return int64(0)
	case datatype.Float:
		return float32(0)
	case datatype.Double:
		return float64(0)
	case datatype.Boolean:
		return false
	case datatype.Timestamp:
		return time.Time{}
	case datatype.Blob:
		return []byte{}
	case datatype.Varchar:
		if !isPrimaryKey {
			return []byte{}
		}
		return string("")
	}
	return nil
}

// parseComplexOperations() parses collection and counter mutations
func parseComplexOperations(tableConfig *schemaMapping.TableConfig, columns []*types.Column, values []ComplexAssignment) (*AdHocQueryValues, error) {
	output := &AdHocQueryValues{ComplexOps: make(map[types.ColumnFamily]*ComplexOperation)}
	for i, col := range columns {
		if col.IsPrimaryKey {
			continue
		}
		val := values[i]
		if col.CQLType.IsCollection() {
			colFamily := tableConfig.GetColumnFamily(col.Name)
			switch col.CQLType.Code() {
			case types.LIST:
				lt, ok := col.CQLType.(*types.ListType)
				if !ok {
					return nil, fmt.Errorf("failed to convert list type for %s", col.CQLType.String())
				}
				if err := handleListOperation(val, col, lt, colFamily, output); err != nil {
					return nil, err
				}
			case types.SET:
				st, ok := col.CQLType.(*types.SetType)
				if !ok {
					return nil, fmt.Errorf("failed to convert set type for %s", col.CQLType.String())
				}
				if err := handleSetOperation(val, col, st, colFamily, output); err != nil {
					return nil, err
				}
			case types.MAP:
				mt, ok := col.CQLType.(*types.MapType)
				if !ok {
					return nil, fmt.Errorf("failed to convert map type for %s", col.CQLType.String())
				}
				if err := handleMapOperation(val, col, mt, colFamily, output); err != nil {
					return nil, err
				}
			default:
				return nil, fmt.Errorf("cv %s is not a collection type", col.Name)
			}
		} else if col.CQLType.Code() == types.COUNTER {
			if err := handleCounterOperation(val, col, output); err != nil {
				return nil, err
			}
		}
	}
	return output, nil
}

// parseCqlValue parses a CQL value expression.
// Converts CQL value expressions to their corresponding Go types with validation.
// Returns error if expression is invalid or conversion fails.
func parseCqlValue(expr antlr.ParserRuleContext) (types.GoValue, error) {
	// If this is an expression context, check for subcontexts
	if e, ok := expr.(cql.IExpressionContext); ok {
		if m := e.AssignmentMap(); m != nil {
			return parseCqlValue(m)
		}
		if l := e.AssignmentList(); l != nil {
			return parseCqlValue(l)
		}
		if s := e.AssignmentSet(); s != nil {
			return parseCqlValue(s)
		}
		if c := e.Constant(); c != nil {
			return parseCqlConstant(c)
		}
		// Optionally handle tuple, function call, etc.
		return e.GetText(), nil // fallback
	}
	// Handle map context
	if m, ok := expr.(cql.IAssignmentMapContext); ok {
		return parseCqlMapAssignment(m)
	}
	// Handle list context
	if l, ok := expr.(cql.IAssignmentListContext); ok {
		var result []string
		for _, c := range l.AllConstant() {
			val, err := parseCqlConstant(c)
			if err != nil {
				return nil, err
			}
			strval, err := primitivesToString(val)
			if err != nil {
				return nil, err
			}
			result = append(result, strval)
		}
		return result, nil
	}
	// Handle set context
	if s, ok := expr.(cql.IAssignmentSetContext); ok {
		var result []string
		all := s.AllConstant()
		if len(all) == 0 {
			return result, nil
		}
		for _, c := range all {
			if c == nil {
				continue
			}
			val, err := parseCqlConstant(c)
			if err != nil {
				return nil, err
			}
			strval, err := primitivesToString(val)
			if err != nil {
				return nil, err
			}
			result = append(result, strval)
		}
		return result, nil
	}
	return expr.GetText(), nil // fallback
}

// handleListOperation processes list operations in raw queries.
// Manages simple assignment, append, prepend, and index-based operations on list columns.
// Returns error if operation type is invalid or value type doesn't match expected type.
func handleListOperation(val interface{}, column *types.Column, lt *types.ListType, colFamily types.ColumnFamily, output *AdHocQueryValues) error {
	switch v := val.(type) {
	case ComplexAssignment:
		switch v.Operation {
		case "+":
			valueToProcess := v.Right
			if input.PrependColumns != nil && slices.Contains(input.PrependColumns, column.Name) {
				valueToProcess = v.Left
			}
			var listValues []string = valueToProcess.([]string)
			return addListElements(listValues, colFamily, lt, input, output)
		case "-":
			keys, ok := v.Right.([]string)
			if !ok {
				return fmt.Errorf("expected []string for remove operation, got %T", v.Right)
			}
			return removeListElements(keys, colFamily, column, output)
		case "update_index":
			idx, ok := v.Left.(string)
			if !ok {
				return fmt.Errorf("expected string for index, got %T", v.Left)
			}
			listType, ok := column.CQLType.(*types.ListType)
			if !ok {
				return fmt.Errorf("expected list type column for list operation")
			}
			dt := listType.ElementType().DataType()

			return updateListIndex(idx, v.Right, colFamily, dt, output)
		default:
			return fmt.Errorf("unsupported list operation: %s", v.Operation)
		}
	default:
		// Simple assignment (replace)
		output.DelColumnFamily = append(output.DelColumnFamily, column.ColumnFamily)
		var listValues []string
		if val != nil {
			switch v := val.(type) {
			case []string:
				listValues = v
			default:
				return fmt.Errorf("expected []string for list operation, got %T", val)
			}
		}

		return addListElements(listValues, colFamily, lt, output)
	}
}

// addListElements adds elements to a list column in raw queries.
// Handles both append and prepend operations with type validation and conversion.
// Returns error if value type doesn't match list element type or conversion fails.
func addListElements(listValues []string, colFamily types.ColumnFamily, lt *types.ListType, output *AdHocQueryValues) error {
	prepend := false
	if slices.Contains(input.PrependColumns, colFamily) {
		prepend = true
	}
	for i, v := range listValues {

		// Calculate encoded timestamp for the list element
		encTime := getEncodedTimestamp(i, len(listValues), prepend)

		// Create a new column identifier with the encoded timestamp as the name
		c := &types.Column{
			Name:         string(encTime),
			ColumnFamily: colFamily,
			CQLType:      lt.ElementType(),
		}
		// Format the value
		formattedVal, err := encodeValueForBigtable(v, lt.ElementType().DataType(), primitive.ProtocolVersion4)
		if err != nil {
			return fmt.Errorf("error converting string to list<%s> value: %w", lt.ElementType().String(), err)
		}
		output.NewColumns = append(output.NewColumns, c)
		output.NewValues = append(output.NewValues, formattedVal)
	}
	return nil
}

// removeListElements removes elements from a list column in raw queries.
// Processes element removal by index with validation of index bounds.
// Returns error if index is invalid or out of bounds.
func removeListElements(keys []string, colFamily types.ColumnFamily, column *types.Column, output *AdHocQueryValues) error {
	var listDelete [][]byte
	listType, ok := column.CQLType.DataType().(datatype.ListType)
	if !ok {
		return fmt.Errorf("failed to assert list type for %s", column.CQLType.String())
	}

	listElementType := listType.GetElementType()

	output.ComplexOps[colFamily] = &ComplexOperation{
		Delete:     true,
		ListDelete: true,
	}

	for _, col := range keys {
		formattedVal, err := encodeValueForBigtable(col, listElementType, primitive.ProtocolVersion4)
		if err != nil {
			return fmt.Errorf("error converting string to list<%s> value: %w", listElementType, err)
		}
		listDelete = append(listDelete, formattedVal)
	}
	if len(listDelete) > 0 {
		if meta, ok := output.ComplexOps[colFamily]; ok {
			meta.ListDeleteValues = listDelete
		} else {
			output.ComplexOps[colFamily] = &ComplexOperation{ListDeleteValues: listDelete}
		}
	}
	return nil
}

// updateListIndex updates a specific index in a list column.
// Handles type conversion and validation for the new value.
// Returns error if index is invalid or value type doesn't match list element type.
func updateListIndex(index string, value interface{}, colFamily types.ColumnFamily, dt datatype.DataType, output *AdHocQueryValues) error {

	valStr, err := primitivesToString(value)
	if err != nil {
		return err
	}

	val, err := encodeValueForBigtable(valStr, dt, primitive.ProtocolVersion4)
	if err != nil {
		return err
	}
	output.ComplexOps[colFamily] = &ComplexOperation{
		UpdateListIndex: index,
		Value:           val,
	}
	return nil
}

// handleSetOperation processes set operations in raw queries.
// Manages simple assignment, add, and remove operations on set columns.
// Returns error if operation type is invalid or value type doesn't match set element type.
func handleSetOperation(val interface{}, column *types.Column, st *types.SetType, colFamily types.ColumnFamily, output *AdHocQueryValues) error {
	switch v := val.(type) {
	case ComplexAssignment:
		switch v.Operation {
		case "+":
			setValues, ok := v.Right.([]string)
			if !ok {
				return fmt.Errorf("expected []string for add operation, got %T", v.Right)
			}
			return addSetElements(setValues, colFamily, st, output)
		case "-":
			keys, ok := v.Right.([]string)
			if !ok {
				return fmt.Errorf("expected []string for remove operation, got %T", v.Right)
			}
			return removeSetElements(keys, colFamily, output)
		default:
			return fmt.Errorf("unsupported set operation: %s", v.Operation)
		}
	default:
		// Simple assignment (replace)
		output.DelColumnFamily = append(output.DelColumnFamily, column.ColumnFamily)
		return addSetElements(val.([]string), colFamily, st, output)
	}
}

// addSetElements adds elements to a set column in raw queries.
// Handles element addition with type validation and conversion.
// Returns error if value type doesn't match set element type or conversion fails.
func addSetElements(setValues []string, colFamily types.ColumnFamily, st *types.SetType, output *AdHocQueryValues) error {
	if output == nil {
		return fmt.Errorf("output cannot be nil")
	}

	for _, v := range setValues {
		// For boolean values, we need to convert true/false to 1/0
		if st.ElementType().Code() == types.BOOLEAN {
			boolVal, err := strconv.ParseBool(v)
			if err != nil {
				return fmt.Errorf("invalid boolean string: %v", v)
			}
			if boolVal {
				v = "1"
			} else {
				v = "0"
			}
		}
		col := &types.Column{
			Name:         types.ColumnName(v),
			ColumnFamily: colFamily,
			CQLType:      st.ElementType(),
		}
		output.NewColumns = append(output.NewColumns, col)
		// value parameter is intentionally empty because column identifier holds the value
		output.NewValues = append(output.NewValues, []byte(""))
	}
	return nil
}

// removeSetElements removes elements from a set column in raw queries.
// Processes element removal with validation of element existence.
// Returns error if element type doesn't match set element type.
func removeSetElements(keys []string, colFamily types.ColumnFamily, output *AdHocQueryValues) error {
	for _, key := range keys {
		output.DelColumns = append(output.DelColumns, &types.BigtableColumn{Family: colFamily, Column: types.ColumnQualifier(key)})
	}
	return nil
}

func handleCounterOperation(val ComplexAssignment, column *types.Column, output *AdHocQueryValues) error {
	switch val.Operation {
	case "+", "-":
		var valueStr string
		// Check for `col = col + val` or `col = val + col`
		if rStr, ok := val.Right.(string); ok && (val.Left.(string)) == string(column.Name) {
			valueStr = rStr
		} else if lStr, ok := val.Left.(string); ok && val.Right.(string) == string(column.Name) {
			valueStr = lStr
		} else {
			return fmt.Errorf("invalid counter operation structure for column %s", column.Name)
		}

		intVal, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil {
			return err
		}
		var op = Increment
		if val.Operation == "-" {
			op = Decrement
		}
		output.ComplexOps[column.ColumnFamily] = &ComplexOperation{
			IncrementType:  op,
			IncrementValue: intVal,
		}
		return nil
	default:
		return fmt.Errorf("unsupported counter operation: %s", val.Operation)
	}
}

// handleMapOperation processes map operations in raw queries.
// Manages simple assignment/replace complete map, add, remove, and update at index operations on map columns.
// Returns error if operation type is invalid or value type doesn't match map key/value types.
func handleMapOperation(val interface{}, column *types.Column, mt *types.MapType, colFamily types.ColumnFamily, output *AdHocQueryValues) error {
	// Check if key type is VARCHAR or TIMESTAMP
	if mt.KeyType().DataType() == datatype.Varchar || mt.KeyType().DataType() == datatype.Timestamp {
		switch v := val.(type) {
		case ComplexAssignment:
			switch v.Operation {
			case "+":
				return addMapEntries(v.Right, colFamily, mt, column, output)
			case "-":
				return removeMapEntries(v.Right, colFamily, column, output)
			case "update_index":
				return updateMapIndex(v.Left, v.Right, mt, colFamily, output)
			default:
				return fmt.Errorf("unsupported map operation: %s", v.Operation)
			}
		default:
			// Simple assignment (replace)
			output.DelColumnFamily = append(output.DelColumnFamily, column.ColumnFamily)
			return addMapEntries(val, colFamily, mt, column, output)
		}
	} else {
		return fmt.Errorf("unsupported map key type: %s", mt.KeyType().String())
	}
}

// addMapEntries adds key-value pairs to a map column in raw queries.
// Handles type validation and conversion for both keys and values.
// Returns error if key/value types don't match map types or conversion fails.
func addMapEntries(val interface{}, colFamily types.ColumnFamily, mt *types.MapType, column *types.Column, output *AdHocQueryValues) error {
	mapValue, ok := val.(map[string]string)
	if !ok {
		return fmt.Errorf("expected map[string]interface{} for add operation, got %T", val)
	}

	mapValueType := mt.ValueType()
	if mapValueType.Code() == types.BOOLEAN || mapValueType.Code() == types.INT {
		mapValueType = types.TypeBigint
	}
	for k, v := range mapValue {
		valueFormatted, err := encodeValueForBigtable(v, mt.ValueType().DataType(), primitive.ProtocolVersion4)
		if err != nil {
			return fmt.Errorf("error converting string to %s value: %w", mt.String(), err)
		}
		output.NewColumns = append(output.NewColumns, &types.BigtableColumn{Family: colFamily, Column: types.ColumnQualifier(k)})
		output.NewValues = append(output.NewValues, valueFormatted)
	}
	return nil
}

// removeMapEntries removes key-value pairs from a map column in raw queries.
// Processes key removal with validation of key existence and type.
// Returns error if key type doesn't match map key type.
func removeMapEntries(val interface{}, colFamily types.ColumnFamily, column *types.Column, output *AdHocQueryValues) error {
	keys, ok := val.([]string)
	if !ok {
		return fmt.Errorf("expected []string for remove operation, got %T", val)
	}
	for _, col := range keys {
		output.DelColumns = append(output.DelColumns, &types.Column{
			Name:         col,
			ColumnFamily: colFamily,
		})
	}
	return nil
}

// updateMapIndex updates a specific key in a map column.
// Handles type conversion and validation for both key and value.
// Returns error if key doesn't exist or value type doesn't match map value type.
func updateMapIndex(key interface{}, value interface{}, dt *types.MapType, colFamily types.ColumnFamily, output *AdHocQueryValues) error {
	k, ok := key.(string)
	if !ok {
		return fmt.Errorf("expected string for map key, got %T", key)
	}
	v, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string for map value, got %T", key)
	}

	val, err := encodeValueForBigtable(v, dt.ValueType().DataType(), primitive.ProtocolVersion4)
	if err != nil {
		return err
	}
	// For map index update, treat as a single entry add
	output.NewColumns = append(output.NewColumns, &types.BigtableColumn{
		Column: types.ColumnQualifier(k),
		Family: colFamily,
	})
	output.NewValues = append(output.NewValues, val)
	return nil
}

// decodePreparedValues handles collection operations in prepared queries.
// Processes set, list, and map operations.
// Returns error if collection type is invalid or value encoding fails.
func decodePreparedValues(table *schemaMapping.TableConfig, columns []*types.Column, complexOps map[string]*ComplexOperation, values []*primitive.Value, pv primitive.ProtocolVersion) (*PreparedValues, error) {
	output := &PreparedValues{
		GoValues: make(map[types.ColumnName]types.GoValue),
	}

	for i, column := range columns {
		output.IndexEnd = i
		goValue, err := proxycore.DecodeType(column.CQLType.DataType(), pv, values)
		if err != nil {
			return nil, err
		}
		output.GoValues[column.Name] = goValue
		// todo validate the column exists again, in case the column was dropped after the query was prepared.
		if column.CQLType.IsCollection() {
			colFamily := table.GetColumnFamily(column.Name)
			switch column.CQLType.Code() {
			case types.LIST:
				if err := handlePrepareListOperation(complexOps, values[i].Contents, column, colFamily, pv, output); err != nil {
					return nil, err
				}
				continue
			case types.SET:
				if err := handlePrepareSetOperation(values[i], column, colFamily, pv, output); err != nil {
					return nil, err
				}
				continue
			case types.MAP:
				if err := handlePrepareMapOperation(values[i], column, colFamily, pv, output); err != nil {
					return nil, err
				}
				continue
			default:
				return nil, fmt.Errorf("column %s is not a collection type", column.Name)
			}
		} else if column.CQLType.Code() == types.COUNTER {
			decodedValue, err := proxycore.DecodeType(datatype.Counter, pv, values[i].Contents)
			if err != nil {
				return nil, err
			}
			intVal, ok := decodedValue.(int64)
			if !ok {
				return nil, fmt.Errorf("failed to convert counter param")
			}

			meta, ok := input.ComplexMeta[column.Name]
			if !ok {
				return nil, fmt.Errorf("unexpected state: no existing operation for counter param")
			}

			// The increment value is part of the prepared statement, so we update the existing ComplexOperations meta with the value provided at execution time.
			meta.IncrementValue = intVal
		} else {
			valInInterface, err := DataConversionInInsertionIfRequired(values[i].Contents, pv, column.CQLType.DataType())
			if err != nil {
				return nil, fmt.Errorf("error converting primitives: %w", err)
			}
			output.Values = append(output.Values, valInInterface.([]byte))
		}
	}
	return output, nil
}

// handlePrepareListOperation processes list operations in prepared queries.
// Manages list operations with protocol version-specific value encoding.
// Returns error if value type doesn't match list element type or encoding fails.
func handlePrepareListOperation(complexOps map[string]*ComplexOperation, val []byte, column *types.Column, colFamily types.ColumnFamily, pv primitive.ProtocolVersion, output *PreparedValues) error {

	lt, ok := column.CQLType.(*types.ListType)
	if !ok {
		return fmt.Errorf("failed to assert list type for %s", column.CQLType.String())
	}
	et := lt.ElementType()

	if meta, ok := complexOps[column.Name]; ok {
		var complexUpdateMeta *ComplexOperation = meta

		if complexUpdateMeta.UpdateListIndex != "" {
			// list index operation e.g. list[index]=val
			valInInterface, err := DataConversionInInsertionIfRequired(val, pv, et.DataType())
			if err != nil {
				return fmt.Errorf("error while encoding %s value: %w", lt.String(), err)
			}
			complexUpdateMeta.Value = valInInterface.([]byte)
		} else {
			return preprocessList(val, colFamily, complexUpdateMeta, lt, column, output)
		}
	} else {
		output.DelColumnFamily = append(output.DelColumnFamily, column.ColumnFamily)
		return preprocessList(val, colFamily, nil, lt, column, output)
	}
	return nil
}

// preprocessList prepares list values for processing in prepared queries.
// Handles initial list value preparation, type validation, and conversion.
// Returns error if value type doesn't match list element type or conversion fails.
func preprocessList(val []byte, colFamily types.ColumnFamily, complexUpdateMeta *ComplexOperation, lt *types.ListType, column *types.Column, output *PreparedValues) error {

	switch lt.ElementType().Code() {
	case types.VARCHAR, types.TEXT, types.ASCII:
		return processList[string](val, colFamily, complexUpdateMeta, lt, column, input, output)
	case types.INT:
		return processList[int32](val, colFamily, complexUpdateMeta, lt, column, input, output)
	case types.BIGINT, types.TIMESTAMP:
		return processList[int64](val, colFamily, complexUpdateMeta, lt, column, input, output)
	case types.FLOAT:
		return processList[float32](val, colFamily, complexUpdateMeta, lt, column, input, output)
	case types.DOUBLE:
		return processList[float64](val, colFamily, complexUpdateMeta, lt, column, input, output)
	case types.BOOLEAN:
		return processList[bool](val, colFamily, complexUpdateMeta, lt, column, input, output)
	default:
		return fmt.Errorf("Invalid list value type")
	}
}

// processList() handles generic list processing operations in prepared queries.
// Manages list operations with type-specific handling and protocol version encoding.
// Returns error if value type doesn't match list element type or encoding fails.
func processList[V any](val []byte, colFamily types.ColumnFamily, complexUpdateMeta *ComplexOperation, lt *types.ListType, column *types.Column, pv primitive.ProtocolVersion, output *PreparedValues) error {
	listValDatatype := lt.ElementType()
	listDT := lt
	if lt.ElementType().Code() == types.TIMESTAMP {
		listValDatatype = types.TypeBigint
		listDT = utilities.ListOfBigInt
	}

	decodedValue, err := collectiondecoder.DecodeCollection(listDT.DataType(), pv, val)
	if err != nil {
		return fmt.Errorf("error decoding string to %s value: %w", column.CQLType.String(), err)
	}
	listValues, ok := decodedValue.([]V)
	if !ok {
		return fmt.Errorf("unexpected type %s:", listValDatatype)
	}

	var listDelete [][]byte
	prepend := false
	// Iterate over the elements in the List
	for i, elem := range listValues {
		valueStr, err := primitivesToString(elem)
		if err != nil {
			return fmt.Errorf("failed to convert element to string: %w", err)
		}
		if complexUpdateMeta != nil && complexUpdateMeta.ListDelete {
			val, err := encodeValueForBigtable(valueStr, listValDatatype.DataType(), pv)
			if err != nil {
				return err
			}
			listDelete = append(listDelete, val)
			continue
		}
		if complexUpdateMeta != nil {
			prepend = complexUpdateMeta.PrependList

		}

		encTime := getEncodedTimestamp(i, len(listValues), prepend)

		c := &types.Column{
			Name:         string(encTime),
			ColumnFamily: colFamily,
			CQLType:      listValDatatype,
		}
		// The value parameter is intentionally empty because the column identifier has the value
		val, err := encodeValueForBigtable(valueStr, listValDatatype.DataType(), pv)
		if err != nil {
			return fmt.Errorf("failed to convert value for list value: %w", err)
		}
		output.Values = append(output.Values, &ColumnAndValue{Column: c, Value: val})
	}
	if len(listDelete) > 0 {
		complexUpdateMeta.ListDeleteValues = listDelete
	}
	return nil
}

// handlePrepareMapOperation processes map operations in prepared queries.
// Manages map operations, append, delete, and update at index and simple assignment.
// Returns error if value type doesn't match map key/value types or encoding fails.
func handlePrepareMapOperation(complexOps map[string]*ComplexOperation, val *primitive.Value, column *types.Column, colFamily types.ColumnFamily, pv primitive.ProtocolVersion, output *PreparedValues) error {
	mapType, ok := column.CQLType.(*types.MapType)
	if !ok {
		return fmt.Errorf("failed to assert map type for %s", column.CQLType.String())
	}
	if meta, ok := complexOps[column.Name]; ok {

		if meta.Append && meta.mapKey != nil { //handling update for specific key e.g map[key]=val
			return processMapKeyAppend(column, meta, pv, val, output, mapType)

		} else if meta.Delete { //handling Delete for specific key e.g map=map-['key1','key2']

			expectedDt := meta.ExpectedDatatype
			if expectedDt == nil {
				expectedDt = column.CQLType.DataType()
			}

			if mapType.KeyType().Code() == types.TIMESTAMP {
				expectedDt = utilities.SetOfBigInt.DataType()
			}
			return processDeleteOperationForMapAndSet(column, val, pv, output, expectedDt)
		} else {
			if err := handleMapProcessing(val, column, colFamily, mapType, pv, output); err != nil {
				return err
			}
		}
	} else {
		// case of when new value is being set for collection type {e.g collection=newCollection}
		output.DelColumnFamily = append(output.DelColumnFamily, column.Name)
		if err := handleMapProcessing(val, column, colFamily, mapType, pv, output); err != nil {
			return err
		}
	}
	return nil
}

// handleMapProcessing processes map values for prepared queries.
// Handles map value preparation, calls processMap based on value type.
// Returns error if value type doesn't match map key/value types or encoding fails.
func handleMapProcessing(val *primitive.Value, column *types.Column, colFamily types.ColumnFamily, mt *types.MapType, protocolV primitive.ProtocolVersion, output *PreparedValues) error {
	switch mt.ValueType().DataType() {
	case datatype.Varchar:
		if err := processMap[string](val, column, colFamily, mt, protocolV, output); err != nil {
			return err
		}
	case datatype.Int:
		if err := processMap[int32](val, column, colFamily, mt, protocolV, output); err != nil {
			return err
		}
	case datatype.Bigint, datatype.Timestamp:
		if err := processMap[int64](val, column, colFamily, mt, protocolV, output); err != nil {
			return err
		}
	case datatype.Float:
		if err := processMap[float32](val, column, colFamily, mt, protocolV, output); err != nil {
			return err
		}
	case datatype.Double:
		if err := processMap[float64](val, column, colFamily, mt, protocolV, output); err != nil {
			return err
		}
	case datatype.Boolean:
		if err := processMap[bool](val, column, colFamily, mt, protocolV, output); err != nil {
			return err
		}
	}
	return nil
}

// processMap handles generic map processing operations in prepared queries.
// Manages map operations with type-specific handling map(timestamp,varchar) or map(varchar,timestamp)
// Returns error if value type doesn't match map key/value types or encoding fails.
func processMap[V any](
	val *primitive.Value,
	column *types.Column,
	colFamily types.ColumnFamily,
	mt *types.MapType,
	protocolV primitive.ProtocolVersion,
	output *PreparedValues,
) error {
	correctedMapType := mt
	if mt.KeyType().DataType() == datatype.Varchar && mt.ValueType().DataType() == datatype.Timestamp {
		correctedMapType = utilities.MapOfStrToBigInt // Cassandra stores timestamps as bigint
	} else if mt.KeyType().DataType() == datatype.Timestamp && mt.ValueType().DataType() == datatype.Timestamp {
		correctedMapType = utilities.MapOfTimeToBigInt // Cassandra stores timestamps as bigint
	}

	if mt.KeyType().DataType() == datatype.Varchar {
		return processVarcharMap[V](mt, correctedMapType, protocolV, val, column, colFamily, output)
	} else if mt.KeyType().DataType() == datatype.Timestamp {
		return processTimestampMap[V](mt, correctedMapType, protocolV, val, column, colFamily, output)
	} else {
		return fmt.Errorf("Invalid map type")
	}
}

// processVarcharMap handles map operations with varchar keys in prepared queries.
// Processes varchar map operations.
// Returns error if key type isn't varchar or value encoding fails.
func processVarcharMap[V any](originalMapType *types.MapType, mapType *types.MapType, protocolV primitive.ProtocolVersion, val *primitive.Value, column *types.Column, colFamily types.ColumnFamily, output *PreparedValues) error {
	decodedValue, err := proxycore.DecodeType(mapType.DataType(), protocolV, val.Contents)
	if err != nil {
		return fmt.Errorf("error decoding string to %s value: %w", column.CQLType.DataType(), err)
	}
	pointerMap, ok := decodedValue.(map[*string]*V)
	if !ok {
		return fmt.Errorf("error decoding string to %s value: %w", column.CQLType.String(), err)
	}

	decodedMap := make(map[string]V)
	for k, v := range pointerMap {
		decodedMap[*k] = *v
	}

	for k, v := range decodedMap {
		valueStr, err := primitivesToString(v)
		if err != nil {
			return fmt.Errorf("failed to convert value for key '%s': %w", k, err)
		}

		val, err := encodeValueForBigtable(valueStr, mapType.ValueType().DataType(), protocolV)
		if err != nil {
			return fmt.Errorf("failed to convert value for map value: %w", err)
		}
		output.Data = append(output.Data, &types.Column{
			Name:         k,
			ColumnFamily: colFamily,
			CQLType:      originalMapType.ValueType(),
		})
		output.NewValues = append(output.NewValues, val)
	}
	return nil
}

func processTimestampMap[V any](originalMapType *types.MapType, mapType *types.MapType, protocolV primitive.ProtocolVersion, val *primitive.Value, column *types.Column, colFamily types.ColumnFamily, output *PreparedValues) error {
	decodedValue, err := collectiondecoder.DecodeCollection(mapType.DataType(), protocolV, val.Contents)
	if err != nil {
		return fmt.Errorf("error decoding string to %s value: %w", column.CQLType.DataType(), err)
	}
	decodedMap := make(map[int64]V)
	pointerMap, ok := decodedValue.(map[time.Time]V)
	if ok {
		for k, v := range pointerMap {
			key := k.UnixMilli()
			decodedMap[key] = v
		}
		for k, v := range decodedMap {

			c := &types.Column{
				Name:         strconv.FormatInt(k, 10),
				ColumnFamily: colFamily,
				CQLType:      originalMapType.ValueType(),
			}
			valueStr, err := primitivesToString(v)
			if err != nil {
				return fmt.Errorf("failed to convert value for key '%v': %w", k, err)
			}

			val, err := encodeValueForBigtable(valueStr, mapType.ValueType().DataType(), protocolV)
			if err != nil {
				return fmt.Errorf("failed to convert value for map value: %w", err)
			}
			output.Data = append(output.Data, c)
			output.NewValues = append(output.NewValues, val)
		}
		return nil
	} else {
		return fmt.Errorf("error decoding string to %s value: %w", column.CQLType.String(), err)
	}

}

// processDeleteOperationForMapAndSet handles delete operations for maps and sets in prepared queries.
// Processes deletion of map entries and set elements with type validation.
// Returns error if value type doesn't match collection element type or deletion fails.
func processDeleteOperationForMapAndSet(
	column *types.Column,
	val *primitive.Value,
	pv primitive.ProtocolVersion,
	output *PreparedValues,
	expectedDt datatype.DataType,
) error {
	var err error

	decodedValue, err := collectiondecoder.DecodeCollection(expectedDt, pv, val.Contents)
	if err != nil {
		return fmt.Errorf("error decoding string to %s value: %w", column.CQLType.DataType(), err)
	}
	setValue, err := convertToInterfaceSlice(decodedValue)
	if err != nil {
		return err
	}
	for _, v := range setValue {
		setVal, err := primitivesToString(v)
		if err != nil {
			return fmt.Errorf("failed to convert value: %w", err)
		}
		output.DelColumns = append(output.DelColumns, &types.Column{
			Name:         setVal,
			ColumnFamily: column.Name,
		})
	}
	return nil
}

// convertToInterfaceSlice converts a value to a slice of interfaces.
// Handles type conversion for collection operations with validation.
// Returns error if value cannot be converted to a slice.
func convertToInterfaceSlice(decodedValue interface{}) ([]interface{}, error) {
	val := reflect.ValueOf(decodedValue)
	if val.Kind() != reflect.Slice {
		return nil, fmt.Errorf("expected a slice type, got %T", decodedValue)
	}

	setValue := make([]interface{}, val.Len())
	for i := 0; i < val.Len(); i++ {
		setValue[i] = val.Index(i).Interface()
	}
	return setValue, nil
}

// processMapKeyAppend handles map key updates operations in prepared queries.
// Processes updation at index and simple assignment.
// Returns error if key/value types don't match map types or encoding fails.
func processMapKeyAppend(
	column *types.Column,
	meta *ComplexOperation,
	pv primitive.ProtocolVersion,
	val *primitive.Value,
	output *PreparedValues,
	mt *types.MapType,
) error {
	expectedDT := meta.ExpectedDatatype
	//if expected datatype is timestamp, convert it to bigint
	if meta.ExpectedDatatype == datatype.Timestamp {
		expectedDT = datatype.Bigint
	}
	decodedValue, err := proxycore.DecodeType(expectedDT, pv, val.Contents)

	if err != nil {
		return fmt.Errorf("error decoding string to %s value: %w", column.CQLType.DataType(), err)
	}
	mpKey := meta.mapKey

	//if key is timestamp, convert it to unix milliseconds
	if mt.KeyType().DataType() == datatype.Timestamp {
		mp, err := parseTimestamp(mpKey.(string))
		if err != nil {
			return fmt.Errorf("error while typecasting to timestamp %v, -> %s", mpKey.(string), err)
		}
		mpKey = mp.UnixMilli()
	}

	setKey, err := primitivesToString(mpKey)
	if err != nil {
		return fmt.Errorf("failed to convert/read key: %w", err)
	}
	setValue, err := primitivesToString(decodedValue)
	if err != nil {
		return fmt.Errorf("failed to convert value for key '%s': %w", setKey, err)
	}

	c := &types.Column{
		Name:         setKey,
		ColumnFamily: column.Name,
		CQLType:      mt.ValueType(),
	}
	formattedValue, err := encodeValueForBigtable(setValue, mt.ValueType().DataType(), pv)
	if err != nil {
		return fmt.Errorf("failed to format value: %w", err)
	}
	output.Data = append(output.Data, c)
	output.NewValues = append(output.NewValues, formattedValue)

	return nil
}

// handlePrepareSetOperation processes set operations in prepared queries.
// Manages set operations with protocol version-specific value encoding.
// Returns error if value type doesn't match set element type or encoding fails.
func handlePrepareSetOperation(complexOps map[string]*ComplexOperation, val *primitive.Value, column *types.Column, colFamily types.ColumnFamily, pv primitive.ProtocolVersion, output *PreparedValues) error {
	if op, ok := complexOps[column.Name]; ok {
		//handling Delete for specific key e.g map=map-['key1','key2']
		if op.Delete {
			setType, ok := column.CQLType.(*types.SetType)
			if !ok {
				return fmt.Errorf("failed to assert set type for %s", column.CQLType.String())
			}
			if setType.ElementType().DataType() == datatype.Timestamp {
				setType = utilities.SetOfBigInt
			}
			return processDeleteOperationForMapAndSet(column, val, pv, output, setType.DataType())
		}
		// handling set operations for append.
		return handleSetProcessing(val, column, colFamily, pv, output)
	} else {
		// case of when new value is being set for collection type {e.g collection=newCollection}
		output.DelColumnFamily = append(output.DelColumnFamily, column.Name)
		return handleSetProcessing(val, column, colFamily, pv, output)
	}
}

// handleSetProcessing processes set values for prepared queries.
// Handles set operations for append, delete, and update at index and simple assignment.
// Returns error if value type doesn't match set element type or encoding fails.
func handleSetProcessing(
	val *primitive.Value,
	column *types.Column,
	colFamily types.ColumnFamily,
	protocolV primitive.ProtocolVersion,
	output *PreparedValues,
) error {
	setType, ok := column.CQLType.(*types.SetType)
	if !ok {
		return fmt.Errorf("failed to assert set type for %s", column.CQLType.String())
	}
	if setType.ElementType().Code() == types.TIMESTAMP {
		setType = utilities.SetOfBigInt
	}
	switch setType.ElementType().DataType() {
	case datatype.Varchar:
		return processSet[string](val, column, colFamily, protocolV, setType, output)
	case datatype.Int:
		return processSet[int32](val, column, colFamily, protocolV, setType, output)
	case datatype.Bigint, datatype.Timestamp:
		return processSet[int64](val, column, colFamily, protocolV, setType, output)
	case datatype.Float:
		return processSet[float32](val, column, colFamily, protocolV, setType, output)
	case datatype.Double:
		return processSet[float64](val, column, colFamily, protocolV, setType, output)
	case datatype.Boolean:
		return processSet[bool](val, column, colFamily, protocolV, setType, output)
	default:
		return fmt.Errorf("unexpected set element type: %T", column.CQLType.DataType())
	}
}

// processSet handles generic set processing operations in prepared queries.
// Manages set operations with type-specific handling and protocol version encoding.
// Returns error if value type doesn't match set element type or encoding fails.
func processSet[V any](
	val *primitive.Value,
	column *types.Column,
	colFamily types.ColumnFamily,
	protocolV primitive.ProtocolVersion,
	setType *types.SetType,
	output *PreparedValues,

) error {

	decodedValue, err := collectiondecoder.DecodeCollection(setType.DataType(), protocolV, val.Contents)
	if err != nil {
		return fmt.Errorf("error decoding string to %s value: %w", column.CQLType.DataType(), err)
	}

	setValues, ok := decodedValue.([]V)
	if !ok {
		return fmt.Errorf("unexpected set element type: %T", decodedValue)
	}

	// Common processing logic for all types
	for _, elem := range setValues {
		valueStr, err := primitivesToString(elem)
		if err != nil {
			return fmt.Errorf("failed to convert element to string: %w", err)
		}
		c := &types.Column{
			Name:         valueStr,
			ColumnFamily: colFamily,
			CQLType:      setType.ElementType(),
		}
		// The value parameter is intentionally empty because the column identifier has the value
		val, err := encodeValueForBigtable("", datatype.Varchar, protocolV)
		if err != nil {
			return fmt.Errorf("failed to convert value for set: %w", err)
		}
		output.Data = append(output.Data, c)
		output.NewValues = append(output.NewValues, val)
	}
	return nil
}

// buildWhereClause(): takes a slice of Condition structs and returns a string representing the WHERE clause of a bigtable SQL query.
// It iterates over the clauses and constructs the WHERE clause by combining the column name, operator, and value of each clause.
// If the operator is "IN", the value is wrapped with the UNNEST function.
// The constructed WHERE clause is returned as a string.
func buildWhereClause(clauses []types.Condition, tableConfig *schemaMapping.TableConfig) (string, error) {
	whereClause := ""
	columnFamily := tableConfig.SystemColumnFamily
	for _, val := range clauses {
		column := "`" + val.Column + "`"
		value := val.Value
		if colMeta, ok := tableConfig.Columns[val.Column]; ok {
			// Check if the column is a primitive type and prepend the column family
			if !colMeta.CQLType.IsCollection() {
				var castErr error
				column, castErr = castColumns(colMeta, columnFamily)
				if castErr != nil {
					return "", castErr
				}
			}
		}
		if whereClause != "" && val.Operator != constants.BETWEEN_AND {
			whereClause += " AND "
		}
		if val.Operator == constants.BETWEEN {
			whereClause += fmt.Sprintf("%s BETWEEN %s", column, val.Value)
		} else if val.Operator == constants.BETWEEN_AND {
			whereClause += fmt.Sprintf(" AND %s", val.Value)
		} else if val.Operator == constants.IN {
			whereClause += fmt.Sprintf("%s IN UNNEST(%s)", column, val.Value)
		} else if val.Operator == constants.MAP_CONTAINS_KEY {
			whereClause += fmt.Sprintf("MAP_CONTAINS_KEY(%s, %s)", column, value)
		} else if val.Operator == constants.ARRAY_INCLUDES {
			whereClause += fmt.Sprintf("ARRAY_INCLUDES(MAP_VALUES(%s), %s)", column, value)
		} else {
			whereClause += fmt.Sprintf("%s %s %s", column, val.Operator, value)
		}
	}

	if whereClause != "" {
		whereClause = " WHERE " + whereClause
	}
	return whereClause, nil
}

// castColumns handles column type casting in queries.
// Manages type conversion for column values with validation.
// Returns error if column type is invalid or conversion fails.
func castColumns(colMeta *types.Column, columnFamily string) (string, error) {
	var nc string
	switch colMeta.CQLType.DataType() {
	case datatype.Int:
		if colMeta.IsPrimaryKey {
			nc = colMeta.Name
		} else {
			nc = fmt.Sprintf("TO_INT64(%s['%s'])", columnFamily, colMeta.Name)
		}
	case datatype.Bigint:
		if colMeta.IsPrimaryKey {
			nc = colMeta.Name
		} else {
			nc = fmt.Sprintf("TO_INT64(%s['%s'])", columnFamily, colMeta.Name)
		}
	case datatype.Float:
		nc = fmt.Sprintf("TO_FLOAT32(%s['%s'])", columnFamily, colMeta.Name)
	case datatype.Double:
		nc = fmt.Sprintf("TO_FLOAT64(%s['%s'])", columnFamily, colMeta.Name)
	case datatype.Boolean:
		nc = fmt.Sprintf("TO_INT64(%s['%s'])", columnFamily, colMeta.Name)
	case datatype.Timestamp:
		nc = fmt.Sprintf("TO_TIME(%s['%s'])", columnFamily, colMeta.Name)
	case datatype.Counter:
		nc = fmt.Sprintf("%s['']", colMeta.Name)
	case datatype.Blob:
		nc = fmt.Sprintf("TO_BLOB(%s['%s'])", columnFamily, colMeta.Name)
	case datatype.Varchar:
		if colMeta.IsPrimaryKey {
			nc = colMeta.Name
		} else {
			nc = fmt.Sprintf("%s['%s']", columnFamily, colMeta.Name)
		}
	default:
		return "", fmt.Errorf("unsupported CQL type: %s", colMeta.CQLType.DataType())
	}
	return nc, nil
}

// parseWhereByClause parses the WHERE clause from a CQL query.
// Extracts and processes WHERE conditions with type validation.
// Returns error if clause parsing fails or invalid conditions are found.
func parseWhereByClause(input cql.IWhereSpecContext, tableConfig *schemaMapping.TableConfig) (*WhereClause, error) {
	if input == nil {
		return nil, errors.New("no input parameters found for clauses")
	}

	elements := input.RelationElements().AllRelationElement()
	if elements == nil {
		return nil, errors.New("no input parameters found for clauses")
	}

	if len(elements) == 0 {
		return &WhereClause{}, nil
	}
	var clauses []types.Condition
	var response WhereClause
	var paramKeys []string

	params := make(map[string]interface{})

	placeholderCount := 0
	for _, val := range elements {
		if val == nil {
			return nil, errors.New("could not parse column object")
		}
		s := strconv.Itoa(placeholderCount + 1)
		placeholderCount++
		placeholder := "value" + s
		secondPlaceholder := "value" + strconv.Itoa(placeholderCount+1) // It will be used for second value of BETWEEN clause
		operator := ""
		paramKeys = append(paramKeys, placeholder)

		isInOperator := false
		isBetweenOperator := false
		colName := ""
		value := ""
		if val.OPERATOR_EQ() != nil {
			operator = val.OPERATOR_EQ().GetText()
		} else if val.OPERATOR_GT() != nil {
			operator = val.OPERATOR_GT().GetSymbol().GetText()
		} else if val.OPERATOR_LT() != nil {
			operator = val.OPERATOR_LT().GetSymbol().GetText()
		} else if val.OPERATOR_GTE() != nil {
			operator = val.OPERATOR_GTE().GetSymbol().GetText()
		} else if val.OPERATOR_LTE() != nil {
			operator = val.OPERATOR_LTE().GetSymbol().GetText()
		} else if val.KwIn() != nil {
			operator = "IN"
			isInOperator = true
		} else if val.RelalationContains() != nil {
			operator = "CONTAINS"
			relContains := val.RelalationContains()
			colName = relContains.OBJECT_NAME().GetText()
			value = relContains.Constant().GetText()
		} else if val.RelalationContainsKey() != nil {
			operator = "CONTAINS KEY"
			relContainsKey := val.RelalationContainsKey()
			colName = relContainsKey.OBJECT_NAME().GetText()
			value = relContainsKey.Constant().GetText()
		} else if val.KwLike() != nil {
			operator = "LIKE"
		} else if val.KwBetween() != nil {
			operator = constants.BETWEEN
			isBetweenOperator = true
		} else {
			return nil, errors.New("no supported operator found")
		}

		if colName == "" {
			colObj := val.OBJECT_NAME(0)
			if colObj == nil {
				return nil, errors.New("could not parse column object")
			}
			colName = colObj.GetText()
			if colName == "" {
				return nil, errors.New("could not parse column name")
			}
		}
		column, err := tableConfig.GetColumn(colName)
		if err != nil {
			return nil, err
		}
		if column != nil && column.CQLType.DataType() != nil {
			if operator == constants.CONTAINS || operator == constants.CONTAINS_KEY {
				if value == "" {
					return nil, errors.New("could not parse value from query for one of the clauses")
				}
				value = trimQuotes(value)
				typeCode := column.CQLType.DataType().GetDataTypeCode()
				if value != questionMark {
					if operator == constants.CONTAINS {
						if typeCode == primitive.DataTypeCodeList { // list
							operator = constants.ARRAY_INCLUDES
						} else if typeCode == primitive.DataTypeCodeSet { // set
							operator = constants.MAP_CONTAINS_KEY
						} else {
							return nil, errors.New("CONTAINS are only supported for set and list")
						}
					} else {
						if typeCode == primitive.DataTypeCodeMap { // map
							operator = constants.MAP_CONTAINS_KEY
						} else {
							return nil, errors.New("CONTAINS KEY are only supported for map")
						}
					}
					params[placeholder] = []byte(value)
				} else {
					if operator == "CONTAINS" {
						if typeCode == primitive.DataTypeCodeSet { // set
							setType, ok := column.CQLType.(*types.SetType)
							if !ok {
								return nil, errors.New("expected set type column")
							}
							elementType := setType.ElementType()
							params[placeholder] = cqlTypeToEmptyPrimitive(elementType.DataType(), false)
							operator = constants.MAP_CONTAINS_KEY
						} else if typeCode == primitive.DataTypeCodeList { // list
							listType, ok := column.CQLType.DataType().(datatype.ListType)
							if !ok {
								return nil, errors.New("expected list type column")
							}
							elementType := listType.GetElementType()
							params[placeholder] = cqlTypeToEmptyPrimitive(elementType, false)
							operator = constants.ARRAY_INCLUDES
						} else {
							return nil, errors.New("CONTAINS are only supported for set and list")
						}
					} else {
						// If it's not contains, then it's a CONTAINS KEY operator
						if typeCode == primitive.DataTypeCodeMap { // map
							// keyType := mapType.GetKeyType()
							params[placeholder] = cqlTypeToEmptyPrimitive(datatype.Boolean, false)
							operator = constants.MAP_CONTAINS_KEY
						} else {
							return nil, errors.New("CONTAINS KEY are only supported for map")
						}
					}
				}
			} else if isBetweenOperator {
				value := val.Constant(0).GetText()       // As per the testing val.Constant(0) values never be nil
				secondValue := val.Constant(1).GetText() // Same here, As per the testing val.Constant(1) values never be nil
				if value == "" || secondValue == "" || value == "<EOF>" || secondValue == "<EOF>" {
					return nil, errors.New("could not parse value from query for one of the clauses")
				}
				value = trimQuotes(value)
				secondValue = trimQuotes(secondValue)
				if value != questionMark {
					val, err := stringToPrimitives(value, column.CQLType.DataType())
					if err != nil {
						return nil, err
					}
					secondVal, err := stringToPrimitives(secondValue, column.CQLType.DataType())
					if err != nil {
						return nil, err
					}
					params[placeholder] = val
					params[secondPlaceholder] = secondVal
				} else {
					params[placeholder] = cqlTypeToEmptyPrimitive(column.CQLType.DataType(), column.IsPrimaryKey)
					params[secondPlaceholder] = params[placeholder] // CQLType will be same for both the values in the between clause
				}
				placeholderCount++ // we need to increase the placeholder count to get the next placeholder as BETWEEN clause has two values
				paramKeys = append(paramKeys, secondPlaceholder)
			} else if !isInOperator && !isBetweenOperator {
				valConst := val.Constant(0)
				if valConst == nil {
					return nil, errors.New("could not parse value from query for one of the clauses")
				}
				value := val.Constant(0).GetText()
				if value == "" {
					return nil, errors.New("could not parse value from query for one of the clauses")
				}
				value = trimQuotes(value)

				if value != questionMark {

					val, err := stringToPrimitives(value, column.CQLType.DataType())
					if err != nil {
						return nil, err
					}

					params[placeholder] = val
				} else {
					params[placeholder] = cqlTypeToEmptyPrimitive(column.CQLType.DataType(), column.IsPrimaryKey)
				}
			} else {
				lower := strings.ToLower(val.GetText())
				if !strings.Contains(lower, "?") {
					valueFn := val.FunctionArgs()
					if valueFn == nil {
						return nil, errors.New("could not parse Function arguments")
					}
					value := valueFn.AllConstant()
					if value == nil {
						return nil, errors.New("could not parse all values inside IN operator")
					}
					switch column.CQLType.DataType() {
					case datatype.Int:
						var allValues []int
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := trimQuotes(inVal.GetText())
							i, err := strconv.Atoi(valueTxt)
							if err != nil {
								return nil, err
							}
							allValues = append(allValues, i)
						}
						params[placeholder] = allValues
					case datatype.Bigint:
						var allValues []int64
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := trimQuotes(inVal.GetText())
							i, err := strconv.ParseInt(valueTxt, 10, 64)
							if err != nil {
								return nil, err
							}
							allValues = append(allValues, i)
						}
						params[placeholder] = allValues
					case datatype.Float:
						var allValues []float32
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							i, err := strconv.ParseFloat(valueTxt, 32)

							if err != nil {
								return nil, err
							}
							allValues = append(allValues, float32(i))
						}
						params[placeholder] = allValues
					case datatype.Double:
						var allValues []float64
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := inVal.GetText()
							i, err := strconv.ParseFloat(valueTxt, 64)
							if err != nil {
								return nil, err
							}
							allValues = append(allValues, i)
						}
						params[placeholder] = allValues
					case datatype.Boolean:
						var allValues []bool
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := trimQuotes(inVal.GetText())
							val := strings.ToLower(valueTxt) == "true"
							allValues = append(allValues, val)
						}
						params[placeholder] = allValues
					case datatype.Blob:
					case datatype.Varchar, datatype.Timestamp:
						var allValues []string
						for _, inVal := range value {
							if inVal == nil {
								return nil, errors.New("could not parse value")
							}
							valueTxt := trimQuotes(inVal.GetText())
							allValues = append(allValues, valueTxt)
						}
						params[placeholder] = allValues

					default:
						err := errors.New("no correct Datatype found for column")
						return nil, err
					}
				} else {
					// Create an empty array placeholder value based on the CQL type
					switch column.CQLType.DataType() {
					case datatype.Int:
						params[placeholder] = make([]int, 0)
					case datatype.Bigint:
						params[placeholder] = make([]int64, 0)
					case datatype.Float:
						params[placeholder] = make([]float32, 0)
					case datatype.Double:
						params[placeholder] = make([]float64, 0)
					case datatype.Boolean:
						params[placeholder] = make([]bool, 0)
					case datatype.Blob:
						params[placeholder] = make([][]byte, 0)
					case datatype.Varchar, datatype.Timestamp:
						params[placeholder] = make([]string, 0)
					default:
						return nil, fmt.Errorf("unsupported array CQL type: %s", column.CQLType.DataType())
					}
				}
			}
			clause := &types.Condition{
				Column:   column,
				Operator: operator,
				Value:    "@" + placeholder,
			}

			clauses = append(clauses, *clause)
			if isBetweenOperator {
				clause = &types.Condition{
					Column:   column,
					Operator: constants.BETWEEN_AND,
					Value:    "@" + secondPlaceholder,
				}
				clauses = append(clauses, *clause)
			}
		}
	}
	response.Conditions = clauses
	response.Params = params
	response.ParamKeys = paramKeys
	return &response, nil
}

// getTimestampValue extracts timestamp value from a query.
// Parses timestamp specifications in USING TIMESTAMP clauses with validation.
// Returns error if timestamp format is invalid or parsing fails.
func getTimestampValue(spec cql.IUsingTtlTimestampContext) (string, error) {
	if spec == nil {
		return "", errors.New("invalid input")
	}

	spec.KwUsing()
	tsSpec := spec.Timestamp()

	if tsSpec == nil {
		return questionMark, nil
	}
	tsSpec.KwTimestamp()
	valLiteral := tsSpec.DecimalLiteral()

	if valLiteral == nil {
		return "", errors.New("no value found for Timestamp")
	}

	resp := trimQuotes(valLiteral.GetText())
	return resp, nil
}

func trimQuotes(s string) string {
	if len(s) < 2 {
		return s
	}
	if s[0] == '\'' && s[len(s)-1] == '\'' {
		s = s[1 : len(s)-1]
	}
	return strings.ReplaceAll(s, `''`, `'`)
}

// GetTimestampInfo retrieves timestamp information from an insert query.
// Extracts and processes timestamp values with validation and conversion.
// Returns error if timestamp format is invalid or conversion fails.
func GetTimestampInfo(insertObj cql.IInsertContext, index int32) (TimestampInfo, error) {
	var timestampInfo TimestampInfo
	if insertObj.UsingTtlTimestamp() != nil {
		tsSpec := insertObj.UsingTtlTimestamp()
		if tsSpec == nil {
			return timestampInfo, errors.New("error parsing timestamp spec")
		}
		tsValue, err := getTimestampValue(tsSpec)
		if err != nil {
			return timestampInfo, err
		}
		return convertToBigtableTimestamp(tsValue, index)
	}
	return timestampInfo, nil
}

// convertToBigtableTimestamp converts a timestamp to Bigtable format.
// Handles timestamp conversion and formatting with validation.
// Returns error if timestamp format is invalid or conversion fails.
func convertToBigtableTimestamp(tsValue string, index int32) (TimestampInfo, error) {
	var timestampInfo TimestampInfo
	unixTime := int64(0)
	var err error
	if tsValue != "" && !strings.Contains(tsValue, questionMark) {
		unixTime, err = strconv.ParseInt(tsValue, 10, 64)
		if err != nil {
			return timestampInfo, err
		}
	}
	t := time.Unix(unixTime, 0)
	switch len(tsValue) {
	case 13: // Milliseconds
		t = time.Unix(0, unixTime*int64(time.Millisecond))
	case 16: // Microseconds
		t = time.Unix(0, unixTime*int64(time.Microsecond))
	}
	microsec := t.UnixMicro()
	timestampInfo.Timestamp = bigtable.Timestamp(microsec)
	timestampInfo.HasUsingTimestamp = true
	timestampInfo.Index = index
	return timestampInfo, nil
}

// GetTimestampInfoForRawDelete retrieves timestamp information for delete operations.
// Extracts timestamp values from raw delete queries with validation.
// Returns error if timestamp format is invalid or extraction fails.
func GetTimestampInfoForRawDelete(deleteObj cql.IDelete_Context) (TimestampInfo, error) {
	var timestampInfo TimestampInfo
	hasUsingTimestamp := deleteObj.UsingTimestampSpec() != nil
	timestampInfo.HasUsingTimestamp = false
	if hasUsingTimestamp {
		tsSpec := deleteObj.UsingTimestampSpec()
		if tsSpec == nil {
			return timestampInfo, ErrParsingTs
		}

		timestampSpec := tsSpec.Timestamp()
		if timestampSpec == nil {
			return timestampInfo, ErrParsingDelObj
		}

		if tsSpec.KwUsing() != nil {
			tsSpec.KwUsing()
		}
		valLiteral := timestampSpec.DecimalLiteral()
		if valLiteral == nil {
			return timestampInfo, ErrTsNoValue
		}
		tsValue := valLiteral.GetText()
		return convertToBigtableTimestamp(tsValue, 0)
	}
	return timestampInfo, nil
}

// GetTimestampInfoByUpdate retrieves timestamp information for update operations.
// Extracts timestamp values from update queries with validation.
// Returns error if timestamp format is invalid or extraction fails.
func GetTimestampInfoByUpdate(updateObj cql.IUpdateContext) (TimestampInfo, error) {
	var timestampInfo TimestampInfo
	ttlTimestampObj := updateObj.UsingTtlTimestamp()
	if ttlTimestampObj != nil {
		tsValue, err := getTimestampValue(updateObj.UsingTtlTimestamp())
		if err != nil {
			return TimestampInfo{}, err
		}
		timestampInfo, err = convertToBigtableTimestamp(tsValue, 0)
		if err != nil {
			return TimestampInfo{}, err
		}
	}
	return timestampInfo, nil
}

// getTimestampInfoForPrepareQuery retrieves timestamp information from prepared queries.
// Extracts timestamp values from prepared statement parameters with validation.
// Returns error if timestamp format is invalid or extraction fails.
func getTimestampInfoForPrepareQuery(values []*primitive.Value, index int32, offset int32) (TimestampInfo, error) {
	var timestampInfo TimestampInfo
	timestampInfo.HasUsingTimestamp = true
	if values[index] == nil {
		err := fmt.Errorf("error processing timestamp column in prepare insert")
		return timestampInfo, err
	}
	vBytes := values[index].Contents
	decode, err := proxycore.DecodeType(datatype.Bigint, 4, vBytes)
	if err != nil {
		fmt.Println("error while decoding timestamp column in prepare insert:", err)
		return timestampInfo, err
	}
	timestamp := decode.(int64)
	var t time.Time
	if timestamp < 10000000000 { // Assuming it's in seconds
		t = time.Unix(timestamp, 0)
	} else if timestamp < 10000000000000 { // Assuming it's in milliseconds
		t = time.Unix(0, timestamp*int64(time.Millisecond))
	} else { // As, we are supporting till microseconds, we are not converting it to microseconds
		t = time.Unix(0, timestamp*int64(time.Microsecond))
	}
	microsec := t.UnixMicro()
	timestampInfo.Timestamp = bigtable.Timestamp(microsec)
	return timestampInfo, nil
}

// getTimestampInfo processes timestamp values for insert operations.
// Handles timestamp processing and validation with type conversion.
// Returns error if timestamp format is invalid or processing fails.
func getTimestampInfo(st *PreparedInsertQuery, values []*primitive.Value) (TimestampInfo, error) {
	if st.TimestampInfo.HasUsingTimestamp {
		return getTimestampInfoForPrepareQuery(values, st.TimestampInfo.Index, 0)
	}
	return TimestampInfo{HasUsingTimestamp: false}, nil
}

// ProcessTimestampByUpdate processes timestamp values for update operations.
// Handles timestamp processing and validation with type conversion.
// Returns error if timestamp format is invalid or processing fails.
func ProcessTimestampByUpdate(st *UpdateQueryMapping, values []*primitive.Value) (TimestampInfo, []*primitive.Value, []*message.ColumnMetadata, error) {
	if st.TimestampInfo.HasUsingTimestamp {
		timestampInfo, err := getTimestampInfoForPrepareQuery(values, st.TimestampInfo.Index, 0)
		if err != nil {
			return timestampInfo, values, st.VariableMetadata, err
		}
		values = values[1:]
		return timestampInfo, values, st.VariableMetadata[1:], nil
	}
	return TimestampInfo{HasUsingTimestamp: false}, values, st.VariableMetadata, nil
}

// ProcessTimestampByDelete processes timestamp values for delete operations.
// Handles timestamp processing and validation with type conversion.
// Returns error if timestamp format is invalid or processing fails.
func ProcessTimestampByDelete(st *DeleteQueryMapping, values []*primitive.Value) (TimestampInfo, []*primitive.Value, error) {
	var timestampInfo TimestampInfo
	timestampInfo.HasUsingTimestamp = false
	var err error
	if st.TimestampInfo.HasUsingTimestamp {
		timestampInfo, err = getTimestampInfoForPrepareQuery(values, st.TimestampInfo.Index, 1)
		if err != nil {
			return timestampInfo, values, err
		}
		values = values[1:]
	}
	return timestampInfo, values, nil
}

func ValidateRequiredPrimaryKeysOnly(tableConfig *schemaMapping.TableConfig, provided []types.ColumnName) error {
	err := ValidateRequiredPrimaryKeys(tableConfig, provided)
	if err != nil {
		return err
	}
	if len(tableConfig.PrimaryKeys) != len(provided) {
		for _, colName := range provided {
			col, err := tableConfig.GetColumn(colName)
			if err != nil {
				return err
			}
			if !col.IsPrimaryKey {
				return fmt.Errorf("non-primary key found in where clause: '%s'", col.Name)
			}
		}
	}

	return nil
}
func ValidateRequiredPrimaryKeys(tableConfig *schemaMapping.TableConfig, provided []types.ColumnName) error {
	// primary key counts are very small for legitimate use cases so greedy iterations are fine
	for _, wantKey := range tableConfig.PrimaryKeys {
		count := 0
		for _, gotKey := range provided {
			if wantKey.Name == gotKey {
				count++
			}
		}
		if count == 0 {
			return fmt.Errorf("missing primary key: '%s'", wantKey.Name)
		} else if count > 1 {
			return fmt.Errorf("multiple occurences of primary key: '%s'", wantKey.Name)
		}
	}

	return nil
}

// encodeTimestamp encodes a timestamp value into bytes.
// Converts timestamp values to byte representation with validation.
// Returns error if timestamp format is invalid or encoding fails.
func encodeTimestamp(millis int64, nanos int32) []byte {
	buf := make([]byte, 12) // 8 bytes for millis + 4 bytes for nanos
	binary.BigEndian.PutUint64(buf[0:8], uint64(millis))
	binary.BigEndian.PutUint32(buf[8:12], uint32(nanos))
	return buf
}

// getEncodedTimestamp generates encoded timestamp bytes.
// Creates timestamp byte representation for specific positions with validation.
// Returns error if position is invalid or encoding fails.
func getEncodedTimestamp(index int, totalLength int, prepend bool) []byte {
	now := time.Now().UnixMilli()
	if prepend {
		now = referenceTime - (now - referenceTime)
	}

	nanos := maxNanos - int32(totalLength) + int32(index)
	return encodeTimestamp(now, nanos)
}

// ExtractWritetimeValue extracts writetime value from a string.
// Parses and extracts writetime specifications with validation.
// Returns error if format is invalid or extraction fails.
func ExtractWritetimeValue(s string) (string, bool) {
	s = strings.ToLower(s)
	// Check if the input starts with "writetime(" and ends with ")"
	if strings.HasPrefix(s, "writetime(") && strings.HasSuffix(s, ")") {
		// Extract the content between "writetime(" and ")"
		value := s[len("writetime(") : len(s)-1]
		return value, true
	}
	// Return empty string and false if the format is incorrect
	return "", false
}

// convertAllValuesToRowKeyType converts values to row key types.
// Handles type conversion for row key values with validation.
// Returns error if value type is invalid or conversion fails.
func convertAllValuesToRowKeyType(primaryKeys []*types.Column, values map[types.ColumnName]types.GoValue) (map[types.ColumnName]types.GoValue, error) {
	result := make(map[types.ColumnName]types.GoValue)
	for _, pmk := range primaryKeys {
		if !pmk.IsPrimaryKey {
			continue
		}

		value, exists := values[pmk.Name]
		if !exists {
			return nil, fmt.Errorf("missing primary key `%s`", pmk.Name)
		}
		switch pmk.CQLType.DataType() {
		case datatype.Int:
			switch v := value.(type) {
			// bigtable row keys don't support int32 so convert all int32 values to int64
			case int:
				result[pmk.Name] = int64(v)
			case int32:
				result[pmk.Name] = int64(v)
			case int64:
				result[pmk.Name] = v
			case string:
				i, err := strconv.Atoi(v)
				if err != nil {
					return nil, fmt.Errorf("failed to convert Int value %s for key %s", value.(string), pmk.Name)
				}
				result[pmk.Name] = int64(i)
			default:
				return nil, fmt.Errorf("failed to convert %T to Int for key %s", value, pmk.Name)
			}
		case datatype.Bigint:
			switch v := value.(type) {
			case int:
				result[pmk.Name] = int64(v)
			case int32:
				result[pmk.Name] = int64(v)
			case int64:
				result[pmk.Name] = value
			case string:
				i, err := strconv.ParseInt(v, 10, 0)
				if err != nil {
					return nil, fmt.Errorf("failed to convert BigInt value %s for key %s", value.(string), pmk.Name)
				}
				result[pmk.Name] = i
			default:
				return nil, fmt.Errorf("failed to convert %T to BigInt for key %s", value, pmk.Name)
			}
		case datatype.Varchar:
			switch v := value.(type) {
			case string:
				// todo move this validation to all columns not just keys
				if !utf8.Valid([]byte(v)) {
					return nil, fmt.Errorf("invalid utf8 value provided for varchar row key field %s", pmk.Name)
				}
				result[pmk.Name] = v
			default:
				return nil, fmt.Errorf("failed to convert %T to BigInt for key %s", value, pmk.Name)
			}
		case datatype.Blob:
			switch v := value.(type) {
			case string:
				result[pmk.Name] = v
			default:
				return nil, fmt.Errorf("failed to convert %T to BigInt for key %s", value, pmk.Name)
			}
		default:
			return nil, fmt.Errorf("unsupported primary key type %s for key %s", pmk.CQLType.String(), pmk.Name)
		}
	}
	return result, nil
}

var kOrderedCodeEmptyField = []byte("\x00\x00")
var kOrderedCodeDelimiter = []byte("\x00\x01")

// createOrderedCodeKey creates an ordered row key.
// Generates a byte-encoded row key from primary key values with validation.
// Returns error if key type is invalid or encoding fails.
func createOrderedCodeKey(tableConfig *schemaMapping.TableConfig, values map[types.ColumnName]types.GoValue) (types.RowKey, error) {
	fixedValues, err := convertAllValuesToRowKeyType(tableConfig.PrimaryKeys, values)
	if err != nil {
		return "", err
	}

	var result []byte
	var trailingEmptyFields []byte
	for i, pmk := range tableConfig.PrimaryKeys {
		if i != pmk.PkPrecedence-1 {
			return "", fmt.Errorf("wrong order for primary keys")
		}
		value, exists := fixedValues[pmk.Name]
		if !exists {
			return "", fmt.Errorf("missing primary key `%s`", pmk.Name)
		}

		var orderEncodedField []byte
		var err error
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

func DataConversionInInsertionIfRequired(value interface{}, pv primitive.ProtocolVersion, cqlType datatype.DataType) (interface{}, error) {
	switch cqlType {
	case datatype.Boolean:
		return encodeBoolForBigtable(value, pv)
	case datatype.Bigint, datatype.Int:
		return encodeBigIntForBigtable(value, pv)
	default:
		return value, nil
	}
}

func encodeTimestampForBigtable(value interface{}, clientPv primitive.ProtocolVersion) ([]byte, error) {
	var t time.Time
	switch v := value.(type) {
	case string:
		var err error
		t, err = parseTimestamp(v)
		if err != nil {
			return nil, fmt.Errorf("error converting string to timestamp: %w", err)
		}
	case int64:
		t = time.UnixMilli(v)
	default:
		return nil, fmt.Errorf("unsupported timestamp type: %T", value)
	}
	return proxycore.EncodeType(datatype.Timestamp, primitive.ProtocolVersion4, t)
}

// encodeBoolForBigtable encodes boolean values to bytes.
// Converts boolean values to byte representation with validation.
// Returns error if value is invalid or encoding fails.
func encodeBoolForBigtable(value interface{}, clientPv primitive.ProtocolVersion) ([]byte, error) {
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
		bd, err := proxycore.EncodeType(datatype.Bigint, clientPv, intVal)
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
		bd, err := proxycore.EncodeType(datatype.Bigint, clientPv, valInBigint)
		if err != nil {
			return nil, err
		}
		return bd, nil
	case []byte:
		vaInInterface, err := proxycore.DecodeType(datatype.Boolean, clientPv, v)
		if err != nil {
			return nil, err
		}
		if vaInInterface == nil {
			return nil, nil
		}
		if vaInInterface.(bool) {
			return proxycore.EncodeType(datatype.Bigint, clientPv, 1)
		} else {
			return proxycore.EncodeType(datatype.Bigint, clientPv, 0)
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

// encodeBigIntForBigtable encodes bigint values to bytes.
// Converts bigint values to byte representation with validation.
// Returns error if value is invalid or encoding fails.
func encodeBigIntForBigtable(value interface{}, pv primitive.ProtocolVersion) ([]byte, error) {
	intVal, err := parseCassandraValueToInt64(value, pv)
	if err != nil {
		return nil, err
	}
	result, err := proxycore.EncodeType(datatype.Bigint, primitive.ProtocolVersion4, intVal)
	if err != nil {
		return nil, fmt.Errorf("failed to encode bigint: %w", err)
	}
	return result, err
}

// encodeBigIntForBigtable encodes bigint values to bytes.
// Converts bigint values to byte representation with validation.
// Returns error if value is invalid or encoding fails.
func encodeFloat64ForBigtable(value interface{}, pv primitive.ProtocolVersion) ([]byte, error) {
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
		floatAny, err := proxycore.DecodeType(datatype.Double, pv, v)
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
	result, err := proxycore.EncodeType(datatype.Double, pv, floatVal)
	if err != nil {
		return nil, fmt.Errorf("failed to encode double: %w", err)
	}
	return result, err
}

// ProcessComplexUpdate processes complex update operations.
// Handles complex column updates including collections and counters with validation.
// Returns error if update type is invalid or processing fails.
func (t *Translator) ProcessComplexUpdate(columns []*types.Column, values []ComplexAssignment) (map[types.ColumnFamily]*ComplexOperation, error) {
	complexMeta := make(map[types.ColumnFamily]*ComplexOperation)
	for i, col := range columns {
		ca := values[i]
		meta := &ComplexOperation{}

		switch col.CQLType.Code() {
		case types.LIST:
			// a. + operation (append): only set append true
			if ca.Operation == "+" && ca.Left == col.Name {
				meta.Append = true
			}
			// b. + operation (prepend): only set prepend true
			if ca.Operation == "+" && ca.Right == col.Name {
				meta.PrependList = true
			}
			// c. marks[1]=? (update for index): set updateListIndex, set expected datatype as value type
			if ca.Operation == "update_index" {
				if idx, ok := ca.Left.(string); ok {
					meta.UpdateListIndex = idx
				}
				if listType, ok := col.CQLType.DataType().(datatype.ListType); ok {
					meta.ExpectedDatatype = listType.GetElementType()
				}
			}
			// d. - operation: just mark listDelete and delete as true
			if ca.Operation == "-" {
				meta.ListDelete = true
				meta.Delete = true
			}
			complexMeta[col.ColumnFamily] = meta

		case types.MAP:
			// 1. + operation: only mark append as true
			if ca.Operation == "+" {
				meta.Append = true
			}
			// 2. - operation: set expected datatype as SET<value_datatype>, mark delete as true
			if ca.Operation == "-" {
				meta.Delete = true
				// get map value datatype
				if mapType, ok := col.CQLType.DataType().(datatype.MapType); ok {
					meta.ExpectedDatatype = datatype.NewSetType(mapType.GetKeyType())
				}
			}
			// 3. map[key]=? (update for particular key): set updateMapKey, set expected datatype as value type, mark append as true
			if ca.Operation == "update_key" || ca.Operation == "update_index" {
				meta.Append = true
				if mapType, ok := col.CQLType.DataType().(datatype.MapType); ok {
					meta.ExpectedDatatype = mapType.GetValueType()
				}
				if key, ok := ca.Left.(string); ok {
					meta.mapKey = key
				}
			}
			complexMeta[col.ColumnFamily] = meta
		case types.SET:
			if ca.Operation == "-" {
				meta.Delete = true
			}
			complexMeta[col.ColumnFamily] = meta
		case types.COUNTER:
			if ca.Operation == "+" || ca.Operation == "-" {
				var op = Increment
				if ca.Operation == "-" {
					op = Decrement
				}
				meta.IncrementType = op
				meta.ExpectedDatatype = datatype.Bigint
				complexMeta[col.ColumnFamily] = meta
			} else {
				return nil, fmt.Errorf("unsupported counter operation: `%s`", ca.Operation)
			}
		default:
			return nil, fmt.Errorf("column %s is not a collection type", col.Name)
		}
	}
	return complexMeta, nil
}

// getFromSpecElement extracts the FROM clause element from a query.
// Parses and returns the FROM specification element with validation.
// Returns error if FROM clause is invalid or parsing fails.
func getFromSpecElement(input cql.IFromSpecContext) (cql.IFromSpecElementContext, error) {
	if input == nil {
		return nil, errors.New("input context is nil")
	}
	fromSpec := input.FromSpecElement()
	if fromSpec == nil {
		return nil, errors.New("error while parsing fromSpec")
	}
	return fromSpec, nil
}

// getAllObjectNames extracts all object names from a FROM clause.
// Returns a list of all object names in the FROM specification with validation.
// Returns error if no objects found or parsing fails.
func getAllObjectNames(fromSpec cql.IFromSpecElementContext) ([]antlr.TerminalNode, error) {
	allObj := fromSpec.AllOBJECT_NAME()
	if allObj == nil {
		return nil, errors.New("error while parsing all objects from the fromSpec")
	}
	if len(allObj) == 0 {
		return nil, errors.New("could not find table and keyspace name")
	}
	return allObj, nil
}

// getTableAndKeyspaceObjects extracts table and keyspace names.
// Parses and returns the table and keyspace names from object names with validation.
// Returns error if names are invalid or parsing fails.
func getTableAndKeyspaceObjects(allObj []antlr.TerminalNode) (string, string, error) {
	var keyspaceName, tableName string

	if len(allObj) == 2 {
		keyspaceName = allObj[0].GetText()
		tableName = allObj[1].GetText()
	} else if len(allObj) == 1 {
		keyspaceName = ""
		tableName = allObj[0].GetText()
	} else {
		return "", "", errors.New("could not find table or keyspace name or some extra parameter provided")
	}

	if tableName == "" {
		return "", "", fmt.Errorf("table is missing")
	}

	return keyspaceName, tableName, nil
}

// NewCqlParser creates a new CQL parser instance.
// Initializes and returns a parser for CQL queries with validation.
// Returns error if query is invalid or parser initialization fails.
func NewCqlParser(cqlQuery string, isDebug bool) (*cql.CqlParser, error) {
	if cqlQuery == "" {
		return nil, fmt.Errorf("invalid input string")
	}

	lexer := cql.NewCqlLexer(antlr.NewInputStream(cqlQuery))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)
	if p == nil {
		return nil, fmt.Errorf("error while creating parser object")
	}

	if !isDebug {
		p.RemoveErrorListeners()
	}
	return p, nil
}

func parseCqlListAssignment(l cql.IAssignmentListContext) ([]string, error) {
	var result []string
	for _, c := range l.AllConstant() {
		val, err := parseCqlConstant(c)
		if err != nil {
			return nil, err
		}
		strval, err := primitivesToString(val)
		if err != nil {
			return nil, err
		}
		result = append(result, strval)
	}
	return result, nil
}
func parseCqlMapAssignment(m cql.IAssignmentMapContext) (map[string]string, error) {
	result := make(map[string]string)
	all := m.AllConstant()
	for i := 0; i+1 < len(all); i += 2 {
		keyRaw, err := parseCqlConstant(all[i])
		if err != nil {
			return nil, err
		}
		val, err := parseCqlConstant(all[i+1])
		if err != nil {
			return nil, err
		}
		key, err := primitivesToString(keyRaw)
		if err != nil {
			return nil, err
		}
		strval, err := primitivesToString(val)
		if err != nil {
			return nil, err
		}
		result[key] = strval
	}
	return result, nil
}
func parseCqlSetAssignment(s cql.IAssignmentSetContext) ([]string, error) {
	var result []string
	all := s.AllConstant()
	if len(all) == 0 {
		return result, nil
	}
	for _, c := range all {
		if c == nil {
			continue
		}
		val, err := parseCqlConstant(c)
		if err != nil {
			return nil, err
		}
		strval, err := primitivesToString(val)
		if err != nil {
			return nil, err
		}
		result = append(result, strval)
	}
	return result, nil
}

// parseCqlConstant parses a CQL constant value.
// Converts CQL constant values to their corresponding Go types with validation.
// Returns error if constant is invalid or conversion fails.
func parseCqlConstant(c cql.IConstantContext) (interface{}, error) {

	if c.StringLiteral() != nil {
		return trimQuotes(c.StringLiteral().GetText()), nil
	}

	if c.DecimalLiteral() != nil {
		if c.DecimalLiteral().GetText() == "?" {
			return "?", nil
		}
		return strconv.Atoi(c.DecimalLiteral().GetText())
	}
	if c.FloatLiteral() != nil {
		return strconv.ParseFloat(c.FloatLiteral().GetText(), 64)
	}
	if c.BooleanLiteral() != nil {
		val := c.BooleanLiteral().GetText()
		if val == "true" {
			return true, nil
		} else if val == "false" {
			return false, nil
		}
		return nil, fmt.Errorf("unknown boolean literal: %s", val)
	}
	if c.KwNull() != nil {
		return nil, nil
	}
	return nil, fmt.Errorf("unknown constant: %s", c.GetText())
}

// DecodeBytesToCassandraColumnType(): Function to decode incoming bytes parameter
// for handleExecute scenario into corresponding go datatype
//
// Parameters:
//   - b: []byte
//   - choice:  datatype.DataType
//   - protocolVersion: primitive.ProtocolVersion
//
// Returns: (interface{}, error)
func decodePreparedQueryValueToGo(b []byte, choice datatype.PrimitiveType, protocolVersion primitive.ProtocolVersion) (any, error) {
	switch choice.GetDataTypeCode() {
	case primitive.DataTypeCodeVarchar:
		return proxycore.DecodeType(datatype.Varchar, protocolVersion, b)
	case primitive.DataTypeCodeDouble:
		return proxycore.DecodeType(datatype.Double, protocolVersion, b)
	case primitive.DataTypeCodeFloat:
		return proxycore.DecodeType(datatype.Float, protocolVersion, b)
	case primitive.DataTypeCodeBigint, primitive.DataTypeCodeCounter:
		return proxycore.DecodeType(datatype.Bigint, protocolVersion, b)
	case primitive.DataTypeCodeTimestamp:
		return proxycore.DecodeType(datatype.Timestamp, protocolVersion, b)
	case primitive.DataTypeCodeInt:
		var decodedInt int64
		if len(b) == 8 {
			decoded, err := proxycore.DecodeType(datatype.Bigint, protocolVersion, b)
			if err != nil {
				return nil, err
			}
			decodedInt = decoded.(int64)
		} else {
			decoded, err := proxycore.DecodeType(datatype.Int, protocolVersion, b)
			if err != nil {
				return nil, err
			}
			decodedInt = int64(decoded.(int32))
		}
		return decodedInt, nil
	case primitive.DataTypeCodeBoolean:
		return proxycore.DecodeType(datatype.Boolean, protocolVersion, b)
	case primitive.DataTypeCodeDate:
		return proxycore.DecodeType(datatype.Date, protocolVersion, b)
	case primitive.DataTypeCodeBlob:
		return proxycore.DecodeType(datatype.Blob, protocolVersion, b)
	default:
		res, err := decodePreparedQueryCollection(choice, b)
		return res, err
	}
}

// decodePreparedQueryCollection() Decodes non-primitive types like list, list, and list from byte data based on the provided datatype choice. Returns the decoded collection or an error if unsupported.
func decodePreparedQueryCollection(choice datatype.PrimitiveType, b []byte) (any, error) {
	var err error
	// Check if it's a list type
	if choice.GetDataTypeCode() == primitive.DataTypeCodeList {
		// Get the element type
		listType := choice.(datatype.ListType)
		elementType := listType.GetElementType()

		// Now check the element type's code
		switch elementType.GetDataTypeCode() {
		case primitive.DataTypeCodeVarchar:
			decodedList, err := collectiondecoder.DecodeCollection(utilities.ListOfStr.DataType(), primitive.ProtocolVersion4, b)
			if err != nil {
				return nil, err
			}
			return decodedList, err
		case primitive.DataTypeCodeInt:
			decodedList, err := collectiondecoder.DecodeCollection(utilities.ListOfInt.DataType(), primitive.ProtocolVersion4, b)
			if err != nil {
				return nil, err
			}
			return decodedList, err
		case primitive.DataTypeCodeBigint:
			decodedList, err := collectiondecoder.DecodeCollection(utilities.ListOfBigInt.DataType(), primitive.ProtocolVersion4, b)
			if err != nil {
				return nil, err
			}
			return decodedList, err
		case primitive.DataTypeCodeDouble:
			decodedList, err := collectiondecoder.DecodeCollection(utilities.ListOfDouble.DataType(), primitive.ProtocolVersion4, b)
			if err != nil {
				return nil, err
			}
			return decodedList, err
		default:
			err = fmt.Errorf("unsupported list element type to decode - %v", elementType.GetDataTypeCode())
			return nil, err
		}
	}

	err = fmt.Errorf("unsupported Datatype to decode - %v", choice.GetDataTypeCode())
	return nil, err
}
