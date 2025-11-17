package translator

import (
	"encoding/binary"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"strings"
	"time"
)

func BindMutations(assignments []Assignment, values *QueryParameterValues, b *BigtableWriteMutation) error {
	for _, assignment := range assignments {
		switch v := assignment.(type) {
		case AssignmentCounterIncrement:
			value, err := values.GetValueInt64(v.Value)
			if err != nil {
				return err
			}
			b.CounterOps = append(b.CounterOps, BigtableCounterOp{
				Family: v.Column().ColumnFamily,
				Value:  value,
			})
		case ComplexAssignmentSet:
			value, err := values.GetValue(v.Value)
			if err != nil {
				return err
			}
			// if we're setting a collection we need to remove all previous values, which are stored in separate columns
			if v.Column().CQLType.IsCollection() {
				b.DelColumnFamily = append(b.DelColumnFamily, v.Column().ColumnFamily)
			}
			data, err := encodeGoValueToBigtable(v.Column(), value)
			b.Data = append(b.Data, data...)
		case ComplexAssignmentAdd:
			colType := v.Column().CQLType.Code()
			if colType == types.LIST {
				lt := v.Column().CQLType.(types.ListType)
				value, err := values.GetValueSlice(v.Value)
				if err != nil {
					return err
				}
				err = addListElements(value, v.Column().ColumnFamily, &lt, v.IsPrepend, b)
				if err != nil {
					return err
				}
			} else if colType == types.SET {
				value, err := values.GetValueSlice(v.Value)
				if err != nil {
					return err
				}
				data, err := addSetElements(value, v.Column().ColumnFamily)
				if err != nil {
					return err
				}
				b.Data = append(b.Data, data...)
			} else if colType == types.MAP {
				mt := v.Column().CQLType.(types.MapType)
				value, err := values.GetValueMap(v.Value)
				if err != nil {
					return err
				}
				err = addMapEntries(value, &mt, assignment.Column(), b)
				if err != nil {
					return err
				}
			} else {
				return fmt.Errorf("uhandled add assignment column type: %s", v.Column().CQLType.String())
			}
		// todo implement other assignments
		default:
			return fmt.Errorf("unhandled assignment op type %T", v)
		}
		return nil
	}
	return nil
}

// handleListOperation processes list operations in raw queries.
// Manages simple assignment, append, prepend, and index-based operations on list columns.
// Returns error if operation type is invalid or value type doesn't match expected type.
//func handleListOperation(val interface{}, column *types.Column, lt *types.ListType, colFamily types.ColumnFamily, output *AdHocQueryValues) error {
//	switch v := val.(type) {
//	case ComplexAssignment:
//		switch v.Operation {
//		case "+":
//			valueToProcess := v.Right
//			if v.IsPrepend {
//				valueToProcess = v.Left
//			}
//			return addListElements(valueToProcess.([]string), colFamily, lt, v.IsPrepend, output)
//		case "-":
//			keys, ok := v.Right.([]string)
//			if !ok {
//				return fmt.Errorf("expected []string for remove operation, got %T", v.Right)
//			}
//			return removeListElements(keys, colFamily, column, output)
//		case "update_index":
//			idx, ok := v.Left.(string)
//			if !ok {
//				return fmt.Errorf("expected string for index, got %T", v.Left)
//			}
//			listType, ok := column.CQLType.(*types.ListType)
//			if !ok {
//				return fmt.Errorf("expected list type column for list operation")
//			}
//			dt := listType.ElementType().DataType()
//			return updateListIndex(idx, v.Right, colFamily, dt, output)
//		default:
//			return fmt.Errorf("unsupported list operation: %s", v.Operation)
//		}
//	default:
//		// Simple assignment (replace)
//		output.DelColumnFamily = append(output.DelColumnFamily, column.ColumnFamily)
//		var listValues []string
//		if val != nil {
//			switch v := val.(type) {
//			case []string:
//				listValues = v
//			default:
//				return fmt.Errorf("expected []string for list operation, got %T", val)
//			}
//		}
//
//		return addListElements(listValues, colFamily, lt, false, output)
//	}
//}

// addListElements adds elements to a list column in raw queries.
// Handles both append and prepend operations with type validation and conversion.
// Returns error if value type doesn't match list element type or conversion fails.
func addListElements(listValues []types.GoValue, colFamily types.ColumnFamily, lt *types.ListType, isPrepend bool, output *BigtableWriteMutation) error {
	for i, v := range listValues {
		// Calculate encoded timestamp for the list element
		column := getListIndexColumn(i, len(listValues), isPrepend)

		// Format the value
		formattedVal, err := encodeScalarForBigtable(v, lt.ElementType().DataType())
		if err != nil {
			return fmt.Errorf("error converting string to %s value: %w", lt.String(), err)
		}
		output.Data = append(output.Data, &types.BigtableData{Family: colFamily, Column: column, Bytes: formattedVal})
	}
	return nil
}

// getListIndexColumn generates encoded timestamp bytes.
// Creates timestamp byte representation for specific positions with validation.
// Returns error if position is invalid or encoding fails.
func getListIndexColumn(index int, totalLength int, prepend bool) types.ColumnQualifier {
	now := time.Now().UnixMilli()
	if prepend {
		now = referenceTime - (now - referenceTime)
	}

	nanos := maxNanos - int32(totalLength) + int32(index)
	encodedStr := string(encodeTimestampIndex(now, nanos))
	return types.ColumnQualifier(encodedStr)
}

// encodeTimestampIndex encodes a timestamp value into bytes.
// Converts timestamp values to byte representation with validation.
// Returns error if timestamp format is invalid or encoding fails.
func encodeTimestampIndex(millis int64, nanos int32) []byte {
	buf := make([]byte, 12) // 8 bytes for millis + 4 bytes for nanos
	binary.BigEndian.PutUint64(buf[0:8], uint64(millis))
	binary.BigEndian.PutUint32(buf[8:12], uint32(nanos))
	return buf
}

// removeListElements removes elements from a list column in raw queries.
// Processes element removal by index with validation of index bounds.
// Returns error if index is invalid or out of bounds.
//func removeListElements(keys []string, colFamily types.ColumnFamily, column *types.Column, output *AdHocQueryValues) error {
//	var listDelete [][]byte
//	listType, ok := column.CQLType.DataType().(datatype.ListType)
//	if !ok {
//		return fmt.Errorf("failed to assert list type for %s", column.CQLType.String())
//	}
//
//	listElementType := listType.GetElementType()
//
//	output.ComplexOps[colFamily] = &ComplexOperation{
//		Delete:     true,
//		ListDelete: true,
//	}
//
//	for _, col := range keys {
//		formattedVal, err := encodeScalarForBigtable(col, listElementType, primitive.ProtocolVersion4)
//		if err != nil {
//			return fmt.Errorf("error converting string to list<%s> value: %w", listElementType, err)
//		}
//		listDelete = append(listDelete, formattedVal)
//	}
//	if len(listDelete) > 0 {
//		if meta, ok := output.ComplexOps[colFamily]; ok {
//			meta.ListDeleteValues = listDelete
//		} else {
//			output.ComplexOps[colFamily] = &ComplexOperation{ListDeleteValues: listDelete}
//		}
//	}
//	return nil
//}

// updateListIndex updates a specific index in a list column.
// Handles type conversion and validation for the new value.
// Returns error if index is invalid or value type doesn't match list element type.
//func updateListIndex(index string, value interface{}, colFamily types.ColumnFamily, dt datatype.DataType, output *AdHocQueryValues) error {
//
//	valStr, err := scalarToString(value)
//	if err != nil {
//		return err
//	}
//
//	val, err := encodeScalarForBigtable(valStr, dt, primitive.ProtocolVersion4)
//	if err != nil {
//		return err
//	}
//	output.ComplexOps[colFamily] = &ComplexOperation{
//		UpdateListIndex: index,
//		Value:           val,
//	}
//	return nil
//}

// handleSetOperation processes set operations in raw queries.
// Manages simple assignment, add, and remove operations on set columns.
// Returns error if operation type is invalid or value type doesn't match set element type.
//func handleSetOperation(val interface{}, column *types.Column, st *SetType, colFamily types.ColumnFamily, output *AdHocQueryValues) error {
//	switch v := val.(type) {
//	case ComplexAssignment:
//		switch v.Operation {
//		case "+":
//			setValues, ok := v.Right.([]string)
//			if !ok {
//				return fmt.Errorf("expected []string for add operation, got %T", v.Right)
//			}
//			return addSetElements(setValues, colFamily, st, output)
//		case "-":
//			keys, ok := v.Right.([]string)
//			if !ok {
//				return fmt.Errorf("expected []string for remove operation, got %T", v.Right)
//			}
//			return removeSetElements(keys, colFamily, output)
//		default:
//			return fmt.Errorf("unsupported set operation: %s", v.Operation)
//		}
//	default:
//		// Simple assignment (replace)
//		output.DelColumnFamily = append(output.DelColumnFamily, column.ColumnFamily)
//		return addSetElements(val.([]string), colFamily, st, output)
//	}
//}

// addSetElements adds elements to a set column in raw queries.
// Handles element addition with type validation and conversion.
// Returns error if value type doesn't match set element type or conversion fails.
func addSetElements(setValues []types.GoValue, colFamily types.ColumnFamily) ([]*types.BigtableData, error) {
	var results []*types.BigtableData
	for _, v := range setValues {
		col, err := scalarToColumnQualifier(v)
		if err != nil {
			return nil, err
		}
		results = append(results, &types.BigtableData{Family: colFamily, Column: col, Bytes: []byte("")})
	}
	return results, nil
}

func scalarToColumnQualifier(v types.GoValue) (types.ColumnQualifier, error) {
	s, err := scalarToString(v)
	if err != nil {
		return "", err
	}
	return types.ColumnQualifier(s), nil
}

// removeSetElements removes elements from a set column in raw queries.
// Processes element removal with validation of element existence.
// Returns error if element type doesn't match set element type.
func removeSetElements(keys []types.GoValue, colFamily types.ColumnFamily, output *BigtableWriteMutation) error {
	for _, key := range keys {
		c, err := scalarToColumnQualifier(key)
		if err != nil {
			return err
		}
		output.DelColumns = append(output.DelColumns, &types.BigtableColumn{Family: colFamily, Column: c})
	}
	return nil
}

// handleMapOperation processes map operations in raw queries.
// Manages simple assignment/replace complete map, add, remove, and update at index operations on map columns.
// Returns error if operation type is invalid or value type doesn't match map key/value
//func handleMapOperation(val interface{}, column *types.Column, mt *types.MapType, colFamily types.ColumnFamily, output *BigtableWriteMutation) error {
//	// Check if key type is VARCHAR or TIMESTAMP
//	if mt.KeyType().DataType() == datatype.Varchar || mt.KeyType().DataType() == datatype.Timestamp {
//		switch v := val.(type) {
//		case Assignment:
//			switch v.Operation {
//			case "+":
//				return addMapEntries(v.Right, mt, column, output)
//			case "-":
//				return removeMapEntries(v.Right, column, output)
//			case "update_index":
//				return updateMapIndex(v.Left, v.Right, mt, colFamily, output)
//			default:
//				return fmt.Errorf("unsupported map operation: %s", v.Operation)
//			}
//		default:
//			// Simple assignment (replace)
//			output.DelColumnFamily = append(output.DelColumnFamily, column.ColumnFamily)
//			return addMapEntries(val, mt, column, output)
//		}
//	} else {
//		return fmt.Errorf("unsupported map key type: %s", mt.KeyType().String())
//	}
//}

// addMapEntries adds key-value pairs to a map column in raw queries.
// Handles type validation and conversion for both keys and values.
// Returns error if key/value types don't match map types or conversion fails.
func addMapEntries(mapValue map[types.GoValue]types.GoValue, mt *types.MapType, column *types.Column, output *BigtableWriteMutation) error {
	for k, v := range mapValue {
		col, err := scalarToColumnQualifier(k)
		if err != nil {
			return fmt.Errorf("error converting map key: %w", err)
		}
		valueFormatted, err := encodeScalarForBigtable(v, mt.ValueType().DataType())
		if err != nil {
			return fmt.Errorf("error converting string to %s value: %w", mt.String(), err)
		}
		output.Data = append(output.Data, &types.BigtableData{Family: column.ColumnFamily, Column: col, Bytes: valueFormatted})
	}
	return nil
}

// removeMapEntries removes key-value pairs from a map column in raw queries.
// Processes key removal with validation of key existence and type.
// Returns error if key type doesn't match map key type.
func removeMapEntries(val interface{}, column *types.Column, output *BigtableWriteMutation) error {
	keys, ok := val.([]string)
	if !ok {
		return fmt.Errorf("expected []string for remove operation, got %T", val)
	}
	for _, key := range keys {
		output.DelColumns = append(output.DelColumns, &types.BigtableColumn{
			Family: column.ColumnFamily,
			Column: types.ColumnQualifier(key),
		})
	}
	return nil
}

// updateMapIndex updates a specific key in a map column.
// Handles type conversion and validation for both key and value.
// Returns error if key doesn't exist or value type doesn't match map value type.
func updateMapIndex(key interface{}, value interface{}, dt *types.MapType, colFamily types.ColumnFamily, output *BigtableWriteMutation) error {
	k, ok := key.(string)
	if !ok {
		return fmt.Errorf("expected string for map key, got %T", key)
	}
	v, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string for map value, got %T", key)
	}

	val, err := encodeScalarForBigtable(v, dt.ValueType().DataType())
	if err != nil {
		return err
	}
	// For map index update, treat as a single entry add
	output.Data = append(output.Data, &types.BigtableData{Family: colFamily, Column: types.ColumnQualifier(k), Bytes: val})
	return nil
}

// BindQueryParams handles collection operations in prepared queries.
// Processes set, list, and map operations.
// Returns error if collection type is invalid or value encoding fails.
func BindQueryParams(params *QueryParameters, values []*primitive.Value, pv primitive.ProtocolVersion) (*QueryParameterValues, error) {
	if params.Count() != len(values) {
		return nil, fmt.Errorf("expected %d prepared values but got %d", params.Count(), len(values))
	}

	result := NewQueryParameterValues(params)

	for i, param := range params.AllKeys() {
		value := values[i]
		md := params.GetMetadata(param)
		goVal, err := cassandraValueToGoValue(md.Type, value, pv)
		if err != nil {
			return nil, err
		}
		err = result.SetValue(param, goVal)
		if err != nil {
			return nil, err
		}
	}

	// make sure the param counts match to prevent silent failures
	err := ValidateAllParamsSet(result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func BindSelectColumns(table *schemaMapping.TableConfig, selectedColumns []SelectedColumn, values *QueryParameterValues) ([]BoundSelectColumn, error) {
	var boundColumns []BoundSelectColumn
	for _, selectedColumn := range selectedColumns {
		var bc BoundSelectColumn
		col, err := table.GetColumn(types.ColumnName(selectedColumn.ColumnName))
		if err != nil {
			return nil, err
		}
		if selectedColumn.ListIndex != "" {
			val, err := values.GetValueInt64(selectedColumn.ListIndex)
			if err != nil {
				return nil, err
			}
			bc = NewBoundIndexColumn(col, int(val))
		} else if selectedColumn.MapKey != "" {
			val, err := values.GetValue(selectedColumn.ListIndex)
			if err != nil {
				return nil, err
			}
			c, err := scalarToColumnQualifier(val)
			if err != nil {
				return nil, err
			}
			bc = NewBoundKeyColumn(col, c)
		} else {
			return nil, fmt.Errorf("unhandled select column type found binding")
		}
		boundColumns = append(boundColumns, bc)
	}
	return boundColumns, nil
}

func ValidateZeroParamsSet(q *QueryParameterValues) error {
	var setParams []string
	keys := q.Params().AllKeys()
	for _, p := range keys {
		_, err := q.GetValue(p)
		if err == nil {
			setParams = append(setParams, string(p))
		}
	}
	if len(setParams) > 0 {
		return fmt.Errorf("expected no set params but found %d parameters: %s", len(setParams), strings.Join(setParams, ", "))
	}
	return nil
}

func ValidateAllParamsSet(q *QueryParameterValues) error {
	var missingParams []string
	keys := q.Params().AllKeys()
	for _, p := range keys {
		_, err := q.GetValue(p)
		if err != nil {
			missingParams = append(missingParams, string(p))
		}
	}
	if len(missingParams) > 0 {
		return fmt.Errorf("missing %d/%d parameters: %s", len(keys)-len(missingParams), len(keys), strings.Join(missingParams, ", "))
	}
	return nil
}

func BindUsingTimestamp(values *QueryParameterValues) (*BoundTimestampInfo, error) {
	if values.Has(UsingTimePlaceholder) {
		t, err := values.GetValueInt64(UsingTimePlaceholder)
		if err != nil {
			return nil, err
		}
		return &BoundTimestampInfo{
			// USING TIMESTAMP is in micros
			Timestamp:         time.UnixMicro(t),
			HasUsingTimestamp: true,
		}, nil
	} else {
		return &BoundTimestampInfo{
			// USING TIMESTAMP is in micros
			Timestamp:         time.Now(),
			HasUsingTimestamp: false,
		}, nil
	}
}
