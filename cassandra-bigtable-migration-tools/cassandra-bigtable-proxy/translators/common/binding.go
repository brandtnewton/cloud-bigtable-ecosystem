package common

import (
	"encoding/binary"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"strconv"
	"strings"
	"time"
)

func BindMutations(assignments []types.Assignment, values *types.QueryParameterValues, b *types.BigtableWriteMutation) error {
	for _, assignment := range assignments {
		// skip primary keys because those are part of the row key which is handled separately
		if assignment.Column().IsPrimaryKey {
			continue
		}
		switch v := assignment.(type) {
		case *types.AssignmentCounterIncrement:
			value, err := values.GetValueInt64(v.Value)
			if err != nil {
				return err
			}
			b.AddMutations(types.NewBigtableCounterOp(v.Column().ColumnFamily, value))
		case *types.ComplexAssignmentSet:
			mutations, err := encodeSetValue(v, values)
			if err != nil {
				return err
			}
			b.AddMutations(mutations...)
		case *types.ComplexAssignmentAppend:
			colType := v.Column().CQLType.Code()
			if colType == types.LIST {
				lt := v.Column().CQLType.(*types.ListType)
				if v.Operator == types.PLUS {
					value, err := values.GetValueSlice(v.Placeholder)
					if err != nil {
						return err
					}
					mutations, err := addListElements(value, v.Column().ColumnFamily, lt, v.IsPrepend)
					if err != nil {
						return err
					}
					b.AddMutations(mutations...)
				} else if v.Operator == types.MINUS {
					value, err := values.GetValueSlice(v.Placeholder)
					if err != nil {
						return err
					}
					var bigtableValues []types.BigtableValue
					for _, lv := range value {
						bv, err := encodeScalarForBigtable(lv, lt.ElementType())
						if err != nil {
							return err
						}
						bigtableValues = append(bigtableValues, bv)
					}
					b.AddMutations(types.NewBigtableDeleteListElementsOp(v.Column().ColumnFamily, bigtableValues))
				} else {
					return fmt.Errorf("unsupported append operation on list %s", v.Operator)
				}
			} else if colType == types.SET {
				if v.Operator == types.PLUS {
					value, err := values.GetValueSlice(v.Placeholder)
					if err != nil {
						return err
					}
					ops, err := addSetElements(value, v.Column().ColumnFamily)
					if err != nil {
						return err
					}
					b.AddMutations(ops...)
				} else if v.Operator == types.MINUS {
					value, err := values.GetValueSlice(v.Placeholder)
					if err != nil {
						return err
					}
					err = removeSetElements(value, v.Column().ColumnFamily, b)
					if err != nil {
						return err
					}
				} else {
					return fmt.Errorf("unsupported append operation on set %s", v.Operator)
				}
			} else if colType == types.MAP {
				mt := v.Column().CQLType.(*types.MapType)
				if v.Operator == types.PLUS {
					value, err := values.GetValueMap(v.Placeholder)
					if err != nil {
						return err
					}
					err = addMapEntries(value, mt, assignment.Column(), b)
					if err != nil {
						return err
					}
				} else if v.Operator == types.MINUS {
					value, err := values.GetValueSlice(v.Placeholder)
					if err != nil {
						return err
					}
					err = removeMapEntries(value, v.Column(), b)
					if err != nil {
						return err
					}
				} else {
					return fmt.Errorf("unsupported append operation on set %s", v.Operator)
				}
			} else if colType == types.COUNTER {
				value, err := values.GetValueInt64(v.Placeholder)
				if err != nil {
					return err
				}
				if v.Operator == types.MINUS {
					// bigtable only supports adding, so we need to flip the sign
					value = value * -1
				}
				b.AddMutations(types.NewBigtableCounterOp(assignment.Column().ColumnFamily, value))
			} else {
				return fmt.Errorf("unhandled add assignment column type: %s", v.Column().CQLType.String())
			}
		case *types.ComplexAssignmentUpdateListIndex:
			value, err := values.GetValue(v.Value)
			if err != nil {
				return err
			}
			lt, ok := v.Column().CQLType.(*types.ListType)
			if !ok {
				return fmt.Errorf("cannot set list value on column type %s", v.Column().CQLType.String())
			}
			encoded, err := encodeScalarForBigtable(value, lt.ElementType())
			if err != nil {
				return err
			}
			b.AddMutations(types.NewBigtableSetIndexOp(v.Column().ColumnFamily, v.Index, encoded))
		case *types.ComplexAssignmentUpdateMapValue:
			value, err := values.GetValue(v.Value)
			if err != nil {
				return err
			}
			mt, ok := v.Column().CQLType.(*types.MapType)
			if !ok {
				return fmt.Errorf("cannot set map value on column type %s", v.Column().CQLType.String())
			}
			op, err := mapValueToBigtable(v.Key, value, mt, v.Column().ColumnFamily)
			if err != nil {
				return err
			}
			b.AddMutations(op)
		default:
			return fmt.Errorf("unhandled assignment op type %T", v)
		}
	}

	var err error
	b.UsingTimestamp, err = BindUsingTimestamp(values)
	if err != nil {
		return err
	}
	return nil
}

func encodeSetValue(assignment *types.ComplexAssignmentSet, values *types.QueryParameterValues) ([]types.IBigtableMutationOp, error) {
	col := assignment.Column()

	var results []types.IBigtableMutationOp
	if col.CQLType.IsCollection() {
		// clear what's already there
		results = append(results, types.NewDeleteCellsOp(col.ColumnFamily))
	}
	if col.CQLType.Code() == types.MAP {
		mt := col.CQLType.(*types.MapType)
		mv, err := values.GetValueMap(assignment.Value)
		if err != nil {
			return nil, err
		}
		for k, v := range mv {
			keyEncoded, err := scalarToColumnQualifier(k)
			if err != nil {
				return nil, err
			}
			valueBytes, err := encodeScalarForBigtable(v, mt.ValueType())
			if err != nil {
				return nil, err
			}
			results = append(results, types.NewWriteCellOp(col.ColumnFamily, keyEncoded, valueBytes))
		}
	} else if col.CQLType.Code() == types.LIST {
		lt := col.CQLType.(*types.ListType)
		lv, err := values.GetValueSlice(assignment.Value)
		mutations, err := addListElements(lv, col.ColumnFamily, lt, false)
		if err != nil {
			return nil, err
		}
		results = append(results, mutations...)
	} else if col.CQLType.Code() == types.SET {
		lv, err := values.GetValueSlice(assignment.Value)
		if err != nil {
			return nil, err
		}
		mutations, err := addSetElements(lv, col.ColumnFamily)
		if err != nil {
			return nil, err
		}
		results = append(results, mutations...)
	} else {
		value, err := values.GetValue(assignment.Value)
		if err != nil {
			return nil, err
		}
		v, err := encodeScalarForBigtable(value, col.CQLType)
		if err != nil {
			return nil, err
		}
		results = append(results, types.NewWriteCellOp(col.ColumnFamily, types.ColumnQualifier(col.Name), v))
	}
	return results, nil
}

// handleListOperation processes list operations in raw queries.
// Manages simple assignment, append, prepend, and index-based operations on list columns.
// Returns error if operation type is invalid or value type doesn't match expected type.
//func handleListOperation(val interface{}, column *types.Columns, lt *types.ListType, colFamily types.ColumnFamily, output *AdHocQueryValues) error {
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

func addListElements(listValues []types.GoValue, cf types.ColumnFamily, lt *types.ListType, isPrepend bool) ([]types.IBigtableMutationOp, error) {
	var results []types.IBigtableMutationOp
	now := time.Now()
	for i, v := range listValues {
		// Calculate encoded timestamp for the list element
		column := getListIndexColumn(now, i, len(listValues), isPrepend)

		// Format the value
		formattedVal, err := encodeScalarForBigtable(v, lt.ElementType())
		if err != nil {
			return nil, fmt.Errorf("error converting string to %s value: %w", lt.String(), err)
		}
		results = append(results, types.NewWriteCellOp(cf, column, formattedVal))
	}
	return results, nil
}

// getListIndexColumn generates encoded timestamp bytes.
// Creates timestamp byte representation for specific positions with validation.
// Returns error if position is invalid or encoding fails.
func getListIndexColumn(now time.Time, index int, totalLength int, prepend bool) types.ColumnQualifier {
	nowMilli := now.UnixMilli()
	if prepend {
		nowMilli = referenceTime - (nowMilli - referenceTime)
	}

	nanos := maxNanos - int32(totalLength) + int32(index)
	encodedStr := string(encodeTimestampIndex(nowMilli, nanos))
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

// addSetElements adds elements to a set column in raw queries.
// Handles element addition with type validation and conversion.
// Returns error if value type doesn't match set element type or conversion fails.
func addSetElements(setValues []types.GoValue, cf types.ColumnFamily) ([]types.IBigtableMutationOp, error) {
	var results []types.IBigtableMutationOp
	for _, v := range setValues {
		col, err := scalarToColumnQualifier(v)
		if err != nil {
			return nil, err
		}
		// todo use correct col value
		results = append(results, types.NewWriteCellOp(cf, col, []byte("")))
	}
	return results, nil
}

func scalarToColumnQualifier(val types.GoValue) (types.ColumnQualifier, error) {
	switch v := val.(type) {
	case string:
		return types.ColumnQualifier(v), nil
	case *string:
		return types.ColumnQualifier(*v), nil
	case int32:
		return types.ColumnQualifier(strconv.Itoa(int(v))), nil
	case *int32:
		return types.ColumnQualifier(strconv.Itoa(int(*v))), nil
	case int:
		return types.ColumnQualifier(strconv.Itoa(v)), nil
	case *int:
		return types.ColumnQualifier(strconv.Itoa(*v)), nil
	case int64:
		return types.ColumnQualifier(strconv.FormatInt(v, 10)), nil
	case *int64:
		return types.ColumnQualifier(strconv.FormatInt(*v, 10)), nil
	case float32:
		return types.ColumnQualifier(strconv.FormatFloat(float64(v), 'f', -1, 32)), nil
	case *float32:
		return types.ColumnQualifier(strconv.FormatFloat(float64(*v), 'f', -1, 32)), nil
	case float64:
		return types.ColumnQualifier(strconv.FormatFloat(v, 'f', -1, 64)), nil
	case *float64:
		return types.ColumnQualifier(strconv.FormatFloat(*v, 'f', -1, 64)), nil
	case bool:
		if v {
			return "1", nil
		} else {
			return "0", nil
		}
	case *bool:
		if *v {
			return "1", nil
		} else {
			return "0", nil
		}
	case *time.Time:
		return types.ColumnQualifier(strconv.FormatInt(v.UnixMilli(), 10)), nil
	case time.Time:
		return types.ColumnQualifier(strconv.FormatInt(v.UnixMilli(), 10)), nil
	default:
		return "", fmt.Errorf("unsupported type: %T", v)
	}
}

// removeSetElements removes elements from a set column in raw queries.
// Processes element removal with validation of element existence.
// Returns error if element type doesn't match set element type.
func removeSetElements(keys []types.GoValue, colFamily types.ColumnFamily, output *types.BigtableWriteMutation) error {
	for _, key := range keys {
		c, err := scalarToColumnQualifier(key)
		if err != nil {
			return err
		}
		output.AddMutations(types.NewDeleteColumnOp(types.BigtableColumn{Family: colFamily, Column: c}))
	}
	return nil
}

// handleMapOperation processes map operations in raw queries.
// Manages simple assignment/replace complete map, add, remove, and update at index operations on map columns.
// Returns error if operation type is invalid or value type doesn't match map key/value
//func handleMapOperation(val interface{}, column *types.Columns, mt *types.MapType, colFamily types.ColumnFamily, output *BigtableWriteMutation) error {
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
func addMapEntries(mapValue map[types.GoValue]types.GoValue, mt *types.MapType, column *types.Column, output *types.BigtableWriteMutation) error {
	for k, v := range mapValue {
		op, err := mapValueToBigtable(k, v, mt, column.ColumnFamily)
		if err != nil {
			return fmt.Errorf("error converting map key: %w", err)
		}
		output.AddMutations(op)
	}
	return nil
}

func mapValueToBigtable(k types.GoValue, v types.GoValue, mt *types.MapType, cf types.ColumnFamily) (types.IBigtableMutationOp, error) {
	col, err := scalarToColumnQualifier(k)
	if err != nil {
		return nil, fmt.Errorf("error converting map key: %w", err)
	}
	valueFormatted, err := encodeScalarForBigtable(v, mt.ValueType())
	if err != nil {
		return nil, fmt.Errorf("error converting string to %s value: %w", mt.String(), err)
	}
	return types.NewWriteCellOp(cf, col, valueFormatted), nil
}

func removeMapEntries(keys []types.GoValue, column *types.Column, output *types.BigtableWriteMutation) error {
	for _, key := range keys {
		encodedKey, err := scalarToColumnQualifier(key)
		if err != nil {
			return err
		}
		output.AddMutations(types.NewDeleteColumnOp(types.BigtableColumn{
			Family: column.ColumnFamily,
			Column: encodedKey,
		}))
	}
	return nil
}

// updateMapIndex updates a specific key in a map column.
// Handles type conversion and validation for both key and value.
// Returns error if key doesn't exist or value type doesn't match map value type.
func updateMapIndex(key interface{}, value interface{}, dt *types.MapType, colFamily types.ColumnFamily, output *types.BigtableWriteMutation) error {
	k, ok := key.(string)
	if !ok {
		return fmt.Errorf("expected string for map key, got %T", key)
	}
	v, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string for map value, got %T", key)
	}

	val, err := encodeScalarForBigtable(v, dt.ValueType())
	if err != nil {
		return err
	}
	// For map index update, treat as a single entry add
	output.AddMutations(types.NewWriteCellOp(colFamily, types.ColumnQualifier(k), val))
	return nil
}

// BindQueryParams handles collection operations in prepared queries.
// Processes set, list, and map operations.
// Returns error if collection type is invalid or value encoding fails.
func BindQueryParams(params *types.QueryParameters, initialValues map[types.Placeholder]types.GoValue, values []*primitive.Value, pv primitive.ProtocolVersion) (*types.QueryParameterValues, error) {
	providedValueCount := len(initialValues) + len(values)
	if params.Count() != providedValueCount {
		return nil, fmt.Errorf("expected %d prepared values but got %d", params.Count(), providedValueCount)
	}

	result := types.NewQueryParameterValues(params)
	err := result.SetInitialValues(initialValues)
	if err != nil {
		return nil, err
	}

	// only iterate through values that we still need
	for i, param := range params.RemainingKeys(initialValues) {
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
	if result.CountSetValues() != params.Count() {
		err = ValidateAllParamsSet(params, result.AsMap())
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func BindSelectColumns(table *schemaMapping.TableConfig, selectedColumns []types.SelectedColumn, values *types.QueryParameterValues) ([]types.BoundSelectColumn, error) {
	var boundColumns []types.BoundSelectColumn
	for _, selectedColumn := range selectedColumns {
		var bc types.BoundSelectColumn
		col, err := table.GetColumn(selectedColumn.ColumnName)
		if err != nil {
			return nil, err
		}
		if selectedColumn.ListIndex != -1 {
			bc = types.NewBoundIndexColumn(col, int(selectedColumn.ListIndex))
		} else if selectedColumn.MapKey != "" {
			bc = types.NewBoundKeyColumn(col, selectedColumn.MapKey)
		} else {
			return nil, fmt.Errorf("unhandled select column type found binding")
		}
		boundColumns = append(boundColumns, bc)
	}
	return boundColumns, nil
}

func ValidateAllParamsSet(q *types.QueryParameters, values map[types.Placeholder]types.GoValue) error {
	var missingParams []string
	keys := q.AllKeys()
	for _, p := range keys {
		_, ok := values[p]
		if !ok {
			missingParams = append(missingParams, string(p))
		}
	}
	if len(missingParams) > 0 {
		return fmt.Errorf("missing %d/%d parameters: %s", len(keys)-len(missingParams), len(keys), strings.Join(missingParams, ", "))
	}
	return nil
}

func BindUsingTimestamp(values *types.QueryParameterValues) (*types.BoundTimestampInfo, error) {
	if values.Has(types.UsingTimePlaceholder) {
		t, err := values.GetValueInt64(types.UsingTimePlaceholder)
		if err != nil {
			return nil, err
		}
		return &types.BoundTimestampInfo{
			// USING TIMESTAMP is in micros
			Timestamp:         time.UnixMicro(t),
			HasUsingTimestamp: true,
		}, nil
	} else {
		return &types.BoundTimestampInfo{
			// USING TIMESTAMP is in micros
			Timestamp:         time.Now(),
			HasUsingTimestamp: false,
		}, nil
	}
}
