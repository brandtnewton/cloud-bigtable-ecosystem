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
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/bindings"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/collectiondecoder"
	constants "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/constants"
	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/antlr4-go/antlr/v4"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	missingUndefined = "<missing undefined>"
	missing          = "<missing"
	questionMark     = "?"
	STAR             = "*"
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

type AssignmentOperation string

const (
	AssignAdd    AssignmentOperation = "+"
	AssignRemove AssignmentOperation = "-"
	AssignIndex  AssignmentOperation = "update_index"
)

// Assignment represents a complex update operation on a column.
// It contains the column name, operation type, and left/right operands for the operation.
//type Assignment struct {
//	Column    types.ColumnName
//	Operation AssignmentOperation
//	IsPrepend bool
//	Values     interface{}
//}

type Assignment interface {
	Column() *types.Column
}

type ComplexAssignmentAdd struct {
	column    *types.Column
	IsPrepend bool
	Value     types.Placeholder
}

func NewComplexAssignmentAdd(column *types.Column, isPrepend bool, value types.Placeholder) *ComplexAssignmentAdd {
	return &ComplexAssignmentAdd{column: column, IsPrepend: isPrepend, Value: value}
}

func (c ComplexAssignmentAdd) Column() *types.Column {
	return c.column
}

type AssignmentCounterIncrement struct {
	column *types.Column
	Op     IncrementOperationType
	Value  types.Placeholder
}

func NewAssignmentCounterIncrement(column *types.Column, op IncrementOperationType, value types.Placeholder) *AssignmentCounterIncrement {
	return &AssignmentCounterIncrement{column: column, Op: op, Value: value}
}

func (c AssignmentCounterIncrement) Column() *types.Column {
	return c.column
}

type ComplexAssignmentRemove struct {
	column *types.Column
	Value  types.Placeholder
}

func NewComplexAssignmentRemove(column *types.Column, value types.Placeholder) *ComplexAssignmentRemove {
	return &ComplexAssignmentRemove{column: column, Value: value}
}

func (c ComplexAssignmentRemove) Column() *types.Column {
	return c.column
}

type ComplexAssignmentUpdateIndex struct {
	column *types.Column
	Index  int64
	Value  types.Placeholder
}

func NewComplexAssignmentUpdateIndex(column *types.Column, index int64, value types.Placeholder) *ComplexAssignmentUpdateIndex {
	return &ComplexAssignmentUpdateIndex{column: column, Index: index, Value: value}
}

func (c ComplexAssignmentUpdateIndex) Column() *types.Column {
	return c.column
}

type ComplexAssignmentSet struct {
	column *types.Column
	Value  types.Placeholder
}

func NewComplexAssignmentSet(column *types.Column, value types.Placeholder) *ComplexAssignmentSet {
	return &ComplexAssignmentSet{column: column, Value: value}
}

func (c ComplexAssignmentSet) Column() *types.Column {
	return c.column
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
			strval, err := scalarToString(val)
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
			strval, err := scalarToString(val)
			if err != nil {
				return nil, err
			}
			result = append(result, strval)
		}
		return result, nil
	}
	return expr.GetText(), nil // fallback
}

func extractValuesFromWhereClause(whereClause *types.WhereClause) map[types.ColumnName]types.GoValue {
	valueMap := make(map[types.ColumnName]types.GoValue)
	for _, clause := range whereClause.Conditions {
		v, _ := whereClause.Params.GetValue(clause.ValuePlaceholder)
		valueMap[clause.Column.Name] = v
	}
	return valueMap
}

// handlePrepareMapOperation processes map operations in prepared queries.
// Manages map operations, append, delete, and update at index and simple assignment.
// Returns error if value type doesn't match map key/value types or encoding fails.
func handlePrepareMapOperation(complexOps map[types.ColumnName]*ComplexOperation, val *primitive.Value, column *types.Column, pv primitive.ProtocolVersion, output *PreparedValues) error {
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
			if err := parsePreparedMap(mapType, pv, val, column, output); err != nil {
				return err
			}
		}
	} else {
		// case of when new value is being set for collection type {e.g collection=newCollection}
		output.DelColumnFamily = append(output.DelColumnFamily, column.ColumnFamily)
		if err := parsePreparedMap(mapType, pv, val, column, output); err != nil {
			return err
		}
	}
	return nil
}

// processVarcharMap handles map operations with varchar keys in prepared queries.
// Processes varchar map operations.
// Returns error if key type isn't varchar or value encoding fails.
func parsePreparedMap(mapType *types.MapType, protocolV primitive.ProtocolVersion, val *primitive.Value, column *types.Column, output *PreparedValues) error {
	decodedValue, err := proxycore.DecodeType(mapType.DataType(), protocolV, val.Contents)
	if err != nil {
		return fmt.Errorf("error decoding string to %s value: %w", column.CQLType.DataType(), err)
	}
	decodedMap, ok := decodedValue.(map[any]any)
	if !ok {
		return fmt.Errorf("error decoding string to %s value: %w", column.CQLType.String(), err)
	}

	for k, v := range decodedMap {
		col, err := collectionElementValueToColumnName(k)
		if err != nil {
			return fmt.Errorf("failed to convert value for key '%s': %w", k, err)
		}

		val, err := bindings.encodeScalarForBigtable(v, mapType.ValueType().DataType(), protocolV)
		if err != nil {
			return fmt.Errorf("failed to convert value for map value: %w", err)
		}
		output.Data = append(output.Data, types.BigtableData{
			Family: column.ColumnFamily,
			Column: col,
			Bytes:  val,
		})
	}
	return nil
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

	decodedValue, err := collectiondecoder.DecodeCollection(expectedDt, pv, val)
	if err != nil {
		return fmt.Errorf("error decoding string to %s value: %w", column.CQLType.DataType(), err)
	}
	setValue, err := convertToInterfaceSlice(decodedValue)
	if err != nil {
		return err
	}
	for _, v := range setValue {
		setVal, err := scalarToString(v)
		if err != nil {
			return fmt.Errorf("failed to convert value: %w", err)
		}
		output.DelColumns = append(output.DelColumns, &types.BigtableColumn{
			Family: column.ColumnFamily,
			Column: types.ColumnQualifier(setVal),
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

	setKey, err := scalarToString(mpKey)
	if err != nil {
		return fmt.Errorf("failed to convert/read key: %w", err)
	}
	setValue, err := scalarToString(decodedValue)
	if err != nil {
		return fmt.Errorf("failed to convert value for key '%s': %w", setKey, err)
	}

	formattedValue, err := bindings.encodeScalarForBigtable(setValue, mt.ValueType().DataType(), pv)
	if err != nil {
		return fmt.Errorf("failed to format value: %w", err)
	}
	output.Data = append(output.Data, types.BigtableData{
		Family: column.ColumnFamily,
		Column: types.ColumnQualifier(setKey),
		Bytes:  formattedValue,
	})

	return nil
}

// handlePrepareSetOperation processes set operations in prepared queries.
// Manages set operations with protocol version-specific value encoding.
// Returns error if value type doesn't match set element type or encoding fails.
func handlePrepareSetOperation(complexOps map[types.ColumnName]*ComplexOperation, val *primitive.Value, column *types.Column, pv primitive.ProtocolVersion, output *PreparedValues) error {
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
		return handleSetProcessing(val, column, pv, output)
	} else {
		// case of when new value is being set for collection type {e.g collection=newCollection}
		output.DelColumnFamily = append(output.DelColumnFamily, column.ColumnFamily)
		return handleSetProcessing(val, column, pv, output)
	}
}

// handleSetProcessing processes set values for prepared queries.
// Handles set operations for append, delete, and update at index and simple assignment.
// Returns error if value type doesn't match set element type or encoding fails.
func handleSetProcessing(
	val *primitive.Value,
	column *types.Column,
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
		return processSet[string](val, column, protocolV, setType, output)
	case datatype.Int:
		return processSet[int32](val, column, protocolV, setType, output)
	case datatype.Bigint, datatype.Timestamp:
		return processSet[int64](val, column, protocolV, setType, output)
	case datatype.Float:
		return processSet[float32](val, column, protocolV, setType, output)
	case datatype.Double:
		return processSet[float64](val, column, protocolV, setType, output)
	case datatype.Boolean:
		return processSet[bool](val, column, protocolV, setType, output)
	default:
		return fmt.Errorf("unexpected set element type: %T", column.CQLType.DataType())
	}
}

// processSet handles generic set processing operations in prepared queries.
// Manages set operations with type-specific handling and protocol version encoding.
// Returns error if value type doesn't match set element type or encoding fails.
func processSet(
	val *primitive.Value,
	column *types.Column,
	protocolV primitive.ProtocolVersion,
	setType *types.SetType,
	output *PreparedValues,

) error {
	decodedValue, err := collectiondecoder.DecodeCollection(setType.DataType(), protocolV, val)
	if err != nil {
		return fmt.Errorf("error decoding string to %s value: %w", column.CQLType.DataType(), err)
	}

	setValues, ok := decodedValue.([]any)
	if !ok {
		return fmt.Errorf("unexpected set element type: %T", decodedValue)
	}

	// Common processing logic for all types
	for _, elem := range setValues {
		valueStr, err := collectionElementValueToColumnName(elem)
		if err != nil {
			return fmt.Errorf("failed to convert element to string: %w", err)
		}
		output.Data = append(output.Data, types.BigtableData{
			Family: column.ColumnFamily,
			Column: valueStr,
			Bytes:  []byte(""),
		})
	}
	return nil
}

func collectionElementValueToColumnName(value any) (types.ColumnQualifier, error) {
	if t, ok := value.(time.Time); ok {
		value = t.UnixMilli()
	}
	result, err := scalarToString(value)
	if err != nil {
		return "", err
	}
	return types.ColumnQualifier(result), nil
}

// castScalarColumn handles column type casting in queries.
// Manages type conversion for column values with validation.
// Returns error if column type is invalid or conversion fails.
func castScalarColumn(colMeta *types.Column) (string, error) {
	if colMeta.CQLType.IsCollection() {
		return "", fmt.Errorf("cannot cast collection type column '%s'", colMeta.Name)
	}
	if colMeta.IsPrimaryKey {
		// primary keys are stored in structured row keys, not column families, and have type information already, so no need to case
		return string(colMeta.Name), nil
	}

	switch colMeta.CQLType.DataType() {
	case datatype.Int:
		return fmt.Sprintf("TO_INT64(%s['%s'])", colMeta.ColumnFamily, colMeta.Name), nil
	case datatype.Bigint:
		return fmt.Sprintf("TO_INT64(%s['%s'])", colMeta.ColumnFamily, colMeta.Name), nil
	case datatype.Float:
		return fmt.Sprintf("TO_FLOAT32(%s['%s'])", colMeta.ColumnFamily, colMeta.Name), nil
	case datatype.Double:
		return fmt.Sprintf("TO_FLOAT64(%s['%s'])", colMeta.ColumnFamily, colMeta.Name), nil
	case datatype.Boolean:
		return fmt.Sprintf("TO_INT64(%s['%s'])", colMeta.ColumnFamily, colMeta.Name), nil
	case datatype.Timestamp:
		return fmt.Sprintf("TO_TIME(%s['%s'])", colMeta.ColumnFamily, colMeta.Name), nil
	case datatype.Counter:
		return fmt.Sprintf("%s['']", colMeta.Name), nil
	case datatype.Blob:
		return fmt.Sprintf("TO_BLOB(%s['%s'])", colMeta.ColumnFamily, colMeta.Name), nil
	case datatype.Varchar:
		return fmt.Sprintf("%s['%s']", colMeta.ColumnFamily, colMeta.Name), nil
	default:
		return "", fmt.Errorf("unsupported CQL type: %s", colMeta.CQLType.DataType())
	}
}

// parseWhereByClause parses the WHERE clause from a CQL query.
// Extracts and processes WHERE conditions with type validation.
// Returns error if clause parsing fails or invalid conditions are found.
func parseWhereByClause(input cql.IWhereSpecContext, tableConfig *schemaMapping.TableConfig, params *types.QueryParameters, values *types.QueryParameterValues) (*types.WhereClause, error) {
	if input == nil {
		return nil, nil
	}

	elements := input.RelationElements().AllRelationElement()
	if elements == nil {
		return nil, errors.New("no input parameters found for clauses")
	}

	if len(elements) == 0 {
		return nil, nil
	}
	response := types.NewWhereClause(params)

	for _, val := range elements {
		if val == nil {
			return nil, errors.New("could not parse column object")
		}
		hasValue := val.QUESTION_MARK() == nil

		column, err := parseColumn(tableConfig, val)
		if err != nil {
			return nil, err
		}

		if val.OPERATOR_EQ() != nil {
			p := response.PushCondition(column, constants.EQ, column.CQLType)
			if hasValue {
				val, err := parseConstantValue(column, val)
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, val)
				if err != nil {
					return nil, err
				}
			}
		} else if val.OPERATOR_GT() != nil {
			p := response.PushCondition(column, constants.GT, column.CQLType)
			if hasValue {
				val, err := parseConstantValue(column, val)
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, val)
				if err != nil {
					return nil, err
				}
			}
		} else if val.OPERATOR_LT() != nil {
			p := response.PushCondition(column, constants.LT, column.CQLType)
			if hasValue {
				val, err := parseConstantValue(column, val)
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, val)
				if err != nil {
					return nil, err
				}
			}
		} else if val.OPERATOR_GTE() != nil {
			p := response.PushCondition(column, constants.GTE, column.CQLType)
			if hasValue {
				val, err := parseConstantValue(column, val)
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, val)
				if err != nil {
					return nil, err
				}
			}
		} else if val.OPERATOR_LTE() != nil {
			p := response.PushCondition(column, constants.LTE, column.CQLType)
			if hasValue {
				val, err := parseConstantValue(column, val)
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, val)
				if err != nil {
					return nil, err
				}
			}
		} else if val.KwIn() != nil {
			p := response.PushCondition(column, constants.IN, column.CQLType)
			if hasValue {
				valueFn := val.FunctionArgs()
				if valueFn == nil {
					return nil, errors.New("could not parse Function arguments")
				}
				all := valueFn.AllConstant()
				if all == nil {
					return nil, errors.New("could not parse all values inside IN operator")
				}
				var inValues []any
				for _, v := range all {
					parsed, err := stringToPrimitives(trimQuotes(v.GetText()), column.CQLType.DataType())
					if err != nil {
						return nil, err
					}
					inValues = append(inValues, parsed)
				}
				err = values.SetValue(p, val)
				if err != nil {
					return nil, err
				}
			}
		} else if val.RelalationContains() != nil {
			relContains := val.RelalationContains()
			var operator constants.Operator
			var elementType types.CqlDataType
			if column.CQLType.Code() == types.LIST {
				operator = constants.ARRAY_INCLUDES
				elementType = column.CQLType.(types.ListType).ElementType()
			} else if column.CQLType.Code() == types.SET {
				operator = constants.MAP_CONTAINS_KEY
				elementType = column.CQLType.(types.SetType).ElementType()
			} else {
				return nil, errors.New("CONTAINS are only supported for set and list")
			}

			p := response.PushCondition(column, operator, elementType)
			if hasValue {
				value, err := parseContainsValue(column, relContains)
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, value)
				if err != nil {
					return nil, err
				}
			}
		} else if val.RelalationContainsKey() != nil {
			relContains := val.RelalationContainsKey()
			if column.CQLType.Code() != types.MAP {
				return nil, errors.New("CONTAINS KEY are only supported for map")
			}
			keyType := column.CQLType.(types.MapType).KeyType()
			p := response.PushCondition(column, constants.MAP_CONTAINS_KEY, keyType)
			if hasValue {
				value, err := parseContainsKeyValue(column, relContains)
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, value)
				if err != nil {
					return nil, err
				}
			}
		} else if val.KwLike() != nil {
			p := response.PushCondition(column, constants.LIKE, column.CQLType)
			if hasValue {
				value, err := parseConstantValue(column, val)
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, value)
				if err != nil {
					return nil, err
				}
			}
		} else if val.KwBetween() != nil {
			p := response.PushCondition(column, constants.BETWEEN, column.CQLType)
			if hasValue {
				value, err := parseConstantValue(column, val)
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, value)
				if err != nil {
					return nil, err
				}
			}
		} else {
			return nil, errors.New("no supported operator found")
		}
	}
	return response, nil
}

func parseConstantValue(col *types.Column, e cql.IRelationElementContext) (any, error) {
	valConst := e.Constant(0)
	if valConst == nil {
		return nil, errors.New("could not parse value from query for one of the clauses")
	}
	value := e.Constant(0).GetText()
	if value == "" {
		return nil, errors.New("could not parse value from query for one of the clauses")
	}
	val, err := stringToPrimitives(trimQuotes(value), col.CQLType.DataType())
	if err != nil {
		return nil, err
	}
	return val, nil
}

func parseContainsValue(col *types.Column, e cql.IRelalationContainsContext) (any, error) {
	if e.Constant() == nil {
		return nil, errors.New("could not parse value from query for one of the clauses")
	}
	value := e.Constant().GetText()
	val, err := stringToPrimitives(trimQuotes(value), col.CQLType.DataType())
	if err != nil {
		return nil, err
	}
	return val, nil
}

func parseContainsKeyValue(col *types.Column, e cql.IRelalationContainsKeyContext) (any, error) {
	if e.Constant() == nil {
		return nil, errors.New("could not parse value from query for one of the clauses")
	}
	value := e.Constant().GetText()
	val, err := stringToPrimitives(trimQuotes(value), col.CQLType.DataType())
	if err != nil {
		return nil, err
	}
	return val, nil
}

func parseColumn(tableConfig *schemaMapping.TableConfig, e cql.IRelationElementContext) (*types.Column, error) {
	if e.RelalationContainsKey() != nil {
		relContainsKey := e.RelalationContainsKey()
		name := relContainsKey.OBJECT_NAME().GetText()
		return tableConfig.GetColumn(types.ColumnName(name))
	} else if e.RelalationContains() != nil {
		relContains := e.RelalationContains()
		name := relContains.OBJECT_NAME().GetText()
		return tableConfig.GetColumn(types.ColumnName(name))
	} else if len(e.AllOBJECT_NAME()) != 0 {
		valConst := e.OBJECT_NAME(0)
		if valConst == nil {
			return nil, errors.New("could not parse value from query for one of the clauses")
		}
		name := e.OBJECT_NAME(0).GetText()
		if name == "" {
			return nil, errors.New("could not parse value from query for one of the clauses")
		}
		return tableConfig.GetColumn(types.ColumnName(name))
	} else {
		return nil, fmt.Errorf("unable to determine column from clause")
	}
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

func GetTimestampInfo(timestampContext cql.IUsingTtlTimestampContext, params *types.QueryParameters, values *types.QueryParameterValues) error {
	if timestampContext == nil {
		return nil
	}
	timestampInfo := timestampContext.Timestamp()
	if timestampInfo == nil {
		return nil
	}
	literal := timestampInfo.DecimalLiteral()
	if literal == nil {
		return nil
	}
	params.AddParameterWithoutColumn(types.UsingTimePlaceholder, types.TypeBigint)
	if literal.DECIMAL_LITERAL() != nil {
		value, err := stringToPrimitives(literal.DECIMAL_LITERAL().GetText(), datatype.Bigint)
		if err != nil {
			return err
		}
		err = values.SetValue(types.UsingTimePlaceholder, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func ValidateRequiredPrimaryKeysOnly(tableConfig *schemaMapping.TableConfig, params *types.QueryParameters) error {
	err := ValidateRequiredPrimaryKeys(tableConfig, params)
	if err != nil {
		return err
	}
	if len(tableConfig.PrimaryKeys) != len(params.AllColumns()) {
		for _, c := range params.AllColumns() {
			col, err := tableConfig.GetColumn(c)
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
func ValidateRequiredPrimaryKeys(tableConfig *schemaMapping.TableConfig, params *types.QueryParameters) error {
	// primary key counts are very small for legitimate use cases so greedy iterations are fine
	for _, wantKey := range tableConfig.PrimaryKeys {
		_, ok := params.GetPlaceholderForColumn(wantKey.Name)
		if !ok {
			return fmt.Errorf("missing primary key: '%s'", wantKey.Name)
		}
	}
	return nil
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

// ProcessComplexUpdate processes complex update operations.
// Handles complex column updates including collections and counters with validation.
// Returns error if update type is invalid or processing fails.
func (t *Translator) ProcessComplexUpdate(columns []*types.Column, values []Assignment) (map[types.ColumnFamily]*ComplexOperation, error) {
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
		strval, err := scalarToString(val)
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
		key, err := scalarToString(keyRaw)
		if err != nil {
			return nil, err
		}
		strval, err := scalarToString(val)
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
		strval, err := scalarToString(val)
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
func decodePreparedQueryValueToGo(val *primitive.Value, choice datatype.PrimitiveType, protocolVersion primitive.ProtocolVersion) (any, error) {
	switch choice.GetDataTypeCode() {
	case primitive.DataTypeCodeVarchar:
		return proxycore.DecodeType(datatype.Varchar, protocolVersion, val.Contents)
	case primitive.DataTypeCodeDouble:
		return proxycore.DecodeType(datatype.Double, protocolVersion, val.Contents)
	case primitive.DataTypeCodeFloat:
		return proxycore.DecodeType(datatype.Float, protocolVersion, val.Contents)
	case primitive.DataTypeCodeBigint, primitive.DataTypeCodeCounter:
		return proxycore.DecodeType(datatype.Bigint, protocolVersion, val.Contents)
	case primitive.DataTypeCodeTimestamp:
		return proxycore.DecodeType(datatype.Timestamp, protocolVersion, val.Contents)
	case primitive.DataTypeCodeInt:
		var decodedInt int64
		if len(val.Contents) == 8 {
			decoded, err := proxycore.DecodeType(datatype.Bigint, protocolVersion, val.Contents)
			if err != nil {
				return nil, err
			}
			decodedInt = decoded.(int64)
		} else {
			decoded, err := proxycore.DecodeType(datatype.Int, protocolVersion, val.Contents)
			if err != nil {
				return nil, err
			}
			decodedInt = int64(decoded.(int32))
		}
		return decodedInt, nil
	case primitive.DataTypeCodeBoolean:
		return proxycore.DecodeType(datatype.Boolean, protocolVersion, val.Contents)
	case primitive.DataTypeCodeDate:
		return proxycore.DecodeType(datatype.Date, protocolVersion, val.Contents)
	case primitive.DataTypeCodeBlob:
		return proxycore.DecodeType(datatype.Blob, protocolVersion, val.Contents)
	default:
		res, err := decodePreparedQueryCollection(choice, val)
		return res, err
	}
}

// decodePreparedQueryCollection() Decodes non-primitive types like list, list, and list from byte data based on the provided datatype choice. Returns the decoded collection or an error if unsupported.
func decodePreparedQueryCollection(choice datatype.PrimitiveType, val *primitive.Value) (any, error) {
	var err error
	// Check if it's a list type
	if choice.GetDataTypeCode() == primitive.DataTypeCodeList {
		// Get the element type
		listType := choice.(datatype.ListType)
		elementType := listType.GetElementType()

		// Now check the element type's code
		switch elementType.GetDataTypeCode() {
		case primitive.DataTypeCodeVarchar:
			decodedList, err := collectiondecoder.DecodeCollection(utilities.ListOfStr.DataType(), bigtableEncodingVersion, val)
			if err != nil {
				return nil, err
			}
			return decodedList, err
		case primitive.DataTypeCodeInt:
			decodedList, err := collectiondecoder.DecodeCollection(utilities.ListOfInt.DataType(), bigtableEncodingVersion, val)
			if err != nil {
				return nil, err
			}
			return decodedList, err
		case primitive.DataTypeCodeBigint:
			decodedList, err := collectiondecoder.DecodeCollection(utilities.ListOfBigInt.DataType(), bigtableEncodingVersion, val)
			if err != nil {
				return nil, err
			}
			return decodedList, err
		case primitive.DataTypeCodeDouble:
			decodedList, err := collectiondecoder.DecodeCollection(utilities.ListOfDouble.DataType(), bigtableEncodingVersion, val)
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
