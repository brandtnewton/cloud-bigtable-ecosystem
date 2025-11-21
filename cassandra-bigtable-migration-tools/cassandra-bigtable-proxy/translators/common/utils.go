package common

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/collectiondecoder"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/constants"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/proxycore"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/antlr4-go/antlr/v4"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	validTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
)

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

// ParseCqlValue parses a CQL value expression.
// Converts CQL value expressions to their corresponding Go types with validation.
// Returns error if expression is invalid or conversion fails.
func ParseCqlValue(expr antlr.ParserRuleContext, expectedType types.CqlDataType) (types.GoValue, error) {
	// If this is an expression context, check for subcontexts
	if e, ok := expr.(cql.IExpressionContext); ok {
		if m := e.AssignmentMap(); m != nil {
			return ParseCqlValue(m, expectedType)
		}
		if l := e.AssignmentList(); l != nil {
			return ParseCqlValue(l, expectedType)
		}
		if s := e.AssignmentSet(); s != nil {
			return ParseCqlValue(s, expectedType)
		}
		if c := e.Constant(); c != nil {
			return GetCqlConstant(c, expectedType)
		}
		// Optionally handle tuple, function call, etc.
		return e.GetText(), nil // fallback
	}
	// Handle map context
	if m, ok := expr.(cql.IAssignmentMapContext); ok {
		return ParseCqlMapAssignment(m, expectedType)
	}
	// Handle list context
	if l, ok := expr.(cql.IAssignmentListContext); ok {
		var result []types.GoValue
		for _, c := range l.AllConstant() {
			val, err := GetCqlConstant(c, expectedType)
			if err != nil {
				return nil, err
			}
			result = append(result, val)
		}
		return result, nil
	}
	// Handle set context
	if s, ok := expr.(cql.IAssignmentSetContext); ok {
		var result []types.GoValue
		all := s.AllConstant()
		if len(all) == 0 {
			return result, nil
		}
		for _, c := range all {
			if c == nil {
				continue
			}
			val, err := GetCqlConstant(c, expectedType)
			if err != nil {
				return nil, err
			}
			result = append(result, val)
		}
		return result, nil
	}
	return expr.GetText(), nil // fallback
}

// CastScalarColumn handles column type casting in queries.
// Manages type conversion for column values with validation.
// Returns error if column type is invalid or conversion fails.
func CastScalarColumn(colMeta *types.Column) (string, error) {
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

func ParseDecimalLiteral(d cql.IDecimalLiteralContext, cqlType types.CqlDataType, params *types.QueryParameters, values *types.QueryParameterValues) (types.Placeholder, error) {
	if d == nil {
		return "", nil
	}

	p := params.PushParameterWithoutColumn(cqlType)

	if d.QUESTION_MARK() != nil {
		return p, nil
	}

	val, err := utilities.StringToGo(d.DECIMAL_LITERAL().GetText(), cqlType.DataType())
	if err != nil {
		return "", err
	}
	err = values.SetValue(p, val)
	if err != nil {
		return "", err
	}
	return p, nil
}

func ParseStringLiteral(d cql.IStringLiteralContext, params *types.QueryParameters, values *types.QueryParameterValues) (types.Placeholder, error) {
	if d == nil {
		return "", nil
	}

	p := params.PushParameterWithoutColumn(types.TypeVarchar)

	val, err := utilities.StringToGo(TrimQuotes(d.STRING_LITERAL().GetText()), datatype.Varchar)
	if err != nil {
		return "", err
	}
	err = values.SetValue(p, val)
	if err != nil {
		return "", err
	}
	return p, nil
}

func ParseWhereClause(input cql.IWhereSpecContext, tableConfig *schemaMapping.TableConfig, params *types.QueryParameters, values *types.QueryParameterValues, isPrepared bool) ([]types.Condition, error) {
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

	var conditions []types.Condition

	for _, val := range elements {
		if val == nil {
			return nil, errors.New("could not parse column object")
		}

		column, err := parseColumn(tableConfig, val)
		if err != nil {
			return nil, err
		}

		if val.OPERATOR_EQ() != nil {
			p := params.PushParameter(column, column.CQLType, false)
			conditions = append(conditions, types.Condition{
				Column:           column,
				Operator:         types.EQ,
				ValuePlaceholder: p,
			})
			if !isPrepared {
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
			p := params.PushParameter(column, column.CQLType, false)
			conditions = append(conditions, types.Condition{
				Column:           column,
				Operator:         types.GT,
				ValuePlaceholder: p,
			})
			if !isPrepared {
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
			p := params.PushParameter(column, column.CQLType, false)
			conditions = append(conditions, types.Condition{
				Column:           column,
				Operator:         types.LT,
				ValuePlaceholder: p,
			})
			if !isPrepared {
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
			p := params.PushParameter(column, column.CQLType, false)
			conditions = append(conditions, types.Condition{
				Column:           column,
				Operator:         types.GTE,
				ValuePlaceholder: p,
			})
			if !isPrepared {
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
			p := params.PushParameter(column, column.CQLType, false)
			conditions = append(conditions, types.Condition{
				Column:           column,
				Operator:         types.LTE,
				ValuePlaceholder: p,
			})
			if !isPrepared {
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
			p := params.PushParameter(column, column.CQLType, false)
			conditions = append(conditions, types.Condition{
				Column:           column,
				Operator:         types.IN,
				ValuePlaceholder: p,
			})
			if !isPrepared {
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
					parsed, err := utilities.StringToGo(TrimQuotes(v.GetText()), column.CQLType.DataType())
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
		} else if val.RelationContains() != nil {
			relContains := val.RelationContains()
			var operator types.Operator
			var elementType types.CqlDataType
			if column.CQLType.Code() == types.LIST {
				operator = types.ARRAY_INCLUDES
				elementType = column.CQLType.(types.ListType).ElementType()
			} else if column.CQLType.Code() == types.SET {
				operator = types.MAP_CONTAINS_KEY
				elementType = column.CQLType.(types.SetType).ElementType()
			} else {
				return nil, errors.New("CONTAINS are only supported for set and list")
			}

			p := params.PushParameter(column, elementType, true)
			conditions = append(conditions, types.Condition{
				Column:           column,
				Operator:         operator,
				ValuePlaceholder: p,
			})
			if !isPrepared {
				value, err := parseContainsValue(column, relContains)
				if err != nil {
					return nil, err
				}
				err = values.SetValue(p, value)
				if err != nil {
					return nil, err
				}
			}
		} else if val.RelationContainsKey() != nil {
			relContains := val.RelationContainsKey()
			if column.CQLType.Code() != types.MAP {
				return nil, errors.New("CONTAINS KEY are only supported for map")
			}
			keyType := column.CQLType.(types.MapType).KeyType()
			p := params.PushParameter(column, keyType, false)
			conditions = append(conditions, types.Condition{
				Column:           column,
				Operator:         types.MAP_CONTAINS_KEY,
				ValuePlaceholder: p,
			})
			if !isPrepared {
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
			p := params.PushParameter(column, column.CQLType, false)
			conditions = append(conditions, types.Condition{
				Column:           column,
				Operator:         types.LIKE,
				ValuePlaceholder: p,
			})
			if !isPrepared {
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
			p := params.PushParameter(column, column.CQLType, false)
			conditions = append(conditions, types.Condition{
				Column:           column,
				Operator:         types.BETWEEN,
				ValuePlaceholder: p,
			})
			if !isPrepared {
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
	return conditions, nil
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
	val, err := utilities.StringToGo(TrimQuotes(value), col.CQLType.DataType())
	if err != nil {
		return nil, err
	}
	return val, nil
}

func parseContainsValue(col *types.Column, e cql.IRelationContainsContext) (any, error) {
	if e.Constant() == nil {
		return nil, errors.New("could not parse value from query for one of the clauses")
	}
	value := e.Constant().GetText()
	val, err := utilities.StringToGo(TrimQuotes(value), col.CQLType.DataType())
	if err != nil {
		return nil, err
	}
	return val, nil
}

func parseContainsKeyValue(col *types.Column, e cql.IRelationContainsKeyContext) (any, error) {
	if e.Constant() == nil {
		return nil, errors.New("could not parse value from query for one of the clauses")
	}
	value := e.Constant().GetText()
	val, err := utilities.StringToGo(TrimQuotes(value), col.CQLType.DataType())
	if err != nil {
		return nil, err
	}
	return val, nil
}

func parseColumn(tableConfig *schemaMapping.TableConfig, e cql.IRelationElementContext) (*types.Column, error) {
	if e.RelationContainsKey() != nil {
		relContainsKey := e.RelationContainsKey()
		name := relContainsKey.OBJECT_NAME().GetText()
		return tableConfig.GetColumn(types.ColumnName(name))
	} else if e.RelationContains() != nil {
		relContains := e.RelationContains()
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
		return nil, fmt.Errorf("unable to determine column from clause: '%s'", e.GetText())
	}
}

func TrimQuotes(s string) string {
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
		value, err := utilities.StringToGo(literal.DECIMAL_LITERAL().GetText(), datatype.Bigint)
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

func ParseCqlListAssignment(l cql.IAssignmentListContext, dt types.CqlDataType) ([]types.GoValue, error) {
	lt := dt.(types.ListType)
	var result []types.GoValue
	for _, c := range l.AllConstant() {
		val, err := GetCqlConstant(c, lt.ElementType())
		if err != nil {
			return nil, err
		}
		result = append(result, val)
	}
	return result, nil
}

func ParseCqlMapAssignment(m cql.IAssignmentMapContext, dt types.CqlDataType) (map[types.GoValue]types.GoValue, error) {
	result := make(map[types.GoValue]types.GoValue)
	all := m.AllConstant()
	for i := 0; i+1 < len(all); i += 2 {
		key, err := GetCqlConstant(all[i], dt)
		if err != nil {
			return nil, err
		}
		val, err := GetCqlConstant(all[i+1], dt)
		if err != nil {
			return nil, err
		}
		result[key] = val
	}
	return result, nil
}

func ParseCqlSetAssignment(s cql.IAssignmentSetContext, dt types.CqlDataType) ([]types.GoValue, error) {
	st := dt.(types.SetType)
	var result []types.GoValue
	all := s.AllConstant()
	if len(all) == 0 {
		return result, nil
	}
	for _, c := range all {
		if c == nil {
			continue
		}
		val, err := GetCqlConstant(c, st.ElementType())
		if err != nil {
			return nil, err
		}
		result = append(result, val)
	}
	return result, nil
}

// GetCqlConstant parses a CQL constant value.
// Converts CQL constant values to their corresponding Go types with validation.
// Returns error if constant is invalid or conversion fails.
func GetCqlConstant(c cql.IConstantContext, dt types.CqlDataType) (types.GoValue, error) {
	if c.QUESTION_MARK() != nil {
		return nil, fmt.Errorf("cannot get constant from prepared query")
	}
	if c.StringLiteral() != nil {
		return utilities.StringToGo(TrimQuotes(c.StringLiteral().GetText()), dt.DataType())
	}
	if c.DecimalLiteral() != nil {
		return utilities.StringToGo(c.DecimalLiteral().GetText(), dt.DataType())
	}
	if c.FloatLiteral() != nil {
		return utilities.StringToGo(c.FloatLiteral().GetText(), dt.DataType())
	}
	if c.BooleanLiteral() != nil {
		return utilities.StringToGo(c.BooleanLiteral().GetText(), dt.DataType())
	}
	if c.KwNull() != nil {
		return nil, nil
	}
	return nil, fmt.Errorf("unhandled constant: %s", c.GetText())
}

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
			decodedList, err := collectiondecoder.DecodeCollection(utilities.ListOfStr.DataType(), constants.BigtableEncodingVersion, val)
			if err != nil {
				return nil, err
			}
			return decodedList, err
		case primitive.DataTypeCodeInt:
			decodedList, err := collectiondecoder.DecodeCollection(utilities.ListOfInt.DataType(), constants.BigtableEncodingVersion, val)
			if err != nil {
				return nil, err
			}
			return decodedList, err
		case primitive.DataTypeCodeBigint:
			decodedList, err := collectiondecoder.DecodeCollection(utilities.ListOfBigInt.DataType(), constants.BigtableEncodingVersion, val)
			if err != nil {
				return nil, err
			}
			return decodedList, err
		case primitive.DataTypeCodeDouble:
			decodedList, err := collectiondecoder.DecodeCollection(utilities.ListOfDouble.DataType(), constants.BigtableEncodingVersion, val)
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
	var intVal int64
	switch v := value.(type) {
	case int32:
		intVal = int64(v)
	case int64:
		intVal = v
	default:
		return nil, fmt.Errorf("unsupported type for bigint: %T", value)
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
	case float32:
		floatVal = float64(v)
	case float64:
		floatVal = v
	default:
		return nil, fmt.Errorf("unsupported type for float: %T", value)
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
	default:
		return nil, fmt.Errorf("unsupported type: %T", value)
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

type TableOperation interface {
	Table() cql.ITableContext
	Keyspace() cql.IKeyspaceContext
}

func ParseTarget(op TableOperation, sessionKeyspace types.Keyspace, config *schemaMapping.SchemaMappingConfig) (types.Keyspace, types.TableName, error) {
	if op.Table() == nil || op.Table().GetText() == "" {
		return "", "", errors.New("invalid input parameters found for table")
	}

	tableNameString := op.Table().GetText()
	if !validTableName.MatchString(tableNameString) {
		return "", "", errors.New("invalid table name parsed from query")
	}
	if utilities.IsReservedCqlKeyword(tableNameString) {
		return "", "", fmt.Errorf("table name cannot be reserved cql word: '%s'", tableNameString)
	}
	tableName := types.TableName(tableNameString)

	if tableName == config.SchemaMappingTableName {
		return "", "", fmt.Errorf("table name cannot be the same as the configured schema mapping table name '%s'", tableName)
	}

	keyspaceName := sessionKeyspace
	if op.Keyspace() != nil && op.Keyspace().GetText() != "" {
		keyspaceName = types.Keyspace(op.Keyspace().GetText())
	}

	if keyspaceName == "" {
		return "", "", errors.New("missing keyspace. keyspace is required")
	}

	return keyspaceName, tableName, nil
}
