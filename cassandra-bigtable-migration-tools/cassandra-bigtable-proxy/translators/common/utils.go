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

func ParseArithmeticOperator(a cql.IArithmeticOperatorContext) (types.ArithmeticOperator, error) {
	if a == nil {
		return "", fmt.Errorf("nil arithmetic operator")
	}
	if a.PLUS() != nil {
		return types.PLUS, nil
	} else if a.MINUS() != nil {
		return types.MINUS, nil
	}
	return "", fmt.Errorf("unsupported arithmetic operator: `%s`", a.GetText())
}

func ParseDecimalLiteral(d cql.IDecimalLiteralContext, cqlType types.CqlDataType, params *types.QueryParameters, values *types.QueryParameterValues) (types.Placeholder, error) {
	if d == nil {
		return "", nil
	}

	p := params.PushParameterWithoutColumn(cqlType)

	if d.QUESTION_MARK() != nil {
		return p, nil
	}

	val, err := GetDecimalLiteral(d, cqlType)
	if err != nil {
		return "", err
	}
	err = values.SetValue(p, val)
	if err != nil {
		return "", err
	}
	return p, nil
}

func GetDecimalLiteral(d cql.IDecimalLiteralContext, cqlType types.CqlDataType) (types.GoValue, error) {
	if d.DECIMAL_LITERAL() == nil {
		return nil, fmt.Errorf("missing decimal literal value")
	}
	return utilities.StringToGo(d.DECIMAL_LITERAL().GetText(), cqlType.DataType())
}

func ParseBigInt(d cql.IDecimalLiteralContext) (int64, error) {
	if d.DECIMAL_LITERAL() == nil {
		return 0, fmt.Errorf("missing decimal literal value")
	}
	return utilities.ParseBigInt(d.GetText())
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

func ParseOperator(op cql.ICompareOperatorContext) (types.Operator, error) {
	if op == nil {
		return "", fmt.Errorf("nil operator")
	}
	if op.OPERATOR_EQ() != nil {
		return types.EQ, nil
	}
	if op.OPERATOR_LT() != nil {
		return types.LT, nil
	}
	if op.OPERATOR_GT() != nil {
		return types.GT, nil
	}
	if op.OPERATOR_LTE() != nil {
		return types.LTE, nil
	}
	if op.OPERATOR_GTE() != nil {
		return types.GTE, nil
	}
	return "", fmt.Errorf("unknown operator type: `%s`", op.GetText())
}

func ParseWhereClause(input cql.IWhereSpecContext, tableConfig *schemaMapping.TableConfig, params *types.QueryParameters, values *types.QueryParameterValues, isPrepared bool) ([]types.Condition, error) {
	if input == nil {
		return nil, nil
	}

	var conditions []types.Condition
	for _, val := range input.RelationElements().AllRelationElement() {
		var err error
		var condition types.Condition
		if val.RelationCompare() != nil {
			condition, err = parseWhereCompare(val.RelationCompare(), tableConfig, params, values)
		} else if val.RelationLike() != nil {
			condition, err = parseWhereLike(val.RelationLike(), tableConfig, params, values)
		} else if val.RelationContainsKey() != nil {
			condition, err = parseWhereContainsKey(val.RelationContainsKey(), tableConfig, params, values)
		} else if val.RelationContains() != nil {
			condition, err = parseWhereContains(val.RelationContains(), tableConfig, params, values)
		} else if val.RelationBetween() != nil {
			condition, err = parseWhereBetween(val.RelationBetween(), tableConfig, params, values)
		} else if val.RelationIn() != nil {
			condition, err = parseWhereIn(val.RelationIn(), tableConfig, params, values)
		} else {
			return nil, fmt.Errorf("unsupported condition type: `%s`", val.GetText())
		}
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, condition)
	}
	return conditions, nil
}

func parseWhereCompare(compare cql.IRelationCompareContext, tableConfig *schemaMapping.TableConfig, params *types.QueryParameters, values *types.QueryParameterValues) (types.Condition, error) {
	op, err := ParseOperator(compare.CompareOperator())
	if err != nil {
		return types.Condition{}, err
	}

	column, err := ParseColumnContext(tableConfig, compare.Column())
	if err != nil {
		return types.Condition{}, err
	}

	p := params.PushParameter(column, column.CQLType, false)
	if !IsConstantPrepared(compare.Constant()) {
		val, err := ParseCqlConstant(compare.Constant(), column.CQLType)
		if err != nil {
			return types.Condition{}, err
		}
		err = values.SetValue(p, val)
		if err != nil {
			return types.Condition{}, err
		}
	}
	return types.Condition{
		Column:           column,
		Operator:         op,
		ValuePlaceholder: p,
	}, nil
}
func parseWhereLike(like cql.IRelationLikeContext, tableConfig *schemaMapping.TableConfig, params *types.QueryParameters, values *types.QueryParameterValues) (types.Condition, error) {
	column, err := ParseColumnContext(tableConfig, like.Column())
	if err != nil {
		return types.Condition{}, err
	}

	p := params.PushParameter(column, column.CQLType, false)
	if !IsConstantPrepared(like.Constant()) {
		val, err := ParseCqlConstant(like.Constant(), column.CQLType)
		if err != nil {
			return types.Condition{}, err
		}
		err = values.SetValue(p, val)
		if err != nil {
			return types.Condition{}, err
		}
	}
	return types.Condition{
		Column:           column,
		Operator:         types.LIKE,
		ValuePlaceholder: p,
	}, nil
}
func parseWhereContainsKey(containsKey cql.IRelationContainsKeyContext, tableConfig *schemaMapping.TableConfig, params *types.QueryParameters, values *types.QueryParameterValues) (types.Condition, error) {
	column, err := ParseColumnContext(tableConfig, containsKey.Column())
	if err != nil {
		return types.Condition{}, err
	}
	if column.CQLType.Code() != types.MAP {
		return types.Condition{}, errors.New("CONTAINS KEY are only supported for map")
	}
	keyType := column.CQLType.(*types.MapType).KeyType()
	p := params.PushParameter(column, keyType, false)
	if !IsConstantPrepared(containsKey.Constant()) {
		value, err := ParseCqlConstant(containsKey.Constant(), keyType)
		if err != nil {
			return types.Condition{}, err
		}
		err = values.SetValue(p, value)
		if err != nil {
			return types.Condition{}, err
		}
	}
	return types.Condition{
		Column:           column,
		Operator:         types.MAP_CONTAINS_KEY,
		ValuePlaceholder: p,
	}, nil
}

func parseWhereContains(contains cql.IRelationContainsContext, tableConfig *schemaMapping.TableConfig, params *types.QueryParameters, values *types.QueryParameterValues) (types.Condition, error) {
	column, err := ParseColumnContext(tableConfig, contains.Column())
	if err != nil {
		return types.Condition{}, err
	}
	var operator types.Operator
	var elementType types.CqlDataType
	if column.CQLType.Code() == types.LIST {
		operator = types.ARRAY_INCLUDES
		elementType = column.CQLType.(*types.ListType).ElementType()
	} else if column.CQLType.Code() == types.SET {
		operator = types.MAP_CONTAINS_KEY
		elementType = column.CQLType.(*types.SetType).ElementType()
	} else {
		return types.Condition{}, fmt.Errorf("CONTAINS are only supported for set and list type columns, not %s", column.CQLType.String())
	}

	p := params.PushParameter(column, elementType, true)
	if !IsConstantPrepared(contains.Constant()) {
		value, err := ParseCqlConstant(contains.Constant(), elementType)
		if err != nil {
			return types.Condition{}, err
		}
		err = values.SetValue(p, value)
		if err != nil {
			return types.Condition{}, err
		}
	}
	return types.Condition{
		Column:           column,
		Operator:         operator,
		ValuePlaceholder: p,
	}, nil
}

func parseWhereBetween(between cql.IRelationBetweenContext, tableConfig *schemaMapping.TableConfig, params *types.QueryParameters, values *types.QueryParameterValues) (types.Condition, error) {
	column, err := ParseColumnContext(tableConfig, between.Column())
	if err != nil {
		return types.Condition{}, err
	}

	p1 := params.PushParameter(column, column.CQLType, false)
	p2 := params.PushParameter(column, column.CQLType, false)
	if !IsConstantPrepared(between.Constant(0)) {
		val, err := ParseCqlConstant(between.Constant(0), column.CQLType)
		if err != nil {
			return types.Condition{}, err
		}
		err = values.SetValue(p1, val)
		if err != nil {
			return types.Condition{}, err
		}
	}
	if !IsConstantPrepared(between.Constant(1)) {
		val, err := ParseCqlConstant(between.Constant(1), column.CQLType)
		if err != nil {
			return types.Condition{}, err
		}
		err = values.SetValue(p2, val)
		if err != nil {
			return types.Condition{}, err
		}
	}
	return types.Condition{
		Column:            column,
		Operator:          types.BETWEEN,
		ValuePlaceholder:  p1,
		ValuePlaceholder2: p2,
	}, nil
}

func parseWhereIn(whereIn cql.IRelationInContext, tableConfig *schemaMapping.TableConfig, params *types.QueryParameters, values *types.QueryParameterValues) (types.Condition, error) {
	column, err := ParseColumnContext(tableConfig, whereIn.Column())
	if err != nil {
		return types.Condition{}, err
	}
	p := params.PushParameter(column, column.CQLType, false)
	if whereIn.QUESTION_MARK() == nil {
		valueFn := whereIn.FunctionArgs()
		if valueFn == nil {
			return types.Condition{}, errors.New("could not parse Function arguments")
		}
		all := valueFn.AllConstant()
		if all == nil {
			return types.Condition{}, errors.New("could not parse all values inside IN operator")
		}
		var inValues []any
		for _, v := range all {
			parsed, err := utilities.StringToGo(TrimQuotes(v.GetText()), column.CQLType.DataType())
			if err != nil {
				return types.Condition{}, err
			}
			inValues = append(inValues, parsed)
		}
		err = values.SetValue(p, inValues)
		if err != nil {
			return types.Condition{}, err
		}
	}
	return types.Condition{
		Column:           column,
		Operator:         types.IN,
		ValuePlaceholder: p,
	}, nil
}

func ParseValueAny(v cql.IValueAnyContext, col *types.Column, p types.Placeholder, values *types.QueryParameterValues) error {
	if v.QUESTION_MARK() != nil {
		return nil
	}
	if v.Constant() != nil {
		return ParseConstantContext(v.Constant(), col, p, values)
	}
	if v.ValueList() != nil {
		value, err := ParseListValue(v.ValueList(), col.CQLType)
		if err != nil {
			return err
		}
		err = values.SetValue(p, value)
		if err != nil {
			return err
		}
	}
	if v.ValueMap() != nil {
		value, err := ParseCqlMapAssignment(v.ValueMap(), col.CQLType)
		if err != nil {
			return err
		}
		err = values.SetValue(p, value)
		if err != nil {
			return err
		}
	}
	if v.ValueSet() != nil {
		value, err := ParseCqlSetAssignment(v.ValueSet(), col.CQLType)
		if err != nil {
			return err
		}
		err = values.SetValue(p, value)
		if err != nil {
			return err
		}
	}
	return fmt.Errorf("unhandled value set `%s`", v.GetText())
}

func ParseConstantContext(v cql.IConstantContext, col *types.Column, p types.Placeholder, values *types.QueryParameterValues) error {
	if v.QUESTION_MARK() != nil {
		return nil
	}

	goValue, err := ParseCqlConstant(v, col.CQLType)
	if err != nil {
		return err
	}

	err = values.SetValue(p, goValue)
	if err != nil {
		return err
	}
	return nil
}

func ParseColumnContext(table *schemaMapping.TableConfig, r cql.IColumnContext) (*types.Column, error) {
	if r == nil || r.OBJECT_NAME() == nil {
		return nil, fmt.Errorf("nil column")
	}

	col := TrimDoubleQuotes(r.OBJECT_NAME().GetText())

	return table.GetColumn(types.ColumnName(col))
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

func TrimDoubleQuotes(s string) string {
	if len(s) < 2 {
		return s
	}
	if s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	return strings.ReplaceAll(s, `""`, `"`)
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

func ValidateRequiredPrimaryKeysOnly(tableConfig *schemaMapping.TableConfig, conditions []types.Condition) error {
	seen := make(map[types.ColumnName]bool)
	for _, c := range conditions {
		if !c.Column.IsPrimaryKey {
			return fmt.Errorf("non-primary key found in where clause: '%s'", c.Column.Name)
		}
		seen[c.Column.Name] = true
	}

	for _, pmk := range tableConfig.PrimaryKeys {
		if _, ok := seen[pmk.Name]; !ok {
			return fmt.Errorf("missing primary key in where clause: '%s'", pmk.Name)
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

func ParseListValue(l cql.IValueListContext, dt types.CqlDataType) ([]types.GoValue, error) {
	lt := dt.(types.ListType)
	var result []types.GoValue
	for _, c := range l.AllConstant() {
		val, err := ParseCqlConstant(c, lt.ElementType())
		if err != nil {
			return nil, err
		}
		result = append(result, val)
	}
	return result, nil
}

func ParseCqlMapAssignment(m cql.IValueMapContext, dt types.CqlDataType) (map[types.GoValue]types.GoValue, error) {
	result := make(map[types.GoValue]types.GoValue)
	all := m.AllConstant()
	for i := 0; i+1 < len(all); i += 2 {
		key, err := ParseCqlConstant(all[i], dt)
		if err != nil {
			return nil, err
		}
		val, err := ParseCqlConstant(all[i+1], dt)
		if err != nil {
			return nil, err
		}
		result[key] = val
	}
	return result, nil
}

func ParseCqlSetAssignment(s cql.IValueSetContext, dt types.CqlDataType) ([]types.GoValue, error) {
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
		val, err := ParseCqlConstant(c, st.ElementType())
		if err != nil {
			return nil, err
		}
		result = append(result, val)
	}
	return result, nil
}

func IsConstantPrepared(c cql.IConstantContext) bool {
	return c.QUESTION_MARK() != nil || c.GetText() == "?"
}

// ParseCqlConstant parses a CQL constant value.
// Converts CQL constant values to their corresponding Go types with validation.
// Returns error if constant is invalid or conversion fails.
func ParseCqlConstant(c cql.IConstantContext, dt types.CqlDataType) (types.GoValue, error) {
	if c.QUESTION_MARK() != nil || c.GetText() == "?" {
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
		mt := column.CQLType.(*types.MapType)
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
		lt := column.CQLType.(*types.ListType)
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
		st := column.CQLType.(*types.SetType)
		lv, ok := value.([]any)
		if !ok {
			return nil, errors.New("failed to parse set")
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

	// the proxy codec returns []*string/whatever type which isn't quite what we're expecting downstream
	//if dt.Code() == types.LIST || dt.Code() == types.SET {
	//	goValue = dereferenceSlice(goValue)
	//}

	return goValue, nil
}

func dereferenceSlice(goValue types.GoValue) types.GoValue {
	switch v := goValue.(type) {
	case []*string:
		var s = make([]string, len(v))
		for i, ps := range v {
			s[i] = *ps
		}
		return s
	case []*int32:
		var s = make([]int32, len(v))
		for i, ps := range v {
			s[i] = *ps
		}
		return s
	case []*int64:
		var s = make([]int64, len(v))
		for i, ps := range v {
			s[i] = *ps
		}
		return s
	case []*bool:
		var s = make([]bool, len(v))
		for i, ps := range v {
			s[i] = *ps
		}
		return s
	case []*float32:
		var s = make([]float32, len(v))
		for i, ps := range v {
			s[i] = *ps
		}
		return s
	case []*float64:
		var s = make([]float64, len(v))
		for i, ps := range v {
			s[i] = *ps
		}
		return s
	}
	return goValue
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
