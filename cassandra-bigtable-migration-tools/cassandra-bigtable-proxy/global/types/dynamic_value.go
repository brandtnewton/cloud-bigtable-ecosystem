package types

import "fmt"

type DynamicValue interface {
	GetValue(values *QueryParameterValues) (GoValue, error)
	IsIdempotent() bool
	GetPlaceholder() Placeholder
	GetType() CqlDataType
}

type ParameterizedValue struct {
	Placeholder Placeholder
	dt          CqlDataType
}

func (p *ParameterizedValue) GetType() CqlDataType {
	return p.dt
}

func (p *ParameterizedValue) GetPlaceholder() Placeholder {
	return p.Placeholder
}

func (p *ParameterizedValue) IsIdempotent() bool {
	return true
}

func (p *ParameterizedValue) GetValue(values *QueryParameterValues) (GoValue, error) {
	return values.GetValue(p.Placeholder)
}

func NewParameterizedValue(placeholder Placeholder, dt CqlDataType) DynamicValue {
	return &ParameterizedValue{Placeholder: placeholder, dt: dt}
}

type LiteralValue struct {
	Value GoValue
	dt    CqlDataType
}

func (l *LiteralValue) GetType() CqlDataType {
	return l.dt
}

func (l *LiteralValue) GetPlaceholder() Placeholder {
	return ""
}

func (l *LiteralValue) IsIdempotent() bool {
	return true
}

func (l *LiteralValue) GetValue(_ *QueryParameterValues) (GoValue, error) {
	return l.Value, nil
}

func NewLiteralValue(value GoValue, dt CqlDataType) DynamicValue {
	return &LiteralValue{Value: value, dt: dt}
}

type FunctionValue struct {
	Placeholder Placeholder
	Func        *CqlFuncSpec
	Args        []DynamicValue
}

func (f FunctionValue) GetType() CqlDataType {
	return f.Func.ReturnType
}

func (f FunctionValue) GetPlaceholder() Placeholder {
	return f.Placeholder
}

func (f FunctionValue) GetValue(values *QueryParameterValues) (GoValue, error) {
	apply, err := f.Func.Apply(f.Args, values)
	if err != nil {
		return nil, err
	}
	return apply, nil
}

func (f FunctionValue) IsIdempotent() bool {
	return false
}

func NewFunctionValue(placeholder Placeholder, f *CqlFuncSpec, args ...DynamicValue) *FunctionValue {
	return &FunctionValue{
		Placeholder: placeholder,
		Func:        f,
		Args:        args,
	}
}

type ColumnValue struct {
	Column *Column
}

func (c ColumnValue) GetType() CqlDataType {
	return c.Column.CQLType
}

func (c ColumnValue) GetValue(values *QueryParameterValues) (GoValue, error) {
	return nil, fmt.Errorf("cannot get value on a column")
}

func (c ColumnValue) IsIdempotent() bool {
	return true
}

func (c ColumnValue) GetPlaceholder() Placeholder {
	return ""
}

func NewColumnValue(column *Column) DynamicValue {
	return &ColumnValue{Column: column}
}

type SelectStarValue struct {
}

func (c SelectStarValue) GetType() CqlDataType {
	return TypeText
}

func (c SelectStarValue) GetValue(values *QueryParameterValues) (GoValue, error) {
	return nil, fmt.Errorf("cannot get value on a select * value")
}

func (c SelectStarValue) IsIdempotent() bool {
	return true
}

func (c SelectStarValue) GetPlaceholder() Placeholder {
	return ""
}

func NewSelectStarValue() DynamicValue {
	return &SelectStarValue{}
}

type MapAccessValue struct {
	Column  *Column
	mapType *MapType
	// placeholders are NOT allowed
	MapKey ColumnQualifier
}

func NewMapAccessValue(column *Column, mapType *MapType, mapKey ColumnQualifier) DynamicValue {
	return &MapAccessValue{Column: column, mapType: mapType, MapKey: mapKey}
}

func (m MapAccessValue) GetValue(values *QueryParameterValues) (GoValue, error) {
	return nil, fmt.Errorf("GetValue not allowed")
}

func (m MapAccessValue) IsIdempotent() bool {
	return true
}

func (m MapAccessValue) GetPlaceholder() Placeholder {
	return ""
}

func (m MapAccessValue) GetType() CqlDataType {
	return m.mapType.valueType
}

type ListElementValue struct {
	Column   *Column
	listType *ListType
	// placeholders are NOT allowed
	ListIndex int64
}

func NewListElementValue(column *Column, listType *ListType, listIndex int64) DynamicValue {
	return &ListElementValue{Column: column, listType: listType, ListIndex: listIndex}
}

func (l ListElementValue) GetValue(values *QueryParameterValues) (GoValue, error) {
	return nil, fmt.Errorf("GetValue not allowed")
}

func (l ListElementValue) IsIdempotent() bool {
	return true
}

func (l ListElementValue) GetPlaceholder() Placeholder {
	return ""
}

func (l ListElementValue) GetType() CqlDataType {
	return l.listType.elementType
}

func IsSelectStar(v DynamicValue) bool {
	_, ok := v.(SelectStarValue)
	return ok
}

func IsColumn(v DynamicValue) bool {
	_, ok := v.(ColumnValue)
	return ok
}
