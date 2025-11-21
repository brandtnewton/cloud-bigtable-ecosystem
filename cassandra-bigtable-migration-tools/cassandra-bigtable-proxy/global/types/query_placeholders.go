package types

import (
	"fmt"
	"reflect"
)

const LimitPlaceholder Placeholder = "@limitValue"

type Placeholder string

type PlaceholderMetadata struct {
	Key Placeholder
	// nil if limit or collection index/key
	Column          *Column
	Type            CqlDataType
	IsCollectionKey bool
}

func newPlaceholderMetadata(key Placeholder, column *Column, tpe CqlDataType, isCollectionKey bool) PlaceholderMetadata {
	return PlaceholderMetadata{Key: key, Column: column, Type: tpe, IsCollectionKey: isCollectionKey}
}

type QueryParameters struct {
	ordered []Placeholder
	// note: not all placeholders will be associated with a column - e.g. limit ?
	columnLookup map[ColumnName]Placeholder
	// might be different from the column - like if we're doing a "CONTAINS" on a list, this would be the element type
	metadata map[Placeholder]PlaceholderMetadata
}

func NewQueryParameters() *QueryParameters {
	return &QueryParameters{
		ordered:      nil,
		columnLookup: make(map[ColumnName]Placeholder),
		metadata:     make(map[Placeholder]PlaceholderMetadata),
	}
}

func (q *QueryParameters) AllColumns() []ColumnName {
	var names []ColumnName
	for name := range q.columnLookup {
		names = append(names, name)
	}
	return names
}
func (q *QueryParameters) AllKeys() []Placeholder {
	return q.ordered
}

// RemainingKeys builds a list of placeholders that still need values
func (q *QueryParameters) RemainingKeys(initialValues map[Placeholder]GoValue) []Placeholder {
	var result []Placeholder
	for _, p := range q.AllKeys() {
		if _, ok := initialValues[p]; !ok {
			result = append(result, p)
		}
	}
	return result
}

func (q *QueryParameters) Count() int {
	return len(q.ordered)
}

func (q *QueryParameters) Index(p Placeholder) int {
	for i := range q.ordered {
		if q.ordered[i] == p {
			return i
		}
	}
	return -1
}

func (q *QueryParameters) Has(p Placeholder) bool {
	return q.Index(p) != -1
}

func (q *QueryParameters) GetParameter(i int) Placeholder {
	return q.ordered[i]
}

func (q *QueryParameters) GetPlaceholderForColumn(c ColumnName) (Placeholder, bool) {
	p, ok := q.columnLookup[c]
	return p, ok
}

func (q *QueryParameters) getNextParameter() Placeholder {
	return Placeholder(fmt.Sprintf("@value%d", len(q.ordered)))
}

func (q *QueryParameters) PushParameter(c *Column, dataType CqlDataType, isCollectionKey bool) Placeholder {
	p := q.getNextParameter()
	q.AddParameter(c, p, dataType, isCollectionKey)
	return p
}
func (q *QueryParameters) PushParameterWithoutColumn(dataType CqlDataType) Placeholder {
	p := q.getNextParameter()
	q.AddParameterWithoutColumn(p, dataType)
	return p
}

func (q *QueryParameters) AddParameter(c *Column, p Placeholder, dt CqlDataType, isCollectionKey bool) {
	q.ordered = append(q.ordered, p)
	q.metadata[p] = newPlaceholderMetadata(p, c, dt, isCollectionKey)
	q.columnLookup[c.Name] = p
}

func (q *QueryParameters) AddParameterWithoutColumn(p Placeholder, dt CqlDataType) {
	q.ordered = append(q.ordered, p)
	q.metadata[p] = newPlaceholderMetadata(p, nil, dt, false)
}

const UsingTimePlaceholder Placeholder = "@usingTimeValue"

func (q *QueryParameters) GetMetadata(p Placeholder) PlaceholderMetadata {
	// assume you are passing in a valid placeholder
	d, _ := q.metadata[p]
	return d
}

type QueryParameterValues struct {
	params *QueryParameters
	values map[Placeholder]GoValue
}

func (q *QueryParameterValues) Params() *QueryParameters {
	return q.params
}

func EmptyQueryParameterValues() *QueryParameterValues {
	return &QueryParameterValues{params: NewQueryParameters(), values: make(map[Placeholder]GoValue)}
}

func NewQueryParameterValues(params *QueryParameters) *QueryParameterValues {
	return &QueryParameterValues{params: params, values: make(map[Placeholder]GoValue)}
}

func (q *QueryParameterValues) SetInitialValues(initialValues map[Placeholder]GoValue) error {
	for p, v := range initialValues {
		if !q.Has(p) {
			return fmt.Errorf("unexpected placeholder %s", p)
		}
		err := q.SetValue(p, v)
		if err != nil {
			return fmt.Errorf("error setting initial value for placeholder %s: %w", p, err)
		}
	}
	return nil
}

func (q *QueryParameterValues) Has(p Placeholder) bool {
	return q.params.Has(p)
}

func (q *QueryParameterValues) SetValue(p Placeholder, value any) error {
	md, ok := q.params.metadata[p]
	if !ok {
		return fmt.Errorf("no param metadata for %s", p)
	}

	// ensure the correct type is being set - more for checking internal implementation rather than the user
	err := validateGoType(md.Type, value)
	if err != nil {
		return err
	}

	// todo validate
	q.values[p] = value
	return nil
}

func validateGoType(expected CqlDataType, v any) error {
	if expected.GoType() != reflect.TypeOf(v) {
		return fmt.Errorf("got %T, expected %s (%s)", v, expected.GoType().String(), expected.String())
	}
	return nil
}

func (q *QueryParameterValues) GetValue(p Placeholder) (any, error) {
	if v, ok := q.values[p]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("no query param for %s", p)
}

func (q *QueryParameterValues) GetValueInt64(p Placeholder) (int64, error) {
	v, err := q.GetValue(p)
	if err != nil {
		return 0, err
	}
	intVal, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("query param is a %T, not an int64", v)
	}
	return intVal, nil
}

func (q *QueryParameterValues) GetValueSlice(p Placeholder) ([]GoValue, error) {
	v, err := q.GetValue(p)
	if err != nil {
		return nil, err
	}
	slice, ok := v.([]GoValue)
	if !ok {
		return nil, fmt.Errorf("query param is a %T, not a slice", v)
	}
	return slice, nil
}

func (q *QueryParameterValues) GetValueMap(p Placeholder) (map[GoValue]GoValue, error) {
	v, err := q.GetValue(p)
	if err != nil {
		return nil, err
	}
	slice, ok := v.(map[GoValue]GoValue)
	if !ok {
		return nil, fmt.Errorf("query param is a %T, not a map", v)
	}
	return slice, nil
}

func (q *QueryParameterValues) GetValueByColumn(c ColumnName) (any, error) {
	p, ok := q.params.columnLookup[c]
	if !ok {
		return nil, fmt.Errorf("no parameter assosiated with column %s", c)
	}
	if v, ok := q.values[p]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("no query param for %s", p)
}

func (q *QueryParameterValues) CountSetValues() int {
	return len(q.values)
}
func (q *QueryParameterValues) AsMap() map[Placeholder]GoValue {
	return q.values
}

func (q *QueryParameterValues) BigtableParamMap() map[string]any {
	var result = make(map[string]any)
	for placeholder, value := range q.values {
		// drop the leading '@' symbol
		result[string(placeholder)[1:]] = value
	}
	return result
}
