package placeholders

import (
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
)

type Placeholder string

type QueryParameters struct {
	keys []Placeholder
	// note: not all placeholders will be associated with a column - e.g. limit ?
	columnLookup map[types.ColumnName]Placeholder
	// might be different than the column - like if we're doing a "CONTAINS" on a list, this would be the element type
	types map[Placeholder]types.CqlDataType
}

func NewQueryParameters() *QueryParameters {
	return &QueryParameters{
		keys:  nil,
		types: nil,
	}
}

func (q *QueryParameters) AllColumns() []types.ColumnName {
	var names []types.ColumnName
	for name := range q.columnLookup {
		names = append(names, name)
	}
	return names
}
func (q *QueryParameters) AllKeys() []Placeholder {
	return q.keys
}

func (q *QueryParameters) Count() int {
	return len(q.keys)
}

func (q *QueryParameters) Index(p Placeholder) int {
	for i := range q.keys {
		if q.keys[i] == p {
			return i
		}
	}
	return -1
}

func (q *QueryParameters) Has(p Placeholder) bool {
	return q.Index(p) != -1
}

func (q *QueryParameters) GetParameter(i int) Placeholder {
	return q.keys[i]
}

func (q *QueryParameters) GetPlaceholderForColumn(c types.ColumnName) (Placeholder, bool) {
	p, ok := q.columnLookup[c]
	return p, ok
}

func (q *QueryParameters) getNextParameter() Placeholder {
	return Placeholder(fmt.Sprintf("value%d", len(q.keys)))
}

func (q *QueryParameters) PushParameter(c types.ColumnName, dataType types.CqlDataType) Placeholder {
	p := q.getNextParameter()
	q.AddParameter(c, p, dataType)
	return p
}
func (q *QueryParameters) PushParameterWithoutColumn(dataType types.CqlDataType) Placeholder {
	p := q.getNextParameter()
	q.AddParameterWithoutColumn(p, dataType)
	return p
}

func (q *QueryParameters) AddParameter(c types.ColumnName, p Placeholder, dataType types.CqlDataType) {
	q.keys = append(q.keys, p)
	q.types[p] = dataType
	q.columnLookup[c] = p
}

func (q *QueryParameters) AddParameterWithoutColumn(p Placeholder, dataType types.CqlDataType) {
	q.keys = append(q.keys, p)
	q.types[p] = dataType
}

const UsingTimePlaceholder Placeholder = "usingTimeValue"

func (q *QueryParameters) GetDataType(p Placeholder) types.CqlDataType {
	// assume you are passing in a valid placeholder
	d, _ := q.types[p]
	return d
}

type QueryParameterValues struct {
	params *QueryParameters
	values map[Placeholder]types.GoValue
}

func (q *QueryParameterValues) Params() *QueryParameters {
	return q.params
}

func NewQueryParameterValues(params *QueryParameters) *QueryParameterValues {
	return &QueryParameterValues{params: params, values: make(map[Placeholder]types.GoValue)}
}

func (q *QueryParameterValues) PushParameterAndValue(c types.ColumnName, dataType types.CqlDataType, value any) Placeholder {
	p := q.params.PushParameter(c, dataType)
	q.values[p] = value
	return p
}

func (q *QueryParameterValues) Has(p Placeholder) bool {
	return q.params.Has(p)
}

func (q *QueryParameterValues) SetValue(p Placeholder, value any) error {
	dt, ok := q.params.types[p]
	if !ok {
		return fmt.Errorf("no param type info for %s", p)
	}

	// ensure the correct type is being set - more for checking internal implementation rather than the user
	// todo only validate when running in strict mode
	err := validateGoType(value, dt)
	if err != nil {
		return err
	}

	// todo validate
	q.values[p] = value
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

func (q *QueryParameterValues) GetValueSlice(p Placeholder) ([]types.GoValue, error) {
	v, err := q.GetValue(p)
	if err != nil {
		return nil, err
	}
	slice, ok := v.([]types.GoValue)
	if !ok {
		return nil, fmt.Errorf("query param is a %T, not a slice", v)
	}
	return slice, nil
}

func (q *QueryParameterValues) GetValueMap(p Placeholder) (map[types.GoValue]types.GoValue, error) {
	v, err := q.GetValue(p)
	if err != nil {
		return nil, err
	}
	slice, ok := v.(map[types.GoValue]types.GoValue)
	if !ok {
		return nil, fmt.Errorf("query param is a %T, not a map", v)
	}
	return slice, nil
}

func (q *QueryParameterValues) GetValueByColumn(c types.ColumnName) (any, error) {
	p, ok := q.params.columnLookup[c]
	if !ok {
		return nil, fmt.Errorf("no parameter assosiated with column %s", c)
	}
	if v, ok := q.values[p]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("no query param for %s", p)
}

func (q *QueryParameterValues) AsMap() map[string]any {
	var result = make(map[string]any)
	for placeholder, value := range q.values {
		result[string(placeholder)] = value
	}
	return result
}
