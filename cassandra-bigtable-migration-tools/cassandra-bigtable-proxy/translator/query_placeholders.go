package translator

import (
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
)

const limitPlaceholder Placeholder = "limitValue"

type Placeholder string

type PlaceholderMetadata struct {
	Key Placeholder
	// nil if limit or collection index/key
	Column *types.Column
	Type   types.CqlDataType
}

type QueryParameters struct {
	ordered []Placeholder
	// note: not all placeholders will be associated with a column - e.g. limit ?
	columnLookup map[types.ColumnName]Placeholder
	// might be different from the column - like if we're doing a "CONTAINS" on a list, this would be the element type
	metadata map[Placeholder]PlaceholderMetadata
}

func NewQueryParameters() *QueryParameters {
	return &QueryParameters{
		ordered:      nil,
		columnLookup: make(map[types.ColumnName]Placeholder),
		metadata:     make(map[Placeholder]PlaceholderMetadata),
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
	return q.ordered
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

func (q *QueryParameters) GetPlaceholderForColumn(c types.ColumnName) (Placeholder, bool) {
	p, ok := q.columnLookup[c]
	return p, ok
}

func (q *QueryParameters) getNextParameter() Placeholder {
	return Placeholder(fmt.Sprintf("value%d", len(q.ordered)))
}

func (q *QueryParameters) PushParameter(c *types.Column, dataType types.CqlDataType) Placeholder {
	p := q.getNextParameter()
	q.AddParameter(c, p, dataType)
	return p
}
func (q *QueryParameters) PushParameterWithoutColumn(dataType types.CqlDataType) Placeholder {
	p := q.getNextParameter()
	q.AddParameterWithoutColumn(p, dataType)
	return p
}

func (q *QueryParameters) AddParameter(c *types.Column, p Placeholder, dt types.CqlDataType) {
	q.ordered = append(q.ordered, p)
	q.metadata[p] = PlaceholderMetadata{
		Key:    p,
		Column: c,
		Type:   dt,
	}
	q.columnLookup[c.Name] = p
}

func (q *QueryParameters) AddParameterWithoutColumn(p Placeholder, dt types.CqlDataType) {
	q.ordered = append(q.ordered, p)
	q.metadata[p] = PlaceholderMetadata{
		Key:    p,
		Column: nil,
		Type:   dt,
	}
}

const UsingTimePlaceholder Placeholder = "usingTimeValue"

func (q *QueryParameters) GetMetadata(p Placeholder) PlaceholderMetadata {
	// assume you are passing in a valid placeholder
	d, _ := q.metadata[p]
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

func (q *QueryParameterValues) PushParameterAndValue(c *types.Column, dataType types.CqlDataType, value any) Placeholder {
	p := q.params.PushParameter(c, dataType)
	q.values[p] = value
	return p
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
	// todo only validate when running in strict mode
	err := utilities.ValidateGoType(value, md.Type)
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
