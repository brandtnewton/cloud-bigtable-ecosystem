package types

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

type Placeholder string

type PlaceholderMetadata struct {
	Key             Placeholder
	Type            CqlDataType
	IsUserParameter bool
}

func newPlaceholderMetadata(key Placeholder, tpe CqlDataType, isUserParameter bool) PlaceholderMetadata {
	return PlaceholderMetadata{
		Key:             key,
		Type:            tpe,
		IsUserParameter: isUserParameter,
	}
}

// todo maybe a system placeholder slice for execution time function values?
type QueryParameters struct {
	ordered []Placeholder

	// might be different from the column - like if we're doing a "CONTAINS" on a list, this would be the element type
	metadata map[Placeholder]PlaceholderMetadata
}

func NewQueryParameters() *QueryParameters {
	return &QueryParameters{
		ordered:  nil,
		metadata: make(map[Placeholder]PlaceholderMetadata),
	}
}

func (q *QueryParameters) AllUserKeys() []PlaceholderMetadata {
	var result []PlaceholderMetadata
	for _, p := range q.ordered {
		md := q.metadata[p]
		if md.IsUserParameter {
			result = append(result, md)
		}
	}
	return result
}

func (q *QueryParameters) AllMetadata() []PlaceholderMetadata {
	var result []PlaceholderMetadata
	for _, p := range q.ordered {
		result = append(result, q.metadata[p])
	}
	return result
}

func (q *QueryParameters) CountUserParameters() int {
	count := 0
	for _, metadata := range q.metadata {
		if metadata.IsUserParameter {
			count += 1
		}
	}
	return count
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

func (q *QueryParameters) getNextParameter() Placeholder {
	return Placeholder(fmt.Sprintf("@value%d", len(q.ordered)))
}

func (q *QueryParameters) BuildParameter(dataType CqlDataType, isUserParameter bool) *QueryParameters {
	_ = q.PushParameter(dataType, isUserParameter)
	return q
}

func (q *QueryParameters) PushParameter(dataType CqlDataType, isUserParameter bool) Placeholder {
	p := q.getNextParameter()
	q.AddParameter(p, dataType, isUserParameter)
	return p
}

func (q *QueryParameters) AddParameter(p Placeholder, dt CqlDataType, isUserParameter bool) {
	q.ordered = append(q.ordered, p)
	q.metadata[p] = newPlaceholderMetadata(p, dt, isUserParameter)
}

func (q *QueryParameters) GetMetadata(p Placeholder) PlaceholderMetadata {
	// assume you are passing in a valid placeholder
	d, _ := q.metadata[p]
	return d
}

type QueryParameterValues struct {
	params *QueryParameters
	time   time.Time
	values map[Placeholder]GoValue
}

func (q *QueryParameterValues) Params() *QueryParameters {
	return q.params
}

func (q *QueryParameterValues) Time() time.Time {
	return q.time
}

func NewQueryParameterValues(params *QueryParameters) *QueryParameterValues {
	return &QueryParameterValues{params: params, values: make(map[Placeholder]GoValue), time: time.Now().UTC()}
}

func (q *QueryParameterValues) SetValue(p Placeholder, value any) error {
	// ensure the correct type is being set - more for checking internal implementation rather than the user
	//err := validateGoType(md.Types, value)
	//if err != nil {
	//	return err
	//}

	// todo validate
	q.values[p] = value
	return nil
}

func validateGoType(expected CqlDataType, v any) error {
	if v == nil {
		return nil
	}
	// hack to ignore pointers in comparison
	gotType := strings.ReplaceAll(reflect.TypeOf(v).String(), "*", "")
	expectedType := expected.GoType().String()
	if gotType != expectedType {
		return fmt.Errorf("got %s, expected %s (%s)", gotType, expectedType, expected.String())
	}
	return nil
}

func (q *QueryParameterValues) GetValue(p Placeholder) (GoValue, error) {
	if v, ok := q.values[p]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("no query param for %s", p)
}

func (q *QueryParameterValues) AllValuesSet() bool {
	for _, md := range q.params.metadata {
		if !md.IsUserParameter {
			continue
		}
		_, ok := q.values[md.Key]
		if !ok {
			return false
		}
	}
	return true
}
func (q *QueryParameterValues) AsMap() map[Placeholder]GoValue {
	return q.values
}

func (q *QueryParameterValues) GetValueInt32(dv DynamicValue) (int32, error) {
	value, err := dv.GetValue(q)
	if err != nil {
		return 0, err
	}
	intVal, ok := value.(int32)
	if !ok {
		return 0, fmt.Errorf("query value is a %T, not an int32", value)
	}
	return intVal, nil
}

func (q *QueryParameterValues) GetValueInt64(dv DynamicValue) (int64, error) {
	v, err := dv.GetValue(q)
	if err != nil {
		return 0, err
	}
	intVal, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("query value is a %T, not an int64", v)
	}
	return intVal, nil
}

func (q *QueryParameterValues) GetValueTime(dv DynamicValue) (time.Time, error) {
	v, err := dv.GetValue(q)
	if err != nil {
		return time.Time{}, err
	}
	t, ok := v.(time.Time)
	if !ok {
		return time.Time{}, fmt.Errorf("query value is a %T, not a time", v)
	}
	return t, nil
}

func (q *QueryParameterValues) GetValueSlice(dv DynamicValue) ([]GoValue, error) {
	v, err := dv.GetValue(q)
	if err != nil {
		return nil, err
	}

	val := reflect.ValueOf(v)

	if val.Kind() != reflect.Slice {
		return nil, fmt.Errorf("query value is a %T, not a slice", v)
	}

	length := val.Len()
	result := make([]GoValue, length)

	for i := 0; i < length; i++ {
		result[i] = val.Index(i).Interface()
	}

	return result, nil
}

func (q *QueryParameterValues) GetValueMap(dv DynamicValue) (map[GoValue]GoValue, error) {
	v, err := dv.GetValue(q)
	if err != nil {
		return nil, err
	}
	val := reflect.ValueOf(v)

	if val.Kind() != reflect.Map {
		return nil, fmt.Errorf("value is a %T, not a map", v)
	}

	result := make(map[GoValue]GoValue, val.Len())

	// 3. Iterate over the keys of the original map
	iter := val.MapRange()
	for iter.Next() {
		// Get the reflection Placeholder for the key and the value
		keyVal := iter.Key()
		valueVal := iter.Value()

		// 4. Use .Interface() to convert the concrete key/value to an any (interface{})
		keyAny := keyVal.Interface()
		valueAny := valueVal.Interface()

		// 5. Add to the new map[any]any
		result[keyAny] = valueAny
	}
	return result, nil
}
