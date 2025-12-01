package types

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

const LimitPlaceholder Placeholder = "@limitValue"

type Placeholder string

type PlaceholderMetadata struct {
	Key Placeholder

	Type CqlDataType
	// indicates that this is a value for accessing a collection column by index or key
	IsCollectionKey bool
}

func newPlaceholderMetadata(key Placeholder, tpe CqlDataType, isCollectionKey bool) PlaceholderMetadata {
	return PlaceholderMetadata{Key: key, Type: tpe, IsCollectionKey: isCollectionKey}
}

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

func (q *QueryParameters) getNextParameter() Placeholder {
	return Placeholder(fmt.Sprintf("@value%d", len(q.ordered)))
}

func (q *QueryParameters) BuildParameter(dataType CqlDataType, isCollectionKey bool) *QueryParameters {
	_ = q.PushParameter(dataType, isCollectionKey)
	return q
}

func (q *QueryParameters) PushParameter(dataType CqlDataType, isCollectionKey bool) Placeholder {
	p := q.getNextParameter()
	q.AddParameter(p, dataType, isCollectionKey)
	return p
}

func (q *QueryParameters) AddParameter(p Placeholder, dt CqlDataType, isCollectionKey bool) {
	q.ordered = append(q.ordered, p)
	q.metadata[p] = newPlaceholderMetadata(p, dt, isCollectionKey)
}

func (q *QueryParameters) AddParameterWithoutColumn(p Placeholder, dt CqlDataType) {
	q.ordered = append(q.ordered, p)
	q.metadata[p] = newPlaceholderMetadata(p, dt, false)
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

func NewQueryParameterValues(params *QueryParameters, time time.Time) *QueryParameterValues {
	return &QueryParameterValues{params: params, values: make(map[Placeholder]GoValue), time: time}
}

func (q *QueryParameterValues) Has(p Placeholder) bool {
	return q.params.Has(p)
}

func (q *QueryParameterValues) SetValue(p Placeholder, value any) error {
	//md, ok := q.params.metadata[p]
	//if !ok {
	//	return fmt.Errorf("no param metadata for %s", p)
	//}

	// ensure the correct type is being set - more for checking internal implementation rather than the user
	//err := validateGoType(md.Type, value)
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

func (q *QueryParameterValues) GetValueInt32(p Placeholder) (int32, error) {
	v, err := q.GetValue(p)
	if err != nil {
		return 0, err
	}
	intVal, ok := v.(int32)
	if !ok {
		return 0, fmt.Errorf("query param %s is a %T, not an int32", p, v)
	}
	return intVal, nil
}
func (q *QueryParameterValues) GetValueInt64(p Placeholder) (int64, error) {
	v, err := q.GetValue(p)
	if err != nil {
		return 0, err
	}
	intVal, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("query param %s is a %T, not an int64", p, v)
	}
	return intVal, nil
}

func (q *QueryParameterValues) GetValueSlice(p Placeholder) ([]GoValue, error) {
	v, err := q.GetValue(p)
	if err != nil {
		return nil, err
	}

	val := reflect.ValueOf(v)

	if val.Kind() != reflect.Slice {
		return nil, fmt.Errorf("query param %s is a %T, not a slice", p, v)
	}

	length := val.Len()
	result := make([]GoValue, length)

	for i := 0; i < length; i++ {
		result[i] = val.Index(i).Interface()
	}

	return result, nil
}

func (q *QueryParameterValues) GetValueMap(p Placeholder) (map[GoValue]GoValue, error) {
	v, err := q.GetValue(p)
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

func (q *QueryParameterValues) CountSetValues() int {
	return len(q.values)
}
func (q *QueryParameterValues) AsMap() map[Placeholder]GoValue {
	return q.values
}

func (q *QueryParameterValues) BigtableParamMap() map[string]any {
	var result = make(map[string]any)
	for placeholder, value := range q.values {
		md := q.params.GetMetadata(placeholder)
		if md.IsCollectionKey {
			switch v := value.(type) {
			case string:
				value = []byte(v)
			}
		}
		// drop the leading '@' symbol
		result[string(placeholder)[1:]] = value
	}
	return result
}
