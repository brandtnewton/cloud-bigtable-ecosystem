package types

type DynamicValue interface {
	GetValue(values *QueryParameterValues) (GoValue, error)
	IsIdempotent() bool
}

type ParameterizedValue struct {
	Placeholder Placeholder
}

func (p *ParameterizedValue) IsIdempotent() bool {
	return true
}

func (p *ParameterizedValue) GetValue(values *QueryParameterValues) (GoValue, error) {
	return values.GetValue(p.Placeholder)
}

func NewParameterizedValue(placeholder Placeholder) DynamicValue {
	return &ParameterizedValue{Placeholder: placeholder}
}

type TimestampNowValue struct {
}

func (f *TimestampNowValue) IsIdempotent() bool {
	return false
}

func (f *TimestampNowValue) GetValue(values *QueryParameterValues) (GoValue, error) {
	return values.Time(), nil
}

func NewTimestampNowValue() DynamicValue {
	return &TimestampNowValue{}
}

type LiteralValue struct {
	Value GoValue
}

func (l *LiteralValue) IsIdempotent() bool {
	return true
}

func (l *LiteralValue) GetValue(values *QueryParameterValues) (GoValue, error) {
	return l.Value, nil
}

func NewLiteralValue(value GoValue) DynamicValue {
	return &LiteralValue{Value: value}
}
