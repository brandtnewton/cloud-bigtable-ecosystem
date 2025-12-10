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

type LiteralValue struct {
	Value GoValue
}

func (l *LiteralValue) IsIdempotent() bool {
	return true
}

func (l *LiteralValue) GetValue(_ *QueryParameterValues) (GoValue, error) {
	return l.Value, nil
}

func NewLiteralValue(value GoValue) DynamicValue {
	return &LiteralValue{Value: value}
}

type FunctionValue struct {
	Placeholder Placeholder
	Code        CqlFuncCode
	Args        []DynamicValue
}

func (f FunctionValue) GetValue(values *QueryParameterValues) (GoValue, error) {
	cqlFunc, err := GetCqlFunc(f.Code)
	if err != nil {
		return nil, err
	}
	apply, err := cqlFunc.Apply(f.Args, values)
	if err != nil {
		return nil, err
	}
	return apply, nil
}

func (f FunctionValue) IsIdempotent() bool {
	return false
}

func NewFunctionValue(placeholder Placeholder, code CqlFuncCode, args []DynamicValue) DynamicValue {
	return &FunctionValue{
		Placeholder: placeholder,
		Code:        code,
		Args:        args,
	}
}
