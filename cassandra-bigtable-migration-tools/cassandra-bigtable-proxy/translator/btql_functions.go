package translator

import (
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"strings"
)

type BtqlFunction struct {
	code       BtqlFuncCode
	str        string
	returnType types.CqlDataType
}

func newBtqlFunction(code BtqlFuncCode, str string, returnType types.CqlDataType) *BtqlFunction {
	return &BtqlFunction{code: code, str: str, returnType: returnType}
}

func (b BtqlFunction) Code() BtqlFuncCode {
	return b.code
}

func (b BtqlFunction) String() string {
	return b.str
}

func (b BtqlFunction) ReturnType() types.CqlDataType {
	return b.returnType
}

type BtqlFuncCode int

const (
	FuncCodeUnknown BtqlFuncCode = iota
	FuncCodeWriteTime
	FuncCodeCount
	FuncCodeAvg
	FuncCodeSum
	FuncCodeMin
	FuncCodeMax
)

var (
	FuncWriteTime = newBtqlFunction(FuncCodeWriteTime, "WRITETIME", types.TypeBigint)
	FuncCount     = newBtqlFunction(FuncCodeCount, "COUNT", types.TypeBigint)
	FuncAvg       = newBtqlFunction(FuncCodeAvg, "AVG", types.TypeBigint)
	FuncSum       = newBtqlFunction(FuncCodeSum, "SUM", types.TypeBigint)
	FuncMin       = newBtqlFunction(FuncCodeMin, "MIN", types.TypeBigint)
	FuncMax       = newBtqlFunction(FuncCodeMax, "MAX", types.TypeBigint)
)

func ParseCqlFunc(s string) (*BtqlFunction, error) {
	switch strings.ToLower(s) {
	case "writetime":
		return FuncWriteTime, nil
	case "count":
		return FuncCount, nil
	case "avg":
		return FuncAvg, nil
	case "sum":
		return FuncSum, nil
	case "min":
		return FuncMin, nil
	case "max":
		return FuncMax, nil
	default:
		return nil, fmt.Errorf("unsupported function type: '%s'", s)
	}
}
