package common

import (
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"strings"
)

type BtqlFunction struct {
	code       types.BtqlFuncCode
	str        string
	returnType types.CqlDataType
}

func newBtqlFunction(code types.BtqlFuncCode, str string, returnType types.CqlDataType) *BtqlFunction {
	return &BtqlFunction{code: code, str: str, returnType: returnType}
}

func (b BtqlFunction) Code() types.BtqlFuncCode {
	return b.code
}

func (b BtqlFunction) String() string {
	return b.str
}

func (b BtqlFunction) ReturnType() types.CqlDataType {
	return b.returnType
}

var (
	FuncWriteTime = newBtqlFunction(types.FuncCodeWriteTime, "WRITETIME", types.TypeBigint)
	FuncCount     = newBtqlFunction(types.FuncCodeCount, "COUNT", types.TypeBigint)
	FuncAvg       = newBtqlFunction(types.FuncCodeAvg, "AVG", types.TypeBigint)
	FuncSum       = newBtqlFunction(types.FuncCodeSum, "SUM", types.TypeBigint)
	FuncMin       = newBtqlFunction(types.FuncCodeMin, "MIN", types.TypeBigint)
	FuncMax       = newBtqlFunction(types.FuncCodeMax, "MAX", types.TypeBigint)
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
