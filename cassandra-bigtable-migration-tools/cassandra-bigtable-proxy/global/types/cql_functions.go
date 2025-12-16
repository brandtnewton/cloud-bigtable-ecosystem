package types

import (
	"encoding/binary"
	"fmt"
	"github.com/google/uuid"
	"strings"
	"time"
)

type CqlFuncCode int

const (
	FuncCodeUnknown CqlFuncCode = iota
	FuncCodeWriteTime
	FuncCodeCount
	FuncCodeAvg
	FuncCodeSum
	FuncCodeMin
	FuncCodeMax
	FuncCodeNow
	FuncCodeToTimestamp
	FuncCodeMinTimeuuid
	FuncCodeMaxTimeuuid
)

func (c CqlFuncCode) String() string {
	switch c {
	case FuncCodeWriteTime:
		return "writetime"
	case FuncCodeCount:
		return "count"
	case FuncCodeAvg:
		return "avg"
	case FuncCodeSum:
		return "sum"
	case FuncCodeMin:
		return "min"
	case FuncCodeMax:
		return "max"
	case FuncCodeNow:
		return "now"
	case FuncCodeToTimestamp:
		return "toTimestamp"
	case FuncCodeMinTimeuuid:
		return "minTimeuuid"
	case FuncCodeMaxTimeuuid:
		return "maxTimeuuid"
	default:
		return "unknown"
	}
}

func FromString(s string) (CqlFuncCode, error) {
	switch strings.ToLower(s) {
	case "writetime":
		return FuncCodeWriteTime, nil
	case "count":
		return FuncCodeCount, nil
	case "avg":
		return FuncCodeAvg, nil
	case "sum":
		return FuncCodeSum, nil
	case "min":
		return FuncCodeMin, nil
	case "max":
		return FuncCodeMax, nil
	case "now":
		return FuncCodeNow, nil
	case "totimestamp":
		return FuncCodeToTimestamp, nil
	case "mintimeuuid":
		return FuncCodeMinTimeuuid, nil
	case "maxtimeuuid":
		return FuncCodeMaxTimeuuid, nil
	default:
		return FuncCodeUnknown, fmt.Errorf("unrecognized function `%s`", s)
	}
}

type CqlFuncSpec struct {
	Code           CqlFuncCode
	ParameterTypes []CqlFuncParameter
	ReturnType     CqlDataType
	ValidClauses   []QueryClause
	Apply          func(args []DynamicValue, value *QueryParameterValues) (GoValue, error)
}

type CqlFuncParameter struct {
	Types       []CqlDataType
	AllowColumn bool
	AllowStar   bool
}

func NewCqlFuncParameter(Types []CqlDataType, allowColumn bool, allowStar bool) *CqlFuncParameter {
	return &CqlFuncParameter{Types: Types, AllowColumn: allowColumn, AllowStar: allowStar}
}

func NewCqlFuncSpec(code CqlFuncCode, parameterTypes []CqlFuncParameter, returnType CqlDataType, validClauses []QueryClause, apply func(args []DynamicValue, value *QueryParameterValues) (GoValue, error)) *CqlFuncSpec {
	return &CqlFuncSpec{Code: code, ParameterTypes: parameterTypes, ReturnType: returnType, ValidClauses: validClauses, Apply: apply}
}

var (
	FuncWriteTime = NewCqlFuncSpec(
		FuncCodeWriteTime,
		[]CqlFuncParameter{
			*NewCqlFuncParameter(
				AllScalarTypes,
				true,
				false,
			),
		},
		TypeBigInt,
		[]QueryClause{QueryClauseSelect},
		nil)
	FuncCount = NewCqlFuncSpec(
		FuncCodeCount,
		[]CqlFuncParameter{
			*NewCqlFuncParameter(
				AllScalarTypes,
				true,
				true,
			),
		},
		TypeBigInt,
		[]QueryClause{QueryClauseSelect},
		nil)
	FuncAvg = NewCqlFuncSpec(
		FuncCodeAvg,
		[]CqlFuncParameter{
			*NewCqlFuncParameter(
				AllNumericTypes,
				true,
				false,
			),
		},
		TypeDouble,
		[]QueryClause{QueryClauseSelect},
		nil,
	)
	FuncSum = NewCqlFuncSpec(
		FuncCodeSum,
		[]CqlFuncParameter{
			*NewCqlFuncParameter(
				AllNumericTypes,
				true,
				false,
			),
		},
		TypeDouble,
		[]QueryClause{QueryClauseSelect},
		nil)
	FuncMin = NewCqlFuncSpec(
		FuncCodeMin,
		[]CqlFuncParameter{
			*NewCqlFuncParameter(
				AllNumericTypes,
				true,
				false,
			),
		},
		TypeDouble,
		[]QueryClause{QueryClauseSelect},
		nil)
	FuncMax = NewCqlFuncSpec(
		FuncCodeMax,
		[]CqlFuncParameter{
			*NewCqlFuncParameter(
				AllNumericTypes,
				true,
				false,
			),
		},
		TypeDouble,
		[]QueryClause{QueryClauseSelect},
		nil)
	FuncNow = NewCqlFuncSpec(
		FuncCodeNow,
		nil,
		TypeTimeuuid,
		[]QueryClause{QueryClauseWhere},
		now)
	FuncToTimestamp = NewCqlFuncSpec(
		FuncCodeToTimestamp,
		[]CqlFuncParameter{
			*NewCqlFuncParameter(
				[]CqlDataType{
					TypeTimeuuid,
				},
				false,
				false,
			),
		},
		TypeTimestamp,
		[]QueryClause{QueryClauseWhere},
		toTimestamp,
	)
	FuncMinTimeUuid = NewCqlFuncSpec(FuncCodeMinTimeuuid,
		[]CqlFuncParameter{
			*NewCqlFuncParameter(
				[]CqlDataType{
					TypeTimestamp,
				},
				false,
				false),
		},
		TypeTimeuuid,
		[]QueryClause{QueryClauseWhere},
		minTimeUuid)
	FuncMaxTimeUuid = NewCqlFuncSpec(FuncCodeMaxTimeuuid,
		[]CqlFuncParameter{
			*NewCqlFuncParameter(
				[]CqlDataType{
					TypeTimestamp,
				},
				false,
				false),
		},
		TypeTimeuuid,
		[]QueryClause{QueryClauseWhere},
		maxTimeUuid)
)

func GetCqlFunc(code CqlFuncCode) (*CqlFuncSpec, error) {
	switch code {
	case FuncCodeWriteTime:
		return FuncWriteTime, nil
	case FuncCodeCount:
		return FuncCount, nil
	case FuncCodeAvg:
		return FuncAvg, nil
	case FuncCodeSum:
		return FuncSum, nil
	case FuncCodeMin:
		return FuncMin, nil
	case FuncCodeMax:
		return FuncMax, nil
	case FuncCodeNow:
		return FuncNow, nil
	case FuncCodeToTimestamp:
		return FuncToTimestamp, nil
	case FuncCodeMinTimeuuid:
		return FuncMinTimeUuid, nil
	case FuncCodeMaxTimeuuid:
		return FuncMaxTimeUuid, nil
	default:
		return nil, fmt.Errorf("unknown function code %s (%d)", code.String(), code)
	}
}

func now(_ []DynamicValue, _ *QueryParameterValues) (GoValue, error) {
	id, err := uuid.NewUUID()
	if id.Version() != 1 {
		return nil, fmt.Errorf("timeuuid must be v1")
	}
	if err != nil {
		return nil, err
	}
	return id, nil
}

func toTimestamp(args []DynamicValue, values *QueryParameterValues) (GoValue, error) {
	u, err := getUuidArg(0, args, values)
	if err != nil {
		return nil, err
	}
	return getTimeFromUUID(u)
}

func maxTimeUuid(args []DynamicValue, values *QueryParameterValues) (GoValue, error) {
	u, err := getTimeArg(0, args, values)
	if err != nil {
		return nil, err
	}
	return maxUUIDv1ForTime(u)
}

func minTimeUuid(args []DynamicValue, values *QueryParameterValues) (GoValue, error) {
	u, err := getTimeArg(0, args, values)
	if err != nil {
		return nil, err
	}
	return minUUIDv1ForTime(u)
}

func getUuidArg(index int, args []DynamicValue, values *QueryParameterValues) (uuid.UUID, error) {
	value, err := args[index].GetValue(values)
	if err != nil {
		return [16]byte{}, err
	}

	u, ok := value.(uuid.UUID)
	if !ok {
		return [16]byte{}, fmt.Errorf("invalid argment: %T expected uuid", value)
	}
	return u, nil
}

func getTimeArg(index int, args []DynamicValue, values *QueryParameterValues) (time.Time, error) {
	value, err := args[index].GetValue(values)
	if err != nil {
		return time.Time{}, err
	}

	u, ok := value.(time.Time)
	if !ok {
		return time.Time{}, fmt.Errorf("invalid argment: %T expected time.Time", value)
	}
	return u, nil
}

func ValidateFunctionArgs(f *FunctionValue) error {
	if len(f.Func.ParameterTypes) != len(f.Args) {
		return fmt.Errorf("function '%s' expects %d args but got %d", f.Func.Code.String(), len(f.Func.ParameterTypes), len(f.Args))
	}

	for i, arg := range f.Args {
		param := f.Func.ParameterTypes[i]
		star := IsSelectStar(arg)
		if !param.AllowStar && star {
			return fmt.Errorf("function '%s' doesn't allow '*' args for parameter %d", f.Func.Code.String(), i)
		}
		if !param.AllowColumn && IsColumn(arg) {
			return fmt.Errorf("function '%s' doesn't allow column args for parameter %d", f.Func.Code.String(), i)
		}

		// star is allowed so it's ok
		if star {
			continue
		}

		isValidType := false
		for _, dataType := range param.Types {
			if dataType == arg.GetType() {
				isValidType = true
				break
			}
		}
		if !isValidType {
			return fmt.Errorf("function '%s' parameter %d doesn't accept type %s", f.Func.Code.String(), i, arg.GetType().String())
		}
	}
	return nil
}

func getColumnArgOrStar(index int, args []DynamicValue) (DynamicValue, error) {
	value := args[index]
	star, ok := value.(SelectStarValue)
	if ok {
		return star, nil
	}
	col, ok := value.(ColumnValue)
	if ok {
		return col, nil
	}
	return nil, fmt.Errorf("invalid argument: %T expected column or *", value)
}

func getColumnArg(index int, args []DynamicValue) (*Column, error) {
	value := args[index]
	col, ok := value.(ColumnValue)
	if !ok {
		return nil, fmt.Errorf("invalid argument: %T expected a column", value)
	}
	return col.Column, nil
}

// TimeFromGregorianEpoch is the starting point for UUIDv1 timestamps:
// 00:00:00.00, 15 October 1582 UTC.
var timeFromGregorianEpochNanos = time.Date(1582, time.October, 15, 0, 0, 0, 0, time.UTC).UnixNano()

// maxUUIDv1ForTime creates the maximum possible UUIDv1 for a given time.Time.
// This is achieved by setting the timestamp fields based on the input time,
// and setting the Clock Sequence and Node ID fields to their maximum values.
func maxUUIDv1ForTime(t time.Time) (uuid.UUID, error) {
	// For maxTimeuuid, Cassandra adds 9999 to the 100-nanosecond intervals timestamp.
	// 9999 * 100 nanoseconds.
	maxT := t.Add(9999 * 100 * time.Nanosecond)
	u := setUuidV1Time(maxT, [16]byte{})

	// --- Clock Sequence & Node ID Fields (Maxed Values) ---

	// clock_seq_hi_and_variant (byte 8, 8 bits)
	// The variant bits must be 10 (0x80 to 0xBF). We set the clock sequence to max.
	// 0xBF is 0b10111111 (variant 10 and max 6 bits for the clock sequence high part)
	u[8] = 0x7f

	// clock_seq_low (byte 9, 8 bits)
	u[9] = 0x7f

	// Node ID (bytes 10-15, 48 bits) - set to all F's (max)
	u[10] = 0x7f
	u[11] = 0x7f
	u[12] = 0x7f
	u[13] = 0x7f
	u[14] = 0x7f
	u[15] = 0x7f

	// Create the UUID from the byte array
	return u, nil
}

func setUuidV1Time(t time.Time, id uuid.UUID) uuid.UUID {
	const epochOffset = 122192928000000000

	// Convert Unix time to 100-ns intervals
	unixIntervals := t.Unix()*10000000 + int64(t.Nanosecond()/100)

	// Add the offset
	uuidTime := uint64(unixIntervals + epochOffset)

	// 2. Breakdown the 60-bit timestamp into fields
	// UUID v1 Layout:
	//   Time Low (32 bits)
	//   Time Mid (16 bits)
	//   Time High & Version (16 bits)

	timeLow := uint32(uuidTime & 0xFFFFFFFF)
	timeMid := uint16((uuidTime >> 32) & 0xFFFF)
	timeHi := uint16((uuidTime >> 48) & 0x0FFF)

	// 3. Write bytes into the UUID structure (Big Endian)

	// Bytes 0-3: Time Low
	binary.BigEndian.PutUint32(id[0:4], timeLow)

	// Bytes 4-5: Time Mid
	binary.BigEndian.PutUint16(id[4:6], timeMid)

	// Bytes 6-7: Time High + Version
	// We combine the high time bits with the Version 1 identifier (0x1000)
	binary.BigEndian.PutUint16(id[6:8], timeHi|0x1000)
	return id
}

func minUUIDv1ForTime(t time.Time) (uuid.UUID, error) {
	u := setUuidV1Time(t, [16]byte{})

	// --- Clock Sequence & Node ID Fields (Minned Values) ---

	// clock_seq_hi_and_variant (byte 8, 8 bits)
	// The variant bits must be 10 (0x80 to 0xBF). We set the clock sequence to min (0).
	// 0x80 is 0b10000000 (variant 10 and min 6 bits for the clock sequence high part)
	u[8] = 0x80

	// clock_seq_low (byte 9, 8 bits)
	u[9] = 0x80 // Min 8 bits

	// Node ID (bytes 10-15, 48 bits) - set to all 0's (min)
	u[10] = 0x80
	u[11] = 0x80
	u[12] = 0x80
	u[13] = 0x80
	u[14] = 0x80
	u[15] = 0x80

	// Create the UUID from the byte array
	return u, nil
}

func getTimeFromUUID(id uuid.UUID) (time.Time, error) {
	sec, nsec := id.Time().UnixTime()
	return time.Unix(sec, nsec).UTC(), nil
}
