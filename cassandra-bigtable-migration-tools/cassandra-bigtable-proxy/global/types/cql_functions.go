package types

import (
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
	Apply          func(args []DynamicValue, value *QueryParameterValues) (GoValue, error)
}

type CqlFuncParameter struct {
	Name string
	Type CqlDataType
}

func NewCqlFuncParameter(name string, t CqlDataType) *CqlFuncParameter {
	return &CqlFuncParameter{Name: name, Type: t}
}

func NewCqlFuncSpec(code CqlFuncCode, parameterTypes []CqlFuncParameter, returnType CqlDataType, apply func(args []DynamicValue, value *QueryParameterValues) (GoValue, error)) *CqlFuncSpec {
	return &CqlFuncSpec{Code: code, ParameterTypes: parameterTypes, ReturnType: returnType, Apply: apply}
}

var (
	FuncNow         = NewCqlFuncSpec(FuncCodeNow, nil, TypeTimeuuid, now)
	FuncToTimestamp = NewCqlFuncSpec(FuncCodeToTimestamp, []CqlFuncParameter{*NewCqlFuncParameter("time", TypeTimeuuid)}, TypeTimestamp, toTimestamp)
	FuncMinTimeUuid = NewCqlFuncSpec(FuncCodeMinTimeuuid, []CqlFuncParameter{*NewCqlFuncParameter("time", TypeTimestamp)}, TypeTimeuuid, minTimeUuid)
	FuncMaxTimeUuid = NewCqlFuncSpec(FuncCodeMaxTimeuuid, []CqlFuncParameter{*NewCqlFuncParameter("time", TypeTimestamp)}, TypeTimeuuid, maxTimeUuid)
)

func GetCqlFunc(code CqlFuncCode) (*CqlFuncSpec, error) {
	switch code {
	//case FuncCodeUnknown:
	//	return FuncUnknown, nil
	//case FuncCodeWriteTime:
	//	return FuncWriteTime, nil
	//case FuncCodeCount:
	//	return FuncCount, nil
	//case FuncCodeAvg:
	//	return FuncAvg, nil
	//case FuncCodeSum:
	//	return FuncSum, nil
	//case FuncCodeMin:
	//	return FuncMin, nil
	//case FuncCodeMax:
	//	return FuncMax, nil
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
	err := validateArgCount(1, args)
	if err != nil {
		return nil, err
	}

	u, err := getUuidArg(0, args, values)
	if err != nil {
		return nil, err
	}
	return getTimeFromUUID(u)
}

func maxTimeUuid(args []DynamicValue, values *QueryParameterValues) (GoValue, error) {
	err := validateArgCount(1, args)
	if err != nil {
		return nil, err
	}

	u, err := getTimeArg(0, args, values)
	if err != nil {
		return nil, err
	}
	return maxUUIDv1ForTime(u)
}

func minTimeUuid(args []DynamicValue, values *QueryParameterValues) (GoValue, error) {
	err := validateArgCount(1, args)
	if err != nil {
		return nil, err
	}

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

// TimeFromGregorianEpoch is the starting point for UUIDv1 timestamps:
// 00:00:00.00, 15 October 1582 UTC.
var timeFromGregorianEpochNanos = time.Date(1582, time.October, 15, 0, 0, 0, 0, time.UTC).UnixNano()

// maxUUIDv1ForTime creates the maximum possible UUIDv1 for a given time.Time.
// This is achieved by setting the timestamp fields based on the input time,
// and setting the Clock Sequence and Node ID fields to their maximum values.
func maxUUIDv1ForTime(t time.Time) (uuid.UUID, error) {
	var u [16]byte

	err := setUuidV1Time(t, &u)
	if err != nil {
		return uuid.Nil, err
	}

	// --- Clock Sequence & Node ID Fields (Maxed Values) ---

	// clock_seq_hi_and_variant (byte 8, 8 bits)
	// The variant bits must be 10 (0x80 to 0xBF). We set the clock sequence to max.
	// 0xBF is 0b10111111 (variant 10 and max 6 bits for the clock sequence high part)
	u[8] = 0xFF

	// clock_seq_low (byte 9, 8 bits)
	u[9] = 0xFF

	// Node ID (bytes 10-15, 48 bits) - set to all F's (max)
	u[10] = 0xFF
	u[11] = 0xFF
	u[12] = 0xFF
	u[13] = 0xFF
	u[14] = 0xFF
	u[15] = 0xFF

	// Create the UUID from the byte array
	return u, nil
}

func setUuidV1Time(t time.Time, u *[16]byte) error {
	// 1. Calculate the 60-bit timestamp value
	nanosSinceGregorianEpoch := t.In(time.UTC).UnixNano() - timeFromGregorianEpochNanos
	timestamp100ns := uint64(nanosSinceGregorianEpoch) / 100

	if timestamp100ns > 0xFFFFFFFFFFFFFFF {
		return fmt.Errorf("time is out of UUIDv1 valid range (too far in the future)")
	}

	// --- Timestamp Fields (Big-Endian Order) ---
	// The timestamp parts are the same as the Max UUID function, as they
	// depend only on the input time.

	// time_low (bytes 0-3, 32 bits)
	u[0] = byte(timestamp100ns >> 24)
	u[1] = byte(timestamp100ns >> 16)
	u[2] = byte(timestamp100ns >> 8)
	u[3] = byte(timestamp100ns)

	// time_mid (bytes 4-5, 16 bits)
	u[4] = byte(timestamp100ns >> 40)
	u[5] = byte(timestamp100ns >> 32)

	// time_hi_and_version (bytes 6-7, 16 bits)
	// The 4 bits for version 1 must be set.
	// Version = 0x1XXX (0b0001XXXX)
	timeHi := timestamp100ns >> 48
	timeHi |= 1 << 12 // Set version to 1 (0x1000)

	u[6] = byte(timeHi >> 8)
	u[7] = byte(timeHi)
	return nil
}

func minUUIDv1ForTime(t time.Time) (uuid.UUID, error) {
	var u [16]byte

	err := setUuidV1Time(t, &u)
	if err != nil {
		return uuid.Nil, err
	}

	// --- Clock Sequence & Node ID Fields (Minned Values) ---

	// clock_seq_hi_and_variant (byte 8, 8 bits)
	// The variant bits must be 10 (0x80 to 0xBF). We set the clock sequence to min (0).
	// 0x80 is 0b10000000 (variant 10 and min 6 bits for the clock sequence high part)
	u[8] = 0x80

	// clock_seq_low (byte 9, 8 bits)
	u[9] = 0x00 // Min 8 bits

	// Node ID (bytes 10-15, 48 bits) - set to all 0's (min)
	u[10] = 0x00
	u[11] = 0x00
	u[12] = 0x00
	u[13] = 0x00
	u[14] = 0x00
	u[15] = 0x00

	// Create the UUID from the byte array
	return u, nil
}

func validateArgCount(count int, args []DynamicValue) error {
	if len(args) != count {
		return fmt.Errorf("expected %d arguments got %d", count, len(args))
	}
	return nil
}

func getTimeFromUUID(id uuid.UUID) (time.Time, error) {
	sec, nsec := id.Time().UnixTime()
	return time.Unix(sec, nsec).UTC(), nil
}
