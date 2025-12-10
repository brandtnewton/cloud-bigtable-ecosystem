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
	FuncMinTimeUuid = NewCqlFuncSpec(FuncCodeMinTimeuuid, []CqlFuncParameter{*NewCqlFuncParameter("time", TypeTimestamp)}, TypeTimeuuid, toTimestamp)
	FuncMaxTimeUuid = NewCqlFuncSpec(FuncCodeMaxTimeuuid, []CqlFuncParameter{*NewCqlFuncParameter("time", TypeTimestamp)}, TypeTimeuuid, toTimestamp)
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
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	return id, nil
}

func minTimestamp(args []DynamicValue, values *QueryParameterValues) (GoValue, error) {
	err := validateArgCount(1, args)
	if err != nil {
		return nil, err
	}

	t, err := values.GetValueTime(args[0])
	if err != nil {
		return nil, err
	}

	b, err := uuidToTimeBytes(t)
	if err != nil {
		return nil, err
	}

	return b + "\x00", nil
}

func maxTimestamp(args []DynamicValue, values *QueryParameterValues) (GoValue, error) {
	err := validateArgCount(1, args)
	if err != nil {
		return nil, err
	}

	t, err := values.GetValueTime(args[0])
	if err != nil {
		return nil, err
	}

	b, err := uuidToTimeBytes(t)
	if err != nil {
		return nil, err
	}

	return b + "\xff", nil
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
	return getTimeFromUUIDv7(u)
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

func uuidToTimeBytes(t time.Time) (string, error) {

	// 1. Get the time in milliseconds since the Unix epoch.
	// We use UTC to ensure the timestamp is globally consistent.
	// Go's UnixMilli() handles this conversion.
	unixMillis := t.In(time.UTC).UnixMilli()

	// UUIDv7 is defined to use a 48-bit timestamp.
	// A standard 64-bit int64 (which UnixMilli returns) can hold more than
	// 48 bits, but the first 48 bits are what we care about.
	// 48 bits can store a value up to 2^48 - 1, which is over 8,500 years
	// from the Unix epoch, so it's safe for a time.Time value.

	// Check for negative time (pre-1970), which is invalid for UUIDv7 epoch.
	if unixMillis < 0 {
		return "", fmt.Errorf("time %v is before the Unix epoch (Jan 1, 1970) and cannot be used in UUIDv7", t)
	}

	// 2. Convert the int64 timestamp into a 64-bit big-endian byte slice.
	// We start with 8 bytes, then discard the first two (most significant)
	// which will be zero if the time is within the next 8000+ years.
	// Big-endian order is required by the UUID standard.
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(unixMillis))

	// 3. The UUIDv7 timestamp is only 48 bits (6 bytes),
	// corresponding to the last 6 bytes of the 64-bit millisecond value.
	// Since BigEndian puts the *most significant* bytes first,
	// we slice off the first two bytes (buf[0:2]), which should be 0x00, 0x00.
	return string(buf[2:]), nil
}

func validateArgCount(count int, args []DynamicValue) error {
	if len(args) != count {
		return fmt.Errorf("expected %d arguments got %d", count, len(args))
	}
	return nil
}

func getTimeFromUUIDv7(id uuid.UUID) (time.Time, error) {
	// 1. Check the UUID version. UUIDv7 has the '7' in the 4 most significant bits
	//    of the 7th byte (index 6). This is a good sanity check.
	if id.Version() != 7 {
		return time.Time{}, fmt.Errorf("uuid version is %d, expected 7", id.Version())
	}

	// UUIDv7 structure:
	// Bits 0-47: 48-bit Unix Epoch time in milliseconds (6 bytes)
	// Bits 48-51: 4-bit version (7)
	// ... rest is reserved/random
	timestamp64 := binary.BigEndian.Uint64(id[:8])

	// 3. Shift right by 16 bits to discard the version/reserved bits and move
	//    the 48-bit millisecond timestamp to the least significant position.
	milliseconds := int64(timestamp64 >> 16)

	// 4. Convert milliseconds to time.
	// time.Unix takes seconds and nanoseconds, so we need to divide the milliseconds.
	seconds := milliseconds / 1000
	nanos := (milliseconds % 1000) * int64(time.Millisecond) // Remaining millis * 1,000,000 nanosec/millis

	return time.Unix(seconds, nanos), nil
}
