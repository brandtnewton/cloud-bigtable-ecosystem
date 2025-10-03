package types

import (
	"fmt"

	"github.com/datastax/go-cassandra-native-protocol/datatype"
)

type CqlDataType interface {
	// String returns the canonical CQL string representation of the type.
	String() string

	DataType() datatype.DataType
	// isCDataType is an unexported marker method to ensure only types
	// from this package can implement the interface.
	isCDataType()

	IsCollection() bool
	IsFrozen() bool
	IsAnyFrozen() bool
}

// ScalarKind defines the specific kind of a scalar type.
type ScalarKind int

// Enumeration of all Cassandra scalar types.
const (
	ASCII ScalarKind = iota
	VARCHAR
	BIGINT
	BLOB
	BOOLEAN
	COUNTER
	DATE
	DECIMAL
	DOUBLE
	FLOAT
	INET
	INT
	SMALLINT
	TEXT // Also used for VARCHAR
	TIME
	TIMESTAMP
	TIMEUUID
	TINYINT
	UUID
	VARINT
)

// ScalarType represents a primitive, single-value Cassandra type.
type ScalarType struct {
	kind ScalarKind
	dt   datatype.DataType
	name string
}

func (s ScalarType) IsAnyFrozen() bool {
	return false
}

func (s ScalarType) Kind() ScalarKind {
	return s.kind
}

func (s ScalarType) DataType() datatype.DataType {
	return s.dt
}

func (s ScalarType) isCDataType() {}

func (s ScalarType) String() string {
	return s.name
}

func (s ScalarType) IsCollection() bool {
	return false
}

func (s ScalarType) IsFrozen() bool {
	return false
}

// Pre-defined constants for common scalar types for convenience.
var (
	TypeAscii     CqlDataType = ScalarType{name: "ascii", kind: ASCII, dt: datatype.Varchar}
	TypeVarchar   CqlDataType = ScalarType{name: "varchar", kind: VARCHAR, dt: datatype.Varchar}
	TypeBigint    CqlDataType = ScalarType{name: "bigint", kind: BIGINT, dt: datatype.Bigint}
	TypeBlob      CqlDataType = ScalarType{name: "blob", kind: BLOB, dt: datatype.Blob}
	TypeBoolean   CqlDataType = ScalarType{name: "boolean", kind: BOOLEAN, dt: datatype.Boolean}
	TypeCounter   CqlDataType = ScalarType{name: "counter", kind: COUNTER, dt: datatype.Counter}
	TypeDate      CqlDataType = ScalarType{name: "date", kind: DATE, dt: datatype.Date}
	TypeDecimal   CqlDataType = ScalarType{name: "decimal", kind: DECIMAL, dt: datatype.Decimal}
	TypeDouble    CqlDataType = ScalarType{name: "double", kind: DOUBLE, dt: datatype.Double}
	TypeFloat     CqlDataType = ScalarType{name: "float", kind: FLOAT, dt: datatype.Float}
	TypeInet      CqlDataType = ScalarType{name: "inet", kind: INET, dt: datatype.Inet}
	TypeInt       CqlDataType = ScalarType{name: "int", kind: INT, dt: datatype.Int}
	TypeSmallint  CqlDataType = ScalarType{name: "smallint", kind: SMALLINT, dt: datatype.Smallint}
	TypeText      CqlDataType = ScalarType{name: "text", kind: TEXT, dt: datatype.Varchar}
	TypeTime      CqlDataType = ScalarType{name: "time", kind: TIME, dt: datatype.Time}
	TypeTimestamp CqlDataType = ScalarType{name: "timestamp", kind: TIMESTAMP, dt: datatype.Timestamp}
	TypeTimeuuid  CqlDataType = ScalarType{name: "timeuuid", kind: TIMEUUID, dt: datatype.Timeuuid}
	TypeTinyint   CqlDataType = ScalarType{name: "tinyint", kind: TINYINT, dt: datatype.Tinyint}
	TypeUuid      CqlDataType = ScalarType{name: "uuid", kind: UUID, dt: datatype.Uuid}
	TypeVarint    CqlDataType = ScalarType{name: "varint", kind: VARINT, dt: datatype.Varint}
)

type MapType struct {
	keyType   CqlDataType
	valueType CqlDataType
	dt        datatype.DataType
}

func (m MapType) IsAnyFrozen() bool {
	return m.keyType.IsAnyFrozen() || m.valueType.IsAnyFrozen()
}

func (m MapType) KeyType() CqlDataType {
	return m.keyType
}

func (m MapType) ValueType() CqlDataType {
	return m.valueType
}

func NewMapType(keyType CqlDataType, valueType CqlDataType) CqlDataType {
	return MapType{keyType: keyType, valueType: valueType, dt: datatype.NewMapType(keyType.DataType(), valueType.DataType())}
}

func (m MapType) DataType() datatype.DataType {
	return m.dt
}

func (m MapType) isCDataType() {}

func (m MapType) String() string {
	return fmt.Sprintf("map<%s, %s>", m.keyType.String(), m.valueType.String())
}

func (m MapType) IsCollection() bool {
	return true
}

func (m MapType) IsFrozen() bool {
	return false
}

// ListType represents a Cassandra list<elementType>.
type ListType struct {
	elementType CqlDataType
	dt          datatype.DataType
}

func (l ListType) IsAnyFrozen() bool {
	return l.elementType.IsAnyFrozen()
}

func (l ListType) ElementType() CqlDataType {
	return l.elementType
}

func NewListType(elementType CqlDataType) CqlDataType {
	return ListType{elementType: elementType, dt: datatype.NewListType(elementType.DataType())}
}

func (l ListType) DataType() datatype.DataType {
	return l.dt
}

func (l ListType) isCDataType() {}

func (l ListType) String() string {
	return fmt.Sprintf("list<%s>", l.elementType.String())
}

func (l ListType) IsCollection() bool {
	return true
}

func (l ListType) IsFrozen() bool {
	return false
}

// SetType represents a Cassandra set<elementType>.
type SetType struct {
	elementType CqlDataType
	dt          datatype.DataType
}

func (s SetType) IsAnyFrozen() bool {
	return s.elementType.IsAnyFrozen()
}

func NewSetType(elementType CqlDataType) CqlDataType {
	return SetType{elementType: elementType, dt: datatype.NewSetType(elementType.DataType())}
}

func (s SetType) DataType() datatype.DataType {
	return s.dt
}

func (s SetType) ElementType() CqlDataType {
	return s.elementType
}

func (s SetType) isCDataType() {}

func (s SetType) String() string {
	return fmt.Sprintf("set<%s>", s.elementType.String())
}

func (s SetType) IsCollection() bool {
	return true
}

func (s SetType) IsFrozen() bool {
	return false
}

type FrozenType struct {
	innerType CqlDataType
}

func (f FrozenType) IsAnyFrozen() bool {
	return true
}

func (f FrozenType) InnerType() CqlDataType {
	return f.innerType
}

func (f FrozenType) IsCollection() bool {
	return f.innerType.IsCollection()
}

func (f FrozenType) IsFrozen() bool {
	return true
}

func (f FrozenType) DataType() datatype.DataType {
	return f.innerType.DataType()
}

func (f FrozenType) isCDataType() {}

func (f FrozenType) String() string {
	return fmt.Sprintf("frozen<%s>", f.innerType.String())
}

func NewFrozenType(inner CqlDataType) CqlDataType {
	return &FrozenType{innerType: inner}
}
