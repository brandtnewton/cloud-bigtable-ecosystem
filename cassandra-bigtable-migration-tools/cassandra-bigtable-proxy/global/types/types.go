/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package types

import (
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/constants"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translator"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"golang.org/x/exp/maps"
	"time"
)

// ColumnFamily a Bigtable Column Family
type ColumnFamily string

// ColumnName - a Cassandra column name
type ColumnName string

// ColumnQualifier - a Bigtable column qualifier
type ColumnQualifier string

// RowKey - a Bigtable row key
type RowKey string
type Placeholder string

// BigtableValue - a value serialized to bytes for Bigtable
type BigtableValue []byte

// GoValue - a plain Golang value
type GoValue any

type BigtableData struct {
	Family ColumnFamily
	Column ColumnQualifier
	Bytes  BigtableValue
}

type Column struct {
	Name         ColumnName
	ColumnFamily ColumnFamily
	CQLType      CqlDataType
	// todo remove this field because it's redundant - you can use PkPrecedence or KeyType to infer this
	IsPrimaryKey bool
	PkPrecedence int
	KeyType      string
	Metadata     message.ColumnMetadata
}

type BigtableColumn struct {
	Family ColumnFamily
	Column ColumnQualifier
}

type CreateColumn struct {
	Name     ColumnName
	Index    int32
	TypeInfo CqlDataType
}

type Condition struct {
	Column   *Column
	Operator constants.Operator
	// points to a placeholder
	ValuePlaceholder Placeholder
}

type WhereClause struct {
	Conditions []Condition
	Params     *QueryParameters
}

func (w *WhereClause) PushCondition(column *Column, op constants.Operator, dt CqlDataType) Placeholder {
	p := w.Params.PushParameter(column.Name, dt)
	w.Conditions = append(w.Conditions, Condition{
		Column:           column,
		Operator:         op,
		ValuePlaceholder: p,
	})
	return p
}

func NewWhereClause(params *QueryParameters) *WhereClause {
	return &WhereClause{
		Conditions: nil,
		Params:     params,
	}
}

type QueryParameters struct {
	keys []Placeholder
	// note: not all placeholders will be associated with a column - e.g. limit ?
	columnLookup map[ColumnName]Placeholder
	// might be different than the column - like if we're doing a "CONTAINS" on a list, this would be the element type
	types map[Placeholder]CqlDataType
}

// BigtableMutations holds the results from parseComplexOperations.
type BigtableMutations struct {
	RowKey                RowKey
	IfSpec                translator.IfSpec
	UsingTimestamp        *BoundTimestampInfo
	Data                  []*BigtableData
	DelColumnFamily       []ColumnFamily
	DelColumns            []*BigtableColumn
	Counters              []BigtableCounterOp
	SetIndexOps           []BigtableSetIndexOp
	DeleteListElementsOps []BigtableDeleteListElementsOp
}

type BigtableCounterOp struct {
	Family ColumnFamily
	Value  int64
}
type BigtableSetIndexOp struct {
	Family ColumnFamily
	Index  int
	Value  BigtableValue
}
type BigtableDeleteListElementsOp struct {
	Family ColumnFamily
	Values []BigtableValue
}

func NewQueryParameters() *QueryParameters {
	return &QueryParameters{
		keys:  nil,
		types: nil,
	}
}

func (q *QueryParameters) AllColumns() []ColumnName {
	return maps.Keys(q.columnLookup)
}
func (q *QueryParameters) AllKeys() []Placeholder {
	return q.keys
}

func (q *QueryParameters) Count() int {
	return len(q.keys)
}

func (q *QueryParameters) Index(p Placeholder) int {
	for i := range q.keys {
		if q.keys[i] == p {
			return i
		}
	}
	return -1
}

func (q *QueryParameters) Has(p Placeholder) bool {
	return q.Index(p) != -1
}

func (q *QueryParameters) GetParameter(i int) Placeholder {
	return q.keys[i]
}

func (q *QueryParameters) GetPlaceholderForColumn(c ColumnName) (Placeholder, bool) {
	p, ok := q.columnLookup[c]
	return p, ok
}

func (q *QueryParameters) PushParameter(c ColumnName, dataType CqlDataType) Placeholder {
	p := Placeholder(fmt.Sprintf("value%d", len(q.keys)))
	q.AddParameter(c, p, dataType)
	return p
}

func (q *QueryParameters) AddParameter(c ColumnName, p Placeholder, dataType CqlDataType) {
	q.keys = append(q.keys, p)
	q.types[p] = dataType
	q.columnLookup[c] = p
}

func (q *QueryParameters) AddParameterWithoutColumn(p Placeholder, dataType CqlDataType) {
	q.keys = append(q.keys, p)
	q.types[p] = dataType
}

const UsingTimePlaceholder Placeholder = "usingTimeValue"

func (q *QueryParameters) GetDataType(p Placeholder) CqlDataType {
	// assume you are passing in a valid placeholder
	d, _ := q.types[p]
	return d
}

type QueryParameterValues struct {
	params *QueryParameters
	values map[Placeholder]GoValue
}

func (q *QueryParameterValues) Params() *QueryParameters {
	return q.params
}

func NewQueryParameterValues(params *QueryParameters) *QueryParameterValues {
	return &QueryParameterValues{params: params, values: make(map[Placeholder]GoValue)}
}

func (q *QueryParameterValues) PushParameterAndValue(c ColumnName, dataType CqlDataType, value any) Placeholder {
	p := q.params.PushParameter(c, dataType)
	q.values[p] = value
	return p
}

func (q *QueryParameterValues) Has(p Placeholder) bool {
	return q.params.Has(p)
}

func (q *QueryParameterValues) SetValue(p Placeholder, value any) error {
	dt, ok := q.params.types[p]
	if !ok {
		return fmt.Errorf("no param type info for %s", p)
	}

	// ensure the correct type is being set - more for checking internal implementation rather than the user
	// todo only validate when running in strict mode
	err := utilities.ValidateGoType(value, dt)
	if err != nil {
		return err
	}

	// todo validate
	q.values[p] = value
	return nil
}

func (q *QueryParameterValues) GetValue(p Placeholder) (any, error) {
	if v, ok := q.values[p]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("no query param for %s", p)
}

func (q *QueryParameterValues) GetValueInt64(p Placeholder) (int64, error) {
	v, err := q.GetValue(p)
	if err != nil {
		return 0, err
	}
	intVal, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("query param is a %T, not an int64", v)
	}
	return intVal, nil
}

func (q *QueryParameterValues) GetValueSlice(p Placeholder) ([]GoValue, error) {
	v, err := q.GetValue(p)
	if err != nil {
		return nil, err
	}
	slice, ok := v.([]GoValue)
	if !ok {
		return nil, fmt.Errorf("query param is a %T, not a slice", v)
	}
	return slice, nil
}

func (q *QueryParameterValues) GetValueMap(p Placeholder) (map[GoValue]GoValue, error) {
	v, err := q.GetValue(p)
	if err != nil {
		return nil, err
	}
	slice, ok := v.(map[GoValue]GoValue)
	if !ok {
		return nil, fmt.Errorf("query param is a %T, not a map", v)
	}
	return slice, nil
}

func (q *QueryParameterValues) GetValueByColumn(c ColumnName) (any, error) {
	p, ok := q.params.columnLookup[c]
	if !ok {
		return nil, fmt.Errorf("no parameter assosiated with column %s", c)
	}
	if v, ok := q.values[p]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("no query param for %s", p)
}

func (q *QueryParameterValues) AsMap() map[string]any {
	var result = make(map[string]any)
	for placeholder, value := range q.values {
		result[string(placeholder)] = value
	}
	return result
}

// SelectedColumn describes a column that was selected as part of a query. It's
// an output of query translating, and is also used for response construction.
type SelectedColumn struct {
	// Name is the original value of the selected column, including functions. It
	// does not include the alias. e.g. "region" or "count(*)"
	Name   string
	IsFunc bool
	// IsAs is true if an alias is used
	IsAs      bool
	FuncName  string
	Alias     string
	MapKey    Placeholder
	ListIndex Placeholder
	// ColumnName is the name of the underlying column in a function, or map key
	// access. e.g. the column name of "max(price)" is "price"
	ColumnName        string
	KeyType           string
	IsWriteTimeColumn bool
}

type IntRowKeyEncodingType int

const (
	// BigEndianEncoding should not be used for new tables - this is only included for backwards compatability
	BigEndianEncoding IntRowKeyEncodingType = iota
	OrderedCodeEncoding
)

type BoundSelectColumn interface {
	Column() *Column
}

type BoundIndexColumn struct {
	column *Column
	Index  int64
}

func NewBoundIndexColumn(column *Column, index int64) *BoundIndexColumn {
	return &BoundIndexColumn{column: column, Index: index}
}

func (b *BoundIndexColumn) Column() *Column {
	return b.column
}

type BoundKeyColumn struct {
	column *Column
	Key    ColumnQualifier
}

func NewBoundKeyColumn(column *Column, key ColumnQualifier) *BoundKeyColumn {
	return &BoundKeyColumn{column: column, Key: key}
}

func (b *BoundKeyColumn) Column() *Column {
	return b.column
}

type BoundTimestampInfo struct {
	Timestamp         time.Time
	HasUsingTimestamp bool
}

type BoundDeleteQuery struct {
	RowKey  RowKey
	Columns []BoundSelectColumn
}
