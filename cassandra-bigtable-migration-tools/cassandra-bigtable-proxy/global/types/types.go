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
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/message"
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
	p := w.Params.PushParameter(dt)
	w.Conditions = append(w.Conditions, Condition{
		Column:           column,
		Operator:         op,
		ValuePlaceholder: p,
	})
	return p
}

func NewWhereClause() *WhereClause {
	return &WhereClause{
		Conditions: nil,
		Params:     NewQueryParameters(),
	}
}

type QueryParameters struct {
	keys []Placeholder
	// might be different than the column - like if we're doing a "CONTAINS" on a list, this would be the element type
	types  map[Placeholder]CqlDataType
	values map[Placeholder]GoValue
}

func NewQueryParameters() *QueryParameters {
	return &QueryParameters{
		keys:   nil,
		types:  nil,
		values: make(map[Placeholder]any),
	}
}

func (q *QueryParameters) Copy() *QueryParameters {
	return &QueryParameters{
		keys:   q.keys,
		types:  q.types,
		values: make(map[Placeholder]any),
	}
}

func (q *QueryParameters) AllKeys() []Placeholder {
	return q.keys
}
func (q *QueryParameters) Count() int {
	return len(q.keys)
}
func (q *QueryParameters) GetParameter(i int) Placeholder {
	return q.keys[i]
}

func (q *QueryParameters) PushParameterAndValue(dataType CqlDataType, value any) Placeholder {
	p := q.PushParameter(dataType)
	q.values[p] = value
	return p
}
func (q *QueryParameters) SetValue(p Placeholder, value any) error {
	dt, ok := q.types[p]
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
}

func (q *QueryParameters) GetValue(p Placeholder) (any, error) {
	if v, ok := q.values[p]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("no query param for %s", p)
}

func (q *QueryParameters) PushParameter(dataType CqlDataType) Placeholder {
	p := Placeholder(fmt.Sprintf("value%d", len(q.keys)))
	q.AddParameter(p, dataType)
	return p
}

func (q *QueryParameters) AddParameter(p Placeholder, dataType CqlDataType) {
	q.keys = append(q.keys, p)
	q.types[p] = dataType
}

func (q *QueryParameters) GetDataType(p Placeholder) (CqlDataType, error) {
	d, ok := q.types[p]
	if !ok {
		return nil, fmt.Errorf("no datatype for placeholder %s", p)
	}
	return d, nil
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
	MapKey    string
	ListIndex string
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
