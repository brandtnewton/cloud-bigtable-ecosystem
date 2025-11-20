package types

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

import (
	"cloud.google.com/go/bigtable"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
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

// BigtableValue - a value serialized to bytes for Bigtable
type BigtableValue []byte

// GoValue - a plain Golang value
type GoValue any

type TableName string
type Keyspace string

func (k Keyspace) IsSystemKeyspace() bool {
	return k == "system" || k == "system_schema" || k == "system_virtual_schema"
}

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

type IntRowKeyEncodingType int

const (
	// BigEndianEncoding should not be used for new tables - this is only included for backwards compatability
	BigEndianEncoding IntRowKeyEncodingType = iota
	OrderedCodeEncoding
)

type BigtableResultRow struct {
	Values []GoValue
}

type QueryType int

const (
	QueryTypeUnknown QueryType = iota
	QueryTypeSelect
	QueryTypeInsert
	QueryTypeUpdate
	QueryTypeDelete
	// ddl
	QueryTypeCreate
	QueryTypeAlter
	QueryTypeDrop
	QueryTypeTruncate
	QueryTypeUse
	QueryTypeDescribe
)

func (q QueryType) String() string {
	switch q {
	case QueryTypeSelect:
		return "select"
	case QueryTypeInsert:
		return "insert"
	case QueryTypeUpdate:
		return "update"
	case QueryTypeDelete:
		return "delete"
	case QueryTypeCreate:
		return "create"
	case QueryTypeAlter:
		return "alter"
	case QueryTypeDrop:
		return "drop"
	case QueryTypeTruncate:
		return "truncate"
	case QueryTypeUse:
		return "use"
	default:
		return "unknown"
	}
}

type IExecutableQuery interface {
	Keyspace() Keyspace
	Table() TableName
	QueryType() QueryType
	AsBulkMutation() (IBigtableMutation, bool)
	CqlQuery() string
	BigtableQuery() string
}
type IPreparedQuery interface {
	Keyspace() Keyspace
	Table() TableName
	CqlQuery() string
	QueryType() QueryType
	Parameters() *QueryParameters
	ResponseColumns() []*message.ColumnMetadata
	// SetBigtablePreparedQuery - only implemented for "select" queries for now because Bigtable SQL only supports reads
	SetBigtablePreparedQuery(s *bigtable.PreparedStatement)
	BigtableQuery() string
	IsIdempotent() bool
}

type IQueryTranslator interface {
	Translate(query *RawQuery, sessionKeyspace Keyspace, isPreparedQuery bool) (IPreparedQuery, IExecutableQuery, error)
	Bind(st IPreparedQuery, values []*primitive.Value, pv primitive.ProtocolVersion) (IExecutableQuery, error)
	QueryType() QueryType
}

type RawQuery struct {
	header          *frame.Header
	cql             string
	qt              QueryType
	sessionKeyspace Keyspace
	parser          *cql.CqlParser
	startTime       time.Time
}

func NewRawQuery(header *frame.Header, sessionKeyspace Keyspace, cql string, parser *cql.CqlParser, qt QueryType) *RawQuery {
	return NewRawQueryWithTime(header, sessionKeyspace, cql, parser, qt, time.Now().UTC())
}

func NewRawQueryWithTime(header *frame.Header, sessionKeyspace Keyspace, cql string, parser *cql.CqlParser, qt QueryType, t time.Time) *RawQuery {
	return &RawQuery{
		header:          header,
		sessionKeyspace: sessionKeyspace,
		cql:             cql,
		parser:          parser,
		qt:              qt,
		startTime:       t,
	}
}

func (r RawQuery) QueryType() QueryType {
	return r.qt
}

func (r RawQuery) RawCql() string {
	return r.cql
}

func (r RawQuery) Parser() *cql.CqlParser {
	return r.parser
}

func (r RawQuery) StartTime() time.Time {
	return r.startTime
}

func (r RawQuery) Header() *frame.Header {
	return r.header
}

type ICassandraClient interface {
	SetSessionKeyspace(k Keyspace)
}
