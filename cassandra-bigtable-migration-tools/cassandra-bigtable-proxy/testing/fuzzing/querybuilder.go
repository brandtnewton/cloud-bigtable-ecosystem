/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, ProtocolVersion 2.0 (the "License"); you may not
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

package fuzzing

import (
	"fmt"
	"strings"
)

/**
 * Minimalist Query Builder to make fuzz testing very large tables easier. Not intended for production use or to handle complex queries.
 */

type InsertValue struct {
	column string
	value  interface{}
}

type InsertQuery struct {
	keyspace string
	table    string
	values   []InsertValue
}

func (iq InsertQuery) WithValue(name string, value interface{}) InsertQuery {
	iq.values = append(iq.values, InsertValue{
		column: name,
		value:  value,
	})
	return iq
}

func (iq InsertQuery) WithTable(keyspace string, name string) InsertQuery {
	iq.table = name
	iq.keyspace = keyspace
	return iq
}

func (iq InsertQuery) Build() (string, []interface{}) {

	var values []interface{}
	var params []string
	var columns []string
	for _, value := range iq.values {
		columns = append(columns, value.column)
		values = append(values, value.value)
		params = append(params, "?")
	}

	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)", iq.keyspace, iq.table, strings.Join(columns, ", "), strings.Join(params, ", "))

	return query, values
}

type QueryWhereClause struct {
	column string
	op     string
	value  interface{}
}

type SelectQueryColumn struct {
	name string
}

type SelectQuery struct {
	keyspace string
	table    string
	columns  []SelectQueryColumn
	where    []QueryWhereClause
}

func (sq SelectQuery) WithCol(name string) SelectQuery {
	sq.columns = append(sq.columns, SelectQueryColumn{
		name: name,
	})
	return sq
}

func (sq SelectQuery) WithWhere(name string, value interface{}) SelectQuery {
	sq.where = append(sq.where, QueryWhereClause{
		column: name,
		op:     "=",
		value:  value,
	})
	return sq
}

func (sq SelectQuery) WithTable(keyspace string, name string) SelectQuery {
	sq.table = name
	sq.keyspace = keyspace
	return sq
}

func (sq SelectQuery) Build() (string, []interface{}, SelectQuery) {

	var columns []string
	for _, value := range sq.columns {
		columns = append(columns, value.name)
	}
	var where []string
	var values []interface{}
	for _, clause := range sq.where {
		where = append(where, fmt.Sprintf("%s%s?", clause.column, clause.op))
		values = append(values, clause.value)
	}

	query := fmt.Sprintf("SELECT %s FROM %s.%s WHERE %s", strings.Join(columns, ", "), sq.keyspace, sq.table, strings.Join(where, " AND "))

	return query, values, sq
}
