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
package compliance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
* CQLSH is a common way to interact with Cassandra, so we should make sure it works.
*
* All other tests in this package, using gocql are only able test prepared queries
* which uses significantly different code paths than unprepared queries.
*
* This is also the only way we can test system queries like "DESC TABLE...".
 */
func TestCqlshCrud(t *testing.T) {
	t.Parallel()

	_, err := scanCQLSHQuery(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES ('cqlsh_person', 80, 25)`)
	require.NoError(t, err)

	results, err := scanCQLSHQuery(`SELECT name, age, code FROM bigtabledevinstance.user_info where name='cqlsh_person' and age=80`)
	require.NoError(t, err)
	assert.Equal(t, []map[string]string{{"age": "80", "name": "cqlsh_person", "code": "25"}}, results)
}

func TestCqlshDesc(t *testing.T) {
	t.Parallel()

	got, err := runCQLSHDescribe(`desc keyspaces`)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"bigtabledevinstance", "cassandrakeyspace"}, got)
}

// todo 'describe tables'
// Keyspace keyspace1
// -------------------
// table1 table2...
// table12 table13...
//
// Keyspace keyspace2
// -------------------
// table1 table2...
// table12 table13...

// todo 'describe $keyspace.$table'
// CREATE TABLE $keyspace.$table...
// todo 'describe keyspace $keyspace'
// CREATE KEYSPACE $keyspace ...
//
// CREATE TABLE $keyspace.$table...
