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

	_, err := cqlshExec(`INSERT INTO bigtabledevinstance.user_info (name, age, code) VALUES ('cqlsh_person', 80, 25)`)
	require.NoError(t, err)

	results, err := cqlshScanToMap(`SELECT name, age, code FROM bigtabledevinstance.user_info WHERE name='cqlsh_person' AND age=80`)
	require.NoError(t, err)
	assert.Equal(t, []map[string]string{{"age": "80", "name": "cqlsh_person", "code": "25"}}, results)

	_, err = cqlshExec(`UPDATE bigtabledevinstance.user_info SET code=32 WHERE name='cqlsh_person' AND age=80`)

	results, err = cqlshScanToMap(`SELECT name, age, code FROM bigtabledevinstance.user_info WHERE name='cqlsh_person' AND age=80`)
	require.NoError(t, err)
	assert.Equal(t, []map[string]string{{"age": "80", "name": "cqlsh_person", "code": "32"}}, results)

	_, err = cqlshExec(`DELETE FROM bigtabledevinstance.user_info WHERE name='cqlsh_person' AND age=80`)

	results, err = cqlshScanToMap(`SELECT * FROM bigtabledevinstance.user_info WHERE name='cqlsh_person' AND age=80`)
	require.NoError(t, err)
	assert.Empty(t, results)
}

func TestCqlshError(t *testing.T) {
	t.Parallel()

	_, err := cqlshScanToMap(`INSERT INTO bigtabledevinstance.no_such_table (name, age, code) VALUES ('foo', 1, 2)`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "table no_such_table does not exist")
}

func TestCqlshCreateTable(t *testing.T) {
	t.Parallel()

	_, err := cqlshExec(`DROP TABLE IF EXISTS bigtabledevinstance.cqlsh_create_table_test`)
	require.NoError(t, err)

	_, err = cqlshExec(`CREATE TABLE bigtabledevinstance.cqlsh_create_table_test (id TEXT PRIMARY KEY, username TEXT, is_admin BOOLEAN); INSERT INTO bigtabledevinstance.cqlsh_create_table_test (id, username, is_admin) VALUES ('u1', 'admin', false)`)
	require.NoError(t, err)

	results, err := cqlshScanToMap(`SELECT id AS i, username AS u, is_admin FROM bigtabledevinstance.cqlsh_create_table_test WHERE id='u1'`)
	assert.Equal(t, []map[string]string{{"i": "u1", "u": "admin", "is_admin": "False"}}, results)
}

func TestCqlshUseKeyspace(t *testing.T) {
	t.Parallel()

	// make sure queries with no keyspace fail because it's ambiguous
	_, err := cqlshExec(`SELECT * FROM user_info LIMIT 1`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid input parameters found for keyspace")

	// setting the keyspace should allow subsequent queries to omit keyspace because it's now set on the client session
	_, err = cqlshExec(`USE bigtabledevinstance; SELECT * FROM user_info LIMIT 1`)
	require.NoError(t, err)
}

func TestCqlshDesc(t *testing.T) {
	t.Parallel()

	got, err := cqlshDescribe(`desc keyspaces`)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"bigtabledevinstance", "cassandrakeyspace"}, got)
}

func TestCqlshDescTables(t *testing.T) {
	result, err := cqlshExec("DESCRIBE TABLES")
	require.NoError(t, err)

	assert.Contains(t, result, "Keyspace bigtabledevinstance\n-----------------\n")
}

func TestCqlshDescTable(t *testing.T) {
	result, err := cqlshExec("DESCRIBE TABLE bigtabledevinstance.multiple_int_keys;")
	require.NoError(t, err)

	assert.Contains(t, result, "CREATE TABLE bigtabledevinstance.multiple_int_keys (\n\tuser_id bigint,\n\torder_num int,\n\tname varchar,\n\tPRIMARY KEY (user_id, order_num)\n);")
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
