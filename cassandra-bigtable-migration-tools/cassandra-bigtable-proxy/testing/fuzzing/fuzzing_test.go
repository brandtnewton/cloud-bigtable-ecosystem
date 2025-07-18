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

package fuzzing

import (
	"fmt"
	"math"
	"os"
	"testing"
	"unicode/utf8"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

var session *gocql.Session

func TestMain(m *testing.M) {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Port = 9042
	cluster.Keyspace = "bigtabledevinstance"
	cluster.ProtoVersion = 4

	var err error
	session, err = cluster.CreateSession()

	if err != nil {
		panic(err)
	}

	defer session.Close()

	var tablesSql = []string{
		// this table schema is to fuzz test all row key types. There are 2 varchar types because 1 is random (not fuzz generated) to ensure that fuzz workers don't collide row keys)
		"CREATE TABLE IF NOT EXISTS bigtabledevinstance.fuzztestkeys (id varchar, str_key varchar, int_key int, bigint_key bigint, name varchar, PRIMARY KEY (id, str_key, int_key, bigint_key));",
		"CREATE TABLE IF NOT EXISTS bigtabledevinstance.fuzztestcolumns (id text PRIMARY KEY, name text, code int, credited double, balance float, is_active boolean, birth_date timestamp, zip_code bigint, extra_info map<text,text>, map_text_int map<text,int>, map_text_bigint map<text,bigint>, map_text_boolean map<text,boolean>, map_text_ts map<text,timestamp>, map_text_float map<text,float>, map_text_double map<text,double>, ts_text_map map<timestamp,text>, ts_boolean_map map<timestamp,boolean>, ts_float_map map<timestamp,float>, ts_double_map map<timestamp,double>, ts_bigint_map map<timestamp,bigint>, ts_ts_map map<timestamp,timestamp>, ts_int_map map<timestamp,int>, tags set<text>, set_boolean set<boolean>, set_int set<int>, set_bigint set<bigint>, set_float set<float>, set_double set<double>, set_timestamp set<timestamp>, list_text list<text>, list_int list<int>, list_bigint list<bigint>, list_float list<float>, list_double list<double>, list_boolean list<boolean>, list_timestamp list<timestamp>);",
	}

	for i, tableSql := range tablesSql {
		err = session.Query(tableSql).Exec()
		if err != nil {
			panic(fmt.Errorf("error applying create table sql %d: %w", i, err))
		}
	}

	code := m.Run()

	os.Exit(code)
}

func FuzzColumns(f *testing.F) {
	f.Add("sdf#%#!!/,.m)", int32(0), int64(0), float32(1.1231231), float64(2.2087076), true)
	f.Add("sdf", int32(math.MinInt32), int64(math.MinInt64), float32(-1), float64(-1), false)
	f.Add("foo@min", int32(math.MinInt32), int64(math.MinInt64), float32(-5353.4521), float64(-1342.123123115), true)
	f.Fuzz(func(t *testing.T, s string, i int32, l int64, fl float32, d float64, b bool) {
		id := uuid.New().String()

		// todo resume testing empty strings
		if s == "" {
			return
		}

		iq, iv := InsertQuery{}.WithTable("bigtabledevinstance", "fuzztestcolumns").
			WithValue("id", id).
			WithValue("name", s).
			WithValue("code", i).
			WithValue("credited", d).
			WithValue("balance", fl).
			WithValue("is_active", b).
			WithValue("birth_date", l).
			WithValue("zip_code", l).
			WithValue("extra_info", map[string]string{s: s}).
			WithValue("map_text_int", map[string]int32{s: i}).
			WithValue("map_text_bigint", map[string]int64{s: l}).
			WithValue("map_text_boolean", map[string]bool{s: b}).
			WithValue("map_text_ts", map[string]int64{s: l}).
			WithValue("map_text_float", map[string]float32{s: fl}).
			WithValue("map_text_double", map[string]float64{s: d}).
			WithValue("ts_text_map", map[int64]string{l: s}).
			WithValue("ts_boolean_map", map[int64]bool{l: b}).
			WithValue("ts_float_map", map[int64]float32{l: fl}).
			WithValue("ts_double_map", map[int64]float64{l: d}).
			WithValue("ts_bigint_map", map[int64]int64{l: l}).
			WithValue("ts_ts_map", map[int64]int64{l: l}).
			WithValue("ts_int_map", map[int64]int32{l: i}).
			WithValue("tags", []string{s}).
			WithValue("set_boolean", []bool{b}).
			WithValue("set_int", []int32{i}).
			WithValue("set_bigint", []int64{l}).
			WithValue("set_float", []float32{fl}).
			WithValue("set_double", []float64{d}).
			WithValue("set_timestamp", []int64{l}).
			WithValue("list_text", []string{s}).
			WithValue("list_int", []int32{i}).
			WithValue("list_bigint", []int64{l}).
			WithValue("list_float", []float32{fl}).
			WithValue("list_double", []float64{d}).
			WithValue("list_boolean", []bool{b}).
			WithValue("list_timestamp", []int64{l}).
			Build()

		err := session.Query(iq, iv...).Exec()

		if !utf8.Valid([]byte(s)) {
			assert.Error(t, err)
			return
		}

		assert.NoError(t, err)

		var (
			scanCount         int
			gotId             string
			gotName           string
			gotCode           int32
			gotCredited       float64
			gotBalance        float32
			gotIsActive       bool
			gotBirthDate      int64
			gotZipCode        int64
			gotExtraInfo      map[string]string
			gotMapTextInt     map[string]int32
			gotMapTextBigint  map[string]int64
			gotMapTextBoolean map[string]bool
			gotMapTextTs      map[string]int64
			gotMapTextFloat   map[string]float32
			gotMapTextDouble  map[string]float64
			gotTsTextMap      map[int64]string
			gotTsBooleanMap   map[int64]bool
			gotTsFloatMap     map[int64]float32
			gotTsDoubleMap    map[int64]float64
			gotTsBigintMap    map[int64]int64
			gotTsTsMap        map[int64]int64
			gotTsIntMap       map[int64]int32
			gotTags           []string
			gotSetBoolean     []bool
			gotSetInt         []int32
			gotSetBigint      []int64
			gotSetFloat       []float32
			gotSetDouble      []float64
			gotSetTimestamp   []int64
			gotListText       []string
			gotListInt        []int32
			gotListBigint     []int64
			gotListFloat      []float32
			gotListDouble     []float64
			gotListBoolean    []bool
			gotListTimestamp  []int64
		)

		sq, sv, _ := SelectQuery{}.WithTable("bigtabledevinstance", "fuzztestcolumns").
			WithWhere("id", id).
			WithCol("id").
			WithCol("name").
			WithCol("code").
			WithCol("credited").
			WithCol("balance").
			WithCol("is_active").
			WithCol("birth_date").
			WithCol("zip_code").
			WithCol("extra_info").
			WithCol("map_text_int").
			WithCol("map_text_bigint").
			WithCol("map_text_boolean").
			WithCol("map_text_ts").
			WithCol("map_text_float").
			WithCol("map_text_double").
			WithCol("ts_text_map").
			WithCol("ts_boolean_map").
			WithCol("ts_float_map").
			WithCol("ts_double_map").
			WithCol("ts_bigint_map").
			WithCol("ts_ts_map").
			WithCol("ts_int_map").
			WithCol("tags").
			WithCol("set_boolean").
			WithCol("set_int").
			WithCol("set_bigint").
			WithCol("set_float").
			WithCol("set_double").
			WithCol("set_timestamp").
			WithCol("list_text").
			WithCol("list_int").
			WithCol("list_bigint").
			WithCol("list_float").
			WithCol("list_double").
			WithCol("list_boolean").
			WithCol("list_timestamp").
			Build()

		scanner := session.Query(sq, sv...).Iter().Scanner()

		for scanner.Next() {
			scanCount++
			err = scanner.Scan(
				&gotId,
				&gotName,
				&gotCode,
				&gotCredited,
				&gotBalance,
				&gotIsActive,
				&gotBirthDate,
				&gotZipCode,
				&gotExtraInfo,
				&gotMapTextInt,
				&gotMapTextBigint,
				&gotMapTextBoolean,
				&gotMapTextTs,
				&gotMapTextFloat,
				&gotMapTextDouble,
				&gotTsTextMap,
				&gotTsBooleanMap,
				&gotTsFloatMap,
				&gotTsDoubleMap,
				&gotTsBigintMap,
				&gotTsTsMap,
				&gotTsIntMap,
				&gotTags,
				&gotSetBoolean,
				&gotSetInt,
				&gotSetBigint,
				&gotSetFloat,
				&gotSetDouble,
				&gotSetTimestamp,
				&gotListText,
				&gotListInt,
				&gotListBigint,
				&gotListFloat,
				&gotListDouble,
				&gotListBoolean,
				&gotListTimestamp,
			)
			assert.NoError(t, err)
		}

		err = scanner.Err()
		assert.NoError(t, err)
		assert.Equal(t, 1, scanCount)
		assert.Equal(t, id, gotId)
		assert.Equal(t, s, gotName)
		assert.Equal(t, i, gotCode)
		assert.Equal(t, d, gotCredited)
		assert.Equal(t, fl, gotBalance)
		assert.Equal(t, b, gotIsActive)
		assert.Equal(t, l, gotBirthDate)
		assert.Equal(t, l, gotZipCode)
		assert.Equal(t, map[string]string{s: s}, gotExtraInfo)
		assert.Equal(t, map[string]int32{s: i}, gotMapTextInt)
		assert.Equal(t, map[string]int64{s: l}, gotMapTextBigint)
		assert.Equal(t, map[string]bool{s: b}, gotMapTextBoolean)
		assert.Equal(t, map[string]int64{s: l}, gotMapTextTs)
		assert.Equal(t, map[string]float32{s: fl}, gotMapTextFloat)
		assert.Equal(t, map[string]float64{s: d}, gotMapTextDouble)
		assert.Equal(t, map[int64]string{l: s}, gotTsTextMap)
		assert.Equal(t, map[int64]bool{l: b}, gotTsBooleanMap)
		assert.Equal(t, map[int64]float32{l: fl}, gotTsFloatMap)
		assert.Equal(t, map[int64]float64{l: d}, gotTsDoubleMap)
		assert.Equal(t, map[int64]int64{l: l}, gotTsBigintMap)
		assert.Equal(t, map[int64]int64{l: l}, gotTsTsMap)
		assert.Equal(t, map[int64]int32{l: i}, gotTsIntMap)
		assert.Equal(t, []string{s}, gotTags)
		assert.Equal(t, []bool{b}, gotSetBoolean)
		assert.Equal(t, []int32{i}, gotSetInt)
		assert.Equal(t, []int64{l}, gotSetBigint)
		assert.Equal(t, []float32{fl}, gotSetFloat)
		assert.Equal(t, []float64{d}, gotSetDouble)
		assert.Equal(t, []int64{l}, gotSetTimestamp)
		assert.Equal(t, []string{s}, gotListText)
		assert.Equal(t, []int32{i}, gotListInt)
		assert.Equal(t, []int64{l}, gotListBigint)
		assert.Equal(t, []float32{fl}, gotListFloat)
		assert.Equal(t, []float64{d}, gotListDouble)
		assert.Equal(t, []bool{b}, gotListBoolean)
		assert.Equal(t, []int64{l}, gotListTimestamp)
	})
}

func FuzzRowKeys(f *testing.F) {
	f.Add("bob nil", int32(0), int64(0))
	f.Add("foo@min", int32(math.MinInt32), int64(math.MinInt64))
	f.Add("bob max!", int32(math.MaxInt32), int64(math.MaxInt64))
	f.Add("bob", int32(1), int64(1))
	f.Fuzz(func(t *testing.T, strKey string, intKey int32, bigIntKey int64) {
		// the id column is to ensure unique row keys between all parallel fuzz test workers
		id := uuid.New().String()
		name := fmt.Sprintf("name%d", rand.Int63())
		err := session.Query("insert into bigtabledevinstance.fuzztestkeys (id, str_key, int_key, bigint_key, name) values (?, ?, ?, ?, ?)", id, strKey, intKey, bigIntKey, name).Exec()

		assert.NoError(t, err)
		scanner := session.Query("select id, str_key, int_key, bigint_key, name from bigtabledevinstance.fuzztestkeys where id=? AND str_key=? AND int_key=? AND bigint_key=?", id, strKey, intKey, bigIntKey).Iter().Scanner()

		var (
			gotId        string
			gotStrKey    string
			gotIntKey    int32
			gotBigIntKey int64
			gotName      string
			scanCount    int
		)
		for scanner.Next() {
			scanCount++
			err = scanner.Scan(&gotId, &gotStrKey, &gotIntKey, &gotBigIntKey, &gotName)
			assert.NoError(t, err)
		}
		err = scanner.Err()

		assert.NoError(t, err)
		assert.Equal(t, 1, scanCount)
		assert.Equal(t, id, gotId)
		assert.Equal(t, strKey, gotStrKey)
		assert.Equal(t, intKey, gotIntKey)
		assert.Equal(t, bigIntKey, gotBigIntKey)
		assert.Equal(t, name, gotName)

		assert.NoError(t, err)
		name2 := fmt.Sprintf("name2%d", rand.Int63())
		err = session.Query("update bigtabledevinstance.fuzztestkeys set name=? where id=? AND str_key=? AND int_key=? AND bigint_key=?", name2, id, strKey, intKey, bigIntKey).Exec()
		assert.NoError(t, err)

		scanner = session.Query("select id, str_key, int_key, bigint_key, name from bigtabledevinstance.fuzztestkeys where id=? AND str_key=? AND int_key=? AND bigint_key=?", id, strKey, intKey, bigIntKey).Iter().Scanner()
		scanCount = 0
		for scanner.Next() {
			scanCount++
			err = scanner.Scan(&gotId, &gotStrKey, &gotIntKey, &gotBigIntKey, &gotName)
			assert.NoError(t, err)
		}
		err = scanner.Err()

		assert.NoError(t, err)
		assert.Equal(t, 1, scanCount)
		assert.Equal(t, id, gotId)
		assert.Equal(t, strKey, gotStrKey)
		assert.Equal(t, intKey, gotIntKey)
		assert.Equal(t, bigIntKey, gotBigIntKey)
		assert.Equal(t, name2, gotName)

		err = session.Query("delete from bigtabledevinstance.fuzztestkeys where id=? AND str_key=? AND int_key=? AND bigint_key=?", id, strKey, intKey, bigIntKey).Exec()
		assert.NoError(t, err)

		scanner = session.Query("select id, str_key, int_key, bigint_key, name from bigtabledevinstance.fuzztestkeys where id=? AND str_key=? AND int_key=? AND bigint_key=?", id, strKey, intKey, bigIntKey).Iter().Scanner()
		scanCount = 0
		for scanner.Next() {
			scanCount++
			err = scanner.Scan(&gotId, &gotStrKey, &gotIntKey, &gotBigIntKey, &gotName)
			assert.NoError(t, err)
		}
		err = scanner.Err()
		assert.NoError(t, err)
		assert.Equal(t, 0, scanCount)

	})
}
