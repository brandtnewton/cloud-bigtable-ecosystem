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
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Helper to parse RFC3339 timestamp strings used in the tests
func parseTime(t *testing.T, ts string) time.Time {
	t.Helper()
	parsedTime, err := time.Parse(time.RFC3339, ts)
	require.NoError(t, err, "Failed to parse timestamp string")
	return parsedTime.UTC()
}

// Helper to parse "YYYY-MM-DD HH:MM:SS" timestamp strings used in the tests.
func parseSimpleTime(t *testing.T, ts string) time.Time {
	t.Helper()
	parsedTime, err := time.Parse("2006-01-02 15:04:05", ts)
	require.NoError(t, err, "Failed to parse timestamp string")
	return parsedTime.UTC()
}

func uniqueTableName(prefix string) string {
	// add an underscore separator
	if len(prefix) != 0 && prefix[len(prefix)-1] != '_' {
		prefix = prefix + "_"
	}
	return prefix + strings.ReplaceAll(uuid.New().String(), "-", "_")
}

func getCreateTableDDL() []string {
	return []string{
		`
CREATE TABLE IF NOT EXISTS bigtabledevinstance.user_info (
	name text,
	age bigint,
	code int,
	credited double,
	balance float,
	is_active boolean,
	birth_date timestamp,
	zip_code bigint,
	extra_info map<text,text>,
	map_text_int map<text,int>,
	map_text_bigint map<text,bigint>,
	map_text_boolean map<text,boolean>,
	map_text_ts map<text,timestamp>,
	map_text_float map<text,float>,
	map_text_double map<text,double>,
	ts_text_map map<timestamp,text>,
	ts_boolean_map map<timestamp,boolean>,
	ts_float_map map<timestamp,float>,
	ts_double_map map<timestamp,double>,
	ts_bigint_map map<timestamp,bigint>,
	ts_ts_map map<timestamp,timestamp>,
	ts_int_map map<timestamp,int>,
	tags set<text>,
	set_boolean set<boolean>,
	set_int set<int>,
	set_bigint set<bigint>,
	set_float set<float>,
	set_double set<double>,
	set_timestamp set<timestamp>,
	list_text list<text>,
	list_int list<int>,
	list_bigint list<bigint>,
	list_float list<float>,
	list_double list<double>,
	list_boolean list<boolean>,
	list_timestamp list<timestamp>,
	PRIMARY KEY (age, name)
);
`,

		`
CREATE TABLE IF NOT EXISTS bigtabledevinstance.orders (
	user_id varchar,
	order_num int,
	name varchar,
	PRIMARY KEY (user_id, order_num)
);
`,

		`
CREATE TABLE IF NOT EXISTS bigtabledevinstance.aggregation_grouping_test (
	region text,
	category varchar,
	item_id int,
	sale_timestamp timestamp,
	quantity int,
	price float,
	discount double,
	revenue_bigint bigint,
	PRIMARY KEY (category, item_id)
);
`,

		`
CREATE TABLE IF NOT EXISTS bigtabledevinstance.multiple_int_keys (
	user_id bigint,
	order_num int,
	name varchar,
	PRIMARY KEY (user_id, order_num)
);
`,
	}
}
