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
	"log"
	"os"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

type TestTarget int

const (
	TestTargetProxy TestTarget = iota
	TestTargetCassandra
)

func (target TestTarget) String() string {
	switch target {
	case TestTargetProxy:
		return "proxy"
	case TestTargetCassandra:
		return "cassandra"
	}
	return "unknown"
}

// session is a global variable to hold the database session.
var session *gocql.Session

var testTarget = TestTargetProxy

// TestMain sets up the database connection and schema before running tests,
// and tears it down afterward.
func TestMain(m *testing.M) {
	// --- Setup ---
	cluster := gocql.NewCluster("127.0.0.1") // Assumes Cassandra is running locally
	cluster.Timeout = 20 * time.Second

	var err error
	// Connect to the cluster to create the keyspace
	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatalf("could not connect to cassandra: %v", err)
	}

	session, err = cluster.CreateSession()
	if err != nil {
		log.Fatalf("could not connect to keyspace bigtabledevinstance: %v", err)
	}

	// Create the user_info table with a schema inferred from the test queries
	err = session.Query(`CREATE TABLE IF NOT EXISTS bigtabledevinstance.user_info (
		name text,
		age bigint,
		code int,
		credited double,
		PRIMARY KEY (name, age)
	)`).Exec()
	if err != nil {
		log.Fatalf("could not create table: %v", err)
	}

	// --- Run Tests ---
	exitCode := m.Run()

	session.Close()

	os.Exit(exitCode)
}
