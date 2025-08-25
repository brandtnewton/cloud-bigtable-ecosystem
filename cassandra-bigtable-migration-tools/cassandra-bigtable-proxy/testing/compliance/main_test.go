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
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
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

func getTestTarget() TestTarget {
	testTarget := os.Getenv("COMPLIANCE_TEST_TARGET")
	switch testTarget {
	case "":
		println("no test target specified, defaulting to proxy")
		return TestTargetProxy
	case "proxy":
		return TestTargetProxy
	case "cassandra":
		return TestTargetCassandra
	default:
		panic(fmt.Sprintf("unrecognized test target: %s", testTarget))
	}
}

var testTarget = getTestTarget()

func createSession(keyspace string) (*gocql.Session, error) {
	cluster := gocql.NewCluster("127.0.0.1") // Assumes Cassandra is running locally
	cluster.Timeout = 20 * time.Second
	if keyspace != "" {
		cluster.Keyspace = keyspace
	}

	return cluster.CreateSession()
}

// TestMain sets up the database connection and schema before running tests,
// and tears it down afterward.
func TestMain(m *testing.M) {

	fmt.Printf("running against test target '%s'\n", testTarget.String())

	setUpTests()

	// --- Run Tests ---
	exitCode := m.Run()

	cleanUpTests()

	os.Exit(exitCode)
}

// note: we don't destroy any tables here because recreating every table, for every test would cause Bigtable to rate limit us.
func cleanUpTests() {
	session.Close()
}

func setUpTests() {
	var err error
	session, err = createSession("bigtabledevinstance")
	if err != nil {
		log.Fatalf("could not connect to the session: %v", err)
	}

	log.Println("Creating test tables...")
	for i, stmt := range getSchemas() {
		log.Println(fmt.Sprintf("Running create table statement: '%d'...", i))
		err = session.Query(stmt).Exec()
		if err != nil {
			log.Fatalf("could not create table: %v", err)
		}
	}
	log.Println("All test tables successfully created!")
}

func cleanupTable(t *testing.T, table string) {
	err := session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s;", table)).Exec()
	assert.NoError(t, err, "testing cleanup of table %s failed", table)
}
