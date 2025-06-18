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

package schema_setup

import (
	"context"
	"fmt"
	"os"
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/gocql/gocql"
	"github.com/ollionorg/cassandra-to-bigtable-proxy/testing/compliance/utility"
)

const (
	nodes = 1
)

func SetupBigtableInstance(project, instance, zone string) error {
	err := checkAndCreateInstance(project, instance, zone)
	return err
}

func SetupBigtableSchema(cqlSession *gocql.Session, schemaFilePath string) error {
	data, err := os.ReadFile(schemaFilePath)
	if err != nil {
		return fmt.Errorf("failed to read schema file: %v", err)
	}

	// expects each statement to be terminated by a semicolon
	stmts := strings.Split(string(data), ";")
	for i, stmt := range stmts {
		stmt = strings.ReplaceAll(strings.TrimSpace(stmt), "\n", "")
		if stmt == "" {
			continue
		}
		utility.LogInfo(fmt.Sprintf("Running setup query %d: '%s'", i, stmt))
		err = cqlSession.Query(stmt).Exec()
		if err != nil {
			return fmt.Errorf("failed to run setup query %d because %w", i, err)
		}
	}

	utility.LogInfo("Bigtable schema setup completed successfully!")
	return nil
}

// checkAndCreateInstance checks if the Bigtable instance exists and creates it if it doesn't
func checkAndCreateInstance(project, instance, zone string) error {
	ctx := context.Background()
	adminClient, err := bigtable.NewInstanceAdminClient(ctx, project)
	if err != nil {
		return fmt.Errorf("failed to create instance admin client: %v", err)
	}
	defer adminClient.Close()

	// Check if instance exists
	_, err = adminClient.InstanceInfo(ctx, instance)
	if err != nil {
		// Instance does not exist, create it
		utility.LogInfo(fmt.Sprintf("Bigtable instance '%s' does not exist. Attempting to create it...\n", instance))

		// Create instance configuration
		conf := bigtable.InstanceWithClustersConfig{
			InstanceID:  instance,
			DisplayName: instance,
			Clusters: []bigtable.ClusterConfig{
				{
					ClusterID: instance,
					Zone:      zone,
					NumNodes:  nodes,
				},
			},
		}

		// Create instance
		err = adminClient.CreateInstanceWithClusters(ctx, &conf)
		if err != nil {
			return fmt.Errorf("failed to create Bigtable instance '%s': %v", instance, err)
		}

		utility.LogInfo(fmt.Sprintf("Bigtable instance '%s' created successfully.\n", instance))
	} else {
		utility.LogInfo(fmt.Sprintf("Bigtable instance '%s' already exists.\n", instance))
	}

	return nil
}

// teardownBigtableInstance deletes the Bigtable instance to clean up resources.
func TeardownBigtableInstance(project, instance string) error {
	ctx := context.Background()
	adminClient, err := bigtable.NewInstanceAdminClient(ctx, project)
	if err != nil {
		return fmt.Errorf("failed to create instance admin client: %v", err)
	}
	defer adminClient.Close()

	err = adminClient.DeleteInstance(ctx, instance)
	if err != nil {
		utility.LogTestError(fmt.Sprintf("Failed to delete Bigtable instance '%s': %v", instance, err))
	} else {
		utility.LogInfo(fmt.Sprintf("Successfully deleted Bigtable instance '%s'.", instance))
	}
	return err
}
