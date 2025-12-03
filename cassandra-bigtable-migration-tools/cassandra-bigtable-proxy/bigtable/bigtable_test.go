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

package bigtableclient

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/testing/bt_server_wrapper"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/api/option"
)

var bts *bt_server_wrapper.BigtableTestServer
var bigtableConfig = types.BigtableConfig{
	ProjectID:          "my-project",
	Instances:          map[types.Keyspace]*types.InstanceMapping{"ks1": {InstanceId: "bt1", Keyspace: "ks1", AppProfileID: "default"}},
	SchemaMappingTable: "schema_mapping",
	Session: &types.Session{
		GrpcChannels: 3,
	},
	DefaultColumnFamily:      "cf1",
	DefaultIntRowKeyEncoding: types.OrderedCodeEncoding,
}

var mdStore *schemaMapping.MetadataStore
var btc *BigtableAdapter

const ks1 = types.Keyspace("ks1")
const tableName = types.TableName("test_table")

func TestMain(m *testing.M) {
	bts = bt_server_wrapper.NewBigtableTestServer(bigtableConfig)
	bts.SetUp(1)

	mdStore = schemaMapping.NewMetadataStore(zap.NewNop(), bts.Clients(), &bigtableConfig)
	btc = NewBigtableClient(bts.Clients(), zap.NewNop(), &bigtableConfig, mdStore)

	ctx := context.Background()
	err := mdStore.CreateTable(ctx, types.NewCreateTableStatementMap(ks1, tableName, "ignored", false, []types.CreateColumn{
		{
			Name:     "pk1",
			Index:    0,
			TypeInfo: types.TypeText,
		},
		{
			Name:     "name",
			Index:    1,
			TypeInfo: types.TypeText,
		},
	}, []types.CreateTablePrimaryKeyConfig{
		{
			Name:    "pk1",
			KeyType: types.KeyTypePartition,
		},
	}, types.OrderedCodeEncoding))
	if err != nil {
		fmt.Printf("failed to setup table: %v", err)
		os.Exit(1)
	}

	defer bts.Close()
	os.Exit(m.Run())
}

func TestInsertRow(t *testing.T) {
	adminClient, err := bts.Clients().GetAdmin("bt1")
	require.NoError(t, err)

	// Create table
	err = adminClient.CreateTable(t.Context(), "test-table-insert")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	err = adminClient.CreateColumnFamily(t.Context(), "test-table-insert", "cf1")
	if err != nil {
		t.Fatalf("Failed to create column family: %v", err)
	}

	mdStore := schemaMapping.NewMetadataStore(zap.NewNop(), bts.Clients(), &bigtableConfig)
	btc := NewBigtableClient(bts.Clients(), zap.NewNop(), &bigtableConfig, mdStore)

	tests := []struct {
		name          string
		data          *types.BigtableWriteMutation
		keyspace      types.Keyspace
		table         types.TableName
		rowKey        types.RowKey
		ifNotExists   bool
		mutations     []types.IBigtableMutationOp
		expectedError string
		expectedValue *message.RowsResult
	}{
		{
			name:     "insert row",
			keyspace: "ks1",
			table:    "test-table-insert",
			rowKey:   "row1",
			mutations: []types.IBigtableMutationOp{
				types.NewWriteCellOp("cf1", "col1", []byte("value1")),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mut := types.NewBigtableWriteMutation(tt.keyspace, tt.table, "ignored", types.IfSpec{IfNotExists: tt.ifNotExists}, types.QueryTypeInsert, tt.rowKey)
			mut.AddMutations(tt.mutations...)
			_, err := btc.InsertRow(t.Context(), tt.data)
			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				return
			}
			assert.NoError(t, err)
			client, err := btc.clients.GetClient(tt.keyspace)
			require.NoError(t, err)
			table := client.Open(string(tt.table))
			row, err := table.ReadRow(t.Context(), string(tt.rowKey))
			require.NoError(t, err)
			assert.Equal(t, tt.expectedValue, row)
		})
	}
}

func TestDeleteRow(t *testing.T) {
	rowKey := types.RowKey("test-row")
	// Insert initial row
	initialData := types.NewBigtableWriteMutation(ks1, tableName, "ignored", types.IfSpec{}, types.QueryTypeInsert, rowKey)
	initialData.AddMutations(types.NewWriteCellOp("cf1", "name", []byte("initial value")))

	_, err := btc.InsertRow(t.Context(), initialData)
	require.NoError(t, err)

	deleteQuery := types.NewBoundDeleteQuery(ks1, tableName, "", rowKey, false, nil)
	_, err = btc.DeleteRow(t.Context(), deleteQuery)
	require.NoError(t, err)

	// Verify deletion
	tbl, err := btc.clients.GetTableClient(ks1, tableName)
	require.NoError(t, err)

	row, err := tbl.ReadRow(t.Context(), string(rowKey))
	assert.NoError(t, err)
	assert.Empty(t, row)
}

func TestApplyBulkMutation(t *testing.T) {
	_, err := btc.ApplyBulkMutation(t.Context(), ks1, tableName, []types.IBigtableMutation{
		types.NewBigtableWriteMutation(ks1, tableName, "", types.IfSpec{}, types.QueryTypeInsert, "bulk1").
			AddMutations(types.NewWriteCellOp("cf1", "name", []byte("buddy"))),
		types.NewBigtableWriteMutation(ks1, tableName, "", types.IfSpec{}, types.QueryTypeInsert, "bulk2").
			AddMutations(types.NewWriteCellOp("cf1", "name", []byte("santa"))),
		types.NewBigtableWriteMutation(ks1, tableName, "", types.IfSpec{}, types.QueryTypeInsert, "bulk3").
			AddMutations(types.NewWriteCellOp("cf1", "name", []byte("walter"))),
		// update row1
		types.NewBigtableWriteMutation(ks1, tableName, "", types.IfSpec{}, types.QueryTypeUpdate, "bulk1").
			AddMutations(types.NewWriteCellOp("cf1", "name", []byte("buddy-2"))),
		// delete bulk2
		types.NewBoundDeleteQuery(ks1, tableName, "", "bulk2", false, nil),
	})

	tbl, err := btc.clients.GetTableClient(ks1, tableName)
	require.NoError(t, err)


	// Verify mutations
	row1, err := tbl.ReadRow(t.Context(), "bulk1")
	assert.NoError(t, err)
	assert.NotEmpty(t, row1)
	assert.Equal(t, []byte("buddy-2"), row1["cf1"][0].Value)


	row2, err := tbl.ReadRow(t.Context(), "bulk2")
	assert.NoError(t, err)
	assert.Empty(t, row2)

	row3, err := tbl.ReadRow(t.Context(), "bulk3")
	assert.NoError(t, err)
	assert.Empty(t, row3)
	assert.Equal(t, []byte("walter"), row1["cf1"][0].Value)

}

func TestDeleteRowsUsingTimestamp(t *testing.T) {
	client, adminClients, t.Context(), err := getManagerClient(conn)
	if err != nil {
		t.Fatalf("Failed to create Bigtable client: %v", err)
	}

	adminClient, err := bigtable.NewAdminClient(t.Context(), "project", "bt1", option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("Failed to create Bigtable admin client: %v", err)
	}

	btc := NewBigtableClient(client, adminClients, zap.NewNop(), bigtableConfig, nil, &schemaMapping.SchemaMetadata{})

	// Define test table and data
	tableName := "test-table-delete-timestamp"
	columnFamily := "cf1"
	rowKey := "test-row"
	// columns := []string{"col1", "col2"}
	timestamp := translators.TimestampInfo{Timestamp: bigtable.Now()}

	err = adminClient.CreateTable(t.Context(), tableName)
	assert.NoError(t, err)
	err = adminClient.CreateColumnFamily(t.Context(), tableName, columnFamily)
	assert.NoError(t, err)

	tbl := client["bt1"].Open(tableName)
	mut := bigtable.NewMutation()
	mut.Set(columnFamily, "col1", bigtable.Now(), []byte("value1"))
	mut.Set(columnFamily, "col2", bigtable.Now(), []byte("value2"))
	err = tbl.Apply(t.Context(), rowKey, mut)
	assert.NoError(t, err)

	// Verify that the row is present before deletion
	row, err := tbl.ReadRow(t.Context(), rowKey)
	assert.NoError(t, err)
	assert.NotEmpty(t, row[columnFamily], "Expected columns to be present before deletion")
	// Test Case 1: Successful deletion of columns using a timestamp
	timestamp.Timestamp = bigtable.Timestamp(time.Now().Day())
	deleteData := &translators.PreparedDeleteQuery{
		Table:         tableName,
		RowKey:        rowKey,
		Keyspace:      "ks1",
		TimestampInfo: timestamp,
	}
	_, err = btc.DeleteRow(t.Context(), deleteData)
	assert.NoError(t, err)

	// Verify that the columns are deleted
	row, err = tbl.ReadRow(t.Context(), rowKey)
	assert.NoError(t, err)
	assert.Empty(t, row[columnFamily], "Expected columns to be deleted")

	// Test Case 2: Invalid keyspace
	deleteData.TimestampInfo.Timestamp = bigtable.Now()
	deleteData.Keyspace = "invalid-keyspace"
	_, err = btc.DeleteRow(t.Context(), deleteData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "keyspace not found: 'invalid-keyspace'")

	// Test Case 3: Attempt to delete non-existent columns (should not error)
	deleteData.Keyspace = "ks1"
	_, err = btc.DeleteRow(t.Context(), deleteData)
	assert.NoError(t, err)

	// Verify that nothing breaks or changes for non-existent columns
	row, err = tbl.ReadRow(t.Context(), rowKey)
	assert.NoError(t, err)
	assert.Empty(t, row[columnFamily], "GoRow should remain empty as no valid columns existed to delete")
}

func TestMutateRowDeleteColumnFamily(t *testing.T) {
	client, adminClients, t.Context(), err := getManagerClient(conn)
	if err != nil {
		t.Fatalf("Failed to create Bigtable client: %v", err)
	}

	adminClient, err := bigtable.NewAdminClient(t.Context(), "project", "bt1", option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("Failed to create Bigtable admin client: %v", err)
	}

	tableName := "test-table-delete-cf"
	require.NoError(t, adminClient.CreateTable(t.Context(), tableName))
	require.NoError(t, adminClient.CreateColumnFamily(t.Context(), tableName, "cf1"))
	require.NoError(t, adminClient.CreateColumnFamily(t.Context(), tableName, "cf2"))

	btc := NewBigtableClient(client, adminClients, zap.NewNop(), bigtableConfig, nil, schemaMapping.NewSchemaMetadata("schema_mapping", "cf1", zap.NewNop(), nil))

	// Insert initial data
	insertData := &types.BigtableWriteMutation{
		Table:    tableName,
		RowKey:   "row1",
		Columns:  []*types.Column{{ColumnFamily: "cf1", Name: "col1"}, {ColumnFamily: "cf2", Name: "col2"}},
		Columns:  []interface{}{[]byte("v1"), []byte("v2")},
		Keyspace: "ks1",
	}
	_, err = btc.InsertRow(t.Context(), insertData)
	require.NoError(t, err)

	// Delete cf2
	updateData := &translators.PreparedUpdateQuery{
		Table:                tableName,
		RowKey:               "row1",
		DeleteColumnFamilies: []string{"cf2"},
		Keyspace:             "ks1",
	}
	_, err = btc.UpdateRow(t.Context(), updateData)
	require.NoError(t, err)

	// Verify deletion by reading the row
	tbl := client["bt1"].Open(tableName)
	row, err := tbl.ReadRow(t.Context(), "row1")
	require.NoError(t, err)
	assert.NotContains(t, row, "cf2", "cf2 should be deleted")
	assert.Contains(t, row, "cf1", "cf1 should still exist")
}

func TestMutateRowDeleteQualifiers(t *testing.T) {
	client, adminClients, t.Context(), err := getManagerClient(conn)
	require.NoError(t, err)

	adminClient, err := bigtable.NewAdminClient(t.Context(), "project", "bt1", option.WithGRPCConn(conn))
	require.NoError(t, err)

	tableName := "test-table-delete-qualifiers"
	require.NoError(t, adminClient.CreateTable(t.Context(), tableName))
	require.NoError(t, adminClient.CreateColumnFamily(t.Context(), tableName, "cf1"))

	btc := NewBigtableClient(client, adminClients, zap.NewNop(), bigtableConfig, nil, schemaMapping.NewSchemaMetadata("schema_mapping", "cf1", zap.NewNop(), nil))

	// Insert initial data with two columns
	insertData := &types.BigtableWriteMutation{
		Table:    tableName,
		RowKey:   "row1",
		Columns:  []*types.Column{{ColumnFamily: "cf1", Name: "col1"}, {ColumnFamily: "cf1", Name: "col2"}},
		Columns:  []interface{}{[]byte("v1"), []byte("v2")},
		Keyspace: "ks1",
	}
	_, err = btc.InsertRow(t.Context(), insertData)
	require.NoError(t, err)

	// Delete col1
	updateData := &translators.PreparedUpdateQuery{
		Table:                 tableName,
		RowKey:                "row1",
		DeleteColumQualifiers: []*types.Column{{ColumnFamily: "cf1", Name: "col1"}},
		Keyspace:              "ks1",
	}
	_, err = btc.UpdateRow(t.Context(), updateData)
	require.NoError(t, err)

	// Verify deletion by reading the row
	tbl := client["bt1"].Open(tableName)
	row, err := tbl.ReadRow(t.Context(), "row1", bigtable.RowFilter(bigtable.FamilyFilter("cf1")))
	require.NoError(t, err)
	cf := row["cf1"]
	for _, cell := range cf {
		assert.NotEqual(t, "cf1:col1", cell.Column, "col1 should be deleted")
	}
	assert.Equal(t, "cf1:col2", cf[0].Column, "col2 should still exist")
}

func TestMutateRowIfExists(t *testing.T) {
	client, adminClients, t.Context(), err := getManagerClient(conn)
	require.NoError(t, err)

	adminClient, err := bigtable.NewAdminClient(t.Context(), "project", "bt1", option.WithGRPCConn(conn))
	require.NoError(t, err)

	tableName := "test-table-if-exists"
	require.NoError(t, adminClient.CreateTable(t.Context(), tableName))
	require.NoError(t, adminClient.CreateColumnFamily(t.Context(), tableName, "cf1"))

	btc := NewBigtableClient(client, adminClients, zap.NewNop(), bigtableConfig, nil, schemaMapping.NewSchemaMetadata("schema_mapping", "cf1", zap.NewNop(), nil))

	// Insert initial data
	insertData := &types.BigtableWriteMutation{
		Table:    tableName,
		RowKey:   "row1",
		Columns:  []*types.Column{{ColumnFamily: "cf1", Name: "col1"}},
		Columns:  []interface{}{[]byte("v1")},
		Keyspace: "ks1",
	}
	_, err = btc.InsertRow(t.Context(), insertData)
	require.NoError(t, err)

	// Update the row when it exists
	updateData := &translators.PreparedUpdateQuery{
		Table:    tableName,
		RowKey:   "row1",
		Columns:  []*types.Column{{ColumnFamily: "cf1", Name: "col1"}},
		Values:   []interface{}{[]byte("v2")},
		IfExists: true,
		Keyspace: "ks1",
	}
	_, err = btc.UpdateRow(t.Context(), updateData)
	require.NoError(t, err)

	// Verify the update by reading the row
	tbl := client["bt1"].Open(tableName)
	row, err := tbl.ReadRow(t.Context(), "row1", bigtable.RowFilter(bigtable.FamilyFilter("cf1")))
	require.NoError(t, err)
	assert.Equal(t, "v2", string(row["cf1"][0].Value), "value should be updated to v2")

	// Attempt to update a non-existent row
	updateData.RowKey = "row2"
	_, err = btc.UpdateRow(t.Context(), updateData)
	require.NoError(t, err)

	// Verify the non-existent row is not created
	row, err = tbl.ReadRow(t.Context(), "row2")
	require.NoError(t, err)
	assert.Nil(t, row, "row2 should not exist")
}

func TestMutateRowIfNotExists(t *testing.T) {
	client, adminClients, t.Context(), err := getManagerClient(conn)
	require.NoError(t, err)

	adminClient, err := bigtable.NewAdminClient(t.Context(), "project", "bt1", option.WithGRPCConn(conn))
	require.NoError(t, err)

	tableName := "test-table-if-not-exists"
	require.NoError(t, adminClient.CreateTable(t.Context(), tableName))
	require.NoError(t, adminClient.CreateColumnFamily(t.Context(), tableName, "cf1"))

	btc := NewBigtableClient(client, adminClients, zap.NewNop(), bigtableConfig, nil, schemaMapping.NewSchemaMetadata("schema_mapping", "cf1", zap.NewNop(), nil))

	// Insert a row when it does not exist
	InsertData := &types.BigtableWriteMutation{
		Table:       tableName,
		RowKey:      "row1",
		Columns:     []*types.Column{{ColumnFamily: "cf1", Name: "col1"}},
		Columns:     []interface{}{[]byte("v1")},
		IfNotExists: true,
		Keyspace:    "ks1",
	}
	_, err = btc.InsertRow(t.Context(), InsertData)
	require.NoError(t, err)

	// Verify the row is created
	tbl := client["bt1"].Open(tableName)
	row, err := tbl.ReadRow(t.Context(), "row1", bigtable.RowFilter(bigtable.FamilyFilter("cf1")))
	require.NoError(t, err)
	assert.Equal(t, "v1", string(row["cf1"][0].Value), "row1 should be created with value v1")

	// Attempt to insert the same row again
	_, err = btc.InsertRow(t.Context(), InsertData)
	require.NoError(t, err)

	// Verify the row is not updated
	row, err = tbl.ReadRow(t.Context(), "row1", bigtable.RowFilter(bigtable.FamilyFilter("cf1")))
	require.NoError(t, err)
	assert.Equal(t, "v1", string(row["cf1"][0].Value), "row1 should not be updated")
}

func TestMutateRowNonByteValue(t *testing.T) {
	client, adminClients, t.Context(), err := getManagerClient(conn)
	require.NoError(t, err)

	adminClient, err := bigtable.NewAdminClient(t.Context(), "project", "bt1", option.WithGRPCConn(conn))
	require.NoError(t, err)

	tableName := "test-table-non-byte"
	require.NoError(t, adminClient.CreateTable(t.Context(), tableName))
	require.NoError(t, adminClient.CreateColumnFamily(t.Context(), tableName, "cf1"))

	btc := NewBigtableClient(client, adminClients, zap.NewNop(), bigtableConfig, nil, schemaMapping.NewSchemaMetadata("schema_mapping", "cf1", zap.NewNop(), nil))

	updateData := &translators.PreparedUpdateQuery{
		Table:    tableName,
		RowKey:   "row1",
		Columns:  []*types.Column{{ColumnFamily: "cf1", Name: "col1"}},
		Values:   []interface{}{"invalid-value"}, // string instead of []byte
		Keyspace: "ks1",
	}
	_, err = btc.UpdateRow(t.Context(), updateData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not of type []byte")
}

func TestMutateRowInvalidKeyspace(t *testing.T) {
	client, adminClients, t.Context(), err := getManagerClient(conn)
	require.NoError(t, err)

	btc := NewBigtableClient(client, adminClients, zap.NewNop(), bigtableConfig, nil, schemaMapping.NewSchemaMetadata("schema_mapping", "cf1", zap.NewNop(), nil))

	updateData := &translators.PreparedUpdateQuery{
		Table:    "any-table",
		RowKey:   "row1",
		Columns:  []*types.Column{{ColumnFamily: "cf1", Name: "col1"}},
		Values:   []interface{}{[]byte("value")},
		Keyspace: "invalid-keyspace",
	}
	_, err = btc.UpdateRow(t.Context(), updateData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "keyspace not found: 'invalid-keyspace'")
}

func TestComplexUpdateWithListIndex(t *testing.T) {
	client, adminClients, t.Context(), err := getManagerClient(conn)
	require.NoError(t, err)

	adminClient, err := bigtable.NewAdminClient(t.Context(), "project", "bt1", option.WithGRPCConn(conn))
	require.NoError(t, err)

	tableName := "test-table-complex-update-list-index"
	require.NoError(t, adminClient.CreateTable(t.Context(), tableName))
	require.NoError(t, adminClient.CreateColumnFamily(t.Context(), tableName, "cf1"))
	require.NoError(t, adminClient.CreateColumnFamily(t.Context(), tableName, "list"))

	btc := NewBigtableClient(client, adminClients, zap.NewNop(), bigtableConfig, nil, schemaMapping.NewSchemaMetadata("schema_mapping", "cf1", zap.NewNop(), nil))

	// Insert initial data with a list in cf1
	insertData := &types.BigtableWriteMutation{
		Table:    tableName,
		RowKey:   "row1",
		Columns:  []*types.Column{{ColumnFamily: "list", Name: "timestamp1"}, {ColumnFamily: "list", Name: "timestamp2"}},
		Columns:  []interface{}{[]byte("v1"), []byte("v2")},
		Keyspace: "ks1",
	}
	_, err = btc.InsertRow(t.Context(), insertData)
	require.NoError(t, err)

	// Perform a complex update with list index
	ComplexOperation := map[string]*translators.ComplexOperation{
		"list": {
			UpdateListIndex: "1", // Update the second item in the list
			Value:           []byte("updated-v2"),
		},
	}
	updateData := &translators.PreparedUpdateQuery{
		Table:             tableName,
		RowKey:            "row1",
		ComplexOperations: ComplexOperation,
		Keyspace:          "ks1",
		Columns:           []*types.Column{},
		Values:            []interface{}{},
	}
	_, err = btc.UpdateRow(t.Context(), updateData)
	require.NoError(t, err)

	// Verify the update by reading the row
	tbl := client["bt1"].Open(tableName)
	row, err := tbl.ReadRow(t.Context(), "row1", bigtable.RowFilter(bigtable.FamilyFilter("list")))
	require.NoError(t, err)
	cf := row["list"]
	assert.Equal(t, "updated-v2", string(cf[1].Value), "second item in the list should be updated")
}

func TestComplexUpdateWithListDeletion(t *testing.T) {
	client, adminClients, t.Context(), err := getManagerClient(conn)
	require.NoError(t, err)

	adminClient, err := bigtable.NewAdminClient(t.Context(), "project", "bt1", option.WithGRPCConn(conn))
	require.NoError(t, err)

	tableName := "test-table-complex-update-list-delete"
	require.NoError(t, adminClient.CreateTable(t.Context(), tableName))
	require.NoError(t, adminClient.CreateColumnFamily(t.Context(), tableName, "cf1"))
	require.NoError(t, adminClient.CreateColumnFamily(t.Context(), tableName, "list"))

	btc := NewBigtableClient(client, adminClients, zap.NewNop(), bigtableConfig, nil, schemaMapping.NewSchemaMetadata("schema_mapping", "cf1", zap.NewNop(), nil))

	// Insert initial data with a list in the "list" column family
	insertData := &types.BigtableWriteMutation{
		Table:    tableName,
		RowKey:   "row1",
		Columns:  []*types.Column{{ColumnFamily: "list", Name: "timestamp1"}, {ColumnFamily: "list", Name: "timestamp2"}},
		Columns:  []interface{}{[]byte("v1"), []byte("v2")},
		Keyspace: "ks1",
	}
	_, err = btc.InsertRow(t.Context(), insertData)
	require.NoError(t, err)

	// Perform a complex update with list deletion
	ComplexOperation := map[string]*translators.ComplexOperation{
		"list": {
			ListDelete:       true,
			ListDeleteValues: [][]byte{[]byte("v1")}, // Delete the first item in the list
		},
	}
	updateData := &translators.PreparedUpdateQuery{
		Table:             tableName,
		RowKey:            "row1",
		ComplexOperations: ComplexOperation,
		Keyspace:          "ks1",
		Columns:           []*types.Column{},
		Values:            []interface{}{},
	}
	_, err = btc.UpdateRow(t.Context(), updateData)
	require.NoError(t, err)

	// Verify the deletion by reading the row
	tbl := client["bt1"].Open(tableName)
	row, err := tbl.ReadRow(t.Context(), "row1", bigtable.RowFilter(bigtable.FamilyFilter("list")))
	require.NoError(t, err)
	cf := row["list"]
	assert.Equal(t, 1, len(cf), "one item should remain in the list")
	assert.Equal(t, "v2", string(cf[0].Value), "remaining item should be v2")
}

func TestComplexUpdateInvalidKeyspace(t *testing.T) {
	client, adminClients, t.Context(), err := getManagerClient(conn)
	require.NoError(t, err)

	btc := NewBigtableClient(client, adminClients, zap.NewNop(), bigtableConfig, nil, schemaMapping.NewSchemaMetadata("schema_mapping", "cf1", zap.NewNop(), nil))

	// Attempt to perform a complex update with an invalid keyspace
	ComplexOperation := map[string]*translators.ComplexOperation{
		"list": {
			UpdateListIndex: "0",
			Value:           []byte("updated-v1"),
		},
	}
	updateData := &translators.PreparedUpdateQuery{
		Table:             "any-table",
		RowKey:            "row1",
		ComplexOperations: ComplexOperation,
		Keyspace:          "invalid-keyspace",
		Columns:           []*types.Column{},
		Values:            []interface{}{},
	}
	_, err = btc.UpdateRow(t.Context(), updateData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "keyspace not found: 'invalid-keyspace'")
}

func TestComplexUpdateOutOfBoundsIndex(t *testing.T) {
	client, adminClients, t.Context(), err := getManagerClient(conn)
	require.NoError(t, err)

	adminClient := adminClients["bt1"]

	tableName := "test-table-complex-update-out-of-bounds"
	require.NoError(t, adminClient.CreateTable(t.Context(), tableName))
	require.NoError(t, adminClient.CreateColumnFamily(t.Context(), tableName, "cf1"))
	require.NoError(t, adminClient.CreateColumnFamily(t.Context(), tableName, "list"))

	btc := NewBigtableClient(client, adminClients, zap.NewNop(), bigtableConfig, nil, schemaMapping.NewSchemaMetadata("schema_mapping", "cf1", zap.NewNop(), nil))

	// Insert initial data with a list in the "list" column family
	insertData := &types.BigtableWriteMutation{
		Table:    tableName,
		RowKey:   "row1",
		Columns:  []*types.Column{{ColumnFamily: "list", Name: "timestamp1"}},
		Columns:  []interface{}{[]byte("v1")},
		Keyspace: "ks1",
	}
	_, err = btc.InsertRow(t.Context(), insertData)
	require.NoError(t, err)

	// Attempt to perform a complex update with an out-of-bounds index
	ComplexOperation := map[string]*translators.ComplexOperation{
		"list": {
			UpdateListIndex: "1", // Index 1 is out of bounds for a list of size 1
			Value:           []byte("updated-v2"),
		},
	}
	updateData := &translators.PreparedUpdateQuery{
		Table:             tableName,
		RowKey:            "row1",
		ComplexOperations: ComplexOperation,
		Keyspace:          "ks1",
		Columns:           []*types.Column{},
		Values:            []interface{}{},
	}
	_, err = btc.UpdateRow(t.Context(), updateData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "index 1 out of bounds")
}
