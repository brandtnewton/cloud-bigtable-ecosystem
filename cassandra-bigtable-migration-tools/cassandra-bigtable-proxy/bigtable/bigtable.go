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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	otelgo "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/otel"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"go.uber.org/zap"
)

// Events
const (
	applyingBigtableMutation       = "Applying Insert/Update Mutation"
	bigtableMutationApplied        = "Insert/Update Mutation Applied"
	applyingDeleteMutation         = "Applying Delete Mutation"
	deleteMutationApplied          = "Delete Mutation Applied"
	fetchingSchemaMappingConfig    = "Fetching Schema Mapping Configurations"
	schemaMappingConfigFetched     = "Schema Mapping Configurations Fetched"
	applyingBulkMutation           = "Applying Bulk Mutation"
	bulkMutationApplied            = "Bulk Mutation Applied"
	mutationTypeInsert             = "Insert"
	mutationTypeDelete             = "Delete"
	mutationTypeDeleteColumnFamily = "DeleteColumnFamilies"
	mutationTypeUpdate             = "Update"
	schemaMappingTableColumnFamily = "cf"
	// Cassandra doesn't have a time dimension to their counters, so we need to
	// use the same time for all counters
	counterTimestamp            = 0
	smColTableName              = "TableName"
	smColColumnName             = "ColumnName"
	smColColumnType             = "ColumnType"
	smColIsCollection           = "IsCollection"
	smColIsPrimaryKey           = "IsPrimaryKey"
	smColPKPrecedence           = "PK_Precedence"
	smColKeyType                = "KeyType"
	smFullQualifierTableName    = schemaMappingTableColumnFamily + ":" + smColTableName
	smFullQualifierColumnName   = schemaMappingTableColumnFamily + ":" + smColColumnName
	smFullQualifierColumnType   = schemaMappingTableColumnFamily + ":" + smColColumnType
	smFullQualifierIsPrimaryKey = schemaMappingTableColumnFamily + ":" + smColIsPrimaryKey
	smFullQualifierPKPrecedence = schemaMappingTableColumnFamily + ":" + smColPKPrecedence
	smFullQualifierKeyType      = schemaMappingTableColumnFamily + ":" + smColKeyType
)

type BigTableClientIface interface {
	ApplyBulkMutation(context.Context, types.Keyspace, types.TableName, []types.IBigtableMutation) (BulkOperationResponse, error)
	Close()
	Execute(ctx context.Context, query types.IExecutableQuery) (*message.RowsResult, error)
	DeleteRow(context.Context, *types.BoundDeleteQuery) (*message.RowsResult, error)
	ReadTableConfigs(context.Context, types.Keyspace) ([]*schemaMapping.TableConfig, error)
	InsertRow(ctx context.Context, input *types.BigtableWriteMutation) (*message.RowsResult, error)
	SelectStatement(context.Context, *types.BoundSelectQuery) (*message.RowsResult, error)
	AlterTable(ctx context.Context, data *types.AlterTableStatementMap) error
	CreateTable(ctx context.Context, data *types.CreateTableStatementMap) error
	DropAllRows(ctx context.Context, data *types.TruncateTableStatementMap) error
	DropTable(ctx context.Context, data *types.DropTableQuery) error
	UpdateRow(ctx context.Context, input *types.BigtableWriteMutation) (*message.RowsResult, error)
	PrepareStatement(ctx context.Context, query types.IPreparedQuery) (*bigtable.PreparedStatement, error)
	ExecutePreparedStatement(ctx context.Context, query *types.BoundSelectQuery) (*message.RowsResult, error)
	// Functions realted to updating the intance properties.
	LoadConfigs(schemaConfig *schemaMapping.SchemaMappingConfig)
}

// NewBigtableClient - Creates a new instance of BigtableClient.
//
// Parameters:
//   - client: Bigtable client.
//   - logger: Logger instance.
//   - sqlClient: Bigtable SQL client.
//   - config: BigtableConfig configuration object.
//   - grpcConn: grpcConn for calling apis.
//   - schemaMapping: schema mapping for response handling.
//
// Returns:
//   - BigtableClient: New instance of BigtableClient
var NewBigtableClient = func(client map[string]*bigtable.Client, adminClients map[string]*bigtable.AdminClient, logger *zap.Logger, config *types.BigtableConfig, schemaMapping *schemaMapping.SchemaMappingConfig) BigTableClientIface {
	return &BigtableClient{
		Clients:             client,
		AdminClients:        adminClients,
		Logger:              logger,
		BigtableConfig:      config,
		SchemaMappingConfig: schemaMapping,
	}
}

func (btc *BigtableClient) Execute(ctx context.Context, query types.IExecutableQuery) (*message.RowsResult, error) {
	switch q := query.(type) {
	case *types.BoundDeleteQuery:
		return btc.DeleteRow(ctx, q)
	case *types.BigtableWriteMutation:
		return btc.mutateRow(ctx, q)
	case *types.BoundSelectQuery:
		return btc.SelectStatement(ctx, q)
	case *types.CreateTableStatementMap:
		err := btc.CreateTable(ctx, q)
		return emptyRowsResult(), err
	case *types.AlterTableStatementMap:
		err := btc.AlterTable(ctx, q)
		return emptyRowsResult(), err
	case *types.TruncateTableStatementMap:
		err := btc.DropAllRows(ctx, q)
		return emptyRowsResult(), err
	case *types.DropTableQuery:
		err := btc.DropTable(ctx, q)
		return emptyRowsResult(), err
	default:
		return nil, fmt.Errorf("unhandled prepared query type: %T", query)
	}
}

func (btc *BigtableClient) reloadSchemaMappings(ctx context.Context, keyspace types.Keyspace) error {
	tableConfigs, err := btc.ReadTableConfigs(ctx, keyspace)
	if err != nil {
		return fmt.Errorf("error when reloading schema mappings for %s.%s: %w", keyspace, btc.SchemaMappingConfig.SchemaMappingTableName, err)
	}
	err = btc.SchemaMappingConfig.ReplaceTables(keyspace, tableConfigs)
	if err != nil {
		return fmt.Errorf("error updating schema cache for keyspace %s: %w", keyspace, err)
	}
	return nil
}

// scan the schema mapping table to determine if the table exists
func (btc *BigtableClient) tableSchemaExists(ctx context.Context, client *bigtable.Client, tableName types.TableName) (bool, error) {
	table := client.Open(string(btc.SchemaMappingConfig.SchemaMappingTableName))
	exists := false
	err := table.ReadRows(ctx, bigtable.PrefixRange(string(tableName)+"#"), func(row bigtable.Row) bool {
		exists = true
		return false
	}, bigtable.LimitRows(1))
	return exists, err
}

func (btc *BigtableClient) tableResourceExists(ctx context.Context, adminClient *bigtable.AdminClient, table types.TableName) (bool, error) {
	_, err := adminClient.TableInfo(ctx, string(table))
	// todo figure out which error message is for table doesn't exist yet or find better check
	if status.Code(err) == codes.NotFound || status.Code(err) == codes.InvalidArgument {
		return false, nil
	}
	// something went wrong
	if err != nil {
		return false, err
	}
	return true, nil
}

// mutateRow() - Applies mutations to a row in the specified Bigtable table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - tableName: Columns of the table where the row exists.
//   - rowKey: Row key of the row to mutate.
//   - columns: Columns to mutate.
//   - values: Columns to set in the columns.
//   - deleteColumnFamilies: Columns families to delete.
//
// Returns:
//   - error: Error if the mutation fails.
func (btc *BigtableClient) mutateRow(ctx context.Context, input *types.BigtableWriteMutation) (*message.RowsResult, error) {
	table, err := btc.SchemaMappingConfig.GetTableConfig(input.Keyspace(), input.Table())
	if err != nil {
		return nil, err
	}
	otelgo.AddAnnotation(ctx, applyingBigtableMutation)
	mut := bigtable.NewMutation()

	btc.Logger.Info("mutating row", zap.String("key", hex.EncodeToString([]byte(input.RowKey()))))

	client, err := btc.getClient(table.Keyspace)
	if err != nil {
		return nil, err
	}

	tbl := client.Open(string(table.Name))

	mutationCount, err := btc.buildMutation(ctx, table, input, mut)

	if input.IfSpec.IfExists || input.IfSpec.IfNotExists {
		predicateFilter := bigtable.CellsPerRowLimitFilter(1)
		matched := true
		conditionalMutation := bigtable.NewCondMutation(predicateFilter, mut, nil)
		if input.IfSpec.IfNotExists {
			conditionalMutation = bigtable.NewCondMutation(predicateFilter, nil, mut)
		}

		err := tbl.Apply(ctx, string(input.RowKey()), conditionalMutation, bigtable.GetCondMutationResult(&matched))
		otelgo.AddAnnotation(ctx, bigtableMutationApplied)
		if err != nil {
			return nil, err
		}

		return GenerateAppliedRowsResult(table, input.IfSpec.IfExists == matched), nil
	}

	// no-op just return
	if mutationCount == 0 {
		return &message.RowsResult{
			Metadata: &message.RowsMetadata{
				LastContinuousPage: true,
			},
		}, nil
	}

	// If no conditions, apply the mutation directly
	err = tbl.Apply(ctx, string(input.RowKey()), mut)
	otelgo.AddAnnotation(ctx, bigtableMutationApplied)
	if err != nil {
		return nil, err
	}

	return &message.RowsResult{
		Metadata: &message.RowsMetadata{
			LastContinuousPage: true,
		},
	}, nil
}

func emptyRowsResult() *message.RowsResult {
	return &message.RowsResult{
		Metadata: &message.RowsMetadata{
			LastContinuousPage: true,
		},
	}
}

func (btc *BigtableClient) buildMutation(ctx context.Context, table *schemaMapping.TableConfig, input *types.BigtableWriteMutation, mut *bigtable.Mutation) (int, error) {
	var mutationCount = 0
	// Delete column families
	for _, cf := range input.DelColumnFamily {
		mut.DeleteCellsInFamily(string(cf))
		mutationCount++
	}

	for _, counterOp := range input.CounterOps {
		mut.AddIntToCell(string(counterOp.Family), "", counterTimestamp, counterOp.Value)
		mutationCount++
	}

	var timestamp bigtable.Timestamp
	if input.UsingTimestamp != nil && input.UsingTimestamp.HasUsingTimestamp {
		timestamp = bigtable.Time(input.UsingTimestamp.Timestamp)
	} else {
		timestamp = bigtable.Time(time.Now())
	}

	for _, indexOp := range input.SetIndexOps {
		reqTimestamp, err := btc.getIndexOpTimestamp(ctx, table, input.RowKey(), indexOp.Family, indexOp.Index)
		if err != nil {
			return 0, err
		}
		mut.Set(string(indexOp.Family), reqTimestamp, timestamp, indexOp.Value)
		mutationCount++
	}

	for _, deleteOp := range input.DeleteListElementsOps {
		err := btc.setMutationForListDelete(ctx, table, input.RowKey(), deleteOp.Family, deleteOp.Values, mut)
		if err != nil {
			return 0, err
		}
		mutationCount++
	}

	// Delete specific column qualifiers
	for _, q := range input.DelColumns {
		mut.DeleteCellsInColumn(string(q.Family), string(q.Column))
		mutationCount++
	}

	// Set values for columns
	for _, d := range input.Data {
		mut.Set(string(d.Family), string(d.Column), timestamp, d.Bytes)
		mutationCount++
	}
	return mutationCount, nil
}

func (btc *BigtableClient) DropAllRows(ctx context.Context, data *types.TruncateTableStatementMap) error {
	_, err := btc.SchemaMappingConfig.GetTableConfig(data.Keyspace(), data.Table())
	if err != nil {
		return err
	}

	adminClient, err := btc.getAdminClient(data.Keyspace())
	if err != nil {
		return err
	}

	btc.Logger.Info("truncate table: dropping all bigtable rows")
	err = adminClient.DropAllRows(ctx, string(data.Table()))
	if status.Code(err) == codes.NotFound {
		// Table doesn't exist in BigtableConfig, which is fine for a truncate.
		btc.Logger.Info("truncate table: table not found in bigtable, nothing to drop", zap.String("table", string(data.Table())))
		return nil
	}
	if err != nil {
		btc.Logger.Error("truncate table: failed", zap.Error(err))
		return err
	}
	btc.Logger.Info("truncate table: complete")
	return nil
}

func (btc *BigtableClient) DropTable(ctx context.Context, data *types.DropTableQuery) error {
	_, err := btc.SchemaMappingConfig.GetTableConfig(data.Keyspace(), data.Table())
	if err != nil && !data.IfExists {
		return err
	}

	client, err := btc.getClient(data.Keyspace())
	if err != nil {
		return err
	}
	adminClient, err := btc.getAdminClient(data.Keyspace())
	if err != nil {
		return err
	}

	// first clean up table from schema mapping table because that's the SoT
	tbl := client.Open(string(btc.SchemaMappingConfig.SchemaMappingTableName))
	var deleteMuts []*bigtable.Mutation
	var rowKeysToDelete []string
	err = tbl.ReadRows(ctx, bigtable.PrefixRange(string(data.Table())+"#"), func(row bigtable.Row) bool {
		mut := bigtable.NewMutation()
		mut.DeleteRow()
		deleteMuts = append(deleteMuts, mut)
		rowKeysToDelete = append(rowKeysToDelete, row.Key())
		return true
	})

	if err != nil {
		return err
	}

	btc.Logger.Info("drop table: deleting schema rows")
	_, err = tbl.ApplyBulk(ctx, rowKeysToDelete, deleteMuts)
	if err != nil {
		return err
	}

	// do a read to check if the table exists to save on admin API write quota
	exists, err := btc.tableResourceExists(ctx, adminClient, data.Table())
	if err != nil {
		return err
	}
	if exists {
		btc.Logger.Info("drop table: deleting bigtable table")
		err = adminClient.DeleteTable(ctx, string(data.Table()))
	}
	if err != nil {
		return err
	}

	// this error behavior is done independently of the table resource existing or not because the schema mapping table is the SoT, not the table resource
	if len(rowKeysToDelete) == 0 && !data.IfExists {
		return fmt.Errorf("cannot delete table %s because it does not exist", data.Table())
	}

	// only reload schema mapping table if this operation changed it
	if len(rowKeysToDelete) > 0 {
		btc.Logger.Info("reloading schema mappings")
		err = btc.reloadSchemaMappings(ctx, data.Keyspace())
		if err != nil {
			return err
		}
	}
	return nil
}

func (btc *BigtableClient) createSchemaMappingTableMaybe(ctx context.Context, keyspace types.Keyspace) error {
	btc.Logger.Info("ensuring schema mapping table exists for keyspace", zap.String("schema_mapping_table", string(btc.SchemaMappingConfig.SchemaMappingTableName)), zap.String("keyspace", string(keyspace)))
	adminClient, err := btc.getAdminClient(keyspace)
	if err != nil {
		return err
	}

	// do a read to check if the table exists to save on admin API write quota
	exists, err := btc.tableResourceExists(ctx, adminClient, btc.SchemaMappingConfig.SchemaMappingTableName)
	if err != nil {
		return err
	}
	if !exists {
		btc.Logger.Info("schema mapping table not found. Automatically creating it...")
		err = adminClient.CreateTable(ctx, string(btc.SchemaMappingConfig.SchemaMappingTableName))
		if status.Code(err) == codes.AlreadyExists {
			// continue - maybe another Proxy instance raced, and created it instead
		} else if err != nil {
			btc.Logger.Error("failed to create schema mapping table", zap.Error(err))
			return err
		}
		btc.Logger.Info("schema mapping table created.")
	}

	err = adminClient.CreateColumnFamily(ctx, string(btc.SchemaMappingConfig.SchemaMappingTableName), schemaMappingTableColumnFamily)
	if status.Code(err) == codes.AlreadyExists {
		err = nil
	}
	if err != nil {
		return err
	}

	return nil
}

func (btc *BigtableClient) CreateTable(ctx context.Context, data *types.CreateTableStatementMap) error {
	client, err := btc.getClient(data.Keyspace())
	if err != nil {
		return err
	}
	adminClient, err := btc.getAdminClient(data.Keyspace())
	if err != nil {
		return err
	}

	rowKeySchema, err := createBigtableRowKeySchema(data.PrimaryKeys, data.Columns, data.IntRowKeyEncoding)
	if err != nil {
		return err
	}

	columnFamilies, err := btc.addColumnFamilies(data.Columns)
	if err != nil {
		return err
	}

	// add default column family
	columnFamilies[string(btc.BigtableConfig.DefaultColumnFamily)] = bigtable.Family{
		GCPolicy: bigtable.MaxVersionsPolicy(1),
	}

	// create the table conf first, here because it should exist before any reference to it in the schema mapping table is added, otherwise another concurrent request could try to load it and fail.
	btc.Logger.Info("creating bigtable table")
	err = adminClient.CreateTableFromConf(ctx, &bigtable.TableConf{
		TableID:        string(data.Table()),
		ColumnFamilies: columnFamilies,
		RowKeySchema:   rowKeySchema,
	})
	// ignore already exists errors - the schema mapping table is the SoT
	if status.Code(err) == codes.AlreadyExists {
		err = nil
	} else if err != nil {
		btc.Logger.Error("failed to create bigtable table", zap.Error(err))
		return err
	}

	exists, err := btc.tableSchemaExists(ctx, client, data.Table())
	if err != nil {
		return err
	}

	if exists && !data.IfNotExists {
		return fmt.Errorf("cannot create table %s because it already exists", data.Table())
	}

	if !exists {
		btc.Logger.Info("updating table schema")
		err = btc.updateTableSchema(ctx, data.Keyspace(), data.Table(), data.PrimaryKeys, data.Columns, nil)
		if err != nil {
			return err
		}
		err = btc.reloadSchemaMappings(ctx, data.Keyspace())
		if err != nil {
			return err
		}
	}

	return nil
}

func (btc *BigtableClient) AlterTable(ctx context.Context, data *types.AlterTableStatementMap) error {
	adminClient, err := btc.getAdminClient(data.Keyspace())
	if err != nil {
		return err
	}

	// passing nil in as pmks because we don't have access to them here and because primary keys can't be altered
	err = btc.updateTableSchema(ctx, data.Keyspace(), data.Table(), nil, data.AddColumns, data.DropColumns)
	if err != nil {
		return err
	}

	columnFamilies, err := btc.addColumnFamilies(data.AddColumns)
	if err != nil {
		return err
	}

	for family, config := range columnFamilies {
		err = adminClient.CreateColumnFamilyWithConfig(ctx, string(data.Table()), family, config)
		if status.Code(err) == codes.AlreadyExists {
			// This can happen if the ALTER TABLE statement is run more than once.
			continue
		}

		if err != nil {
			return err
		}
	}

	btc.Logger.Info("reloading schema mappings")
	err = btc.reloadSchemaMappings(ctx, data.Keyspace())
	if err != nil {
		return err
	}

	return nil
}

func (btc *BigtableClient) addColumnFamilies(columns []types.CreateColumn) (map[string]bigtable.Family, error) {
	columnFamilies := make(map[string]bigtable.Family)
	for _, col := range columns {
		if !col.TypeInfo.IsCollection() && col.TypeInfo.Code() != types.COUNTER {
			continue
		}

		if string(col.Name) == string(btc.BigtableConfig.DefaultColumnFamily) {
			return nil, fmt.Errorf("counter and collection type columns cannot be named '%s' because it's reserved as the default column family", btc.BigtableConfig.DefaultColumnFamily)
		}

		if col.TypeInfo.IsCollection() {
			columnFamilies[string(col.Name)] = bigtable.Family{
				GCPolicy: bigtable.MaxVersionsPolicy(1),
			}
		} else if col.TypeInfo.Code() == types.COUNTER {
			columnFamilies[string(col.Name)] = bigtable.Family{
				GCPolicy: bigtable.NoGcPolicy(),
				ValueType: bigtable.AggregateType{
					Input:      bigtable.Int64Type{},
					Aggregator: bigtable.SumAggregator{},
				},
			}
		}
	}
	return columnFamilies, nil
}

func (btc *BigtableClient) updateTableSchema(ctx context.Context, keyspace types.Keyspace, tableName types.TableName, pmks []types.CreateTablePrimaryKeyConfig, addCols []types.CreateColumn, dropCols []types.ColumnName) error {
	client, err := btc.getClient(keyspace)
	if err != nil {
		return err
	}

	ts := bigtable.Now()
	var muts []*bigtable.Mutation
	var rowKeys []string
	sort.Slice(addCols, func(i, j int) bool {
		return addCols[i].Index < addCols[j].Index
	})
	for _, col := range addCols {
		mut := bigtable.NewMutation()
		mut.Set(schemaMappingTableColumnFamily, smColColumnName, ts, []byte(col.Name))
		mut.Set(schemaMappingTableColumnFamily, smColColumnType, ts, []byte(col.TypeInfo.String()))
		isCollection := col.TypeInfo.IsCollection()
		// todo this is no longer used. We'll remove this later, in a few releases, but we'll keep it for now so users can roll back to earlier versions of the proxy if needed
		mut.Set(schemaMappingTableColumnFamily, smColIsCollection, ts, []byte(strconv.FormatBool(isCollection)))
		pmkIndex := slices.IndexFunc(pmks, func(c types.CreateTablePrimaryKeyConfig) bool {
			return c.Name == col.Name
		})
		mut.Set(schemaMappingTableColumnFamily, smColIsPrimaryKey, ts, []byte(strconv.FormatBool(pmkIndex != -1)))
		if pmkIndex != -1 {
			pmkConfig := pmks[pmkIndex]
			mut.Set(schemaMappingTableColumnFamily, smColKeyType, ts, []byte(pmkConfig.KeyType))
		} else {
			// overkill, but overwrite any previous KeyType configs which could exist if the table was recreated with different columns
			mut.Set(schemaMappingTableColumnFamily, smColKeyType, ts, []byte(utilities.KEY_TYPE_REGULAR))
		}
		// +1 because we track PKPrecedence as 1 indexed for some reason
		mut.Set(schemaMappingTableColumnFamily, smColPKPrecedence, ts, []byte(strconv.Itoa(pmkIndex+1)))
		mut.Set(schemaMappingTableColumnFamily, smColTableName, ts, []byte(tableName))
		muts = append(muts, mut)
		rowKeys = append(rowKeys, string(tableName)+"#"+string(col.Name))
	}
	// note: we only remove the column from the schema mapping table and don't actually delete any data from the data table
	for _, col := range dropCols {
		mut := bigtable.NewMutation()
		mut.DeleteRow()
		muts = append(muts, mut)
		rowKeys = append(rowKeys, fmt.Sprintf("%s#%s", tableName, col))
	}

	btc.Logger.Info("updating schema mapping table")
	table := client.Open(string(btc.SchemaMappingConfig.SchemaMappingTableName))
	_, err = table.ApplyBulk(ctx, rowKeys, muts)

	if err != nil {
		btc.Logger.Error("update schema mapping table failed")
		return err
	}

	return nil
}

// InsertRow - Inserts a row into the specified bigtable table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - data: PreparedInsertQuery object containing the table, row key, columns, values, and deleteColumnFamilies.
//
// Returns:
//   - error: Error if the insertion fails.
func (btc *BigtableClient) InsertRow(ctx context.Context, input *types.BigtableWriteMutation) (*message.RowsResult, error) {
	return btc.mutateRow(ctx, input)
}

// UpdateRow - Updates a row in the specified bigtable table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - data: PreparedUpdateQuery object containing the table, row key, columns, values, and DeleteColumnFamilies.
//
// Returns:
//   - error: Error if the update fails.
func (btc *BigtableClient) UpdateRow(ctx context.Context, input *types.BigtableWriteMutation) (*message.RowsResult, error) {
	return btc.mutateRow(ctx, input)
}

func (btc *BigtableClient) DeleteRow(ctx context.Context, deleteQueryData *types.BoundDeleteQuery) (*message.RowsResult, error) {
	otelgo.AddAnnotation(ctx, applyingDeleteMutation)
	client, err := btc.getClient(deleteQueryData.Keyspace())
	if err != nil {
		return nil, err
	}
	tbl := client.Open(string(deleteQueryData.Table()))
	mut := bigtable.NewMutation()

	table, err := btc.SchemaMappingConfig.GetTableConfig(deleteQueryData.Keyspace(), deleteQueryData.Table())
	if err != nil {
		return nil, err
	}

	err = btc.buildDeleteMutation(ctx, table, deleteQueryData, mut)
	if err != nil {
		return nil, err
	}
	if deleteQueryData.IfExists {
		predicateFilter := bigtable.CellsPerRowLimitFilter(1)
		conditionalMutation := bigtable.NewCondMutation(predicateFilter, mut, nil)
		matched := true
		if err := tbl.Apply(ctx, string(deleteQueryData.RowKey()), conditionalMutation, bigtable.GetCondMutationResult(&matched)); err != nil {
			return nil, err
		}

		if !matched {
			return GenerateAppliedRowsResult(table, false), nil
		} else {
			return GenerateAppliedRowsResult(table, true), nil
		}
	} else {
		if err := tbl.Apply(ctx, string(deleteQueryData.RowKey()), mut); err != nil {
			return nil, err
		}
	}
	otelgo.AddAnnotation(ctx, deleteMutationApplied)
	var response = message.RowsResult{
		Metadata: &message.RowsMetadata{
			LastContinuousPage: true,
		},
	}
	return &response, nil
}

func (btc *BigtableClient) buildDeleteMutation(ctx context.Context, table *schemaMapping.TableConfig, deleteQueryData *types.BoundDeleteQuery, mut *bigtable.Mutation) error {
	if len(deleteQueryData.Columns) > 0 {
		for _, column := range deleteQueryData.Columns {
			switch c := column.(type) {
			case *types.BoundIndexColumn:
				reqTimestamp, err := btc.getIndexOpTimestamp(ctx, table, deleteQueryData.RowKey(), c.Column().ColumnFamily, c.Index)
				if err != nil {
					return err
				}
				mut.DeleteCellsInColumn(string(c.Column().ColumnFamily), reqTimestamp)
			case *types.BoundKeyColumn:
				mut.DeleteCellsInColumn(string(c.Column().ColumnFamily), string(c.Key))
			default:
				return fmt.Errorf("unhandled delete query column %T", c)
			}
		}
	} else {
		mut.DeleteRow()
	}
	return nil
}

// ReadTableConfigs - Retrieves schema mapping configurations from the specified config table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - schemaMappingTable: Columns of the table containing the configuration.
//
// Returns:
//   - map[string]map[string]*Columns: Table metadata.
//   - map[string][]Columns: Primary key metadata.
//   - error: Error if the retrieval fails.
func (btc *BigtableClient) ReadTableConfigs(ctx context.Context, keyspace types.Keyspace) ([]*schemaMapping.TableConfig, error) {
	// if this is the first time we're getting table configs, ensure the schema mapping table exists
	if btc.SchemaMappingConfig == nil || btc.SchemaMappingConfig.CountTables() == 0 {
		err := btc.createSchemaMappingTableMaybe(ctx, keyspace)
		if err != nil {
			return nil, err
		}
	}

	otelgo.AddAnnotation(ctx, fetchingSchemaMappingConfig)
	client, err := btc.getClient(keyspace)
	if err != nil {
		return nil, err
	}

	table := client.Open(string(btc.SchemaMappingConfig.SchemaMappingTableName))
	filter := bigtable.LatestNFilter(1)

	allColumns := make(map[types.TableName][]*types.Column)

	var readErr error
	err = table.ReadRows(ctx, bigtable.InfiniteRange(""), func(row bigtable.Row) bool {
		// Extract the row key and column values
		var tableName types.TableName
		var columnName, columnType, keyType string
		var isPrimaryKey bool
		var pkPrecedence int
		// Extract column values
		for _, item := range row[schemaMappingTableColumnFamily] {
			switch item.Column {
			case smFullQualifierTableName:
				tableName = types.TableName(item.Value)
			case smFullQualifierColumnName:
				columnName = string(item.Value)
			case smFullQualifierColumnType:
				columnType = string(item.Value)
			case smFullQualifierIsPrimaryKey:
				isPrimaryKey = string(item.Value) == "true"
			case smFullQualifierPKPrecedence:
				pkPrecedence, readErr = strconv.Atoi(string(item.Value))
				if readErr != nil {
					return false
				}
			case smFullQualifierKeyType:
				keyType = string(item.Value)
			}
		}
		dt, err := utilities.ParseCqlTypeString(columnType)
		if err != nil {
			readErr = err
			return false
		}

		keyType = btc.parseKeyType(keyspace, tableName, columnName, keyType, pkPrecedence)

		// Create a new column struct
		column := &types.Column{
			Name:         types.ColumnName(columnName),
			CQLType:      dt,
			IsPrimaryKey: isPrimaryKey,
			PkPrecedence: pkPrecedence,
			KeyType:      keyType,
		}

		allColumns[tableName] = append(allColumns[tableName], column)

		return true
	}, bigtable.RowFilter(filter))

	// combine errors for simpler error handling
	if err == nil {
		err = readErr
	}

	if err != nil {
		btc.Logger.Error("Failed to read rows from bigtable - possible issue with schema_mapping table:", zap.Error(err))
		return nil, err
	}

	adminClient, err := btc.getAdminClient(keyspace)
	if err != nil {
		errorMessage := fmt.Sprintf("failed to get admin client for keyspace '%s'", keyspace)
		btc.Logger.Error(errorMessage, zap.Error(err))
		return nil, fmt.Errorf("%s: %w", errorMessage, err)
	}

	tableConfigs := make([]*schemaMapping.TableConfig, 0, len(allColumns))
	for tableName, tableColumns := range allColumns {
		var intRowKeyEncoding types.IntRowKeyEncodingType
		// if we already know what encoding the table has, just use that, so we don't have to do the extra network request
		if existingTable, err := btc.SchemaMappingConfig.GetTableConfig(keyspace, tableName); err == nil {
			intRowKeyEncoding = existingTable.IntRowKeyEncoding
		} else {
			btc.Logger.Info(fmt.Sprintf("loading table info for table %s.%s", keyspace, tableName))
			tableInfo, err := adminClient.TableInfo(ctx, string(tableName))
			if err != nil {
				return nil, err
			}
			intRowKeyEncoding = detectTableEncoding(tableInfo, types.OrderedCodeEncoding)
		}

		tableConfig := schemaMapping.NewTableConfig(keyspace, tableName, btc.BigtableConfig.DefaultColumnFamily, intRowKeyEncoding, tableColumns)
		tableConfigs = append(tableConfigs, tableConfig)
	}

	otelgo.AddAnnotation(ctx, schemaMappingConfigFetched)

	return tableConfigs, nil
}

// This parsing logic defaults the primary key types, because older versions of the proxy set keyType to an empty string. The only key type that we're guessing about is whether the key is a clustering key or not which, for the purposes of the proxy, doesn't matter.
func (btc *BigtableClient) parseKeyType(keyspace types.Keyspace, table types.TableName, column, keyType string, pkPrecedence int) string {
	keyType = strings.ToLower(keyType)

	// we used to write 'partition' for partition keys but Cassandra calls it 'partition_key'
	if keyType == "partition" {
		keyType = utilities.KEY_TYPE_PARTITION
	}

	if keyType == utilities.KEY_TYPE_PARTITION && pkPrecedence > 0 {
		return utilities.KEY_TYPE_PARTITION
	} else if keyType == utilities.KEY_TYPE_CLUSTERING && pkPrecedence > 1 {
		return utilities.KEY_TYPE_CLUSTERING
	} else if keyType == utilities.KEY_TYPE_REGULAR && pkPrecedence == 0 {
		return utilities.KEY_TYPE_REGULAR
	} else {
		// this is an unknown key type
		var defaultKeyType string
		if pkPrecedence <= 0 {
			// regular columns, that aren't part of the primary key have a precedence of 0.
			defaultKeyType = utilities.KEY_TYPE_REGULAR
		} else if pkPrecedence == 1 {
			// default to partition key because the first key is always a partition key.
			defaultKeyType = utilities.KEY_TYPE_PARTITION
		} else {
			// default to clustering key because clustering keys usually follow (this will be wrong in the case of a composite partition key), and it doesn't really matter for the purposes of the proxy.
			defaultKeyType = utilities.KEY_TYPE_CLUSTERING
		}
		btc.Logger.Warn(fmt.Sprintf("unknown key state KeyType='%s' and pkPrecedence of %d for %s.%s column %s. defaulting key type to '%s'", keyType, pkPrecedence,
			keyspace, table, column, defaultKeyType))

		return defaultKeyType
	}
}

// ApplyBulkMutation - Applies bulk mutations to the specified bigtable table.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - tableName: Columns of the table to apply bulk mutations to.
//   - mutationData: Slice of MutationData objects containing mutation details.
//
// Returns:
//   - BulkOperationResponse: Response indicating the result of the bulk operation.
//   - error: Error if the bulk mutation fails.
func (btc *BigtableClient) ApplyBulkMutation(ctx context.Context, keyspace types.Keyspace, tableName types.TableName, mutationData []types.IBigtableMutation) (BulkOperationResponse, error) {
	table, err := btc.SchemaMappingConfig.GetTableConfig(keyspace, tableName)
	if err != nil {
		return BulkOperationResponse{
			FailedRows: "All Rows are failed",
		}, err
	}

	rowKeyToMutationMap := make(map[types.RowKey]*bigtable.Mutation)
	for _, md := range mutationData {
		rowKey := md.RowKey()
		btc.Logger.Info("mutating row BULK", zap.String("key", hex.EncodeToString([]byte(rowKey))))
		if _, exists := rowKeyToMutationMap[rowKey]; !exists {
			rowKeyToMutationMap[rowKey] = bigtable.NewMutation()
		}
		mut := rowKeyToMutationMap[rowKey]
		switch v := md.(type) {
		case types.BigtableWriteMutation:
			_, err := btc.buildMutation(ctx, table, &v, mut)
			if err != nil {
				return BulkOperationResponse{
					FailedRows: "All Rows are failed",
				}, err
			}
		case types.BoundDeleteQuery:
			err := btc.buildDeleteMutation(ctx, table, &v, mut)
			if err != nil {
				return BulkOperationResponse{
					FailedRows: "All Rows are failed",
				}, err
			}
		default:
			return BulkOperationResponse{
				FailedRows: "All Rows are failed",
			}, fmt.Errorf("unhandled bulk mutation type %T", md)
		}
	}
	// create mutations from mutation data
	var mutations []*bigtable.Mutation
	var rowKeys []string

	for key, mutation := range rowKeyToMutationMap {
		mutations = append(mutations, mutation)
		rowKeys = append(rowKeys, string(key))
	}
	otelgo.AddAnnotation(ctx, applyingBulkMutation)

	client, err := btc.getClient(keyspace)
	if err != nil {
		return BulkOperationResponse{
			FailedRows: "All Rows are failed",
		}, err
	}

	tbl := client.Open(string(tableName))
	errs, err := tbl.ApplyBulk(ctx, rowKeys, mutations)

	if err != nil {
		return BulkOperationResponse{
			FailedRows: "All Rows are failed",
		}, fmt.Errorf("ApplyBulk: %w", err)
	}

	var failedRows []string
	for i, e := range errs {
		if e != nil {
			failedRows = append(failedRows, rowKeys[i])
		}
	}
	var res BulkOperationResponse
	if len(failedRows) > 0 {
		res = BulkOperationResponse{
			FailedRows: fmt.Sprintf("failed rowkeys: %v", failedRows),
		}
	} else {
		res = BulkOperationResponse{
			FailedRows: "",
		}
	}
	otelgo.AddAnnotation(ctx, bulkMutationApplied)
	return res, nil
}

// Close() gracefully shuts down the bigtable client and gRPC connection.
//
// It iterates through all active bigtable clients and closes them before closing the gRPC connection.
func (btc *BigtableClient) Close() {
	for _, clients := range btc.Clients {
		clients.Close()
	}
}

// getIndexOpTimestamp() retrieves the timestamp qualifier for a given list index in a bigtable row.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - tableName: The name of the bigtable table.
//   - rowKey: The row key to look up.
//   - columnFamily: The column family where the list is stored.
//   - keyspace: The keyspace identifier.
//   - index: The index of the list element for which the timestamp is required.
//
// Returns:
//   - string: The timestamp qualifier if found.
//   - error: An error if the row does not exist, the index is out of bounds, or any other retrieval failure occurs.
func (btc *BigtableClient) getIndexOpTimestamp(ctx context.Context, table *schemaMapping.TableConfig, rowKey types.RowKey, columnFamily types.ColumnFamily, index int) (string, error) {
	client, err := btc.getClient(table.Keyspace)
	if err != nil {
		return "", err
	}
	tbl := client.Open(string(table.Name))
	row, err := tbl.ReadRow(ctx, string(rowKey), bigtable.RowFilter(bigtable.ChainFilters(
		bigtable.FamilyFilter(string(columnFamily)),
		bigtable.LatestNFilter(1), // this filter is so that we fetch only the latest timestamp value
	)))
	if err != nil {
		btc.Logger.Error("Failed to read row", zap.String("RowKey", string(rowKey)), zap.Error(err))
		return "", err
	}

	columns := row[string(columnFamily)]
	if len(columns) <= 0 {
		return "", fmt.Errorf("no values found in list %s", string(columnFamily))
	}
	for i, item := range columns {
		if i == index {
			splits := strings.Split(item.Column, ":")
			qualifier := splits[1]
			return qualifier, nil
		}
	}
	return "", fmt.Errorf("index %d out of bounds for list size %d", index, len(columns))
}

// setMutationForListDelete() applies deletions to specific list elements in a bigtable row.
//
// This function identifies and removes the specified elements from a list stored in a column family.
//
// Parameters:
//   - ctx: Context for the operation, used for cancellation and deadlines.
//   - tableName: The name of the bigtable table.
//   - rowKey: The row key containing the list.
//   - columnFamily: The column family storing the list.
//   - keyspace: The keyspace identifier.
//   - deleteList: A slice of byte arrays representing the values to be deleted from the list.
//   - mut: The bigtable mutation object to which delete operations will be added.
//
// Returns:
//   - error: An error if the row does not exist or if list elements cannot be deleted.
func (btc *BigtableClient) setMutationForListDelete(ctx context.Context, table *schemaMapping.TableConfig, rowKey types.RowKey, columnFamily types.ColumnFamily, deleteList []types.BigtableValue, mut *bigtable.Mutation) error {
	client, err := btc.getClient(table.Keyspace)
	if err != nil {
		return err
	}

	tbl := client.Open(string(table.Name))
	row, err := tbl.ReadRow(ctx, string(rowKey), bigtable.RowFilter(bigtable.ChainFilters(
		bigtable.FamilyFilter(string(columnFamily)),
		bigtable.LatestNFilter(1), // this filter is so that we fetch only the latest timestamp value
	)))
	if err != nil {
		btc.Logger.Error("Failed to read row", zap.String("RowKey", string(rowKey)), zap.Error(err))
		return err
	}

	if len(row[string(columnFamily)]) <= 0 {
		return fmt.Errorf("no values found in list %s", columnFamily)
	}
	for _, item := range row[string(columnFamily)] {
		for _, dItem := range deleteList {
			if bytes.Equal(dItem, item.Value) {
				splits := strings.Split(item.Column, ":")
				qualifier := splits[1]
				mut.DeleteCellsInColumn(string(columnFamily), qualifier)
			}
		}
	}
	return nil
}

// LoadConfigs initializes the BigtableClient with the provided response handler
// and schema mapping configuration.
func (btc *BigtableClient) LoadConfigs(schemaConfig *schemaMapping.SchemaMappingConfig) {

	// Set the SchemaMappingConfig to define how data mapping will be handled in bigtable.
	btc.SchemaMappingConfig = schemaConfig
}

// PrepareStatement prepares a query for execution using the bigtable SQL client.
func (btc *BigtableClient) PrepareStatement(ctx context.Context, query types.IPreparedQuery) (*bigtable.PreparedStatement, error) {
	// only select queries can be prepared in Bigtable at the moment
	if query.QueryType() != types.QueryTypeSelect {
		return nil, nil
	}
	client, err := btc.getClient(query.Keyspace())
	if err != nil {
		return nil, err
	}

	paramTypes := make(map[string]bigtable.SQLType)
	for _, p := range query.Parameters().AllKeys() {
		md := query.Parameters().GetMetadata(p)

		if md.IsCollectionKey {
			paramTypes[string(p)] = bigtable.BytesSQLType{}
		} else {
			sqlType, err := toBigtableSQLType(md.Type)
			if err != nil {
				btc.Logger.Error("Failed to infer SQL type for parameter", zap.String("paramName", string(p)), zap.Error(err))
				return nil, fmt.Errorf("failed to infer SQL type for parameter %s: %w", p, err)
			}
			paramTypes[string(p)] = sqlType
		}
	}
	preparedStatement, err := client.PrepareStatement(ctx, query.BigtableQuery(), paramTypes)
	if err != nil {
		btc.Logger.Error("Failed to prepare statement", zap.String("query", query.BigtableQuery()), zap.Error(err))
		return nil, fmt.Errorf("failed to prepare statement '%s': %w", query.CqlQuery(), err)
	}

	return preparedStatement, nil
}

// getClient retrieves a bigtable client for a given keyspace.
// It looks up the instance information associated with the keyspace in the InstancesMap,
// then returns the corresponding client from the Clients map.
//
// Parameters:
//   - keyspace: string representing the Cassandra keyspace name
//
// Returns:
//   - *bigtable.Client: the bigtable client instance associated with the keyspace
//   - error: returns an error if either the keyspace is not found in InstancesMap
//     or if no client exists for the corresponding bigtable instance
func (btc *BigtableClient) getClient(keyspace types.Keyspace) (*bigtable.Client, error) {
	instanceInfo, ok := btc.BigtableConfig.Instances[keyspace]
	if !ok {
		return nil, fmt.Errorf("keyspace not found: '%s'", keyspace)
	}
	client, ok := btc.Clients[instanceInfo.BigtableInstance]
	if !ok {
		return nil, fmt.Errorf("client not found for instance '%s' (from keyspace '%s')", instanceInfo.BigtableInstance, keyspace)
	}
	return client, nil
}

// getAdminClient retrieves the bigtable AdminClient associated with the given keyspace.
// It looks up the instance information for the provided keyspace in the InstancesMap,
// then fetches the corresponding AdminClient from the AdminClients map using the
// bigtable instance name. If the keyspace or the admin client is not found, it returns
// an error describing the missing resource.
//
// Parameters:
//   - keyspace: The keyspace name to look up.
//
// Returns:
//   - *bigtable.AdminClient: The admin client associated with the keyspace.
//   - error: An error if the keyspace or admin client is not found.
func (btc *BigtableClient) getAdminClient(keyspace types.Keyspace) (*bigtable.AdminClient, error) {
	instanceInfo, ok := btc.BigtableConfig.Instances[keyspace]
	if !ok {
		return nil, fmt.Errorf("keyspace not found: '%s'", keyspace)
	}
	Adminclient, ok := btc.AdminClients[instanceInfo.BigtableInstance]
	if !ok {
		return nil, fmt.Errorf("admin client not found for instance '%s' (from keyspace '%s')", instanceInfo.BigtableInstance, keyspace)
	}
	return Adminclient, nil
}
