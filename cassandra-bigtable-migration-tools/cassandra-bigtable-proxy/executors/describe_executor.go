package executors

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/datastax/go-cassandra-native-protocol/datatype"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"sort"
)

type describeExecutor struct {
	schemaMappings *schemaMapping.SchemaMappingConfig
}

func (d *describeExecutor) CanRun(q types.IExecutableQuery) bool {
	return q.QueryType() == types.QueryTypeDescribe
}

func (d *describeExecutor) Execute(_ context.Context, _ types.ICassandraClient, q types.IExecutableQuery) (message.Message, error) {
	describeStmt, ok := q.(*types.DescribeQuery)
	if !ok {
		return nil, fmt.Errorf("invalid describe query type")
	}
	if describeStmt.Keyspaces {
		return d.handleDescribeKeyspaces(describeStmt)
	} else if describeStmt.Tables {
		return d.handleDescribeTables(describeStmt)
	} else if describeStmt.Table() != "" {
		return d.handleDescribeTable(describeStmt)
	} else if describeStmt.Keyspace() != "" {
		return d.handleDescribeKeyspace(describeStmt)
	} else {
		return nil, fmt.Errorf("unhandled describe query")
	}
}

func newDescribeExecutor(schemaMappings *schemaMapping.SchemaMappingConfig) IQueryExecutor {
	return &describeExecutor{schemaMappings: schemaMappings}
}

// handleDescribeKeyspaces handles the DESCRIBE KEYSPACES command
func (d *describeExecutor) handleDescribeKeyspaces(desc *types.DescribeQuery) (message.Message, error) {
	// Get all keyspaces from the schema mapping
	keyspaces := make([]string, 0, len(d.schemaMappings.GetAllTables()))

	// Add custom keyspaces from schema mapping
	for keyspace := range d.schemaMappings.GetAllTables() {
		keyspaces = append(keyspaces, string(keyspace))
	}

	// Sort the keyspaces for consistent output
	sort.Strings(keyspaces)

	columns := []*message.ColumnMetadata{
		{
			Name:     "name", // Changed from "keyspace_name" to "name" to match cqlsh's expectation
			Type:     datatype.Varchar,
			Table:    "keyspaces",
			Keyspace: "system_virtual_schema",
		},
	}
	var rows []message.Row
	for _, ks := range keyspaces {
		rows = append(rows, message.Row{[]byte(ks)})
	}
	return &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: int32(len(columns)),
			Columns:     columns,
		},
		Data: rows,
	}, nil
}

// handleDescribeTables handles the DESCRIBE TABLES command
func (d *describeExecutor) handleDescribeTables(desc *types.DescribeQuery) (message.Message, error) {
	// Return a list of tables in
	tables := []struct {
		keyspace string
		table    string
	}{
		{"system_virtual_schema", "keyspaces"},
		{"system_virtual_schema", "tables"},
		{"system_virtual_schema", "metaDataColumns"},
	}

	for _, keyspaceTables := range d.schemaMappings.GetAllTables() {
		for _, table := range keyspaceTables {
			tables = append(tables, struct {
				keyspace string
				table    string
			}{string(table.Keyspace), string(table.Name)})
		}
	}

	columns := []*message.ColumnMetadata{
		{
			Name:     "keyspace_name",
			Type:     datatype.Varchar,
			Table:    "tables",
			Keyspace: "system",
		},
		{
			Name:     "name",
			Type:     datatype.Varchar,
			Table:    "tables",
			Keyspace: "system",
		},
	}
	var rows []message.Row
	for _, t := range tables {
		rows = append(rows, message.Row{
			[]byte(t.keyspace),
			[]byte(t.table),
		})
	}
	return &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: int32(len(columns)),
			Columns:     columns,
		},
		Data: rows,
	}, nil
}

// handleDescribeTable handles the DESCRIBE TABLE command for a specific table
func (d *describeExecutor) handleDescribeTable(desc *types.DescribeQuery) (message.Message, error) {
	tableConfig, err := d.schemaMappings.GetTableConfig(desc.Keyspace(), desc.Table())
	if err != nil {
		return nil, fmt.Errorf("error getting describe column metadata: %w", err)
	}

	columns := []*message.ColumnMetadata{
		{
			Name:     "create_statement",
			Type:     datatype.Varchar,
			Table:    string(tableConfig.Name),
			Keyspace: string(tableConfig.Keyspace),
		},
	}

	stmt := tableConfig.Describe()

	return &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: int32(len(columns)),
			Columns:     columns,
		},
		Data: []message.Row{{[]byte(stmt)}},
	}, nil
}

// handleDescribeKeyspace handles the DESCRIBE KEYSPACE <sessionKeyspace> command
func (d *describeExecutor) handleDescribeKeyspace(desc *types.DescribeQuery) (message.Message, error) {
	columns := []*message.ColumnMetadata{
		{
			Name:     "create_statement",
			Type:     datatype.Varchar,
			Table:    "keyspaces",
			Keyspace: string(desc.Keyspace()),
		},
	}

	createStmts := []string{fmt.Sprintf("CREATE KEYSPACE %s WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};", desc.Keyspace())}

	tables, err := d.schemaMappings.GetKeyspace(desc.Keyspace())
	if err != nil {
		return nil, err
	}

	for _, tableConfig := range tables {
		createTableStmt := tableConfig.Describe()
		createStmts = append(createStmts, createTableStmt)
	}

	var rows []message.Row
	for _, stmt := range createStmts {
		rows = append(rows, message.Row{[]byte(stmt)})
	}

	return &message.RowsResult{
		Metadata: &message.RowsMetadata{
			ColumnCount: int32(len(columns)),
			Columns:     columns,
		},
		Data: rows,
	}, nil
}
