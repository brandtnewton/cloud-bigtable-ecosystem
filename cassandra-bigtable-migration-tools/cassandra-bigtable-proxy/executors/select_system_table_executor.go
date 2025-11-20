package executors

import (
	"context"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/datastax/parser"
	"github.com/datastax/go-cassandra-native-protocol/frame"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

type selectSystemTableExecutor struct {
	schemaMappings *schemaMapping.SchemaMappingConfig
}

func (d *selectSystemTableExecutor) CanRun(q types.IExecutableQuery) bool {
	return q.QueryType() == types.QueryTypeSelect && q.Keyspace().IsSystemKeyspace()
}

func (d *selectSystemTableExecutor) Execute(_ context.Context, c types.ICassandraClient, q types.IExecutableQuery) (message.Message, error) {
	s, ok := q.(types.BoundSelectQuery)
	if !ok {
		return nil, fmt.Errorf("unsupported query")
	}

	table, err := d.schemaMappings.GetTableConfig(s.Keyspace(), s.Table())
	if err != nil {
		return nil, err
	}

	if s.Keyspace() == systemSchema || s.Keyspace == systemVirtualSchema {
		var localColumns []*message.ColumnMetadata
		var isFound bool
		if s.Keyspace == systemSchema {
			localColumns, isFound = parser.SystemSchematablesColumn[s.Table]
			if isFound {
				tableMetadata := &message.RowsMetadata{
					ColumnCount: int32(len(localColumns)),
					Columns:     localColumns,
				}

				data, err := c.getSystemMetadata(hdr, s)
				if err != nil {
					c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
					return
				}

				c.sender.Send(hdr, &message.RowsResult{
					Metadata: tableMetadata,
					Data:     data,
				})
				return
			}
		} else {
			// get Table metadata for system_virtual_schema schema
			localColumns, isFound = parser.SystemVirtualSchemaColumn[s.Table]
			if isFound {
				c.sender.Send(hdr, &message.RowsResult{
					Metadata: &message.RowsMetadata{
						ColumnCount: int32(len(localColumns)),
						Columns:     localColumns,
					},
				})
				return
			}
		}
		if !isFound {
			c.sender.Send(hdr, &message.Invalid{ErrorMessage: "Error while fetching mocked table info"})
			return
		}
	} else if s.Table == local {
		localColumns := parser.SystemLocalColumns
		if len(c.proxy.cluster.Info.DSEVersion) > 0 {
			localColumns = parser.DseSystemLocalColumns
		}
		if columns, err := parser.FilterColumns(s, localColumns); err != nil {
			c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
		} else if row, err := c.filterSystemLocalValues(s, columns); err != nil {
			c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
		} else {
			c.sender.Send(hdr, &message.RowsResult{
				Metadata: &message.RowsMetadata{
					ColumnCount: int32(len(columns)),
					Columns:     columns,
				},
				Data: []message.Row{row},
			})
		}
	} else if s.Table == "peers" {
		peersColumns := parser.SystemPeersColumns
		if len(c.proxy.cluster.Info.DSEVersion) > 0 {
			peersColumns = parser.DseSystemPeersColumns
		}
		if columns, err := parser.FilterColumns(s, peersColumns); err != nil {
			c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
		} else {
			var data []message.Row
			for _, n := range c.proxy.nodes {
				if n != c.proxy.localNode {
					var row message.Row
					row, err = c.filterSystemPeerValues(s, columns, n, len(c.proxy.nodes)-1)
					if err != nil {
						break
					}
					data = append(data, row)
				}
			}
			if err != nil {
				c.sender.Send(hdr, &message.Invalid{ErrorMessage: err.Error()})
			} else {
				c.sender.Send(hdr, &message.RowsResult{
					Metadata: &message.RowsMetadata{
						ColumnCount: int32(len(columns)),
						Columns:     columns,
					},
					Data: data,
				})
			}
		}
		// CC- metadata is mocked here as well for system queries
	} else if columns, ok := parser.SystemColumnsByName[s.Table]; ok {
		c.sender.Send(hdr, &message.RowsResult{
			Metadata: &message.RowsMetadata{
				ColumnCount: int32(len(columns)),
				Columns:     columns,
			},
		})
	} else {
		c.sender.Send(hdr, &message.Invalid{ErrorMessage: "Doesn't exist"})
	}

}

func newSelectSystemTableExecutor(schemaMappings *schemaMapping.SchemaMappingConfig) IQueryExecutor {
	return &selectSystemTableExecutor{schemaMappings: schemaMappings}
}

// getSystemMetadata retrieves system metadata for `system_schema` keyspaces, tables, or columns.
//
// Parameters:
// - hdr: *frame.Header (request version info)
// - s: *parser.SelectStatement (sessionKeyspace and table info)
//
// Returns:
// - []message.Row: Metadata rows for the requested table; empty if sessionKeyspace/table is invalid.
func (d *selectSystemTableExecutor) getSystemMetadata(hdr *frame.Header, s *parser.SelectStatement) ([]message.Row, error) {
	if s.Keyspace != systemSchema || (s.Table != keyspaces && s.Table != tables && s.Table != metaDataColumns) {
		return nil, nil
	}

	var cache map[primitive.ProtocolVersion][]message.Row
	var errMsg error
	switch s.Table {
	case keyspaces:
		cache = c.proxy.systemQueryMetadataCache.KeyspaceSystemQueryMetadataCache
		errMsg = fmt.Errorf(systemQueryMetadataNotFoundError, "KeyspaceSystemQueryMetadataCache", hdr.Version)
	case tables:
		cache = c.proxy.systemQueryMetadataCache.TableSystemQueryMetadataCache
		errMsg = fmt.Errorf(systemQueryMetadataNotFoundError, "TableSystemQueryMetadataCache", hdr.Version)
	case metaDataColumns:
		cache = c.proxy.systemQueryMetadataCache.ColumnsSystemQueryMetadataCache
		errMsg = fmt.Errorf(systemQueryMetadataNotFoundError, "ColumnsSystemQueryMetadataCache", hdr.Version)
	}

	if data, exist := cache[hdr.Version]; !exist {
		return nil, errMsg
	} else {
		return data, nil
	}
}