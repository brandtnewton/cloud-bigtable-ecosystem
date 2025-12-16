package mem_table

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
)

type tableStore struct {
	Table *schemaMapping.TableSchema
	Data  []types.GoRow
}

func newTableStore(table *schemaMapping.TableSchema, data []types.GoRow) *tableStore {
	return &tableStore{Table: table, Data: data}
}
