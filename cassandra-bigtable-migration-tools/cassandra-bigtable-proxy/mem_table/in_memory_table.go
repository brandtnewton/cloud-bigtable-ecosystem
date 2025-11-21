package mem_table

import (
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
)

type tableStore struct {
	Table *schemaMapping.TableConfig
	Data  []types.GoRow
}

func newTableStore(table *schemaMapping.TableConfig, data []types.GoRow) *tableStore {
	return &tableStore{Table: table, Data: data}
}
