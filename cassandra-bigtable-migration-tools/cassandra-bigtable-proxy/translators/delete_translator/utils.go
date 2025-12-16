package delete_translator

import (
	"errors"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/metadata"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/common"
)

func parseDeleteColumns(deleteColumns cql.IDeleteColumnListContext, tableConfig *schemaMapping.TableSchema) ([]types.SelectedColumn, error) {
	if deleteColumns == nil {
		return nil, nil
	}
	cols := deleteColumns.AllDeleteColumnItem()
	var columns []types.SelectedColumn
	for _, v := range cols {
		alias, err := common.ParseAs(v.AsSpec())
		if err != nil {
			return nil, err
		}

		var col types.SelectedColumn
		if v.SelectColumn() != nil {
			col, err = common.ParseSelectColumn(v.SelectColumn(), alias, tableConfig)
		} else if v.SelectIndex() != nil {
			col, err = common.ParseSelectIndex(v.SelectIndex(), alias, tableConfig)
		} else {
			return nil, errors.New("unhandled delete column clause")
		}
		if err != nil {
			return nil, err
		}
		columns = append(columns, col)
	}
	return columns, nil
}
