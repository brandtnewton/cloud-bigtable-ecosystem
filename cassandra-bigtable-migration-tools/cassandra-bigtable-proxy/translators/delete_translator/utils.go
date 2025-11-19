package delete_translator

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	schemaMapping "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators"
)

// Parses the delete columns from a CQL DELETE statement and returns the selected columns with their associated map keys or list indices.
func parseDeleteColumns(deleteColumns cql.IDeleteColumnListContext, tableConfig *schemaMapping.TableConfig, params *types.QueryParameters, values *types.QueryParameterValues) ([]translators.SelectedColumn, error) {
	if deleteColumns == nil {
		return nil, nil
	}
	cols := deleteColumns.AllDeleteColumnItem()
	var columns []translators.SelectedColumn
	for _, v := range cols {
		var col translators.SelectedColumn
		col.Sql = v.OBJECT_NAME().GetText()
		if v.LS_BRACKET() != nil {
			if v.DecimalLiteral() != nil { // for list index
				p, err := translators.ParseDecimalLiteral(v.DecimalLiteral(), types.TypeInt, params, values)
				if err != nil {
					return nil, err
				}
				col.ListIndex = p
			} else if v.StringLiteral() != nil { //for map Key
				p, err := translators.ParseStringLiteral(v.StringLiteral(), params, values)
				if err != nil {
					return nil, err
				}
				col.MapKey = p
			} else {
				return nil, errors.New("unhandled delete column clause")
			}
		}
		if !tableConfig.HasColumn(types.ColumnName(col.Sql)) {
			return nil, fmt.Errorf("unknown column `%s`", col.Sql)
		}
		columns = append(columns, col)
	}
	return columns, nil
}
