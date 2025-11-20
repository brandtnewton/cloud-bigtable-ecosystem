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

package alter_translator

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/common"
	"github.com/datastax/go-cassandra-native-protocol/primitive"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
)

func (t *AlterTranslator) Translate(query *types.RawQuery, sessionKeyspace types.Keyspace, isPreparedQuery bool) (types.IPreparedQuery, types.IExecutableQuery, error) {
	alterTable := query.Parser().AlterTable()

	if alterTable == nil || alterTable.Table() == nil {
		return nil, nil, errors.New("error while parsing alter statement")
	}

	keyspaceName, tableName, err := common.ParseTarget(alterTable, sessionKeyspace, t.schemaMappingConfig)
	if err != nil {
		return nil, nil, err
	}

	tableConfig, err := t.schemaMappingConfig.GetTableConfig(keyspaceName, tableName)
	if err != nil {
		return nil, nil, err
	}

	if alterTable.AlterTableOperation().AlterTableAlterColumnTypes() != nil {
		return nil, nil, errors.New("alter column type operations are not supported")
	}

	if alterTable.AlterTableOperation().AlterTableWith() != nil {
		return nil, nil, errors.New("table property operations are not supported")
	}

	var dropColumns []types.ColumnName
	if alterTable.AlterTableOperation().AlterTableDropColumns() != nil {
		for _, dropColumn := range alterTable.AlterTableOperation().AlterTableDropColumns().AlterTableDropColumnList().AllColumn() {
			dropColumns = append(dropColumns, types.ColumnName(dropColumn.GetText()))
		}
	}
	var addColumns []types.CreateColumn
	if alterTable.AlterTableOperation().AlterTableAdd() != nil {
		for i, addColumn := range alterTable.AlterTableOperation().AlterTableAdd().AlterTableColumnDefinition().AllColumn() {
			dt, err := utilities.ParseCqlType(alterTable.AlterTableOperation().AlterTableAdd().AlterTableColumnDefinition().DataType(i))
			if err != nil {
				return nil, nil, err
			}

			addColumns = append(addColumns, types.CreateColumn{
				TypeInfo: dt,
				Name:     types.ColumnName(addColumn.GetText()),
				Index:    int32(i),
			})
		}
	}

	if alterTable.AlterTableOperation().AlterTableRename() != nil && len(alterTable.AlterTableOperation().AlterTableRename().AllColumn()) != 0 {
		return nil, nil, errors.New("rename operation in alter table command not supported")
	}

	for _, dropColumn := range dropColumns {
		column, err := tableConfig.GetColumn(dropColumn)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot drop column: %w", err)
		}
		if column.IsPrimaryKey {
			return nil, nil, fmt.Errorf("cannot drop primary key column: '%s'", column.Name)
		}
	}
	for _, addColumn := range addColumns {
		if !utilities.IsSupportedColumnType(addColumn.TypeInfo) {
			return nil, nil, fmt.Errorf("column type '%s' is not supported", addColumn.TypeInfo.String())
		}
		if utilities.IsReservedCqlKeyword(string(addColumn.Name)) {
			return nil, nil, fmt.Errorf("cannot alter a table with reserved keyword as column name: '%s'", addColumn.Name)
		}
		if tableConfig.HasColumn(addColumn.Name) {
			return nil, nil, fmt.Errorf("column '%s' already exists in table", addColumn.Name)
		}
	}

	var stmt = types.NewAlterTableStatementMap(keyspaceName, tableName, query.RawCql(), false, addColumns, dropColumns)
	return nil, stmt, nil
}

func (t *AlterTranslator) Bind(types.IPreparedQuery, []*primitive.Value, primitive.ProtocolVersion) (types.IExecutableQuery, error) {
	return nil, errors.New("bind for alter statements not supported")
}
