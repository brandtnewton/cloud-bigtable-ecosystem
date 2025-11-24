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

package create_translator

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/common"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
	"slices"

	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/utilities"
)

const (
	intRowKeyEncodingOptionName             = "int_row_key_encoding"
	intRowKeyEncodingOptionValueBigEndian   = "big_endian"
	intRowKeyEncodingOptionValueOrderedCode = "ordered_code"
)

func (t *CreateTranslator) Translate(query *types.RawQuery, sessionKeyspace types.Keyspace) (types.IPreparedQuery, error) {
	createTableObj := query.Parser().CreateTable()

	if createTableObj == nil {
		return nil, errors.New("error while parsing create object")
	}

	keyspaceName, tableName, err := common.ParseTarget(createTableObj.TableSpec(), sessionKeyspace, t.schemaMappingConfig)
	if err != nil {
		return nil, err
	}

	var pmks []types.CreateTablePrimaryKeyConfig
	var columns []types.CreateColumn

	if createTableObj.ColumnDefinitionList().GetText() == "" {
		return nil, errors.New("malformed create table statement")
	}

	for i, col := range createTableObj.ColumnDefinitionList().AllColumnDefinition() {
		dt, err := utilities.ParseCqlType(col.DataType())
		if err != nil {
			return nil, err
		}

		if !utilities.IsSupportedColumnType(dt) {
			return nil, fmt.Errorf("column type '%s' is not supported", dt.String())
		}

		columns = append(columns, types.CreateColumn{
			TypeInfo: dt,
			Name:     types.ColumnName(col.Column().GetText()),
			Index:    int32(i),
		})

		if col.PrimaryKeyColumn() != nil {
			pmks = append(pmks, types.CreateTablePrimaryKeyConfig{
				Name:    types.ColumnName(col.Column().GetText()),
				KeyType: types.KeyTypePartition,
			})
		}
	}

	if len(pmks) > 1 {
		return nil, errors.New("multiple inline primary key columns not allowed")
	}

	if len(columns) == 0 {
		return nil, errors.New("no columns found in create table statement")
	}

	rowKeyEncoding := t.defaultIntRowKeyEncoding
	for optionName, optionValue := range createOptionsMap(createTableObj.WithElement()) {
		switch optionName {
		case intRowKeyEncodingOptionName:
			optionValue = common.TrimQuotes(optionValue)
			switch optionValue {
			case intRowKeyEncodingOptionValueBigEndian:
				rowKeyEncoding = types.BigEndianEncoding
			case intRowKeyEncodingOptionValueOrderedCode:
				rowKeyEncoding = types.OrderedCodeEncoding
			default:
				return nil, fmt.Errorf("unsupported encoding '%s' for option '%s'", optionValue, optionName)
			}
		default:
			// fail fast, so the user know we don't support the option rather than silently ignoring it.
			return nil, fmt.Errorf("unsupported table option: '%s'", optionName)
		}
	}

	// nil if inline primary key definition used
	if createTableObj.ColumnDefinitionList().PrimaryKeyElement() != nil {
		if len(pmks) > 0 {
			return nil, errors.New("cannot specify both primary key clause and inline primary key")
		}
		singleKey := createTableObj.ColumnDefinitionList().PrimaryKeyElement().PrimaryKeyDefinition().SinglePrimaryKey()
		compoundKey := createTableObj.ColumnDefinitionList().PrimaryKeyElement().PrimaryKeyDefinition().CompoundKey()
		compositeKey := createTableObj.ColumnDefinitionList().PrimaryKeyElement().PrimaryKeyDefinition().CompositeKey()
		if singleKey != nil {
			pmks = []types.CreateTablePrimaryKeyConfig{
				{
					Name:    types.ColumnName(singleKey.GetText()),
					KeyType: types.KeyTypePartition,
				},
			}
		} else if compoundKey != nil {
			pmks = append(pmks, types.CreateTablePrimaryKeyConfig{
				Name:    types.ColumnName(compoundKey.PartitionKey().GetText()),
				KeyType: types.KeyTypePartition,
			})
			for _, clusterKey := range compoundKey.ClusteringKeyList().AllClusteringKey() {
				pmks = append(pmks, types.CreateTablePrimaryKeyConfig{
					Name:    types.ColumnName(clusterKey.Column().GetText()),
					KeyType: types.KeyTypeClustering,
				})
			}
		} else if compositeKey != nil {
			for _, partitionKey := range compositeKey.PartitionKeyList().AllPartitionKey() {
				pmks = append(pmks, types.CreateTablePrimaryKeyConfig{
					Name:    types.ColumnName(partitionKey.Column().GetText()),
					KeyType: types.KeyTypePartition,
				})
			}
			for _, clusterKey := range compositeKey.ClusteringKeyList().AllClusteringKey() {
				pmks = append(pmks, types.CreateTablePrimaryKeyConfig{
					Name:    types.ColumnName(clusterKey.Column().GetText()),
					KeyType: types.KeyTypeClustering,
				})
			}
		}
	}

	if utilities.IsReservedCqlKeyword(string(tableName)) {
		return nil, fmt.Errorf("table name cannot be reserved cql word: '%s'", tableName)
	}
	for _, column := range columns {
		if utilities.IsReservedCqlKeyword(string(column.Name)) {
			return nil, fmt.Errorf("cannot create a table with reserved keyword as column name: '%s'", column.Name)
		}
	}

	if len(pmks) == 0 {
		return nil, errors.New("no primary key found in create table statement")
	}

	for _, pmk := range pmks {
		colIndex := slices.IndexFunc(columns, func(col types.CreateColumn) bool {
			return col.Name == pmk.Name
		})
		if colIndex == -1 {
			return nil, fmt.Errorf("primary key '%s' has no column definition in create table statement", pmk.Name)
		}
		col := columns[colIndex]
		if !utilities.IsSupportedPrimaryKeyType(col.TypeInfo) {
			return nil, fmt.Errorf("primary key cannot be of type %s", col.TypeInfo.String())
		}
	}
	ifNotExists := createTableObj.IfNotExist() != nil
	var stmt = types.NewCreateTableStatementMap(keyspaceName, tableName, query.RawCql(), ifNotExists, columns, pmks, rowKeyEncoding)
	return stmt, nil
}

func (t *CreateTranslator) Bind(q types.IPreparedQuery, values *types.QueryParameterValues, pv primitive.ProtocolVersion) (types.IExecutableQuery, error) {
	alter, ok := q.(*types.CreateTableStatementMap)
	if !ok {
		return nil, fmt.Errorf("cannot bind to %T", q)
	}
	return alter, nil
}
