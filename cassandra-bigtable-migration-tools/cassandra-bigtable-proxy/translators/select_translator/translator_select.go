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

package select_translator

import (
	"errors"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	sm "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/schema-mapping"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/common"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func (t *SelectTranslator) Translate(query *types.RawQuery, sessionKeyspace types.Keyspace, isPreparedQuery bool) (types.IPreparedQuery, types.IExecutableQuery, error) {
	selectObj := query.Parser().Select_()
	if selectObj == nil {
		return nil, nil, errors.New("failed to parse select query")
	}

	keyspaceName, tableName, err := common.ParseTarget(selectObj.FromSpec(), sessionKeyspace, t.schemaMappingConfig)
	if err != nil {
		return nil, nil, err
	}

	tableConfig, err := t.schemaMappingConfig.GetTableConfig(keyspaceName, tableName)
	if err != nil {
		return nil, nil, err
	}

	selectClause, err := parseSelectClause(selectObj.SelectElements(), tableConfig)
	if err != nil {
		return nil, nil, err
	}

	params := types.NewQueryParameters()
	values := types.NewQueryParameterValues(params)

	conditions, err := common.ParseWhereClause(selectObj.WhereSpec(), tableConfig, params, values, isPreparedQuery)
	if err != nil {
		return nil, nil, err
	}

	var groupBy []string
	if selectObj.GroupSpec() != nil {
		groupBy = parseGroupByColumn(selectObj.GroupSpec())
	}
	var orderBy types.OrderBy
	if selectObj.OrderSpec() != nil {
		orderBy, err = parseOrderByFromSelect(selectObj.OrderSpec())
		if err != nil {
			// pass the original error to provide proper root cause of error.
			return nil, nil, err
		}
	} else {
		orderBy.IsOrderBy = false
	}

	err = parseLimitClause(selectObj.LimitSpec(), params, values)
	if err != nil {
		return nil, nil, err
	}

	resultColumns := selectedColumnsToMetadata(tableConfig, selectClause)

	st := types.NewPreparedSelectQuery(keyspaceName, tableName, query.RawCql(), "", selectClause, conditions, params, orderBy, groupBy, resultColumns)

	translatedResult, err := createBigtableSql(t, st)
	if err != nil {
		return nil, nil, err
	}
	st.TranslatedQuery = translatedResult

	var bound *types.BoundSelectQuery
	if !isPreparedQuery {
		bound, err = t.doBindSelect(st, values, primitive.ProtocolVersion4)
		if err != nil {
			return nil, nil, err
		}
	} else {
		bound = nil
	}

	if isPreparedQuery {
		err = common.ValidateZeroParamsSet(values)
		if err != nil {
			return nil, nil, err
		}
	}

	return st, bound, nil
}

func selectedColumnsToMetadata(table *sm.TableConfig, selectClause *types.SelectClause) []*message.ColumnMetadata {
	if selectClause.IsStar {
		return table.GetMetadata()
	}

	var resultColumns []*message.ColumnMetadata
	for i, c := range selectClause.Columns {
		var col = message.ColumnMetadata{
			Keyspace: string(table.Keyspace),
			Table:    string(table.Name),
			Name:     string(c.ColumnName),
			Index:    int32(i),
			Type:     c.ResultType.DataType(),
		}
		resultColumns = append(resultColumns, &col)
	}
	return resultColumns
}

func (t *SelectTranslator) Bind(st types.IPreparedQuery, cassandraValues []*primitive.Value, pv primitive.ProtocolVersion) (types.IExecutableQuery, error) {
	sst := st.(*types.PreparedSelectQuery)
	values, err := common.BindQueryParams(sst.Params, cassandraValues, pv)
	if err != nil {
		return nil, err
	}
	return t.doBindSelect(sst, values, pv)
}

func (t *SelectTranslator) doBindSelect(st *types.PreparedSelectQuery, values *types.QueryParameterValues, pv primitive.ProtocolVersion) (*types.BoundSelectQuery, error) {
	query := types.NewBoundSelectQuery(st, pv, values)
	return query, nil
}
