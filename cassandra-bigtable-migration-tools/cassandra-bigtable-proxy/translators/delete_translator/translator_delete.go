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

package delete_translator

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/common"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func (t *DeleteTranslator) Translate(query *types.RawQuery, sessionKeyspace types.Keyspace, isPreparedQuery bool) (types.IPreparedQuery, types.IExecutableQuery, error) {
	deleteObj := query.Parser()
	if deleteObj == nil {
		return nil, nil, errors.New("error while parsing delete object")
	}

	keyspaceName, tableName, err := common.ParseTarget(deleteObj.FromSpec(), sessionKeyspace, t.schemaMappingConfig)
	if err != nil {
		return nil, nil, err
	}

	tableConfig, err := t.schemaMappingConfig.GetTableConfig(keyspaceName, tableName)
	if err != nil {
		return nil, nil, err
	}

	var ifExist = deleteObj.IfExist() != nil

	params := types.NewQueryParameters()
	values := types.NewQueryParameterValues(params)

	selectedColumns, err := parseDeleteColumns(deleteObj.DeleteColumnList(), tableConfig, params, values)
	if err != nil {
		return nil, nil, err
	}

	conditions, err := common.ParseWhereClause(deleteObj.WhereSpec(), tableConfig, params, values, isPreparedQuery)
	if err != nil {
		return nil, nil, err
	}

	for _, condition := range conditions {
		if condition.Operator != "=" {
			return nil, nil, fmt.Errorf("primary key conditions can only be equals")
		}
	}

	err = common.ValidateRequiredPrimaryKeysOnly(tableConfig, params)
	if err != nil {
		return nil, nil, err
	}

	if params.Has(types.UsingTimePlaceholder) {
		return nil, nil, fmt.Errorf("delete USING TIMESTAMP not supported")
	}

	st := types.NewPreparedDeleteQuery(keyspaceName, tableName, ifExist, query.RawCql(), conditions, params, selectedColumns)

	var bound *types.BoundDeleteQuery
	if !isPreparedQuery {
		bound, err = t.doBind(st, values)
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

func (t *DeleteTranslator) Bind(st types.IPreparedQuery, cassandraValues []*primitive.Value, pv primitive.ProtocolVersion) (types.IExecutableQuery, error) {
	dst := st.(*types.PreparedDeleteQuery)
	values, err := common.BindQueryParams(dst.Parameters(), cassandraValues, pv)
	if err != nil {
		return nil, err
	}
	return t.doBind(dst, values)
}

func (t *DeleteTranslator) doBind(st *types.PreparedDeleteQuery, values *types.QueryParameterValues) (*types.BoundDeleteQuery, error) {
	tableConfig, err := t.schemaMappingConfig.GetTableConfig(st.Keyspace(), st.Table())
	if err != nil {
		return nil, err
	}

	rowKey, err := common.BindRowKey(tableConfig, values)
	if err != nil {
		return nil, fmt.Errorf("key encoding failed: %w", err)
	}

	cols, err := common.BindSelectColumns(tableConfig, st.SelectedColumns, values)
	if err != nil {
		return nil, err
	}
	return types.NewBoundDeleteQuery(st.Keyspace(), st.Table(), st.CqlQuery(), rowKey, st.IfExists, cols), nil
}
