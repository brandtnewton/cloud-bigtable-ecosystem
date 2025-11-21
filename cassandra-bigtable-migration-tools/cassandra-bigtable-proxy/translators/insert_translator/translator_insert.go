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

package insert_translator

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/common"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

// TranslateInsert() parses Columns from the Insert CqlQuery
//
// Parameters:
//   - queryStr: Read the query, parse its columns, values, table name, type of query and keyspaces etc.
//   - protocolV: Array of Columns Names
//
// Returns: PreparedInsertQuery, build the PreparedInsertQuery and return it with nil value of error. In case of error
// PreparedInsertQuery will return as nil and error will contains the error object

func (t *InsertTranslator) Translate(query *types.RawQuery, sessionKeyspace types.Keyspace, isPreparedQuery bool) (types.IPreparedQuery, error) {
	insertObj := query.Parser().Insert()
	if insertObj == nil {
		return nil, errors.New("could not parse insert object")
	}

	keyspaceName, tableName, err := common.ParseTarget(insertObj, sessionKeyspace, t.schemaMappingConfig)
	if err != nil {
		return nil, err
	}

	tableConfig, err := t.schemaMappingConfig.GetTableConfig(keyspaceName, tableName)
	if err != nil {
		return nil, err
	}

	ifNotExists := insertObj.IfNotExist() != nil

	params := types.NewQueryParameters()
	values := types.NewQueryParameterValues(params)

	assignments, err := parseInsertColumns(insertObj.InsertColumnSpec(), tableConfig, params)
	if err != nil {
		return nil, err
	}

	err = parseInsertValues(insertObj.InsertValuesSpec(), assignments, params, values, isPreparedQuery)
	if err != nil {
		return nil, err
	}

	err = common.GetTimestampInfo(insertObj.UsingTtlTimestamp(), params, values)
	if err != nil {
		return nil, err
	}

	err = common.ValidateRequiredPrimaryKeys(tableConfig, params)
	if err != nil {
		return nil, err
	}

	st := types.NewPreparedInsertQuery(keyspaceName, tableName, ifNotExists, query.RawCql(), params, assignments, values)

	return st, nil
}

func (t *InsertTranslator) Bind(st types.IPreparedQuery, values *types.QueryParameterValues, pv primitive.ProtocolVersion) (types.IExecutableQuery, error) {
	ist, ok := st.(*types.PreparedInsertQuery)
	if !ok {
		return nil, fmt.Errorf("cannot bind to %T", st)
	}
	tableConfig, err := t.schemaMappingConfig.GetTableConfig(ist.Keyspace(), ist.Table())
	if err != nil {
		return nil, err
	}

	rowKey, err := common.BindRowKey(tableConfig, values)
	if err != nil {
		return nil, fmt.Errorf("key encoding failed: %w", err)
	}

	mutations := types.NewBigtableWriteMutation(ist.Keyspace(), ist.Table(), ist.CqlQuery(), types.IfSpec{IfNotExists: ist.IfNotExists}, types.QueryTypeInsert, rowKey)
	err = common.BindMutations(ist.Assignments, values, mutations)
	if err != nil {
		return nil, err
	}
	return mutations, nil
}
