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
	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
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

func (t *InsertTranslator) Translate(query string, sessionKeyspace types.Keyspace, isPreparedQuery bool) (types.IPreparedQuery, types.IExecutableQuery, error) {
	p, err := common.NewCqlParser(query, false)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse cql")
	}

	insertObj := p.Insert()
	if insertObj == nil {
		return nil, nil, errors.New("could not parse insert object")
	}
	kwInsertObj := insertObj.KwInsert()
	if kwInsertObj == nil {
		return nil, nil, errors.New("could not parse insert object")
	}
	insertObj.KwInto()

	keyspaceName, tableName, err := common.ParseTarget(insertObj, sessionKeyspace, t.schemaMappingConfig)
	if err != nil {
		return nil, nil, err
	}

	tableConfig, err := t.schemaMappingConfig.GetTableConfig(keyspaceName, tableName)
	if err != nil {
		return nil, nil, err
	}

	ifNotExists := insertObj.IfNotExist() != nil

	params := types.NewQueryParameters()
	values := types.NewQueryParameterValues(params)

	assignments, err := parseInsertColumns(insertObj.InsertColumnSpec(), tableConfig, params)
	if err != nil {
		return nil, nil, err
	}

	err = parseInsertValues(insertObj.InsertValuesSpec(), assignments, params, values, isPreparedQuery)
	if err != nil {
		return nil, nil, err
	}

	err = common.GetTimestampInfo(insertObj.UsingTtlTimestamp(), params, values)
	if err != nil {
		return nil, nil, err
	}

	err = common.ValidateRequiredPrimaryKeys(tableConfig, params)
	if err != nil {
		return nil, nil, err
	}

	st := types.NewPreparedInsertQuery(keyspaceName, tableName, ifNotExists, query, params, assignments)
	var bound *types.BigtableWriteMutation
	if !isPreparedQuery {
		bound, err = t.doBindInsert(st, values)
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

func (t *InsertTranslator) Bind(st types.IPreparedQuery, cassandraValues []*primitive.Value, pv primitive.ProtocolVersion) (types.IExecutableQuery, error) {
	ist := st.(*types.PreparedInsertQuery)
	values, err := common.BindQueryParams(ist.Parameters(), cassandraValues, pv)
	if err != nil {
		return nil, err
	}
	return t.doBindInsert(ist, values)
}

func (t *InsertTranslator) doBindInsert(st *types.PreparedInsertQuery, values *types.QueryParameterValues) (*types.BigtableWriteMutation, error) {
	tableConfig, err := t.schemaMappingConfig.GetTableConfig(st.Keyspace(), st.Table())
	if err != nil {
		return nil, err
	}

	rowKey, err := common.BindRowKey(tableConfig, values)
	if err != nil {
		return nil, fmt.Errorf("key encoding failed: %w", err)
	}

	mutations := types.NewBigtableWriteMutation(st.Keyspace(), st.Table(), types.IfSpec{IfNotExists: st.IfNotExists}, types.QueryTypeInsert, rowKey)
	err = common.BindMutations(st.Assignments, values, mutations)
	if err != nil {
		return nil, err
	}

	return mutations, nil
}
