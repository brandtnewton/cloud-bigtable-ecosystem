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

package update_translator

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/common"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func (t *UpdateTranslator) Translate(query *types.RawQuery, sessionKeyspace types.Keyspace, isPreparedQuery bool) (types.IPreparedQuery, *types.QueryParameterValues, error) {
	updateObj := query.Parser().Update()

	if updateObj == nil {
		return nil, nil, errors.New("error parsing the update object")
	}

	keyspaceName, tableName, err := common.ParseTarget(updateObj, sessionKeyspace, t.schemaMappingConfig)
	if err != nil {
		return nil, nil, err
	}

	tableConfig, err := t.schemaMappingConfig.GetTableConfig(keyspaceName, tableName)
	if err != nil {
		return nil, nil, err
	}

	updateObj.KwSet()
	if updateObj.Assignments() == nil || updateObj.Assignments().AllAssignmentElement() == nil {
		return nil, nil, errors.New("error parsing the assignment object")
	}

	assignmentObj := updateObj.Assignments()
	allAssignmentObj := assignmentObj.AllAssignmentElement()
	if allAssignmentObj == nil {
		return nil, nil, errors.New("error parsing all the assignment object")
	}

	params := types.NewQueryParameters()
	values := types.NewQueryParameterValues(params)

	assignments, err := parseUpdateValues(allAssignmentObj, tableConfig, params, values)
	if err != nil {
		return nil, nil, err
	}

	if updateObj.WhereSpec() != nil {
		return nil, nil, errors.New("error parsing update where clause")
	}

	whereClause, err := common.ParseWhereClause(updateObj.WhereSpec(), tableConfig, params, values, isPreparedQuery)
	if err != nil {
		return nil, nil, err
	}

	err = common.GetTimestampInfo(updateObj.UsingTtlTimestamp(), params, values)
	if err != nil {
		return nil, nil, err
	}

	var ifExist = updateObj.IfExist() != nil

	err = common.ValidateRequiredPrimaryKeysOnly(tableConfig, params)
	if err != nil {
		return nil, nil, err
	}

	st := types.NewPreparedUpdateQuery(keyspaceName, tableName, ifExist, query.RawCql(), assignments, whereClause, params)

	return st, values, nil
}

func (t *UpdateTranslator) Bind(st types.IPreparedQuery, values *types.QueryParameterValues, pv primitive.ProtocolVersion) (types.IExecutableQuery, error) {
	ust, ok := st.(*types.PreparedUpdateQuery)
	if !ok {
		return nil, fmt.Errorf("cannot bind to %T", st)
	}
	tableConfig, err := t.schemaMappingConfig.GetTableConfig(ust.Keyspace(), ust.Table())
	if err != nil {
		return nil, err
	}

	rowKey, err := common.BindRowKey(tableConfig, values)
	if err != nil {
		return nil, err
	}

	mutations := types.NewBigtableWriteMutation(ust.Keyspace(), ust.Table(), ust.CqlQuery(), types.IfSpec{IfExists: ust.IfExists}, types.QueryTypeUpdate, rowKey)
	err = common.BindMutations(ust.Values, values, mutations)
	if err != nil {
		return nil, err
	}

	return mutations, err
}
