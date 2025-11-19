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
	types "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/common"
	"github.com/antlr4-go/antlr/v4"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func (t *UpdateTranslator) Translate(query string, sessionKeyspace types.Keyspace, isPreparedQuery bool) (types.IPreparedQuery, types.IExecutableQuery, error) {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)
	updateObj := p.Update()

	if updateObj == nil || updateObj.KwUpdate() == nil {
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

	whereClause, err := common.ParseWhereClause(updateObj.WhereSpec(), tableConfig, params, values)
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

	st := &PreparedUpdateQuery{
		cqlQuery: query,
		table:    tableName,
		IfExists: ifExist,
		keyspace: keyspaceName,
		Clauses:  whereClause,
		params:   params,
		Values:   assignments,
	}

	var bound *common.BigtableWriteMutation
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

func (t *UpdateTranslator) Bind(st types.IPreparedQuery, cassandraValues []*primitive.Value, pv primitive.ProtocolVersion) (types.IExecutableQuery, error) {
	ust := st.(*PreparedUpdateQuery)
	values, err := common.BindQueryParams(ust.params, cassandraValues, pv)
	if err != nil {
		return nil, err
	}
	return t.doBind(ust, values)
}

func (t *UpdateTranslator) doBind(st *PreparedUpdateQuery, values *types.QueryParameterValues) (*common.BigtableWriteMutation, error) {
	tableConfig, err := t.schemaMappingConfig.GetTableConfig(st.Keyspace(), st.Table())
	if err != nil {
		return nil, err
	}

	rowKey, err := common.BindRowKey(tableConfig, values)
	if err != nil {
		return nil, err
	}

	mutations := common.NewBigtableWriteMutation(st.keyspace, st.table, types.QueryTypeUpdate, rowKey)
	err = common.BindMutations(st.Values, values, mutations)
	if err != nil {
		return nil, err
	}

	return mutations, err
}
