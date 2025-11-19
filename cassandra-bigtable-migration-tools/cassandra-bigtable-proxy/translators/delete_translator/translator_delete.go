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
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/common"
	"github.com/antlr4-go/antlr/v4"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func (t *DeleteTranslator) Translate(query string, sessionKeyspace types.Keyspace, isPreparedQuery bool) (types.IPreparedQuery, types.IExecutableQuery, error) {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)

	deleteObj := p.Delete_()
	if deleteObj == nil || deleteObj.KwDelete() == nil {
		return nil, nil, errors.New("error while parsing delete object")
	}

	keyspaceName, tableName, err := translators.ParseTarget(deleteObj.FromSpec(), sessionKeyspace, t.schemaMappingConfig)
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

	conditions, err := translators.ParseWhereClause(deleteObj.WhereSpec(), tableConfig, params, values)
	if err != nil {
		return nil, nil, err
	}

	for _, condition := range conditions {
		if condition.Operator != "=" {
			return nil, nil, fmt.Errorf("primary key conditions can only be equals")
		}
	}

	err = translators.ValidateRequiredPrimaryKeysOnly(tableConfig, params)
	if err != nil {
		return nil, nil, err
	}

	if params.Has(types.UsingTimePlaceholder) {
		return nil, nil, fmt.Errorf("delete USING TIMESTAMP not supported")
	}

	st := &PreparedDeleteQuery{
		cqlQuery:        query,
		table:           tableName,
		keyspace:        keyspaceName,
		Conditions:      conditions,
		Params:          params,
		IfExists:        ifExist,
		SelectedColumns: selectedColumns,
	}

	var bound *BoundDeleteQuery
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
	dst := st.(*PreparedDeleteQuery)
	values, err := common.BindQueryParams(dst.Params, cassandraValues, pv)
	if err != nil {
		return nil, err
	}
	return t.doBind(dst, values)
}

func (t *DeleteTranslator) doBind(st *PreparedDeleteQuery, values *types.QueryParameterValues) (*BoundDeleteQuery, error) {
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
	return &BoundDeleteQuery{
		keyspace: st.Keyspace(),
		table:    st.Table(),
		IfExists: st.IfExists,
		rowKey:   rowKey,
		Columns:  cols,
	}, nil
}
