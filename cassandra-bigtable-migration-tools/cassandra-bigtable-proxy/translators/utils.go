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

package translators

import (
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func (t *Translator) getTranslator(queryType types.QueryType) (types.IQueryTranslator, error) {
	translator, ok := t.translators[queryType]
	if !ok {
		return nil, fmt.Errorf("unhandled query type '%s'", queryType.String())
	}
	return translator, nil
}

func (t *Translator) TranslateQuery(queryType types.QueryType, sessionKeyspace types.Keyspace, query string, isPrepared bool) (types.IPreparedQuery, types.IExecutableQuery, error) {
	queryTranslator, err := t.getTranslator(queryType)
	if err != nil {
		return nil, nil, err
	}

	return queryTranslator.Translate(query, sessionKeyspace, isPrepared)
}

func (t *Translator) BindQuery(st types.IPreparedQuery, cassandraValues []*primitive.Value, pv primitive.ProtocolVersion) (types.IExecutableQuery, error) {
	queryTranslator, err := t.getTranslator(st.QueryType())
	if err != nil {
		return nil, err
	}

	return queryTranslator.Bind(st, cassandraValues, pv)
}
