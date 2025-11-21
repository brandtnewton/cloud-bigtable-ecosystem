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

package truncate_translator

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/common"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func (t *TruncateTranslator) Translate(query *types.RawQuery, sessionKeyspace types.Keyspace, isPreparedQuery bool) (types.IPreparedQuery, *types.QueryParameterValues, error) {
	truncateTableObj := query.Parser().Truncate()

	if truncateTableObj == nil {
		return nil, nil, errors.New("error while parsing truncate query")
	}

	keyspace, table, err := common.ParseTarget(truncateTableObj, sessionKeyspace, t.schemaMappingConfig)
	if err != nil {
		return nil, nil, err
	}

	stmt := types.NewTruncateTableStatementMap(keyspace, table, query.RawCql())
	return stmt, nil, nil
}

func (t *TruncateTranslator) Bind(st types.IPreparedQuery, _ *types.QueryParameterValues, _ primitive.ProtocolVersion) (types.IExecutableQuery, error) {
	truncate, ok := st.(types.TruncateTableStatementMap)
	if !ok {
		return nil, fmt.Errorf("cannot bind to %T", st)
	}
	return truncate, nil
}
