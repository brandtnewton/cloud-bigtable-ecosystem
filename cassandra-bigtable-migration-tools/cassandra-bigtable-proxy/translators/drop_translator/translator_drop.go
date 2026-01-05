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

package drop_translator

import (
	"errors"
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/translators/common"
	"github.com/datastax/go-cassandra-native-protocol/primitive"
)

func (t *DropTranslator) Translate(query *types.RawQuery, sessionKeyspace types.Keyspace) (types.IPreparedQuery, error) {
	dropTableObj, err := query.Parser().DropTable()
	if err != nil {
		return nil, err
	}

	if dropTableObj == nil {
		return nil, errors.New("error while parsing drop table object")
	}

	keyspaceName, tableName, err := common.ParseTableSpec(dropTableObj.TableSpec(), sessionKeyspace)
	if err != nil {
		return nil, err
	}

	ifExists := dropTableObj.IfExist() != nil

	stmt := types.NewDropTableQuery(keyspaceName, tableName, ifExists)
	return stmt, nil
}

func (t *DropTranslator) Bind(st types.IPreparedQuery, values *types.QueryParameterValues, pv primitive.ProtocolVersion) (types.IExecutableQuery, error) {
	drop, ok := st.(*types.DropTableQuery)
	if !ok {
		return nil, fmt.Errorf("cannot bind to %T", st)
	}
	return drop, nil
}
