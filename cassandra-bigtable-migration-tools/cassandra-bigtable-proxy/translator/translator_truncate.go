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

package translator

import (
	"errors"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"

	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/antlr4-go/antlr/v4"
)

func (t *Translator) TranslateTruncateTableToBigtable(query string, sessionKeyspace types.Keyspace) (*TruncateTableStatementMap, error) {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)

	truncateTableObj := p.Truncate()

	if truncateTableObj == nil {
		return nil, errors.New("error while parsing truncate query")
	}

	keyspaceName, tableName, err := parseTarget(truncateTableObj, sessionKeyspace, t.SchemaMappingConfig)
	if err != nil {
		return nil, err
	}

	var stmt = TruncateTableStatementMap{
		Keyspace: keyspaceName,
		Table:    tableName,
	}

	return &stmt, nil
}
