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
	"fmt"
	"github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/global/types"

	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/antlr4-go/antlr/v4"
)

// antlrErrorListener collects syntax errors from ANTLR parsing
type antlrErrorListener struct {
	antlr.DefaultErrorListener
	errors []string
}

func (l *antlrErrorListener) SyntaxError(recognizer antlr.Recognizer, offendingSymbol interface{}, line, column int, msg string, e antlr.RecognitionException) {
	l.errors = append(l.errors, msg)
}

func (t *Translator) TranslateDrop(query string, sessionKeyspace types.Keyspace) (*DropTableStatementMap, error) {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	errListener := &antlrErrorListener{}
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(errListener)
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)
	p.RemoveErrorListeners()
	p.AddErrorListener(errListener)

	dropTableObj := p.DropTable()

	// Check for syntax errors after parsing
	if len(errListener.errors) > 0 {
		return nil, errors.New("syntax error in DROP TABLE statement: " + errListener.errors[0])
	}

	if dropTableObj == nil || dropTableObj.Table() == nil {
		return nil, errors.New("error while parsing drop table object")
	}

	var keyspaceName types.Keyspace
	var tableName types.TableName

	if dropTableObj != nil && dropTableObj.Table() != nil && dropTableObj.Table().GetText() != "" {
		tableNameString := dropTableObj.Table().GetText()
		if !validTableName.MatchString(tableNameString) {
			return nil, fmt.Errorf("invalid table name parsed from query")
		}
		tableName = types.TableName(tableNameString)
	} else {
		return nil, fmt.Errorf("invalid input paramaters found for table")
	}

	if tableName == t.SchemaMappingConfig.SchemaMappingTableName {
		return nil, fmt.Errorf("cannot drop the configured schema mapping table name '%s'", tableName)
	}

	if dropTableObj != nil && dropTableObj.Keyspace() != nil && dropTableObj.Keyspace().GetText() != "" {
		keyspaceName = types.Keyspace(dropTableObj.Keyspace().GetText())
	} else if sessionKeyspace != "" {
		keyspaceName = sessionKeyspace
	} else {
		return nil, fmt.Errorf("missing keyspace. keyspace is required")
	}

	var stmt = DropTableStatementMap{
		Keyspace: keyspaceName,
		Table:    tableName,
		IfExists: dropTableObj.IfExist() != nil,
	}

	return &stmt, nil
}
