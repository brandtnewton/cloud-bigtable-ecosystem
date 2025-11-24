// Copyright (c) DataStax, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/antlr4-go/antlr/v4"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSelect(t *testing.T) {
	p := newParser("select * from system.local where foo=1;")

	s := p.Select_()

	require.NotNil(t, s)

	require.NotNil(t, s.SelectElements())
	require.NotNil(t, s.SelectElements().STAR())

	require.NotNil(t, s.FromSpec())
	require.NotNil(t, s.FromSpec().Keyspace())
	require.Equal(t, s.FromSpec().Keyspace().GetText(), "system")
	require.NotNil(t, s.FromSpec().Table())
	require.Equal(t, s.FromSpec().Table().GetText(), "local")

	require.NotNil(t, s.SelectElements())
	require.NotNil(t, s.SelectElements().STAR())
	require.NotNil(t, s.WhereSpec())
}
func TestSelectWithKeyColumn(t *testing.T) {
	p := newParser(`select * from system.local where "key"='foo'`)

	s := p.Select_()

	require.NotNil(t, s)

	require.NotNil(t, s.SelectElements())
	require.NotNil(t, s.SelectElements().STAR())

	require.NotNil(t, s.FromSpec())
	require.NotNil(t, s.FromSpec().Keyspace())
	require.Equal(t, s.FromSpec().Keyspace().GetText(), "system")
	require.NotNil(t, s.FromSpec().Table())
	require.Equal(t, s.FromSpec().Table().GetText(), "local")

	require.NotNil(t, s.SelectElements())
	require.NotNil(t, s.SelectElements().STAR())
	require.NotNil(t, s.WhereSpec())
	require.NotNil(t, s.WhereSpec().RelationElements())
	require.NotNil(t, s.WhereSpec().RelationElements().AllRelationElement())
	elements := s.WhereSpec().RelationElements().AllRelationElement()
	require.Equal(t, 1, len(elements))
	require.NotNil(t, elements[0].RelationCompare())
	require.Equal(t, "\"key\"", elements[0].RelationCompare().Column().OBJECT_NAME().GetText())
	require.Equal(t, "'foo'", elements[0].RelationCompare().Constant().StringLiteral().GetText())
}

func TestSelectWithKeyUnquotedColumn(t *testing.T) {
	p := newParser(`select * from system.local where key='foo'`)

	s := p.Select_()

	require.NotNil(t, s)

	require.NotNil(t, s.SelectElements())
	require.NotNil(t, s.SelectElements().STAR())

	require.NotNil(t, s.FromSpec())
	require.NotNil(t, s.FromSpec().Keyspace())
	require.Equal(t, s.FromSpec().Keyspace().GetText(), "system")
	require.NotNil(t, s.FromSpec().Table())
	require.Equal(t, s.FromSpec().Table().GetText(), "local")

	require.NotNil(t, s.SelectElements())
	require.NotNil(t, s.SelectElements().STAR())
	require.NotNil(t, s.WhereSpec())
	require.NotNil(t, s.WhereSpec().RelationElements())
	require.NotNil(t, s.WhereSpec().RelationElements().AllRelationElement())
	elements := s.WhereSpec().RelationElements().AllRelationElement()
	require.Equal(t, 1, len(elements))
	require.NotNil(t, elements[0].RelationCompare())
	require.Equal(t, "key", elements[0].RelationCompare().Column().K_KEY().GetText())
	require.Equal(t, "'foo'", elements[0].RelationCompare().Constant().StringLiteral().GetText())
}

func TestUpdateWhere(t *testing.T) {
	p := newParser("UPDATE bigtabledevinstance.user_info SET code = ? WHERE name = ? AND age = ?")

	u := p.Update()
	require.NotNil(t, u)

	require.NotNil(t, u.WhereSpec())
	require.Equal(t, 2, len(u.WhereSpec().RelationElements().AllRelationElement()))
}

func newParser(query string) *cql.CqlParser {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	return cql.NewCqlParser(stream)
}
