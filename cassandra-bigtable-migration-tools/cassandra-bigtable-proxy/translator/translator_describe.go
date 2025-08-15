package translator

import (
	"errors"

	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/antlr4-go/antlr/v4"
)

func (t *Translator) TranslateDesc(query, sessionKeyspace string) (*DescribeStatementMap, error) {
	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	p := cql.NewCqlParser(stream)

	describeTable := p.DescribeStatement()

	target := describeTable.DescribeTarget()
	if target == nil {
		return nil, errors.New("invalid desc command")
	}

	//
	if target.KwKeyspace() != nil || target.KwKeyspaces() != nil {

	}

	// "DESC TABLES"
	if target.KwTables() != nil {
		return &DescribeStatementMap{
			Keyspace: sessionKeyspace,
			Table:    "",
		}, nil
	}

	// else "DESC TABLE ..."
	var keyspace string
	if target.Keyspace() != nil && !target.Keyspace().IsEmpty() {
		keyspace = target.Keyspace().GetText()
	} else if sessionKeyspace != "" {
		keyspace = sessionKeyspace
	} else {
		// todo confirm this is correct - maybe we just need system tables
		return nil, errors.New("no keyspace provided")
	}

	var table string
	if target.Table() != nil && !target.Table().IsEmpty() {
		table = target.Table().GetText()
	} else {
		return nil, errors.New("no table name provided")
	}

	return &DescribeStatementMap{
		Keyspace: keyspace,
		Table:    table,
	}, nil
}
