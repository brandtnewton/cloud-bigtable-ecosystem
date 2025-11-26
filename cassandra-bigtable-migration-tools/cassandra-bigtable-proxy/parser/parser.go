package parser

import (
	"fmt"
	cql "github.com/GoogleCloudPlatform/cloud-bigtable-ecosystem/cassandra-bigtable-migration-tools/cassandra-bigtable-proxy/third_party/cqlparser"
	"github.com/antlr4-go/antlr/v4"
	"strings"
)

type syntaxErrorListener struct {
	*antlr.DefaultErrorListener
	errs []string
}

func (l *syntaxErrorListener) ValidateNoErrors() error {
	if len(l.errs) > 0 {
		return fmt.Errorf("parsing error: %s", strings.Join(l.errs, ", "))
	}
	return nil
}

// SyntaxError is called when the parser encounters a syntax error.
func (l *syntaxErrorListener) SyntaxError(
	recognizer antlr.Recognizer,
	offendingSymbol interface{},
	line, column int,
	msg string,
	e antlr.RecognitionException,
) {
	// Format a clear error message with location
	l.errs = append(l.errs, msg)

	recognizer.SetError(e)
}

func NewParser(query string) *ProxyCqlParser {
	errorListener := &syntaxErrorListener{
		DefaultErrorListener: antlr.NewDefaultErrorListener(),
		errs:                 make([]string, 0),
	}

	lexer := cql.NewCqlLexer(antlr.NewInputStream(query))
	lexer.AddErrorListener(errorListener)

	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	parser := cql.NewCqlParser(stream)
	parser.AddErrorListener(errorListener)

	return &ProxyCqlParser{
		p:             parser,
		errorListener: errorListener,
	}
}

type ProxyCqlParser struct {
	p             *cql.CqlParser
	errorListener *syntaxErrorListener
}

func (p *ProxyCqlParser) ValidateNoErrors() error {
	if len(p.errorListener.errs) > 0 {
		return fmt.Errorf("parsing error: %s", strings.Join(p.errorListener.errs, ", "))
	}
	return nil
}

func (p *ProxyCqlParser) AlterTable() (cql.IAlterTableContext, error) {
	result := p.p.AlterTable()
	err := p.ValidateNoErrors()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (p *ProxyCqlParser) Delete_() (cql.IDelete_Context, error) {
	result := p.p.Delete_()
	err := p.ValidateNoErrors()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (p *ProxyCqlParser) Use_() (cql.IUse_Context, error) {
	result := p.p.Use_()
	err := p.ValidateNoErrors()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (p *ProxyCqlParser) Select_() (cql.ISelect_Context, error) {
	result := p.p.Select_()
	err := p.ValidateNoErrors()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (p *ProxyCqlParser) Truncate() (cql.ITruncateContext, error) {
	result := p.p.Truncate()
	err := p.ValidateNoErrors()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (p *ProxyCqlParser) DescribeStatement() (cql.IDescribeStatementContext, error) {
	result := p.p.DescribeStatement()
	err := p.ValidateNoErrors()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (p *ProxyCqlParser) CreateTable() (cql.ICreateTableContext, error) {
	result := p.p.CreateTable()
	err := p.ValidateNoErrors()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (p *ProxyCqlParser) DropTable() (cql.IDropTableContext, error) {
	result := p.p.DropTable()
	err := p.ValidateNoErrors()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (p *ProxyCqlParser) Update() (cql.IUpdateContext, error) {
	result := p.p.Update()
	err := p.ValidateNoErrors()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (p *ProxyCqlParser) Insert() (cql.IInsertContext, error) {
	result := p.p.Insert()
	err := p.ValidateNoErrors()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (p *ProxyCqlParser) GetFirstToken() antlr.Token {
	return p.p.GetTokenStream().LT(1)
}
