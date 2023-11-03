package parser

import (
	"garakutadb/parser/statements"
	"github.com/cockroachdb/errors"
	"github.com/xwb1989/sqlparser"
)

type SimpleParser struct {
}

func NewSimpleParser() *SimpleParser {
	return &SimpleParser{}
}

func (sp *SimpleParser) Parse(SqlString string) (Stmt, error) {
	stmt, err := sqlparser.Parse(SqlString)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse sql")
	}

	switch s := stmt.(type) {
	case *sqlparser.Select:
		return statements.BuildSelectStmt(s)
	default:
		return nil, errors.Errorf("not supported: %T", s)
	}
}
