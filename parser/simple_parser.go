package parser

import (
	"fmt"
	"garakutadb/parser/statements"
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
		return nil, err
	}

	switch s := stmt.(type) {
	case *sqlparser.Select:
		return statements.BuildSelectStmt(s)
	default:
		return nil, fmt.Errorf("not supported: %T", s)
	}
}
