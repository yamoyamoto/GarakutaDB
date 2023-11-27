package parser

import (
	"fmt"
	"garakutadb/parser/statements"
	"garakutadb/parser/statements/ddl"
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
	case *sqlparser.Insert:
		return statements.BuildInsertStmt(s)
	case *sqlparser.Update:
		return statements.BuildUpdateStmt(s)
	case *sqlparser.Delete:
		return statements.BuildDeleteStmt(s)
	case *sqlparser.DDL:
		return sp.parseDDLStatement(s)
	default:
		return nil, fmt.Errorf("not supported: %T", s)
	}
}

func (sp *SimpleParser) parseDDLStatement(ddlStatement *sqlparser.DDL) (Stmt, error) {
	switch ddlStatement.Action {
	case "create":
		return ddl.BuildCreateTableStmt(ddlStatement)
	default:
		return nil, fmt.Errorf("not supported DDL action: %s", ddlStatement.Action)
	}
}
