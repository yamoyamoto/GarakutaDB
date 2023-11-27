package planner

import (
	"fmt"
	"garakutadb/catalog"
	"garakutadb/parser"
	"garakutadb/parser/statements"
	"garakutadb/parser/statements/ddl"
)

type SimplePlanner struct {
	catalog *catalog.Catalog
}

func NewSimplePlanner(catalog *catalog.Catalog) *SimplePlanner {
	return &SimplePlanner{
		catalog: catalog,
	}
}

func (p *SimplePlanner) MakePlan(stmt parser.Stmt) (Plan, error) {
	switch s := stmt.(type) {
	case *statements.SelectStmt:
		return BuildSelectPlan(p.catalog, s)
	case *statements.InsertStmt:
		return BuildInsertPlan(p.catalog, s)
	case *ddl.CreateTableStmt:
		return BuildCreateTablePlan(p.catalog, s)
	case *statements.DeleteStmt:
		return BuildDeletePlan(p.catalog, s)
	default:
		return nil, fmt.Errorf("not supported statement type: %T", s)
	}
}
