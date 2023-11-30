package planner

import (
	"fmt"
	"garakutadb/catalog"
	"garakutadb/expression"
	"garakutadb/parser/statements"
)

type DeletePlan struct {
	TableName       string
	WhereExpression expression.Expression
}

func BuildDeletePlan(ct *catalog.Catalog, deleteStmt *statements.DeleteStmt) (*DeletePlan, error) {
	_, err := ct.TableSchemas.Get(deleteStmt.Target)
	if err == catalog.TableSchemaNotFoundError {
		return nil, fmt.Errorf("table not found: %s", deleteStmt.Target)
	}

	return &DeletePlan{
		TableName:       deleteStmt.Target,
		WhereExpression: deleteStmt.Where.Expression,
	}, nil
}
