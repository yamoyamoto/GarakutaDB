package planner

import (
	"fmt"
	"garakutadb/catalog"
	"garakutadb/parser/statements/ddl"
)

func BuildCreateTablePlan(ct *catalog.Catalog, stmt *ddl.CreateTableStmt) (Plan, error) {
	for _, table := range ct.TableSchemas {
		if table.Name == stmt.Into {
			return nil, fmt.Errorf("table already exists: %s", stmt.Into)
		}
	}

	return &CreateTablePlan{
		TableSchema: stmt.TableSchema,
	}, nil
}
