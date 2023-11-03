package planner

import (
	"garakutadb/catalog"
)

type CreateTablePlan struct {
	TableSchema *catalog.TableSchema
}
