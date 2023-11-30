package planner

import (
	"garakutadb/parser"
)

type Planner interface {
	MakePlan(stmt parser.Stmt) (Plan, error)
}

type Plan interface {
}
