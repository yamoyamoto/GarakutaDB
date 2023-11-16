package planner

import "garakutadb/expression"

type SeqScanPlan struct {
	TableName       string
	ColumnNames     []string
	ColumnOrders    []uint64
	WhereExpression expression.Expression
}

type IndexScanPlan struct {
	TableName    string
	ColumnNames  []string
	ColumnOrders []uint64
	SearchKey    string
	IndexName    string
}
