package planner

import (
	"fmt"
	"garakutadb/catalog"
	"garakutadb/expression"
	"garakutadb/parser/statements"
	"slices"
)

type SelectPlan struct{}

func BuildSelectPlan(ct *catalog.Catalog, selectStmt *statements.SelectStmt) (Plan, error) {
	tableSchema, err := ct.TableSchemas.Get(selectStmt.From)
	if err == catalog.TableSchemaNotFoundError {
		return nil, fmt.Errorf("table not found: %s", selectStmt.From)
	}
	if err != nil {
		return nil, err
	}

	columnNames := make([]string, 0)
	columnOrders := make([]uint64, 0)
	for _, col := range selectStmt.ColumnNames {
		order, found := tableSchema.Columns.Contains(col)
		if found {
			columnNames = append(columnNames, col)
			columnOrders = append(columnOrders, order)
		} else {
			return nil, fmt.Errorf("column not found: %s", col)
		}
	}

	var whereExpression expression.Expression
	if selectStmt.Where != nil {
		whereExpression = selectStmt.Where.Expression
	}

	if selectStmt.IsAllColumns {
		for order, col := range tableSchema.Columns {
			if !slices.Contains(columnNames, col.Name) {
				columnNames = append(columnNames, col.Name)
				columnOrders = append(columnOrders, uint64(order))
			}
		}
	}

	return &SeqScanPlan{
		TableName:       tableSchema.Name,
		ColumnNames:     columnNames,
		ColumnOrders:    columnOrders,
		WhereExpression: whereExpression,
	}, nil
}
