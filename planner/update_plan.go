package planner

import (
	"fmt"
	"garakutadb/catalog"
	"garakutadb/expression"
	"garakutadb/parser/statements"
)

type UpdatePlan struct {
	TableName       string
	ColumnNames     []string
	ColumnOrders    []uint64
	ColumnValues    []string
	WhereExpression expression.Expression
}

func BuildUpdatePlan(ct *catalog.Catalog, updateStmt *statements.UpdateStmt) (Plan, error) {
	tableSchema, err := ct.TableSchemas.Get(updateStmt.Target)
	if err == catalog.TableSchemaNotFoundError {
		return nil, fmt.Errorf("table not found: %s", updateStmt.Target)
	}

	columnNames := make([]string, 0)
	columnOrders := make([]uint64, 0)
	columnValues := make([]string, 0)
	for i, colName := range updateStmt.UpdatedColumnNames {
		order, found := tableSchema.Columns.Contains(colName)
		if found {
			columnNames = append(columnNames, colName)
			columnOrders = append(columnOrders, order)
			columnValues = append(columnValues, updateStmt.UpdatedColumnValues[i])
		} else {
			return nil, fmt.Errorf("column not found: %s", colName)
		}
	}

	return &UpdatePlan{
		TableName:       updateStmt.Target,
		ColumnNames:     columnNames,
		ColumnOrders:    columnOrders,
		ColumnValues:    columnValues,
		WhereExpression: updateStmt.Where.Expression,
	}, nil
}
