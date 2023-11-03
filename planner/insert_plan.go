package planner

import (
	"fmt"
	"garakutadb/catalog"
	"garakutadb/parser/statements"
)

type InsertPlan struct {
	Into         string
	ColumnNames  []string
	ColumnOrders []uint64
	Values       []string
	ColumnNum    uint64
}

func BuildInsertPlan(ct *catalog.Catalog, insertStmt *statements.InsertStmt) (Plan, error) {
	tableSchema, err := ct.TableSchemas.Get(insertStmt.Into)
	if err == catalog.TableSchemaNotFoundError {
		return nil, fmt.Errorf("table not found: %s", insertStmt.Into)
	}
	if err != nil {
		return nil, err
	}

	columnNames := make([]string, 0)
	columnOrders := make([]uint64, 0)
	columnValues := make([]string, 0)
	if len(insertStmt.ColumnNames) == 0 {
		for order, col := range tableSchema.Columns {
			columnNames = append(columnNames, col.Name)
			columnOrders = append(columnOrders, uint64(order))
			columnValues = append(columnValues, insertStmt.Values[order])
		}
	} else {
		if len(insertStmt.ColumnNames) != len(insertStmt.Values) {
			return nil, fmt.Errorf("column length and value length are not matched. column length: %d, value length: %d", len(insertStmt.ColumnNames), len(insertStmt.Values))
		}

		for _, col := range insertStmt.ColumnNames {
			order, found := tableSchema.Columns.Contains(col)
			if found {
				columnNames = append(columnNames, col)
				columnOrders = append(columnOrders, order)
				columnValues = append(columnValues, insertStmt.Values[order])
			} else {
				return nil, fmt.Errorf("column not found: %s", col)
			}
		}
	}

	return &InsertPlan{
		Into:         tableSchema.Name,
		ColumnNames:  columnNames,
		ColumnOrders: columnOrders,
		Values:       columnValues,
		ColumnNum:    uint64(len(tableSchema.Columns)),
	}, nil
}
