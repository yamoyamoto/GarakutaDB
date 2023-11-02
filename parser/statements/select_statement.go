package statements

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
)

type SelectStmt struct {
	From string

	// Actual column name (not alias)
	ColumnNames []string

	IsAllColumns bool
}

func BuildSelectStmt(statement *sqlparser.Select) (*SelectStmt, error) {
	if len(statement.From) != 1 {
		return nil, fmt.Errorf("only support one table. got: %d", len(statement.From))
	}

	from, err := getTableNameFromTableExpr(statement.From[0])
	if err != nil {
		return nil, err
	}

	columnNames, err := getColumnNamesFromSelectExprs(statement.SelectExprs)
	if err != nil {
		return nil, err
	}

	return &SelectStmt{
		From:         from,
		ColumnNames:  columnNames,
		IsAllColumns: isAllColumns(statement.SelectExprs),
	}, nil
}

func getTableNameFromTableExpr(from sqlparser.TableExpr) (string, error) {
	switch from.(type) {
	case *sqlparser.AliasedTableExpr:
		aliasedTableExpr := from.(*sqlparser.AliasedTableExpr).Expr
		switch aliasedTableExpr.(type) {
		case sqlparser.TableName:
			return aliasedTableExpr.(sqlparser.TableName).Name.String(), nil
		default:
			return "", fmt.Errorf("not supported table expression type: %T", aliasedTableExpr)
		}
	default:
		return "", fmt.Errorf("not supported table type: %T", from)
	}
}

func getColumnNamesFromSelectExprs(selectExprs sqlparser.SelectExprs) ([]string, error) {
	var columnNames []string
	for _, selectExpr := range selectExprs {
		switch selectExpr.(type) {
		case *sqlparser.AliasedExpr:
			aliasedExpr := selectExpr.(*sqlparser.AliasedExpr)
			switch aliasedExpr.Expr.(type) {
			case *sqlparser.ColName:
				colName := aliasedExpr.Expr.(*sqlparser.ColName).Name.String()
				columnNames = append(columnNames, colName)

			default:
				return nil, fmt.Errorf("not supported column expression type: %T", aliasedExpr.Expr)
			}
		case *sqlparser.StarExpr:
			// '*' will be handled separately and specially
			return nil, nil
		default:
			return nil, fmt.Errorf("not supported select expression type: %T", selectExpr)
		}
	}
	return columnNames, nil
}

func isAllColumns(selectExprs sqlparser.SelectExprs) bool {
	for _, selectExpr := range selectExprs {
		switch selectExpr.(type) {
		case *sqlparser.StarExpr:
			return true
		default:
			return false
		}
	}
	return false
}
