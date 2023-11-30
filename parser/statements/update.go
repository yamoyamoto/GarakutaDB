package statements

import (
	"fmt"
	"garakutadb/expression"
	"github.com/xwb1989/sqlparser"
)

type UpdateStmt struct {
	Target string

	UpdatedColumnNames  []string
	UpdatedColumnValues []string

	Where *Where
}

func BuildUpdateStmt(statement *sqlparser.Update) (*UpdateStmt, error) {
	if len(statement.TableExprs) != 1 {
		return nil, fmt.Errorf("only support one table. got: %d", len(statement.TableExprs))
	}

	target, err := getTableNameFromTableExpr(statement.TableExprs[0])
	if err != nil {
		return nil, err
	}

	updatedColumnNames := make([]string, 0)
	updatedColumnValues := make([]string, 0)
	for _, expr := range statement.Exprs {
		updatedColumnNames = append(updatedColumnNames, expr.Name.Name.String())
		updatedColumnValues = append(updatedColumnValues, sqlparser.String(expr.Expr))
	}

	var whereExpression expression.Expression
	if statement.Where != nil {
		whereExpression, err = expression.GetWhereFromWhereExpr(statement.Where)
		if err != nil {
			return nil, err
		}
	}

	return &UpdateStmt{
		Target:              target,
		UpdatedColumnNames:  updatedColumnNames,
		UpdatedColumnValues: updatedColumnValues,
		Where:               &Where{Expression: whereExpression},
	}, nil
}
