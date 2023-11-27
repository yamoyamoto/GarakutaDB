package statements

import (
	"fmt"
	"garakutadb/expression"
	"github.com/xwb1989/sqlparser"
)

type DeleteStmt struct {
	Target string
	Where  *Where
}

func BuildDeleteStmt(statement *sqlparser.Delete) (*DeleteStmt, error) {
	if len(statement.TableExprs) != 1 {
		return nil, fmt.Errorf("only support one table. got: %d", len(statement.TableExprs))
	}

	target := statement.TableExprs[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()

	var whereExpr expression.Expression
	var err error
	if statement.Where != nil {
		whereExpr, err = expression.GetWhereFromWhereExpr(statement.Where)
		if err != nil {
			return nil, err
		}
	}

	return &DeleteStmt{
		Target: target,
		Where: &Where{
			Expression: whereExpr,
		},
	}, nil
}
