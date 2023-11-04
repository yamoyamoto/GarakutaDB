package expression

import (
	"fmt"
	"github.com/xwb1989/sqlparser"
)

type Expression interface {
	implementExpr()
}

type AndExpression struct {
	Left  Expression
	Right Expression
}

type ComparisonExpression struct {
	Operator string
	Left     Expression
	Right    Expression
}

const (
	OperatorEqual = "="
)

type ValueExpression struct {
	Value string
}

func (e *AndExpression) implementExpr()        {}
func (e *ValueExpression) implementExpr()      {}
func (e *ComparisonExpression) implementExpr() {}

func GetWhereFromWhereExpr(whereExpr *sqlparser.Where) (Expression, error) {
	if whereExpr.Type != sqlparser.WhereStr {
		return nil, fmt.Errorf("not supported where type: %s", whereExpr.Type)
	}

	expression, err := getExpressionFromExpr(whereExpr.Expr)
	if err != nil {
		return nil, err
	}

	return expression, nil
}

func getExpressionFromExpr(expr sqlparser.Expr) (Expression, error) {
	switch expr.(type) {
	case *sqlparser.ComparisonExpr:
		comparisonExpr := expr.(*sqlparser.ComparisonExpr)
		if comparisonExpr.Operator != OperatorEqual {
			return nil, fmt.Errorf("not supported operator: %s", comparisonExpr.Operator)
		}
		return &ComparisonExpression{
			Operator: comparisonExpr.Operator,
			Left:     &ValueExpression{Value: comparisonExpr.Left.(*sqlparser.ColName).Name.String()},
			Right:    &ValueExpression{Value: string(comparisonExpr.Right.(*sqlparser.SQLVal).Val)},
		}, nil

	case *sqlparser.AndExpr:
		andExpr := expr.(*sqlparser.AndExpr)

		left, err := getExpressionFromExpr(andExpr.Left)
		if err != nil {
			return nil, err
		}

		right, err := getExpressionFromExpr(andExpr.Right)
		if err != nil {
			return nil, err
		}

		return &AndExpression{Left: left, Right: right}, nil
	default:
		return nil, fmt.Errorf("not supported expression type: %T", expr)
	}
}
