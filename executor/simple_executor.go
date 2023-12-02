package executor

import (
	// https://github.com/golangci/golangci-lint/issues/3815
	"fmt" //nolint
	"garakutadb/catalog"
	"garakutadb/expression"
	"garakutadb/planner"
	"garakutadb/storage"
)

type SimpleExecutor struct {
	catalog *catalog.Catalog
	storage *storage.Storage
}

func NewSimpleExecutor(ct *catalog.Catalog, st *storage.Storage) *SimpleExecutor {
	return &SimpleExecutor{
		catalog: ct,
		storage: st,
	}
}

// ResultSet
// TODO: Create a new package and move this?
type ResultSet struct {
	Header  []string
	Rows    [][]string
	Message string
}

func (e *SimpleExecutor) Execute(pl planner.Plan, tx *storage.Transaction, txMgr *storage.TransactionManager) (*ResultSet, error) {
	switch p := pl.(type) {
	case *planner.SeqScanPlan:
		return NewSeqScanExecutor(e.storage, tx, txMgr).Execute(*p)
	case *planner.IndexScanPlan:
		return NewIndexScanExecutor(e.storage, tx).Execute(*p)
	case *planner.InsertPlan:
		return NewInsertExecutor(e.catalog, e.storage, tx, txMgr).Execute(*p)
	case *planner.DeletePlan:
		return NewDeleteExecutor(e.catalog, e.storage, tx, txMgr).Execute(*p)
	case *planner.UpdatePlan:
		return NewUpdateExecutor(e.catalog, e.storage, tx, txMgr).Execute(*p)
	case *planner.CreateTablePlan:
		return NewCreateTableExecutor(e.catalog, e.storage).Execute(*p)
	default:
		return nil, fmt.Errorf("not supported plan type: %T", p)
	}
}

func evalWhere(expr expression.Expression, row []string, columnNameNadOrderMap map[string]uint64) (bool, error) {
	switch e := expr.(type) {
	case *expression.AndExpression:
		leftResult, err := evalWhere(e.Left, row, columnNameNadOrderMap)
		if err != nil {
			return false, err
		}
		rightResult, err := evalWhere(e.Right, row, columnNameNadOrderMap)
		if err != nil {
			return false, err
		}
		return leftResult && rightResult, nil
	case *expression.ComparisonExpression:
		return row[columnNameNadOrderMap[e.Left.(*expression.ValueExpression).Value]] ==
			e.Right.(*expression.ValueExpression).Value, nil
	default:
		return false, fmt.Errorf("not supported expression type: %T", expr)
	}
}
