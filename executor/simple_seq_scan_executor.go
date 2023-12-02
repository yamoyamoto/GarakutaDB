package executor

import (
	"garakutadb/planner"
	"garakutadb/storage"
)

type SeqScanExecutor struct {
	storage        *storage.Storage
	transaction    *storage.Transaction
	transactionMgr *storage.TransactionManager
}

func NewSeqScanExecutor(st *storage.Storage, tx *storage.Transaction, txMgr *storage.TransactionManager) *SeqScanExecutor {
	return &SeqScanExecutor{
		storage:        st,
		transaction:    tx,
		transactionMgr: txMgr,
	}
}

func (e *SeqScanExecutor) Execute(pl planner.SeqScanPlan) (*ResultSet, error) {
	it := e.storage.NewTupleIterator(pl.TableName, e.transaction)

	columnNameAndOrderMap := make(map[string]uint64)
	for order, columnName := range pl.ColumnNames {
		columnNameAndOrderMap[columnName] = uint64(order)
	}

	filteredRows := make([][]string, 0)
	for true {
		tuple, found := it.Next(e.transactionMgr)
		if !found {
			break
		}

		if len(tuple.Data) == 0 {
			continue
		}

		row := make([]string, 0)
		for _, columnOrder := range pl.ColumnOrders {
			row = append(row, tuple.Data[columnOrder].Value)
		}

		if pl.WhereExpression != nil {
			evalResult, err := evalWhere(pl.WhereExpression, row, columnNameAndOrderMap)
			if err != nil {
				return nil, err
			}
			if evalResult {
				filteredRows = append(filteredRows, row)
			}
		} else {
			filteredRows = append(filteredRows, row)
		}
	}

	return &ResultSet{
		Header: pl.ColumnNames,
		Rows:   filteredRows,
	}, nil
}
