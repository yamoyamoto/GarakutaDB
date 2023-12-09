package executor

import (
	"garakutadb/planner"
	"garakutadb/storage"
)

type IndexScanExecutor struct {
	storage        *storage.Storage
	transaction    *storage.Transaction
	transactionMgr *storage.TransactionManager
}

func NewIndexScanExecutor(st *storage.Storage, tx *storage.Transaction) *IndexScanExecutor {
	return &IndexScanExecutor{
		storage:     st,
		transaction: tx,
	}
}

func (e *IndexScanExecutor) Execute(pl planner.IndexScanPlan) (*ResultSet, error) {
	btree, err := e.storage.ReadIndex(pl.TableName, pl.IndexName)
	if err != nil {
		return nil, err
	}

	item, found := btree.Search(&storage.StringItem{
		Value: pl.SearchKey,
	})

	if !found {
		return &ResultSet{
			Message: "rows was not found",
		}, nil
	}

	tuple, err := e.storage.GetTupleFromPage(pl.TableName, item.GetPageId(), pl.SearchKey, e.transaction, e.transactionMgr)
	if err != nil {
		return nil, err
	}
	row := make([]string, 0)
	for _, columnOrder := range pl.ColumnOrders {
		row = append(row, tuple.Data[columnOrder].Value)
	}

	return &ResultSet{
		Header: pl.ColumnNames,
		Rows:   [][]string{row},
	}, nil
}
