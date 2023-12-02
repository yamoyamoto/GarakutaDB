package executor

import (
	"garakutadb/planner"
	"garakutadb/storage"
)

type IndexScanExecutor struct {
	storage     *storage.Storage
	transaction *storage.Transaction
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

	page, err := e.storage.ReadPage(pl.TableName, item.GetPageId())
	if err != nil {
		return nil, err
	}

	rows := make([][]string, 0)
	for _, tuple := range page.Tuples {
		if len(tuple.Data) == 0 {
			continue
		}

		if tuple.Data[0].Value != pl.SearchKey {
			continue
		}

		row := make([]string, 0)
		for _, columnOrder := range pl.ColumnOrders {
			row = append(row, tuple.Data[columnOrder].Value)
		}
		rows = append(rows, row)
		break
	}

	return &ResultSet{
		Header: pl.ColumnNames,
		Rows:   rows,
	}, nil
}
