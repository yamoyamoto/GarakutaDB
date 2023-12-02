package executor

import (
	"fmt"
	"garakutadb/catalog"
	"garakutadb/planner"
	"garakutadb/storage"
)

type InsertExecutor struct {
	storage        *storage.Storage
	catalog        *catalog.Catalog
	transaction    *storage.Transaction
	transactionMgr *storage.TransactionManager
}

func NewInsertExecutor(ct *catalog.Catalog, st *storage.Storage, tx *storage.Transaction, txMgr *storage.TransactionManager) *InsertExecutor {
	return &InsertExecutor{
		storage:        st,
		catalog:        ct,
		transaction:    tx,
		transactionMgr: txMgr,
	}
}

func (e *InsertExecutor) Execute(pl planner.InsertPlan) (*ResultSet, error) {
	tableSchema, err := e.catalog.TableSchemas.Get(pl.Into)
	if err != nil {
		return nil, err
	}

	// save index
	btree, err := e.storage.ReadIndex(pl.Into, tableSchema.PK)
	if err != nil {
		return nil, err
	}

	if _, found := btree.Search(&storage.StringItem{
		Value: pl.PKValue,
	}); found {
		return nil, fmt.Errorf("duplicate key value violates unique constraint")
	}

	// save row
	tupleValues := make([]*storage.TupleValue, pl.ColumnNum)
	for i := uint64(0); i < pl.ColumnNum; i++ {
		tupleValues[i] = &storage.TupleValue{
			Value: "NULL", // TODO: support NULL
		}
	}

	for _, order := range pl.ColumnOrders {
		tupleValues[order] = &storage.TupleValue{
			Value: pl.Values[order],
		}
	}

	page, err := e.storage.InsertTuple(pl.Into, &storage.Tuple{
		Data: tupleValues,
	}, e.transaction, e.transactionMgr)
	if err != nil {
		return nil, err
	}

	if err := btree.Insert(&storage.StringItem{
		Value:  pl.PKValue,
		PageId: page.Id,
	}); err != nil {
		return nil, err
	}

	btree.PrintTree()

	if err := e.storage.WriteIndex(btree); err != nil {
		return nil, err
	}

	return &ResultSet{
		Message: "successfully inserted!",
	}, nil
}
