package executor

import (
	"fmt"
	"garakutadb/catalog"
	"garakutadb/planner"
	"garakutadb/storage"
)

type DeleteExecutor struct {
	storage        *storage.Storage
	catalog        *catalog.Catalog
	transaction    *storage.Transaction
	transactionMgr *storage.TransactionManager
}

func NewDeleteExecutor(ct *catalog.Catalog, st *storage.Storage, tx *storage.Transaction, txMgr *storage.TransactionManager) *DeleteExecutor {
	return &DeleteExecutor{
		storage:        st,
		catalog:        ct,
		transaction:    tx,
		transactionMgr: txMgr,
	}
}

func (e *DeleteExecutor) Execute(pl planner.DeletePlan) (*ResultSet, error) {
	isDeleteAll := pl.WhereExpression == nil
	if isDeleteAll {
		return nil, fmt.Errorf("deleting all is not supported yet")
	}

	tableSchema, err := e.catalog.TableSchemas.Get(pl.TableName)
	if err != nil {
		return nil, err
	}

	columnNameAndOrderMap := make(map[string]uint64)
	for order, col := range tableSchema.Columns {
		columnNameAndOrderMap[col.Name] = uint64(order)
	}

	it := e.storage.NewTupleIterator(pl.TableName, e.transaction)
	for true {
		tuple, found := it.Next(e.transactionMgr)
		if !found {
			break
		}

		if len(tuple.Data) == 0 {
			continue
		}

		row := make([]string, len(columnNameAndOrderMap))
		for order := range tableSchema.Columns {
			row[order] = tuple.Data[order].Value
		}

		evalResult, err := evalWhere(pl.WhereExpression, row, columnNameAndOrderMap)
		if err != nil {
			return nil, err
		}
		if evalResult {
			// delete tuple
			if err := e.storage.DeleteTuple(pl.TableName, it.GetTupleId(), e.transaction, e.transactionMgr); err != nil {
				return nil, err
			}

			// delete index entry
			btree, err := e.storage.ReadIndex(pl.TableName, tableSchema.PK)
			if err != nil {
				return nil, err
			}
			btree.Delete(&storage.StringItem{
				Value: tuple.Data[0].Value,
			})
			if err := e.storage.WriteIndex(btree); err != nil {
				return nil, err
			}
		}
	}

	return &ResultSet{
		Message: "deleted!",
	}, nil
}
