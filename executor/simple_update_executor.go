package executor

import (
	"fmt"
	"garakutadb/catalog"
	"garakutadb/planner"
	"garakutadb/storage"
	"log"
	"slices"
)

type UpdateExecutor struct {
	storage        *storage.Storage
	catalog        *catalog.Catalog
	transaction    *storage.Transaction
	transactionMgr *storage.TransactionManager
}

func NewUpdateExecutor(ct *catalog.Catalog, st *storage.Storage, tx *storage.Transaction, txMgr *storage.TransactionManager) *UpdateExecutor {
	return &UpdateExecutor{
		storage:        st,
		catalog:        ct,
		transaction:    tx,
		transactionMgr: txMgr,
	}
}

func (e *UpdateExecutor) Execute(pl planner.UpdatePlan) (*ResultSet, error) {
	isUpdateAll := pl.WhereExpression == nil
	if isUpdateAll {
		return nil, fmt.Errorf("updating all is not supported yet")
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
	updatedTupleIds := make([]string, 0)
	for true {
		tuple, found := it.Next(e.transactionMgr)
		if !found {
			break
		}

		if len(tuple.Data) == 0 || slices.Contains(updatedTupleIds, tuple.Data[0].Value) {
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
			// update tuple
			for i, newValue := range pl.ColumnValues {
				tuple.Data[pl.ColumnOrders[i]].Value = newValue
			}
			e.transactionMgr.UnlockSharedByTupleId(e.transaction, it.GetTupleId())
			if err := e.storage.DeleteTuple(pl.TableName, it.GetTupleId(), e.transaction, e.transactionMgr); err != nil {
				return nil, err
			}
			log.Printf("deleted tuple: %v", tuple)
			insertedTuplePage, err := e.storage.InsertTuple(pl.TableName, tuple, e.transaction, e.transactionMgr)
			if err != nil {
				return nil, err
			}
			log.Printf("inserted tuple: %v", tuple)

			// update index entry
			btree, err := e.storage.ReadIndex(pl.TableName, tableSchema.PK)
			if err != nil {
				return nil, err
			}
			item, found := btree.SearchAndUpdatePageId(&storage.StringItem{
				Value:  tuple.Data[0].Value,
				PageId: insertedTuplePage.Id,
			})
			if !found {
				return nil, fmt.Errorf("index entry not found")
			}

			item.Value = tuple.Data[0].Value

			if err := e.storage.WriteIndex(btree); err != nil {
				return nil, err
			}

			updatedTupleIds = append(updatedTupleIds, tuple.Data[0].Value)
		} else {
			e.transactionMgr.UnlockSharedByTupleId(e.transaction, it.GetTupleId())
		}
	}

	return &ResultSet{
		Message: "updated!",
	}, nil
}
