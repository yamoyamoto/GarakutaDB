package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

type Storage struct {
	diskManager *DiskManager
}

func NewStorage(dm *DiskManager) *Storage {
	return &Storage{
		diskManager: dm,
	}
}

type TupleIterator struct {
	diskManager        *DiskManager
	tableName          string
	pageIteratorCursor *TupleIteratorCursor

	Page        *Page
	transaction *Transaction
}

type TupleIteratorCursor struct {
	pageId      PageId
	tupleOffset uint8
}

func NewTupleIteratorCursor(pageId PageId) *TupleIteratorCursor {
	return &TupleIteratorCursor{
		pageId:      pageId,
		tupleOffset: 0,
	}
}

func (it *TupleIteratorCursor) Next() bool {
	it.tupleOffset++
	if it.tupleOffset >= TupleNumPerPage {
		it.pageId++
		it.tupleOffset = 0
		return true
	}
	return false
}

func (it *TupleIterator) canSee(txMgr *TransactionManager) bool {
	return txMgr.IsLockShared(it.transaction, &TupleId{
		pageId: it.pageIteratorCursor.pageId,
		slotId: it.pageIteratorCursor.tupleOffset,
	}) || txMgr.IsLockExclusive(it.transaction, &TupleId{
		pageId: it.pageIteratorCursor.pageId,
		slotId: it.pageIteratorCursor.tupleOffset,
	}) || txMgr.LockShared(it.transaction, &TupleId{
		pageId: it.pageIteratorCursor.pageId,
		slotId: it.pageIteratorCursor.tupleOffset,
	})
}

func (st *Storage) NewTupleIterator(tableName string, tx *Transaction) *TupleIterator {
	return &TupleIterator{
		diskManager:        st.diskManager,
		tableName:          tableName,
		pageIteratorCursor: NewTupleIteratorCursor(1),

		Page:        nil,
		transaction: tx,
	}
}

func (it *TupleIterator) Next(txMgr *TransactionManager) (*Tuple, bool) {
	tuple, found := it.next(txMgr)
	if !found {
		return nil, false
	}

	if tuple.IsDeleted {
		return it.Next(txMgr)
	}

	return tuple, true
}

func (it *TupleIterator) next(txMgr *TransactionManager) (*Tuple, bool) {
	if it.Page == nil {
		p, err := it.diskManager.ReadPage(it.tableName, it.pageIteratorCursor.pageId)
		if err != nil {
			return nil, false
		}
		it.Page = p
		if p.Tuples[0] == nil {
			return nil, false
		} else {
			return p.Tuples[0], true
		}
	}

	isNextPage := it.pageIteratorCursor.Next()
	if !isNextPage {
		if it.Page.Tuples[it.pageIteratorCursor.tupleOffset].Data == nil {
			return nil, false
		} else if it.canSee(txMgr) {
			return it.Page.Tuples[it.pageIteratorCursor.tupleOffset], true
		} else {
			return it.next(txMgr)
		}
	}

	p, err := it.diskManager.ReadPage(it.tableName, it.pageIteratorCursor.pageId)
	// TODO: add page not found case
	if err != nil {
		return nil, false
	}
	it.Page = p

	if it.Page.Tuples[it.pageIteratorCursor.tupleOffset] == nil {
		return nil, false
	}

	if !it.canSee(txMgr) {
		return it.next(txMgr)
	}
	return it.Page.Tuples[it.pageIteratorCursor.tupleOffset], true
}

func (it *TupleIterator) GetTupleId() *TupleId {
	return &TupleId{
		pageId: it.pageIteratorCursor.pageId,
		slotId: it.pageIteratorCursor.tupleOffset,
	}
}

func (st *Storage) ReadPage(tableName string, pageId PageId) (*Page, error) {
	return st.diskManager.ReadPage(tableName, pageId)
}

func (st *Storage) WritePage(page *Page) error {
	return st.diskManager.WritePage(page)
}

func (st *Storage) ReadIndex(tableName string, indexName string) (*BTree, error) {
	return st.diskManager.ReadIndex(tableName, indexName)
}

func (st *Storage) WriteIndex(btree *BTree) error {
	return st.diskManager.WriteIndex(btree)
}

func (st *Storage) InsertTuple(tableName string, tuple *Tuple, tx *Transaction, txMgr *TransactionManager) (*Page, error) {
	it := st.NewTupleIterator(tableName, tx)

	// TODO: improve performance (want to avoid full scan)
	for true {
		_, found := it.Next(txMgr)
		if !found {
			break
		}
		txMgr.UnlockSharedByTupleId(tx, it.GetTupleId())
	}

	if it.Page == nil {
		newPage := NewPage(tableName, it.pageIteratorCursor.pageId, [TupleNumPerPage]*Tuple{tuple})
		newTupleId := &TupleId{
			pageId: newPage.Id,
			slotId: 0,
		}
		success := txMgr.LockExclusive(tx, newTupleId)
		if !success {
			return nil, fmt.Errorf("failed to lock exclusive")
		}

		if tx.state == ACTIVE {
			tx.AddWriteRecord(
				tableName,
				nil,
				newTupleId,
			)
		}
		return newPage, st.diskManager.WritePage(newPage)
	}

	if it.Page.Tuples.IsFull() {
		newPage := NewPage(tableName, it.pageIteratorCursor.pageId+1, [TupleNumPerPage]*Tuple{tuple})
		newTupleId := &TupleId{
			pageId: newPage.Id,
			slotId: 0,
		}
		success := txMgr.LockExclusive(tx, newTupleId)
		if !success {
			return nil, fmt.Errorf("failed to lock exclusive")
		}

		if tx.state == ACTIVE {
			tx.AddWriteRecord(
				tableName,
				nil,
				newTupleId,
			)
		}
		return newPage, st.diskManager.WritePage(newPage)
	}

	tupleId := &TupleId{
		pageId: it.pageIteratorCursor.pageId,
		slotId: it.pageIteratorCursor.tupleOffset + 1,
	}
	success := txMgr.LockExclusive(tx, tupleId)
	if !success {
		return nil, fmt.Errorf("failed to lock exclusive")
	}

	it.Page.Tuples.Insert(tuple)
	if tx.state == ACTIVE {
		tx.AddWriteRecord(
			tableName,
			nil,
			tupleId,
		)
	}

	return it.Page, st.diskManager.WritePage(it.Page)
}

func (st *Storage) DeleteTuple(tableName string, tupleId *TupleId, tx *Transaction, txMgr *TransactionManager) error {
	page, err := st.diskManager.ReadPage(tableName, tupleId.pageId)
	if err != nil {
		return err
	}

	success := txMgr.LockExclusive(tx, tupleId)
	if !success {
		return errors.New("failed to lock tuple")
	}

	page.Tuples.DeleteTuple(tupleId.slotId)
	if tx.state == ACTIVE {
		tx.AddWriteRecord(tableName, tupleId, nil)
	}

	return st.diskManager.WritePage(page)
}

func (st *Storage) ReadJson(path string, out interface{}) error {
	jsonStr, err := os.ReadFile(st.diskManager.makeGeneralFilePath(path))
	if err != nil {
		return err
	}

	return json.Unmarshal(jsonStr, out)
}

func (st *Storage) WriteJson(path string, in interface{}) error {
	jsonStr, err := json.Marshal(in)
	if err != nil {
		return err
	}

	return os.WriteFile(st.diskManager.makeGeneralFilePath(path), jsonStr, 0644)
}
