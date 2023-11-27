package storage

import (
	"encoding/json"
	"os"
)

type Storage struct {
	diskManager *DiskManager
}

func NewStorage(diskManager *DiskManager) *Storage {
	return &Storage{
		diskManager: diskManager,
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

func (st *Storage) NewTupleIterator(tableName string, transaction *Transaction) *TupleIterator {
	return &TupleIterator{
		diskManager:        st.diskManager,
		tableName:          tableName,
		pageIteratorCursor: NewTupleIteratorCursor(1),

		Page:        nil,
		transaction: transaction,
	}
}

func (it *TupleIterator) Next() (*Tuple, bool) {
	tuple, found := it.next()
	if !found {
		return nil, false
	}

	if tuple.IsDeleted {
		return it.Next()
	}

	return tuple, true
}

func (it *TupleIterator) next() (*Tuple, bool) {
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
		if it.Page.Tuples[it.pageIteratorCursor.tupleOffset] == nil {
			return nil, false
		} else {
			return it.Page.Tuples[it.pageIteratorCursor.tupleOffset], true
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

func (st *Storage) InsertTuple(tableName string, tuple *Tuple, transaction *Transaction) (*Page, error) {
	it := st.NewTupleIterator(tableName, transaction)

	// TODO: improve performance (want to avoid full scan)
	for true {
		_, found := it.Next()
		if !found {
			break
		}
	}

	if it.Page.Tuples.IsFull() {
		newPage := NewPage(tableName, it.pageIteratorCursor.pageId+1, [TupleNumPerPage]*Tuple{tuple})
		if transaction.state == ACTIVE {
			transaction.AddWriteRecord(
				tableName,
				nil,
				&TupleId{
					pageId: newPage.Id,
					slotId: 0,
				},
			)
		}
		return nil, st.diskManager.WritePage(newPage)
	}

	it.Page.Tuples.Insert(tuple)
	if transaction.state == ACTIVE {
		transaction.AddWriteRecord(
			tableName,
			nil,
			&TupleId{
				pageId: it.pageIteratorCursor.pageId,
				slotId: it.pageIteratorCursor.tupleOffset + 1,
			},
		)
	}

	return it.Page, st.diskManager.WritePage(it.Page)
}

func (st *Storage) DeleteTuple(tableName string, tupleId *TupleId, transaction *Transaction) error {
	page, err := st.diskManager.ReadPage(tableName, tupleId.pageId)
	if err != nil {
		return err
	}

	page.Tuples.DeleteTuple(tupleId.slotId)
	if transaction.state == ACTIVE {
		transaction.AddWriteRecord(tableName, tupleId, nil)
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
