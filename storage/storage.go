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

type PageIterator struct {
	diskManager *DiskManager
	tableName   string
	pageId      uint64

	Page *Page
}

func (st *Storage) NewPageIterator(tableName string) *PageIterator {
	return &PageIterator{
		diskManager: st.diskManager,
		tableName:   tableName,
		pageId:      1,
		Page:        NewPage(tableName, 1, [TupleNumPerPage]*Tuple{}),
	}
}

func (it *PageIterator) Next() bool {
	p, err := it.diskManager.ReadPage(it.tableName, it.pageId)
	// TODO: add page not found case
	if err != nil {
		return false
	}

	it.pageId++
	it.Page = p
	return true
}

func (it *PageIterator) IsEnd() bool {
	_, err := it.diskManager.ReadPage(it.tableName, it.pageId+1)
	return err != nil
}

func (st *Storage) ReadPage(tableName string, pageId uint64) (*Page, error) {
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

func (st *Storage) InsertTuple(tableName string, tuple *Tuple) (*Page, error) {
	it := st.NewPageIterator(tableName)

	for it.Next() {
		if it.IsEnd() {
			break
		}
	}

	if it.Page.Tuples.IsFull() {
		newPage := NewPage(tableName, it.pageId+1, [TupleNumPerPage]*Tuple{tuple})
		return nil, st.diskManager.WritePage(newPage)
	}

	it.Page.Tuples.Insert(tuple)
	return it.Page, st.diskManager.WritePage(it.Page)
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
