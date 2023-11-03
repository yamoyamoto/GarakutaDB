package storage

import (
	"encoding/json"
	"os"
)

type Storage struct {
	DiskManager *DiskManager
}

func NewStorage(diskManager *DiskManager) *Storage {
	return &Storage{
		DiskManager: diskManager,
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
		diskManager: st.DiskManager,
		tableName:   tableName,
		pageId:      0,
		Page:        NewPage(tableName, 0, [TupleNumPerPage]*Tuple{}),
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

func (st *Storage) InsertTuple(tableName string, tuple *Tuple) error {
	it := st.NewPageIterator(tableName)

	for it.Next() {
		if it.IsEnd() {
			break
		}
	}

	if it.Page.Tuples.IsFull() {
		newPage := NewPage(tableName, it.pageId+1, [TupleNumPerPage]*Tuple{tuple})
		return st.DiskManager.WritePage(newPage)
	}

	it.Page.Tuples.Insert(tuple)
	return st.DiskManager.WritePage(it.Page)
}

func (st *Storage) ReadJson(path string, out interface{}) error {
	jsonStr, err := os.ReadFile(st.DiskManager.makeGeneralFilePath(path))
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

	return os.WriteFile(st.DiskManager.makeGeneralFilePath(path), jsonStr, 0644)
}
