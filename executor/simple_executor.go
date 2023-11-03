package executor

import (
	"garakutadb/catalog"
	"garakutadb/planner"
	"garakutadb/storage"
	"github.com/cockroachdb/errors"
)

type SimpleExecutor struct {
	catalog *catalog.Catalog
	storage *storage.Storage
}

func NewSimpleExecutor(ct *catalog.Catalog, st *storage.Storage) *SimpleExecutor {
	return &SimpleExecutor{
		catalog: ct,
		storage: st,
	}
}

// ResultSet
// TODO: Create a new package and move this?
type ResultSet struct {
	Header []string
	Rows   [][]string
}

func (e *SimpleExecutor) Execute(pl planner.Plan) (*ResultSet, error) {
	switch p := pl.(type) {
	case *planner.SeqScanPlan:
		return NewSeqScanExecutor(e.storage).Execute(*p)
	default:
		return nil, errors.Errorf("not supported plan type: %T", p)
	}
}

type SeqScanExecutor struct {
	storage *storage.Storage
}

func NewSeqScanExecutor(storage *storage.Storage) *SeqScanExecutor {
	return &SeqScanExecutor{
		storage: storage,
	}
}

func (e *SeqScanExecutor) Execute(pl planner.SeqScanPlan) (*ResultSet, error) {
	it := e.storage.NewPageIterator(pl.TableName)

	pages := make([]*storage.Page, 0)
	for it.Next() {
		pages = append(pages, it.Page)
	}

	rows := make([][]string, 0)
	for _, page := range pages {
		for _, tuple := range page.Tuples {
			if len(tuple.Data) == 0 {
				continue
			}
			row := make([]string, 0)
			for _, columnOrder := range pl.ColumnOrders {
				row = append(row, tuple.Data[columnOrder].Value)
			}
			rows = append(rows, row)
		}
	}

	return &ResultSet{
		Header: pl.ColumnNames,
		Rows:   rows,
	}, nil
}
