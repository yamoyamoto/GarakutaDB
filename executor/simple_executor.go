package executor

import (
	// https://github.com/golangci/golangci-lint/issues/3815
	"fmt" //nolint
	"garakutadb/catalog"
	"garakutadb/planner"
	"garakutadb/storage"
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
	Header  []string
	Rows    [][]string
	Message string
}

func (e *SimpleExecutor) Execute(pl planner.Plan) (*ResultSet, error) {
	switch p := pl.(type) {
	case *planner.SeqScanPlan:
		return NewSeqScanExecutor(e.storage).Execute(*p)
	case *planner.CreateTablePlan:
		return NewCreateTableExecutor(e.catalog, e.storage).Execute(*p)
	default:
		return nil, fmt.Errorf("not supported plan type: %T", p)
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

type CreateTableExecutor struct {
	storage *storage.Storage
	catalog *catalog.Catalog
}

func NewCreateTableExecutor(ct *catalog.Catalog, st *storage.Storage) *CreateTableExecutor {
	return &CreateTableExecutor{
		storage: st,
		catalog: ct,
	}
}

func (e *CreateTableExecutor) Execute(pl planner.CreateTablePlan) (*ResultSet, error) {
	if err := e.catalog.Add(pl.TableSchema); err != nil {
		return nil, err
	}

	return &ResultSet{
		Message: "successfully created table!",
	}, nil
}
