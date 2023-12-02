package executor

import (
	"garakutadb/catalog"
	"garakutadb/planner"
	"garakutadb/storage"
)

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
