package catalog

import (
	"errors"
	"garakutadb/storage"
)

type Catalog struct {
	TableSchemas TableSchemas
	storage      *storage.Storage
}

func NewEmptyCatalog(storage *storage.Storage) *Catalog {
	return &Catalog{
		storage:      storage,
		TableSchemas: TableSchemas{},
	}
}

const catalogPath = "table_schema.json"

func LoadCatalog(storage *storage.Storage) (*Catalog, error) {
	tableSchemas := TableSchemas{}
	if err := storage.ReadJson(catalogPath, &tableSchemas); err != nil {
		return nil, err
	}

	return &Catalog{
		TableSchemas: tableSchemas,
		storage:      storage,
	}, nil
}

func (ct *Catalog) Add(ts *TableSchema) error {
	ct.TableSchemas = append(ct.TableSchemas, *ts)
	return ct.save()
}

func (ct *Catalog) Update(ts *TableSchema) error {
	for i, t := range ct.TableSchemas {
		if t.Name == ts.Name {
			ct.TableSchemas[i] = *ts
			return ct.save()
		}
	}
	return nil
}

func (ct *Catalog) Delete(name string) error {
	for i, t := range ct.TableSchemas {
		if t.Name == name {
			ct.TableSchemas = append(ct.TableSchemas[:i], ct.TableSchemas[i+1:]...)
			return ct.save()
		}
	}
	return nil
}

func (ct *Catalog) save() error {
	return ct.storage.WriteJson(catalogPath, &ct.TableSchemas)
}

type TableSchemas []TableSchema

var TableSchemaNotFoundError = errors.New("table schema not found")

func (t TableSchemas) Get(name string) (*TableSchema, error) {
	for _, ts := range t {
		if ts.Name == name {
			return &ts, nil
		}
	}
	return nil, TableSchemaNotFoundError
}

type TableSchema struct {
	Name    string        `json:"name"`
	Columns ColumnSchemas `json:"columns"`
	PK      string        `json:"pk"`
}

type ColumnSchemas []ColumnSchema

func (c ColumnSchemas) Contains(name string) (uint64, bool) {
	for i, cs := range c {
		if cs.Name == name {
			return uint64(i), true
		}
	}
	return 0, false
}

type ColumnSchema struct {
	Name string     `json:"name"`
	Type ColumnType `json:"type"`
}

type ColumnType uint8

const (
	Unknown ColumnType = iota
	String
)
