package catalog

import "github.com/cockroachdb/errors"

type Catalog struct {
	TableSchemas TableSchemas
}

func LoadCatalog() *Catalog {
	// TODO: load catalog from file (this is just a stub)
	return &Catalog{
		TableSchemas: TableSchemas{
			TableSchema{
				Name: "users",
				Columns: ColumnSchemas{
					ColumnSchema{
						Name: "id",
						Type: String,
					},
					ColumnSchema{
						Name: "name",
						Type: String,
					},
				},
				PK: "id",
			},
		},
	}
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
	Name    string
	Columns ColumnSchemas
	PK      string
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
	Name string
	Type ColumnType
}

type ColumnType uint8

const (
	Unknown ColumnType = iota
	String
)
