package ddl

import (
	"fmt"
	"garakutadb/catalog"
	"github.com/xwb1989/sqlparser"
)

type CreateTableStmt struct {
	Into        string
	TableSchema *catalog.TableSchema
}

func BuildCreateTableStmt(statement *sqlparser.DDL) (*CreateTableStmt, error) {
	var tableName string
	if len(statement.NewName.Name.String()) == 0 {
		return nil, fmt.Errorf("table name is empty")
	}
	tableName = statement.NewName.Name.String()
	if len(statement.TableSpec.Columns) == 0 {
		return nil, fmt.Errorf("columns is empty")
	}

	columns := make([]catalog.ColumnSchema, 0)
	for _, column := range statement.TableSpec.Columns {
		columnType, err := mapType(&column.Type)
		if err != nil {
			return nil, err
		}
		columns = append(columns, catalog.ColumnSchema{
			Name: column.Name.String(),
			Type: columnType,
		})
	}

	pk, err := findPrimaryKey(statement.TableSpec.Columns)
	if err != nil {
		return nil, err
	}

	tableSchema := &catalog.TableSchema{
		Name:    tableName,
		Columns: columns,
		PK:      pk.Name,
	}

	return &CreateTableStmt{
		Into:        tableName,
		TableSchema: tableSchema,
	}, nil
}

func findPrimaryKey(columns []*sqlparser.ColumnDefinition) (*catalog.ColumnSchema, error) {
	for _, column := range columns {
		// WARNING: Do we have to use magic numbers?
		if column.Type.KeyOpt == 1 {
			columnType, err := mapType(&column.Type)
			if err != nil {
				return nil, err
			}
			return &catalog.ColumnSchema{
				Name: column.Name.String(),
				Type: columnType,
			}, nil
		}
	}
	return nil, fmt.Errorf("primary key not found")
}

func mapType(columnType *sqlparser.ColumnType) (catalog.ColumnType, error) {
	switch columnType.Type {
	case "text":
		return catalog.String, nil
	default:
		return catalog.Unknown, fmt.Errorf("unknown type: %s", columnType.Type)
	}
}
