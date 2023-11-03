package statements

import "github.com/xwb1989/sqlparser"

type InsertStmt struct {
	Into        string
	ColumnNames []string
	Values      []string
}

func BuildInsertStmt(statement *sqlparser.Insert) (*InsertStmt, error) {
	var columnNames []string
	for _, colName := range statement.Columns {
		columnNames = append(columnNames, colName.String())
	}

	var values []string
	for _, row := range statement.Rows.(sqlparser.Values) {
		for _, expr := range row {
			values = append(values, string(expr.(*sqlparser.SQLVal).Val))
		}
	}

	return &InsertStmt{
		Into:        statement.Table.Name.String(),
		ColumnNames: columnNames,
		Values:      values,
	}, nil
}
