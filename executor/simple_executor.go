package executor

import (
	// https://github.com/golangci/golangci-lint/issues/3815
	"fmt" //nolint
	"garakutadb/catalog"
	"garakutadb/expression"
	"garakutadb/planner"
	"garakutadb/storage"
	"log"
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
	case *planner.InsertPlan:
		return NewInsertExecutor(e.storage).Execute(*p)
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

	columnNameAndOrderMap := make(map[string]uint64)
	for order, columnName := range pl.ColumnNames {
		columnNameAndOrderMap[columnName] = uint64(order)
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

	if pl.WhereExpression != nil {
		filteredRows := make([][]string, 0)
		for _, row := range rows {
			evalResult, err := evalWhere(pl.WhereExpression, row, &pl, columnNameAndOrderMap)
			if err != nil {
				return nil, err
			}
			log.Println("evalResult", evalResult)
			if evalResult {
				filteredRows = append(filteredRows, row)
			}
		}
		rows = filteredRows
	}

	return &ResultSet{
		Header: pl.ColumnNames,
		Rows:   rows,
	}, nil
}

func evalWhere(expr expression.Expression, row []string, pl *planner.SeqScanPlan, columnNameNadOrderMap map[string]uint64) (bool, error) {
	switch e := expr.(type) {
	case *expression.AndExpression:
		leftResult, err := evalWhere(e.Left, row, pl, columnNameNadOrderMap)
		if err != nil {
			return false, err
		}
		rightResult, err := evalWhere(e.Right, row, pl, columnNameNadOrderMap)
		if err != nil {
			return false, err
		}
		return leftResult && rightResult, nil
	case *expression.ComparisonExpression:
		return row[columnNameNadOrderMap[e.Left.(*expression.ValueExpression).Value]] ==
			e.Right.(*expression.ValueExpression).Value, nil
	default:
		return false, fmt.Errorf("not supported expression type: %T", expr)
	}
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

type InsertExecutor struct {
	storage *storage.Storage
}

func NewInsertExecutor(storage *storage.Storage) *InsertExecutor {
	return &InsertExecutor{
		storage: storage,
	}
}

func (e *InsertExecutor) Execute(pl planner.InsertPlan) (*ResultSet, error) {
	tupleValues := make([]*storage.TupleValue, pl.ColumnNum)
	for i := uint64(0); i < pl.ColumnNum; i++ {
		tupleValues[i] = &storage.TupleValue{
			Value: "NULL", // TODO: support NULL
		}
	}

	for _, order := range pl.ColumnOrders {
		tupleValues[order] = &storage.TupleValue{
			Value: pl.Values[order],
		}
	}

	if err := e.storage.InsertTuple(pl.Into, &storage.Tuple{
		Data: tupleValues,
	}); err != nil {
		return nil, err
	}

	return &ResultSet{
		Message: "successfully inserted!",
	}, nil
}
