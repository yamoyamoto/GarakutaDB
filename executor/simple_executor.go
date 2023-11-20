package executor

import (
	// https://github.com/golangci/golangci-lint/issues/3815
	"fmt" //nolint
	"garakutadb/catalog"
	"garakutadb/expression"
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

func (e *SimpleExecutor) Execute(pl planner.Plan, transaction *storage.Transaction) (*ResultSet, error) {
	switch p := pl.(type) {
	case *planner.SeqScanPlan:
		return NewSeqScanExecutor(e.storage, transaction).Execute(*p)
	case *planner.IndexScanPlan:
		return NewIndexScanExecutor(e.storage, transaction).Execute(*p)
	case *planner.InsertPlan:
		return NewInsertExecutor(e.catalog, e.storage, transaction).Execute(*p)
	case *planner.CreateTablePlan:
		return NewCreateTableExecutor(e.catalog, e.storage).Execute(*p)
	default:
		return nil, fmt.Errorf("not supported plan type: %T", p)
	}
}

type SeqScanExecutor struct {
	storage     *storage.Storage
	transaction *storage.Transaction
}

func NewSeqScanExecutor(storage *storage.Storage, transaction *storage.Transaction) *SeqScanExecutor {
	return &SeqScanExecutor{
		storage:     storage,
		transaction: transaction,
	}
}

func (e *SeqScanExecutor) Execute(pl planner.SeqScanPlan) (*ResultSet, error) {
	it := e.storage.NewTupleIterator(pl.TableName, e.transaction)

	columnNameAndOrderMap := make(map[string]uint64)
	for order, columnName := range pl.ColumnNames {
		columnNameAndOrderMap[columnName] = uint64(order)
	}

	filteredRows := make([][]string, 0)
	for true {
		tuple, found := it.Next()
		if !found {
			break
		}

		if len(tuple.Data) == 0 {
			continue
		}

		row := make([]string, 0)
		for _, columnOrder := range pl.ColumnOrders {
			row = append(row, tuple.Data[columnOrder].Value)
		}

		if pl.WhereExpression != nil {
			evalResult, err := evalWhere(pl.WhereExpression, row, &pl, columnNameAndOrderMap)
			if err != nil {
				return nil, err
			}
			if evalResult {
				filteredRows = append(filteredRows, row)
			}
		} else {
			filteredRows = append(filteredRows, row)
		}
	}

	return &ResultSet{
		Header: pl.ColumnNames,
		Rows:   filteredRows,
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

type IndexScanExecutor struct {
	storage     *storage.Storage
	transaction *storage.Transaction
}

func NewIndexScanExecutor(storage *storage.Storage, transaction *storage.Transaction) *IndexScanExecutor {
	return &IndexScanExecutor{
		storage:     storage,
		transaction: transaction,
	}
}

func (e *IndexScanExecutor) Execute(pl planner.IndexScanPlan) (*ResultSet, error) {
	btree, err := e.storage.ReadIndex(pl.TableName, pl.IndexName)
	if err != nil {
		return nil, err
	}

	item, found := btree.Search(&storage.StringItem{
		Value: pl.SearchKey,
	})

	if !found {
		return &ResultSet{
			Message: "rows was not found",
		}, nil
	}

	page, err := e.storage.ReadPage(pl.TableName, item.GetPageId())
	if err != nil {
		return nil, err
	}

	rows := make([][]string, 0)
	for _, tuple := range page.Tuples {
		if len(tuple.Data) == 0 {
			continue
		}

		if tuple.Data[0].Value != pl.SearchKey {
			continue
		}

		row := make([]string, 0)
		for _, columnOrder := range pl.ColumnOrders {
			row = append(row, tuple.Data[columnOrder].Value)
		}
		rows = append(rows, row)
		break
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

type InsertExecutor struct {
	storage     *storage.Storage
	catalog     *catalog.Catalog
	transaction *storage.Transaction
}

func NewInsertExecutor(catalog *catalog.Catalog, storage *storage.Storage, transaction *storage.Transaction) *InsertExecutor {
	return &InsertExecutor{
		storage:     storage,
		catalog:     catalog,
		transaction: transaction,
	}
}

func (e *InsertExecutor) Execute(pl planner.InsertPlan) (*ResultSet, error) {
	tableSchema, err := e.catalog.TableSchemas.Get(pl.Into)
	if err != nil {
		return nil, err
	}

	// save index
	btree, err := e.storage.ReadIndex(pl.Into, tableSchema.PK)
	if err != nil {
		return nil, err
	}

	if _, found := btree.Search(&storage.StringItem{
		Value: pl.PKValue,
	}); found {
		return nil, fmt.Errorf("duplicate key value violates unique constraint")
	}

	// save row
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

	page, err := e.storage.InsertTuple(pl.Into, &storage.Tuple{
		Data: tupleValues,
	}, e.transaction)
	if err != nil {
		return nil, err
	}

	if err := btree.Insert(&storage.StringItem{
		Value:  pl.PKValue,
		PageId: storage.PageId(page.Id),
	}); err != nil {
		return nil, err
	}

	if err := e.storage.WriteIndex(btree); err != nil {
		return nil, err
	}

	return &ResultSet{
		Message: "successfully inserted!",
	}, nil
}
