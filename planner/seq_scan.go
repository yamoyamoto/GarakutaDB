package planner

type SeqScanPlan struct {
	TableName    string
	ColumnNames  []string
	ColumnOrders []uint64
}
