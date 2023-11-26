package storage

type Transaction struct {
	state TransactionState
	id    TransactionId

	writeRecords []*WriteRecord

	sharedLocks    map[PageId][]TransactionId
	exclusiveLocks map[PageId]TransactionId
}

type WriteRecord struct {
	tableName  string
	oldTupleId *TupleId
	newTupleId *TupleId
}

type TupleId struct {
	pageId PageId
	slotId uint8
}

type TransactionState int32

const (
	ACTIVE TransactionState = iota
	COMMITTED
	ABORTED
)

type TransactionId int32

func NewTransaction(id TransactionId) *Transaction {
	return &Transaction{
		state:          ACTIVE,
		id:             id,
		sharedLocks:    make(map[PageId][]TransactionId, 0),
		exclusiveLocks: make(map[PageId]TransactionId, 0),
	}
}

func (t *Transaction) GetState() TransactionState {
	return t.state
}

func (t *Transaction) GetId() TransactionId {
	return t.id
}

func (t *Transaction) AddWriteRecord(tableName string, oldTupleId *TupleId, newTupleId *TupleId) {
	t.writeRecords = append(t.writeRecords, &WriteRecord{
		tableName:  tableName,
		oldTupleId: oldTupleId,
		newTupleId: newTupleId,
	})
}
