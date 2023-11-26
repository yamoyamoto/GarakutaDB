package storage

import (
	"sync"
)

type TransactionManager struct {
	transactions map[TransactionId]*Transaction

	latestTransactionId TransactionId

	sharedLocks    map[PageId][]TransactionId
	exclusiveLocks map[PageId]TransactionId

	mutex *sync.Mutex

	storage *Storage
}

func NewTransactionManager(storage *Storage) *TransactionManager {
	return &TransactionManager{
		transactions:   make(map[TransactionId]*Transaction, 0),
		sharedLocks:    make(map[PageId][]TransactionId, 0),
		exclusiveLocks: make(map[PageId]TransactionId, 0),
		mutex:          new(sync.Mutex),
		storage:        storage,
	}
}

func (tm *TransactionManager) Begin() *Transaction {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	tm.latestTransactionId++
	tm.transactions[tm.latestTransactionId] = NewTransaction(tm.latestTransactionId)
	return tm.transactions[tm.latestTransactionId]
}

func (tm *TransactionManager) Commit(t *Transaction) error {
	for _, writeRecord := range t.writeRecords {
		if writeRecord.oldTupleId != nil {
			if _, err := tm.storage.DeleteTuple(writeRecord.tableName, writeRecord.oldTupleId, t); err != nil {
				return err
			}
		}
	}
	return nil
}

func (tm *TransactionManager) Abort(t *Transaction) error {
	for _, writeRecord := range t.writeRecords {
		if writeRecord.newTupleId != nil {
			if _, err := tm.storage.DeleteTuple(writeRecord.tableName, writeRecord.newTupleId, t); err != nil {
				return err
			}
		}
	}
	return nil
}
