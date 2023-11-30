package storage

import (
	"log"
	"sync"
)

type TransactionManager struct {
	transactions        map[TransactionId]*Transaction
	latestTransactionId TransactionId

	mutex *sync.Mutex

	exclusiveLockTable map[TupleId]TransactionId
	sharedLockTable    map[TupleId][]TransactionId

	storage *Storage
}

func NewTransactionManager(storage *Storage) *TransactionManager {
	return &TransactionManager{
		transactions:       make(map[TransactionId]*Transaction, 0),
		mutex:              new(sync.Mutex),
		exclusiveLockTable: make(map[TupleId]TransactionId, 0),
		sharedLockTable:    make(map[TupleId][]TransactionId, 0),
		storage:            storage,
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
			if err := tm.storage.DeleteTuple(writeRecord.tableName, writeRecord.oldTupleId, t, tm); err != nil {
				return err
			}
		}
	}
	return nil
}

func (tm *TransactionManager) Abort(t *Transaction) error {
	for _, writeRecord := range t.writeRecords {
		if writeRecord.newTupleId != nil {
			if err := tm.storage.DeleteTuple(writeRecord.tableName, writeRecord.newTupleId, t, tm); err != nil {
				return err
			}
		}
	}
	return nil
}

func (tm *TransactionManager) LockShared(t *Transaction, tupleId *TupleId) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.exclusiveLockTable[*tupleId]; ok {
		log.Printf("shared lock failed. tupleId: %#v", tupleId)
		return false
	}

	if _, ok := tm.sharedLockTable[*tupleId]; !ok {
		tm.sharedLockTable[*tupleId] = make([]TransactionId, 0)
	}
	tm.sharedLockTable[*tupleId] = append(tm.sharedLockTable[*tupleId], t.id)

	log.Printf("shared lock success. tupleId: %#v", tupleId)

	return true
}

func (tm *TransactionManager) LockExclusive(t *Transaction, tupleId *TupleId) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.exclusiveLockTable[*tupleId]; ok {
		return false
	}

	if _, ok := tm.sharedLockTable[*tupleId]; ok {
		return false
	}

	tm.exclusiveLockTable[*tupleId] = t.id
	log.Printf("tupleId: %#v", tupleId)
	log.Printf("lock table: %#v", tm.exclusiveLockTable)
	return true
}

func (tm *TransactionManager) IsLockShared(t *Transaction, tupleId *TupleId) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.sharedLockTable[*tupleId]; !ok {
		return false
	}

	for _, id := range tm.sharedLockTable[*tupleId] {
		if id == t.id {
			return true
		}
	}
	return false
}

func (tm *TransactionManager) IsLockExclusive(t *Transaction, tupleId *TupleId) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.exclusiveLockTable[*tupleId]; !ok {
		return false
	}

	return tm.exclusiveLockTable[*tupleId] == t.id
}

func (tm *TransactionManager) UnlockShared(t *Transaction, tupleId *TupleId) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.sharedLockTable[*tupleId]; !ok {
		return
	}

	for i, id := range tm.sharedLockTable[*tupleId] {
		if id == t.id {
			tm.sharedLockTable[*tupleId] = append(tm.sharedLockTable[*tupleId][:i], tm.sharedLockTable[*tupleId][i+1:]...)
			return
		}
	}
}

func (tm *TransactionManager) UnlockExclusive(t *Transaction, tupleId *TupleId) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.exclusiveLockTable[*tupleId]; !ok {
		return
	}

	delete(tm.exclusiveLockTable, *tupleId)
}
