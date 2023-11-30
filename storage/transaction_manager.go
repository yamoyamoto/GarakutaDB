package storage

import (
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
	tm.UnlockSharedAll(t)
	tm.UnlockExclusiveAll(t)
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
	tm.UnlockSharedAll(t)
	tm.UnlockExclusiveAll(t)
	return nil
}

func (tm *TransactionManager) LockShared(t *Transaction, tupleId *TupleId) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.exclusiveLockTable[*tupleId]; ok {
		return false
	}

	if _, ok := tm.sharedLockTable[*tupleId]; !ok {
		tm.sharedLockTable[*tupleId] = make([]TransactionId, 0)
	}
	tm.sharedLockTable[*tupleId] = append(tm.sharedLockTable[*tupleId], t.id)

	return true
}

func (tm *TransactionManager) LockExclusive(t *Transaction, tupleId *TupleId) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.exclusiveLockTable[*tupleId]; ok {
		if tm.exclusiveLockTable[*tupleId] == t.id {
			return true
		}
		return false
	}

	if _, ok := tm.sharedLockTable[*tupleId]; ok {
		return false
	}

	tm.exclusiveLockTable[*tupleId] = t.id

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

func (tm *TransactionManager) UnlockSharedAll(t *Transaction) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	for tupleId, ids := range tm.sharedLockTable {
		for i, id := range ids {
			if id == t.id {
				ids = append(ids[:i], ids[i+1:]...)
				if len(ids) == 0 {
					delete(tm.sharedLockTable, tupleId)
				} else {
					tm.sharedLockTable[tupleId] = ids
				}
				return
			}
		}
	}
}

func (tm *TransactionManager) UnlockSharedByTupleId(t *Transaction, tupleId *TupleId) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.sharedLockTable[*tupleId]; !ok {
		return
	}

	for i, id := range tm.sharedLockTable[*tupleId] {
		if id == t.id {
			tm.sharedLockTable[*tupleId] = append(tm.sharedLockTable[*tupleId][:i], tm.sharedLockTable[*tupleId][i+1:]...)
			break
		}
	}
	if len(tm.sharedLockTable[*tupleId]) == 0 {
		delete(tm.sharedLockTable, *tupleId)
	}
}

func (tm *TransactionManager) UnlockExclusiveAll(t *Transaction) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	for tupleId, id := range tm.exclusiveLockTable {
		if id == t.id {
			delete(tm.exclusiveLockTable, tupleId)
			return
		}
	}
}
