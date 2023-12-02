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

func NewTransactionManager(st *Storage) *TransactionManager {
	return &TransactionManager{
		transactions:       make(map[TransactionId]*Transaction, 0),
		mutex:              new(sync.Mutex),
		exclusiveLockTable: make(map[TupleId]TransactionId, 0),
		sharedLockTable:    make(map[TupleId][]TransactionId, 0),
		storage:            st,
	}
}

func (tm *TransactionManager) Begin() *Transaction {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	tm.latestTransactionId++
	tm.transactions[tm.latestTransactionId] = NewTransaction(tm.latestTransactionId)
	return tm.transactions[tm.latestTransactionId]
}

func (tm *TransactionManager) Commit(tx *Transaction) error {
	for _, writeRecord := range tx.writeRecords {
		if writeRecord.oldTupleId != nil {
			if err := tm.storage.DeleteTuple(writeRecord.tableName, writeRecord.oldTupleId, tx, tm); err != nil {
				return err
			}
		}
	}
	tm.UnlockSharedAll(tx)
	tm.UnlockExclusiveAll(tx)
	return nil
}

func (tm *TransactionManager) Abort(tx *Transaction) error {
	for _, writeRecord := range tx.writeRecords {
		if writeRecord.newTupleId != nil {
			if err := tm.storage.DeleteTuple(writeRecord.tableName, writeRecord.newTupleId, tx, tm); err != nil {
				return err
			}
		}
	}
	tm.UnlockSharedAll(tx)
	tm.UnlockExclusiveAll(tx)
	return nil
}

func (tm *TransactionManager) LockShared(tx *Transaction, tupleId *TupleId) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.exclusiveLockTable[*tupleId]; ok {
		return false
	}

	if _, ok := tm.sharedLockTable[*tupleId]; !ok {
		tm.sharedLockTable[*tupleId] = make([]TransactionId, 0)
	}
	tm.sharedLockTable[*tupleId] = append(tm.sharedLockTable[*tupleId], tx.id)

	return true
}

func (tm *TransactionManager) LockExclusive(tx *Transaction, tupleId *TupleId) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.exclusiveLockTable[*tupleId]; ok {
		if tm.exclusiveLockTable[*tupleId] == tx.id {
			return true
		}
		return false
	}

	if _, ok := tm.sharedLockTable[*tupleId]; ok {
		return false
	}

	tm.exclusiveLockTable[*tupleId] = tx.id

	return true
}

func (tm *TransactionManager) IsLockShared(tx *Transaction, tupleId *TupleId) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.sharedLockTable[*tupleId]; !ok {
		return false
	}

	for _, id := range tm.sharedLockTable[*tupleId] {
		if id == tx.id {
			return true
		}
	}
	return false
}

func (tm *TransactionManager) IsLockExclusive(tx *Transaction, tupleId *TupleId) bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.exclusiveLockTable[*tupleId]; !ok {
		return false
	}

	return tm.exclusiveLockTable[*tupleId] == tx.id
}

func (tm *TransactionManager) UnlockSharedAll(tx *Transaction) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	for tupleId, ids := range tm.sharedLockTable {
		for i, id := range ids {
			if id == tx.id {
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

func (tm *TransactionManager) UnlockSharedByTupleId(tx *Transaction, tupleId *TupleId) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if _, ok := tm.sharedLockTable[*tupleId]; !ok {
		return
	}

	for i, id := range tm.sharedLockTable[*tupleId] {
		if id == tx.id {
			tm.sharedLockTable[*tupleId] = append(tm.sharedLockTable[*tupleId][:i], tm.sharedLockTable[*tupleId][i+1:]...)
			break
		}
	}
	if len(tm.sharedLockTable[*tupleId]) == 0 {
		delete(tm.sharedLockTable, *tupleId)
	}
}

func (tm *TransactionManager) UnlockExclusiveAll(tx *Transaction) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	for tupleId, id := range tm.exclusiveLockTable {
		if id == tx.id {
			delete(tm.exclusiveLockTable, tupleId)
			return
		}
	}
}
