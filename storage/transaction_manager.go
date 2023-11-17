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
}

func NewTransactionManager() *TransactionManager {
	return &TransactionManager{
		transactions:   make(map[TransactionId]*Transaction, 0),
		sharedLocks:    make(map[PageId][]TransactionId, 0),
		exclusiveLocks: make(map[PageId]TransactionId, 0),
	}
}

func (tm *TransactionManager) Begin() *Transaction {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	tm.latestTransactionId++
	tm.transactions[tm.latestTransactionId] = NewTransaction(tm.latestTransactionId)
	return tm.transactions[tm.latestTransactionId]
}

func (tm *TransactionManager) Commit(t *Transaction) {
	// 古い方を消す

}

func (tm *TransactionManager) Abort(t *Transaction) {
	// 新しい方を消す

}
