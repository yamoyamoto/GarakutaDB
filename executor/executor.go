package executor

import "garakutadb/storage"

type Executor interface {
	Child() Executor
	Next() (*storage.Tuple, error)
}
