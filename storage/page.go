package storage

const (
	TupleNumPerPage = 32
	PageByteSize    = 4096
)

type Page struct {
	TableName string
	Id        uint64
	Tuples    [TupleNumPerPage]Tuple
}

func NewPage(tableName string, id uint64, tuples [TupleNumPerPage]Tuple) *Page {
	return &Page{
		TableName: tableName,
		Id:        id,
		Tuples:    tuples,
	}
}

func (p *Page) Serialize() ([PageByteSize]byte, error) {
	// TODO: implement
}
