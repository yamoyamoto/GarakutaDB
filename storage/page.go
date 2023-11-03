package storage

import (
	"google.golang.org/protobuf/proto"
)

const (
	TupleNumPerPage = 32
	PageByteSize    = 4096
)

type Page struct {
	TableName string
	Id        uint64
	Tuples    Tuples
}

type Tuples [TupleNumPerPage]*Tuple

func (t *Tuples) Insert(tuple *Tuple) {
	for i, v := range t {
		if v == nil || v.Data == nil {
			t[i] = tuple
			return
		}
	}
}

func (t *Tuples) IsFull() bool {
	for _, tuple := range t {
		if tuple == nil || tuple.Data == nil {
			return false
		}
	}
	return true
}

func NewPage(tableName string, id uint64, tuples [TupleNumPerPage]*Tuple) *Page {
	return &Page{
		TableName: tableName,
		Id:        id,
		Tuples:    tuples,
	}
}

func (p *Page) Serialize() ([PageByteSize]byte, error) {
	pageBytes := [PageByteSize]byte{}

	for i, t := range p.Tuples {
		if t == nil || t.Data == nil {
			break
		}
		b, err := proto.Marshal(t)
		if err != nil {
			return [PageByteSize]byte{}, err
		}

		copy(pageBytes[i*128:i*128+128], b)
	}

	return pageBytes, nil
}

func DeserializePage(tableName string, pageId uint64, pageBytes [PageByteSize]byte) (*Page, error) {
	tuples := [TupleNumPerPage]*Tuple{}

	for i := 0; i < TupleNumPerPage; i++ {
		var in [128]byte
		copy(in[:], pageBytes[i*128:i*128+128])

		// TODO: thinking about remove Zero padding?
		byteLen := len(in)
		for j, b := range in {
			if b == 0 {
				byteLen = j
				break
			}
		}

		t := Tuple{}
		err := proto.Unmarshal(pageBytes[i*128:i*128+byteLen], &t)
		if err != nil {
			return nil, err
		}

		tuples[i] = &t
	}

	return NewPage(tableName, pageId, tuples), nil
}
