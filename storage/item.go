package storage

type StringItem struct {
	Value  string
	PageId int64
	Delete bool
}

type Item interface {
	Less(itm Item) bool
	Equal(itm Item) bool
	IsSkip() bool
	GetPageId() int64
}

func (s StringItem) Less(itm Item) bool {
	v, ok := itm.(StringItem)
	if !ok {
		return false
	}
	return s.Value < v.Value
}

func (s StringItem) Equal(itm Item) bool {
	v, ok := itm.(StringItem)
	if !ok {
		return false
	}
	return s.Value == v.Value
}

func (s StringItem) IsSkip() bool {
	return s.Delete
}

func (s StringItem) GetPageId() int64 {
	if s.Delete {
		return -1
	}
	if s.PageId == 0 {
		return -1
	}
	return s.PageId
}
