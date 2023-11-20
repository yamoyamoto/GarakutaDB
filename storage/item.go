package storage

type StringItem struct {
	Value  string
	PageId PageId
	Delete bool
}

type Item interface {
	Less(itm Item) bool
	Equal(itm Item) bool
	IsSkip() bool
	GetPageId() PageId
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

func (s StringItem) GetPageId() PageId {
	if s.Delete {
		return 0
	}
	if s.PageId == 0 {
		return 0
	}

	return PageId(s.PageId)
}
