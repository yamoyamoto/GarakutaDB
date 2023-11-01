package storage

type DiskManager struct {
}

func NewDiskManager() *DiskManager {
	return &DiskManager{}
}

func (d *DiskManager) ReadPage(pageId uint64, tableName string) (*Page, error) {
	// TODO: implement
}

func (d *DiskManager) WritePage(page *Page) error {
	// TODO: implement
}
