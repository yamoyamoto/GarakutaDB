package storage

type Storage struct {
	DiskManager *DiskManager
}

func NewStorage(diskManager DiskManager) *Storage {
	return &Storage{
		DiskManager: &diskManager,
	}
}
