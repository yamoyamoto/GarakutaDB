package storage

import (
	"fmt"
	"os"
)

type DiskManager struct {
	BasePath string
}

func NewDiskManager(basePath string) *DiskManager {
	return &DiskManager{
		BasePath: basePath,
	}
}

func (d *DiskManager) makePageFilePath(tableName string, pageId PageId) string {
	return fmt.Sprintf("%s/%s/%s_%d", d.BasePath, tableName, tableName, pageId)
}

func (d *DiskManager) makeIndexFilePath(tableName string, indexName string) string {
	return fmt.Sprintf("%s/%s/%s.json", d.BasePath, tableName, indexName)
}

func (d *DiskManager) makeGeneralFilePath(path string) string {
	return fmt.Sprintf("%s/%s", d.BasePath, path)
}

func (d *DiskManager) ReadPage(tableName string, pageId PageId) (*Page, error) {
	b, err := os.ReadFile(d.makePageFilePath(tableName, pageId))
	if err != nil {
		return nil, err
	}

	var bytes [4096]byte
	copy(bytes[:], b)

	return DeserializePage(tableName, pageId, bytes)
}

func (d *DiskManager) WritePage(page *Page) error {
	b, err := page.Serialize()
	if err != nil {
		return err
	}

	return os.WriteFile(d.makePageFilePath(page.TableName, page.Id), b[:], 0644)
}

func (d *DiskManager) ReadIndex(tableName string, indexName string) (*BTree, error) {
	b, err := os.ReadFile(d.makeIndexFilePath(tableName, indexName))
	if err != nil {
		return nil, err
	}
	return DeserializeBTree(b)
}

func (d *DiskManager) WriteIndex(btree *BTree) error {
	b, err := btree.Serialize()
	if err != nil {
		return err
	}

	return os.WriteFile(d.makeIndexFilePath(btree.TableName, btree.IndexName), b, 0644)
}
