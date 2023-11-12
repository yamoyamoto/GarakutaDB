package storage_test

import (
	"fmt"
	"garakutadb/storage"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInsert(t *testing.T) {
	btree := storage.NewBTree()
	items := []storage.StringItem{
		{Value: "c", PageId: 0},
		{Value: "a", PageId: 0},
		{Value: "b", PageId: 0},
		{Value: "d", PageId: 0},
		{Value: "f", PageId: 0},
		{Value: "e", PageId: 0},
		{Value: "g", PageId: 0},
	}

	for _, item := range items {
		btree.Insert(item)
	}

	for _, item := range items {
		foundItem, ok := btree.Search(item)
		if !ok {
			assert.Error(t, fmt.Errorf("failed to find item %#v", item))
		}
		assert.Equal(t, item, foundItem)
	}
}

func TestNodeStructure(t *testing.T) {
	btree := storage.NewBTree()
	items := []storage.StringItem{
		{Value: "c", PageId: 0},
		{Value: "a", PageId: 0},
		{Value: "b", PageId: 0},
		{Value: "d", PageId: 0},
		{Value: "f", PageId: 0},
		{Value: "e", PageId: 0},
		{Value: "g", PageId: 0},
	}

	for _, item := range items {
		btree.Insert(item)
	}

	/*
			 b
		   b    f
		  a c  e g
	*/
	assert.Len(t, btree.Top.Items, 1)
	assert.Len(t, btree.Top.Children, 2)
	assert.Len(t, btree.Top.Children[0].Items, 1)
	assert.Len(t, btree.Top.Children[1].Items, 1)
	assert.Len(t, btree.Top.Children[0].Children, 2)
	assert.Len(t, btree.Top.Children[1].Children, 2)
	assert.Len(t, btree.Top.Children[0].Children[0].Items, 1)
	assert.Len(t, btree.Top.Children[1].Children[1].Items, 1)
	assert.Len(t, btree.Top.Children[0].Children[0].Children, 0)
	assert.Len(t, btree.Top.Children[1].Children[1].Children, 0)

	assert.Equal(t, storage.StringItem{
		Value: "d",
	}, btree.Top.Items[0])
	assert.Equal(t, storage.StringItem{
		Value: "b",
	}, btree.Top.Children[0].Items[0])
	assert.Equal(t, storage.StringItem{
		Value: "f",
	}, btree.Top.Children[1].Items[0])
	assert.Equal(t, storage.StringItem{
		Value: "a",
	}, btree.Top.Children[0].Children[0].Items[0])
	assert.Equal(t, storage.StringItem{
		Value: "c",
	}, btree.Top.Children[0].Children[1].Items[0])
	assert.Equal(t, storage.StringItem{
		Value: "e",
	}, btree.Top.Children[1].Children[0].Items[0])
	assert.Equal(t, storage.StringItem{
		Value: "g",
	}, btree.Top.Children[1].Children[1].Items[0])
}

// BTreeの検索機能テスト
func TestSearch(t *testing.T) {
	btree := storage.NewBTree()
	items := []storage.StringItem{
		{Value: "c", PageId: 0},
		{Value: "a", PageId: 0},
		{Value: "b", PageId: 0},
		{Value: "d", PageId: 0},
		{Value: "f", PageId: 0},
		{Value: "e", PageId: 0},
		{Value: "g", PageId: 0},
	}

	for _, item := range items {
		btree.Insert(item)
	}

	for _, item := range items {
		foundItem, ok := btree.Search(item)
		if !ok {
			assert.Error(t, fmt.Errorf("failed to find item %#v", item))
		}
		assert.Equal(t, item, foundItem)
	}
}

func TestBalance(t *testing.T) {
	btree := storage.NewBTree()
	items := []storage.StringItem{
		{Value: "c", PageId: 0},
		{Value: "a", PageId: 0},
		{Value: "b", PageId: 0},
		{Value: "d", PageId: 0},
		{Value: "f", PageId: 0},
		{Value: "e", PageId: 0},
		{Value: "g", PageId: 0},
	}

	for _, item := range items {
		btree.Insert(item)
	}

	assert.True(t, isBalanced(btree.Top))
	for _, child := range btree.Top.Children {
		assert.True(t, isBalanced(child))
	}
}

func isBalanced(node *storage.Node) bool {
	if node == nil {
		return true
	}

	if len(node.Items) > 2 {
		return false
	}

	for _, child := range node.Children {
		if !isBalanced(child) {
			return false
		}
	}

	return true
}
