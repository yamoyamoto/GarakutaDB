package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	MaxItems = 2
)

// Items TODO: should depend on interface
type Items []StringItem

type Node struct {
	Items    Items
	Children []*Node
}

func (n *Node) insertRec(itm StringItem) (*StringItem, *Node, error) {
	if len(n.Children) > 0 {
		for i, item := range n.Items {
			if itm.Equal(item) {
				return nil, nil, errors.New("item already exists")
			}
			if itm.Less(item) {
				median, newNode, err := n.Children[i].insertRec(itm)
				if err != nil {
					return nil, nil, err
				}
				if newNode != nil {
					// move median to parent
					n.Items = append(n.Items[:i], append(Items{*median}, n.Items[i:]...)...)
					n.Children = append(n.Children[:i+1], append([]*Node{newNode}, n.Children[i+1:]...)...)
				}
				if len(n.Items) > MaxItems {
					newItem, newSplitNode := splitNode(n)
					return newItem, newSplitNode, nil
				}
				return nil, nil, nil
			}
		}
		// insert to last child recursively
		median, newNode, err := n.Children[len(n.Children)-1].insertRec(itm)
		if err != nil {
			return nil, nil, err
		}
		if newNode != nil {
			n.Items = append(n.Items, *median)
			n.Children = append(n.Children, newNode)
		}
		if len(n.Items) > MaxItems {
			newItem, newSplitNode := splitNode(n)
			return newItem, newSplitNode, nil
		}
		return nil, nil, nil
	}

	// insert item to leaf node
	alreadyInserted := false
	for i, item := range n.Items {
		if itm.Equal(item) {
			return nil, nil, errors.New("item already exists")
		}
		if itm.Less(item) {
			n.Items = append(n.Items[:i], append(Items{itm}, n.Items[i:]...)...)
			alreadyInserted = true
			break
		}
	}
	if !alreadyInserted {
		n.Items = append(n.Items, itm)
	}

	// leaf node is full
	if len(n.Items) > MaxItems {
		newItem, newSplitNode := splitNode(n)
		return newItem, newSplitNode, nil
	}

	return nil, nil, nil
}

func splitNode(n *Node) (*StringItem, *Node) {
	middleIndex := len(n.Items) / 2
	median := n.Items[middleIndex]

	leftChildren := make([]*Node, 0)
	rightChildren := make([]*Node, 0)
	for i, child := range n.Children {
		if i <= middleIndex {
			leftChildren = append(leftChildren, child)
		} else {
			rightChildren = append(rightChildren, child)
		}
	}

	// right node
	newNode := &Node{
		Items:    n.Items[middleIndex+1:],
		Children: rightChildren,
	}

	// left node
	n.Items = n.Items[:middleIndex]
	n.Children = leftChildren

	return &median, newNode
}

type BTree struct {
	Top       *Node
	TableName string
	IndexName string
	Mutex     sync.RWMutex
}

func NewBTree(tableName string, IndexName string) *BTree {
	return &BTree{
		Top:       nil,
		TableName: tableName,
		IndexName: IndexName,
		Mutex:     sync.RWMutex{},
	}
}

func (b *BTree) Serialize() ([]byte, error) {
	return json.Marshal(b)
}

func DeserializeBTree(data []byte) (*BTree, error) {
	b := &BTree{}
	err := json.Unmarshal(data, b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (b *BTree) Insert(itm *StringItem) error {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	if b.Top == nil {
		b.Top = &Node{
			Items:    Items{*itm},
			Children: nil,
		}
		return nil
	}

	item, newNode, err := b.Top.insertRec(*itm)
	if err != nil {
		return err
	}
	if newNode != nil {
		newRoot := &Node{
			Items:    Items{*item},
			Children: []*Node{b.Top, newNode},
		}
		b.Top = newRoot
	}

	return nil
}

func (n *Node) search(item *StringItem) (*StringItem, bool) {
	for i, itm := range n.Items {
		if item.Less(itm) {
			if len(n.Children) == 0 {
				return nil, false
			}
			return n.Children[i].search(item)
		} else if itm.Value == item.Value {
			return &itm, true
		}
	}

	if len(n.Children) > 0 {
		return n.Children[len(n.Children)-1].search(item)
	}

	return nil, false
}

func (b *BTree) Search(item *StringItem) (*StringItem, bool) {
	b.Mutex.RLock()
	defer b.Mutex.RUnlock()

	if b.Top == nil {
		return nil, false
	}

	return b.Top.search(item)
}

// Delete deletes item from btree
// TODO: improve performance (implement delete method of btree)
func (b *BTree) Delete(item *StringItem) bool {
	b.Mutex.Lock()
	defer b.Mutex.Unlock()

	newBtree := NewBTree(b.TableName, b.IndexName)

	items := b.scan()
	isDeleted := false
	for _, itm := range items {
		if itm.Value == item.Value {
			isDeleted = true
		} else {
			newBtree.Insert(&itm)
		}
	}

	b.Top = newBtree.Top

	return isDeleted
}

// Scan returns all items in btree
// WARNING: this method is not thread safe!
func (b *BTree) scan() []StringItem {
	if b.Top == nil {
		return nil
	}

	return b.Top.scan()
}

func (n *Node) scan() []StringItem {
	items := make([]StringItem, 0)

	for _, child := range n.Children {
		items = append(items, child.scan()...)
	}
	items = append(items, n.Items...)

	return items
}

func (n *Node) printNode(indent int) {
	// 各ノードのアイテムをインデントして表示
	indentStr := strings.Repeat(" ", indent)
	fmt.Printf("%sNode Items: %v\n", indentStr, n.Items)

	// 子ノードを再帰的に表示
	for _, child := range n.Children {
		child.printNode(indent + 2)
	}
}

func (b *BTree) PrintTree() {
	b.Mutex.RLock()
	defer b.Mutex.RUnlock()

	if b.Top != nil {
		b.Top.printNode(0)
	} else {
		fmt.Println("Tree is empty")
	}
}
