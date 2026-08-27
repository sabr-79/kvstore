package main

import (
	"slices"
)

// look into optimizing order num for m2/macOS page sizes

type TreeNode struct {
	isLeaf   bool
	order    int
	keys     []string
	values   []string
	parent   *TreeNode
	children []*TreeNode
	next     *TreeNode
}

type TreeRoot struct {
	root *TreeNode
}

func newTree(order int) *TreeRoot {
	return &TreeRoot{root: &TreeNode{isLeaf: true, order: order}}

}

func (t *TreeRoot) search(key string) (string, bool) {
	curr := t.root
	for !curr.isLeaf {
		l := 0
		r := len(curr.keys)
		for l < r {
			mid := l + (r-l)/2
			if key < curr.keys[mid] {
				r = mid
			}
			if key >= curr.keys[mid] {
				l = mid + 1
			}

		}
		curr = curr.children[l]

	}
	l := 0
	r := len(curr.keys)
	for l < r {
		mid := l + (r-l)/2

		if curr.keys[mid] == key {
			return curr.values[mid], true

		} else if curr.keys[mid] > key {
			r = mid

		} else {
			l = mid + 1
		}
	}
	return "", false

}

func (t *TreeRoot) insert(key string, val string) {
	curr := t.root
	for !curr.isLeaf {
		l := 0
		r := len(curr.keys)
		for l < r {
			mid := l + (r-l)/2
			if key < curr.keys[mid] {
				r = mid
			}
			if key >= curr.keys[mid] {
				l = mid + 1
			}

		}
		curr = curr.children[l]

	}
	l, r := 0, len(curr.keys)

	for l < r {
		mid := l + (r-l)/2
		if curr.keys[mid] > key {
			r = mid

		} else {
			l = mid + 1
		}
	}
	curr.keys = append(curr.keys, "")
	curr.values = append(curr.values, "")
	copy(curr.keys[l+1:], curr.keys[l:])
	copy(curr.values[l+1:], curr.values[l:])
	curr.keys[l] = key
	curr.values[l] = val
	if len(curr.keys) >= t.root.order {
		t.split(curr)
	}

}

func (t *TreeRoot) scan(start string, end string) map[string]string {
	res := make(map[string]string)
	curr := t.root

	for !curr.isLeaf {
		l, r := 0, len(curr.keys)
		for l < r {
			mid := l + (r-l)/2

			if curr.keys[mid] > start {
				r = mid
			} else {
				l = mid + 1
			}
		}
		curr = curr.children[l]
	}

	for curr != nil {
		for i := 0; i < len(curr.keys); i++ {
			key := curr.keys[i]

			if start <= key && key <= end {
				res[key] = curr.values[i]
			}

			if key > end {
				return res
			}

		}

		curr = curr.next
	}

	return res

}

func (t *TreeRoot) remove(key string) {
	curr := t.root

	for !curr.isLeaf {
		l, r := 0, len(curr.keys)
		for l < r {
			mid := l + (r-l)/2

			if curr.keys[mid] > key {
				r = mid
			} else {
				l = mid + 1
			}
		}
		curr = curr.children[l]
	}
	l, r := 0, len(curr.keys)

	for l < r {
		mid := l + (r-l)/2
		if curr.keys[mid] >= key {
			r = mid
		} else {
			l = mid + 1
		}
	}

	if curr.keys[l] == key {
		curr.keys = slices.Delete(curr.keys, l, l+1)
		curr.values = slices.Delete(curr.values, l, l+1)
	}

}

func (t *TreeRoot) split(curr *TreeNode) {
	rightNode := &TreeNode{isLeaf: curr.isLeaf, order: curr.order, parent: curr.parent}

	mid := len(curr.keys) / 2

	var key string

	if curr.isLeaf {
		rightNode.keys = append([]string{}, curr.keys[mid:]...)
		rightNode.values = append([]string{}, curr.values[mid:]...)
		curr.keys = curr.keys[:mid]
		curr.values = curr.values[:mid]
		rightNode.next = curr.next
		curr.next = rightNode
		key = rightNode.keys[0]
	} else {
		key = curr.keys[mid]
		rightNode.keys = append([]string{}, curr.keys[mid+1:]...)
		rightNode.children = append([]*TreeNode{}, curr.children[mid+1:]...)
		for i := 0; i < len(rightNode.children); i++ {
			rightNode.children[i].parent = rightNode
		}
		curr.keys = curr.keys[:mid]
		curr.children = curr.children[:mid+1]
	}
	if curr.parent == nil {
		newRoot := &TreeNode{isLeaf: false, order: curr.order}
		newRoot.keys = []string{key}
		newRoot.children = []*TreeNode{curr, rightNode}
		curr.parent = newRoot
		rightNode.parent = newRoot
		t.root = newRoot

	} else {
		t.insertIntoParent(curr.parent, key, rightNode)
	}

}

func (t *TreeRoot) insertIntoParent(parent *TreeNode, key string, right *TreeNode) {
	l := 0
	r := len(parent.keys)

	for l < r {
		mid := l + (r-l)/2

		if parent.keys[mid] > key {
			r = mid
		} else {
			l = mid + 1
		}

	}

	parent.keys = append(parent.keys, "")
	parent.children = append(parent.children, nil)

	copy(parent.keys[l+1:], parent.keys[l:])
	parent.keys[l] = key

	copy(parent.children[l+2:], parent.children[l+1:])
	parent.children[l+1] = right

	if len(parent.keys) >= parent.order {
		t.split(parent)
	}

}
