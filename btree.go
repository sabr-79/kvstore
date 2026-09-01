package main

import (
	"runtime"
	"slices"

	"golang.org/x/sys/unix"
)

// look into optimizing order num for m2/macOS page sizes

// current page design
// -- HEADER --
// isleaf - 1b
// keycount - 2b
// nextpage - 8b for leaf nodes

// Leaf node layout:

// -- SLOT DIRECTORY --
// growing downwards
// 2b offsets pointing to key entry

// -- KEY/VAL STORAGE --
// raw data, grows upwards
// key length (2b) | key bytes | value length (2b) | val bytes

// Internal node layout:
// Children - 8b page nums

// -- SLOT DIRECTORY --
// 2b offsets pointing to key entry

// -- KEY STORAGE --
// key length (2b) | key bytes

// why doesn't fsync work on macOS???

func (p *Pager) syncFile() error {
	if runtime.GOOS == "darwin" {
		_, err := unix.FcntlInt(p.file.Fd(), unix.F_FULLFSYNC, 0)
		return err
	}
	return p.file.Sync()
}

type TreeNode struct {
	isLeaf   bool
	order    int
	keys     []string
	values   []string
	children []int64
	nextPage int64
}

type TreeRoot struct {
	root    int64
	pager   *Pager
	order   int
	maxpage int64
	// can overflow metadata page due to heavy writes/deletes
	// will opt for having it as a linked list in the future
	freelist  []int64
	batchsize int
	pending   int
}

func newTree(filepath string, pagesize int, order int) *TreeRoot {
	pager := newPager(filepath, pagesize, order, 1024)
	tree := &TreeRoot{root: 1, pager: pager, order: order, maxpage: 1, batchsize: 100}

	info, _ := pager.file.Stat()
	if info.Size() == 0 {
		root := &TreeNode{isLeaf: true, order: order}
		encBuf := pager.getEncodeBuffer()
		pager.writePage(tree.root, encodeNode(root, encBuf))
		pager.putEncodeBuffer(encBuf)
		meta := &Metadata{
			rootpage: 1,
			maxpage:  1,
			freelist: make([]int64, 0),
		}
		pager.writePage(0, encodeMeta(meta, pagesize))
	} else {
		buf, err := pager.readPage(0)
		if err != nil {
			panic(err)
		}
		meta := decodeMeta(buf)
		if meta.rootpage > 0 {
			tree.root = meta.rootpage
			tree.maxpage = meta.maxpage
			tree.freelist = meta.freelist
		}
	}
	return tree
}

func (t *TreeRoot) search(key string) (string, bool) {
	currPage := t.root

	curr := t.pager.loadNode(currPage)

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
		currPage = curr.children[l]
		curr = t.pager.loadNode(currPage)

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
	currPage := t.root

	curr := t.pager.loadNode(currPage)

	path := []int64{currPage}
	for !curr.isLeaf {
		l, r := 0, len(curr.keys)
		for l < r {
			mid := l + (r-l)/2
			if key < curr.keys[mid] {
				r = mid
			} else {
				l = mid + 1
			}
		}
		currPage = curr.children[l]
		path = append(path, currPage)

		curr = t.pager.loadNode(currPage)
	}

	l, r := 0, len(curr.keys)
	for l < r {
		mid := l + (r-l)/2
		if curr.keys[mid] == key {
			l = mid
			break
		}
		if curr.keys[mid] > key {
			r = mid
		} else {
			l = mid + 1
		}
	}

	newLeaf := curr.clone()
	if l < len(curr.keys) && curr.keys[l] == key {
		newLeaf.values[l] = val
	} else {
		newLeaf.keys = append(newLeaf.keys, "")
		newLeaf.values = append(newLeaf.values, "")
		copy(newLeaf.keys[l+1:], newLeaf.keys[l:])
		copy(newLeaf.values[l+1:], newLeaf.values[l:])
		newLeaf.keys[l] = key
		newLeaf.values[l] = val

	}
	superseded := []int64{}
	if nodeEncodedSize(newLeaf) > t.pager.pagesize || len(newLeaf.keys) >= t.order {
		t.split(currPage, newLeaf, path, &superseded)
	} else {
		newLeafPage := t.allocatePage()
		t.pager.writeNode(newLeafPage, newLeaf)
		superseded = append(superseded, currPage)
		t.updateParentLinks(path, currPage, newLeafPage, &superseded)

	}

	t.freelist = append(t.freelist, superseded...)
	t.commitMetadata()
	t.pending++
	if t.pending >= t.batchsize {
		t.pager.flush()
		t.pending = 0
	}
}

func (t *TreeRoot) scan(start string, end string) (keys []string, values []string) {
	keys = []string{}
	values = []string{}

	type stackEntry struct {
		node     *TreeNode
		childIdx int
	}
	stack := []stackEntry{{node: t.pager.loadNode(t.root), childIdx: 0}}

	for len(stack) > 0 {
		entry := &stack[len(stack)-1]
		node := entry.node

		if node.isLeaf {
			for i := 0; i < len(node.keys); i++ {
				key := node.keys[i]
				if key > end {
					break
				}
				if key >= start {
					keys = append(keys, key)
					values = append(values, node.values[i])
				}
			}
			stack = stack[:len(stack)-1]
			continue
		}
		if entry.childIdx >= len(node.children) {
			stack = stack[:len(stack)-1]
			continue
		}

		childIdx := entry.childIdx
		entry.childIdx++

		var childMax string
		if childIdx < len(node.keys) {
			childMax = node.keys[childIdx]
		} else {
			childMax = ""
		}

		if childMax != "" && start >= childMax {
			continue
		}

		var childMin string
		if childIdx > 0 {
			childMin = node.keys[childIdx-1]
		} else {
			childMin = ""
		}
		if childMin != "" && end < childMin {
			continue
		}

		childPage := node.children[childIdx]
		childNode := t.pager.loadNode(childPage)
		stack = append(stack, stackEntry{node: childNode, childIdx: 0})
	}

	return keys, values
}
func (t *TreeRoot) remove(key string) bool {
	currPage := t.root

	curr := t.pager.loadNode(currPage)

	path := []int64{currPage}
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
		currPage = curr.children[l]
		path = append(path, currPage)

		curr = t.pager.loadNode(currPage)
	}
	superseded := []int64{}
	l, r := 0, len(curr.keys)
	for l < r {
		mid := l + (r-l)/2
		if curr.keys[mid] == key {
			newLeaf := curr.clone()
			newLeaf.keys = slices.Delete(curr.keys, mid, mid+1)
			newLeaf.values = slices.Delete(curr.values, mid, mid+1)

			newLeafPage := t.allocatePage()
			t.pager.writeNode(newLeafPage, newLeaf)
			superseded = append(superseded, currPage)
			t.updateParentLinks(path, currPage, newLeafPage, &superseded)
			t.freelist = append(t.freelist, superseded...)

			t.commitMetadata()
			t.pending++
			if t.pending >= t.batchsize {
				t.pager.flush()
				t.pending = 0
			}
			return true
		}
		if curr.keys[mid] > key {
			r = mid
		} else {
			l = mid + 1
		}
	}
	return false
}

func (t *TreeRoot) split(oldPage int64, curr *TreeNode, path []int64, superseded *[]int64) {
	mid := len(curr.keys) / 2
	var key string

	if curr.isLeaf {
		leftNode := curr.clone()
		rightPage := t.allocatePage()
		leftPage := t.allocatePage()
		rightNode := &TreeNode{isLeaf: true, order: curr.order}

		rightNode.keys = append([]string{}, curr.keys[mid:]...)
		rightNode.values = append([]string{}, curr.values[mid:]...)

		oldNext := curr.nextPage
		leftNode.keys = leftNode.keys[:mid]
		leftNode.values = leftNode.values[:mid]
		leftNode.nextPage = rightPage
		rightNode.nextPage = oldNext

		t.pager.writeNode(leftPage, leftNode)

		t.pager.writeNode(rightPage, rightNode)
		*superseded = append(*superseded, oldPage)

		key = rightNode.keys[0]

		if oldPage == t.root || len(path) < 2 {

			newRootPage := t.allocatePage()
			newRoot := &TreeNode{isLeaf: false, order: curr.order}
			newRoot.keys = []string{key}
			newRoot.children = []int64{leftPage, rightPage}
			t.pager.writeNode(newRootPage, newRoot)
			t.root = newRootPage
		} else {
			parentPage := path[len(path)-2]
			newPath := path[:len(path)-1]
			t.insertIntoParent(parentPage, newPath, key, leftPage, rightPage, oldPage, superseded)
		}
	} else {

		rightPage := t.allocatePage()
		rightNode := &TreeNode{isLeaf: false, order: curr.order}

		key = curr.keys[mid]
		rightNode.keys = append([]string{}, curr.keys[mid+1:]...)
		rightNode.children = append([]int64{}, curr.children[mid+1:]...)
		leftNode := curr.clone()
		leftNode.keys = curr.keys[:mid]
		leftNode.children = curr.children[:mid+1]

		leftPage := t.allocatePage()
		t.pager.writeNode(leftPage, leftNode)
		t.pager.writeNode(rightPage, rightNode)
		*superseded = append(*superseded, oldPage)

		if oldPage == t.root || len(path) < 2 {
			newRootPage := t.allocatePage()
			newRoot := &TreeNode{isLeaf: false, order: curr.order}
			newRoot.keys = []string{key}
			newRoot.children = []int64{leftPage, rightPage}
			t.pager.writeNode(newRootPage, newRoot)
			t.root = newRootPage
		} else {
			parentPage := path[len(path)-2]
			newPath := path[:len(path)-1]
			t.insertIntoParent(parentPage, newPath, key, leftPage, rightPage, oldPage, superseded)
		}
	}
}

func (t *TreeRoot) insertIntoParent(parentPage int64, path []int64, key string, left int64, right int64, oldChild int64, superseded *[]int64) {

	parent := t.pager.loadNode(parentPage)
	parentCopy := parent.clone()

	for i := 0; i < len(parentCopy.children); i++ {
		if parentCopy.children[i] == oldChild {
			parentCopy.children[i] = left
			break
		}
	}

	l, r := 0, len(parentCopy.keys)
	for l < r {
		mid := l + (r-l)/2
		if parentCopy.keys[mid] > key {
			r = mid
		} else {
			l = mid + 1
		}
	}
	parentCopy.keys = append(parentCopy.keys, "")
	parentCopy.children = append(parentCopy.children, 0)
	copy(parentCopy.keys[l+1:], parentCopy.keys[l:])
	parentCopy.keys[l] = key
	copy(parentCopy.children[l+2:], parentCopy.children[l+1:])
	parentCopy.children[l+1] = right

	if len(parentCopy.keys) >= parentCopy.order {
		t.split(parentPage, parentCopy, path, superseded)
	} else {
		newParent := t.allocatePage()
		t.pager.writeNode(newParent, parentCopy)
		*superseded = append(*superseded, parentPage)
		t.updateParentLinks(path, parentPage, newParent, superseded)
	}
}

func (t *TreeRoot) updateParentLinks(path []int64, oldPage int64, newPage int64, superseded *[]int64) {
	if len(path) <= 1 || oldPage == t.root {
		t.root = newPage
		return
	}

	parentPage := path[len(path)-2]

	parent := t.pager.loadNode(parentPage)
	parentCopy := parent.clone()
	for i := 0; i < len(parentCopy.children); i++ {
		if parentCopy.children[i] == oldPage {
			parentCopy.children[i] = newPage
			break
		}
	}

	newParent := t.allocatePage()

	t.pager.writeNode(newParent, parentCopy)
	*superseded = append(*superseded, parentPage)

	t.updateParentLinks(path[:len(path)-1], parentPage, newParent, superseded)
}

func (n *TreeNode) clone() *TreeNode {
	return &TreeNode{
		isLeaf:   n.isLeaf,
		order:    n.order,
		keys:     append([]string{}, n.keys...),
		values:   append([]string{}, n.values...),
		children: append([]int64{}, n.children...),
		nextPage: n.nextPage,
	}
}
