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
			curr.values[mid] = val
			if nodeEncodedSize(curr) > t.pager.pagesize {
				superseded := []int64{}
				t.split(currPage, curr, path, &superseded)
				t.freelist = append(t.freelist, superseded...)
				t.commitMetadata()
				t.pending++
				if t.pending >= t.batchsize {
					t.pager.flush()
					t.pending = 0
				}
				return
			}

			t.pager.writeNode(currPage, curr)
			t.pending++
			if t.pending >= t.batchsize {
				t.pager.flush()
				t.pending = 0
			}
			return
		}
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

	superseded := []int64{}

	if nodeEncodedSize(curr) > t.pager.pagesize || len(curr.keys) >= t.order {
		t.split(currPage, curr, path, &superseded)
	} else {
		t.pager.writeNode(currPage, curr)
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

	currPage := t.root

	curr := t.pager.loadNode(currPage)

	for !curr.isLeaf {
		if len(curr.children) == 0 {
			return keys, values
		}
		if len(curr.keys) == 0 {
			currPage = curr.children[0]
			curr = t.pager.loadNode(currPage)
			continue
		}

		l, r := 0, len(curr.keys)
		for l < r {
			mid := l + (r-l)/2
			if curr.keys[mid] > start {
				r = mid
			} else {
				l = mid + 1
			}
		}
		if l >= len(curr.children) {
			l = len(curr.children) - 1
		}
		if l < 0 {
			l = 0
		}
		currPage = curr.children[l]
		curr = t.pager.loadNode(currPage)
	}

	for currPage != 0 {

		if len(curr.keys) == 0 {
			currPage = curr.nextPage
			if currPage == 0 {
				break
			}
			curr = t.pager.loadNode(currPage)
			continue
		}

		keyCount := len(curr.keys)

		for i := 0; i < keyCount; i++ {
			key := curr.keys[i]
			if key > end {
				return keys, values
			}
			if key >= start {
				keys = append(keys, key)
				values = append(values, curr.values[i])
			}
		}

		currPage = curr.nextPage
		if currPage == 0 {
			break
		}
		curr = t.pager.loadNode(currPage)
	}

	return keys, values
}
func (t *TreeRoot) remove(key string) {
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

	l, r := 0, len(curr.keys)
	for l < r {
		mid := l + (r-l)/2
		if curr.keys[mid] == key {
			curr.keys = slices.Delete(curr.keys, mid, mid+1)
			curr.values = slices.Delete(curr.values, mid, mid+1)

			t.pager.writeNode(currPage, curr)

			t.commitMetadata()
			t.pending++
			if t.pending >= t.batchsize {
				t.pager.flush()
				t.pending = 0
			}
			return
		}
		if curr.keys[mid] > key {
			r = mid
		} else {
			l = mid + 1
		}
	}
}

func (t *TreeRoot) split(oldPage int64, curr *TreeNode, path []int64, superseded *[]int64) {
	mid := len(curr.keys) / 2
	var key string

	if curr.isLeaf {
		rightPage := t.allocatePage()
		rightNode := &TreeNode{isLeaf: true, order: curr.order}

		rightNode.keys = append([]string{}, curr.keys[mid:]...)
		rightNode.values = append([]string{}, curr.values[mid:]...)

		oldNext := curr.nextPage
		curr.keys = curr.keys[:mid]
		curr.values = curr.values[:mid]
		curr.nextPage = rightPage
		rightNode.nextPage = oldNext

		t.pager.writeNode(oldPage, curr)

		t.pager.writeNode(rightPage, rightNode)

		key = rightNode.keys[0]

		if oldPage == t.root || len(path) < 2 {

			newRootPage := t.allocatePage()
			newRoot := &TreeNode{isLeaf: false, order: curr.order}
			newRoot.keys = []string{key}
			newRoot.children = []int64{oldPage, rightPage}
			t.pager.writeNode(newRootPage, newRoot)
			t.root = newRootPage
		} else {
			parentPage := path[len(path)-2]
			newPath := path[:len(path)-1]
			t.insertIntoParent(parentPage, newPath, key, oldPage, rightPage, oldPage, superseded)
		}
	} else {

		rightPage := t.allocatePage()
		rightNode := &TreeNode{isLeaf: false, order: curr.order}

		key = curr.keys[mid]
		rightNode.keys = append([]string{}, curr.keys[mid+1:]...)
		rightNode.children = append([]int64{}, curr.children[mid+1:]...)
		curr.keys = curr.keys[:mid]
		curr.children = curr.children[:mid+1]

		leftPage := t.allocatePage()
		t.pager.writeNode(leftPage, curr)
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

	for i := 0; i < len(parent.children); i++ {
		if parent.children[i] == oldChild {
			parent.children[i] = left
			break
		}
	}

	l, r := 0, len(parent.keys)
	for l < r {
		mid := l + (r-l)/2
		if parent.keys[mid] > key {
			r = mid
		} else {
			l = mid + 1
		}
	}
	parent.keys = append(parent.keys, "")
	parent.children = append(parent.children, 0)
	copy(parent.keys[l+1:], parent.keys[l:])
	parent.keys[l] = key
	copy(parent.children[l+2:], parent.children[l+1:])
	parent.children[l+1] = right

	if len(parent.keys) >= parent.order {
		t.split(parentPage, parent, path, superseded)
	} else {
		newParent := t.allocatePage()
		t.pager.writeNode(newParent, parent)
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

	for i := 0; i < len(parent.children); i++ {
		if parent.children[i] == oldPage {
			parent.children[i] = newPage
			break
		}
	}

	t.pager.writeNode(parentPage, parent)

	t.updateParentLinks(path[:len(path)-1], parentPage, parentPage, superseded)
}
