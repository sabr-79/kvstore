package main

import (
	"container/list"
	"fmt"
	"os"
	"runtime"
	"slices"
	"sync"

	"golang.org/x/sys/unix"
)

// look into optimizing order num for m2/macOS page sizes

// current page design
// -- HEADER --
// isleaf - 1b
// keycount - 2b
// nextpage - 8b

// -- SLOT DIRECTORY --
// 2b offsets pointing to key/val
// growing downwards

// -- KEY/VAL STORAGE
// raw data, grows upwards

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
	// can overflow can cause panic for internal splits
	// will opt for having it as a linked list in the future
	freelist  []int64
	batchsize int
	pending   int
}

type Pager struct {
	file       *os.File
	pagesize   int
	mu         sync.Mutex
	dirtyPages map[int64][]byte
	dirtyList  []int64
	cleanPages map[int64][]byte
	cleanList  *list.List // LRU eviction
	maxClean   int

	// reduce GC pressure
	encodePool *sync.Pool
}

func newTree(filepath string, pagesize int, order int) *TreeRoot {
	pager := newPager(filepath, pagesize)
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

func newPager(filepath string, pagesize int) *Pager {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil
	}
	return &Pager{
		file:       file,
		pagesize:   pagesize,
		dirtyPages: make(map[int64][]byte),
		cleanPages: make(map[int64][]byte),
		cleanList:  list.New(),
		maxClean:   8192,
		encodePool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, pagesize)
			},
		},
	}
}

func (t *TreeRoot) allocatePage() int64 {
	var assignedpage int64
	if len(t.freelist) > 0 {
		assignedpage = t.freelist[len(t.freelist)-1]
		t.freelist = t.freelist[:len(t.freelist)-1]
	} else {
		t.maxpage++
		assignedpage = t.maxpage
	}
	return assignedpage
}
func (t *TreeRoot) commitMetadata() {
	meta := &Metadata{
		rootpage: t.root,
		maxpage:  t.maxpage,
		freelist: t.freelist,
	}
	t.pager.writePage(0, encodeMeta(meta, t.pager.pagesize))
}

func (p *Pager) readPage(pageNum int64) ([]byte, error) {
	p.mu.Lock()

	if data, ok := p.dirtyPages[pageNum]; ok {
		p.mu.Unlock()
		return data, nil
	}

	if data, ok := p.cleanPages[pageNum]; ok {
		for e := p.cleanList.Front(); e != nil; e = e.Next() {
			if e.Value.(int64) == pageNum {
				p.cleanList.MoveToFront(e)
				break
			}
		}
		p.mu.Unlock()
		return data, nil
	}

	p.mu.Unlock()

	buf := make([]byte, p.pagesize)
	offset := pageNum * int64(p.pagesize)
	_, err := p.file.ReadAt(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("ReadPage %d: %w", pageNum, err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if data, ok := p.dirtyPages[pageNum]; ok {
		return data, nil
	}
	if data, ok := p.cleanPages[pageNum]; ok {
		return data, nil
	}

	if p.cleanList.Len() >= p.maxClean {
		e := p.cleanList.Back()
		if e != nil {
			victim := e.Value.(int64)
			delete(p.cleanPages, victim)
			p.cleanList.Remove(e)
		}
	}

	p.cleanPages[pageNum] = buf
	p.cleanList.PushFront(pageNum)

	return buf, nil
}

func (p *Pager) writePage(pageNum int64, data []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.cleanPages[pageNum]; ok {
		delete(p.cleanPages, pageNum)
		for e := p.cleanList.Front(); e != nil; e = e.Next() {
			if e.Value.(int64) == pageNum {
				p.cleanList.Remove(e)
				break
			}
		}
	}

	if _, already := p.dirtyPages[pageNum]; !already {
		p.dirtyList = append(p.dirtyList, pageNum)
	}
	copyData := make([]byte, len(data))
	copy(copyData, data)
	p.dirtyPages[pageNum] = copyData
}

func (p *Pager) flush() error {
	p.mu.Lock()
	if len(p.dirtyList) == 0 {
		p.mu.Unlock()
		return nil
	}

	for _, pageNum := range p.dirtyList {
		data := p.dirtyPages[pageNum]
		offset := pageNum * int64(p.pagesize)
		if _, err := p.file.WriteAt(data, offset); err != nil {
			p.mu.Unlock()
			return fmt.Errorf("flush page %d: %w", pageNum, err)
		}
	}

	if err := p.syncFile(); err != nil {
		p.mu.Unlock()
		return err
	}

	for _, pageNum := range p.dirtyList {
		data := p.dirtyPages[pageNum]

		if p.cleanList.Len() >= p.maxClean {
			e := p.cleanList.Back()
			if e != nil {
				victim := e.Value.(int64)
				delete(p.cleanPages, victim)
				p.cleanList.Remove(e)
			}
		}

		p.cleanPages[pageNum] = data
		p.cleanList.PushFront(pageNum)
	}

	p.dirtyPages = make(map[int64][]byte)
	p.dirtyList = nil

	p.mu.Unlock()
	return nil
}

func (p *Pager) close() error {
	if err := p.flush(); err != nil {
		return err
	}
	return p.file.Close()
}

func (t *TreeRoot) search(key string) (string, bool) {
	buf, err := t.pager.readPage(0)
	if err != nil {
		panic(err)
	}
	meta := decodeMeta(buf)
	currPage := meta.rootpage
	buf, err = t.pager.readPage(currPage)
	if err != nil {
		panic(err)
	}
	curr := decodeNode(buf, t.order)
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
		buf, err = t.pager.readPage(currPage)
		if err != nil {
			panic(err)
		}
		curr = decodeNode(buf, t.order)

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
	buf, err := t.pager.readPage(0)
	if err != nil {
		panic(err)
	}
	meta := decodeMeta(buf)

	currPage := meta.rootpage
	buf, err = t.pager.readPage(currPage)
	if err != nil {
		panic(err)
	}
	curr := decodeNode(buf, t.order)

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
		buf, err = t.pager.readPage(currPage)
		if err != nil {
			panic(err)
		}
		curr = decodeNode(buf, t.order)
	}

	l, r := 0, len(curr.keys)
	for l < r {
		mid := l + (r-l)/2
		if curr.keys[mid] == key {
			curr.values[mid] = val
			encBuf := t.pager.getEncodeBuffer()
			t.pager.writePage(currPage, encodeNode(curr, encBuf))
			t.pager.putEncodeBuffer(encBuf)
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

	if len(curr.keys) >= t.order {
		t.split(currPage, curr, path, &superseded)
	} else {
		encBuf := t.pager.getEncodeBuffer()
		t.pager.writePage(currPage, encodeNode(curr, encBuf))
		t.pager.putEncodeBuffer(encBuf)
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

	buf, err := t.pager.readPage(0)
	if err != nil {
		panic(err)
	}
	meta := decodeMeta(buf)
	currPage := meta.rootpage

	buf, err = t.pager.readPage(currPage)
	if err != nil {
		panic(err)
	}
	curr := decodeNode(buf, t.order)

	for !curr.isLeaf {
		if len(curr.children) == 0 {
			return keys, values
		}
		if len(curr.keys) == 0 {
			currPage = curr.children[0]
			buf, err = t.pager.readPage(currPage)
			if err != nil {
				panic(err)
			}
			curr = decodeNode(buf, t.order)
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
		buf, err = t.pager.readPage(currPage)
		if err != nil {
			panic(err)
		}
		curr = decodeNode(buf, t.order)
	}

	pagelimit := 10000
	for currPage != 0 && pagelimit > 0 {
		pagelimit--

		if len(curr.keys) == 0 {
			currPage = curr.nextPage
			if currPage == 0 {
				break
			}
			buf, err = t.pager.readPage(currPage)
			if err != nil {
				panic(err)
			}
			curr = decodeNode(buf, t.order)
			continue
		}

		keyCount := len(curr.keys)
		valCount := len(curr.values)
		if keyCount != valCount {
			if keyCount > valCount {
				keyCount = valCount
			}
		}

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
		buf, err = t.pager.readPage(currPage)
		if err != nil {
			panic(err)
		}
		curr = decodeNode(buf, t.order)
	}

	return keys, values
}
func (t *TreeRoot) remove(key string) {
	buf, err := t.pager.readPage(0)
	if err != nil {
		panic(err)
	}
	meta := decodeMeta(buf)

	currPage := meta.rootpage
	buf, err = t.pager.readPage(currPage)
	if err != nil {
		panic(err)
	}
	curr := decodeNode(buf, t.order)

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
		buf, err = t.pager.readPage(currPage)
		if err != nil {
			panic(err)
		}
		curr = decodeNode(buf, t.order)
	}

	l, r := 0, len(curr.keys)
	for l < r {
		mid := l + (r-l)/2
		if curr.keys[mid] == key {
			curr.keys = slices.Delete(curr.keys, mid, mid+1)
			curr.values = slices.Delete(curr.values, mid, mid+1)

			encBuf := t.pager.getEncodeBuffer()
			t.pager.writePage(currPage, encodeNode(curr, encBuf))
			t.pager.putEncodeBuffer(encBuf)

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

		encBuf := t.pager.getEncodeBuffer()
		t.pager.writePage(oldPage, encodeNode(curr, encBuf))
		t.pager.putEncodeBuffer(encBuf)

		encBuf = t.pager.getEncodeBuffer()
		t.pager.writePage(rightPage, encodeNode(rightNode, encBuf))
		t.pager.putEncodeBuffer(encBuf)

		key = rightNode.keys[0]

		if oldPage == t.root || len(path) < 2 {

			newRootPage := t.allocatePage()
			newRoot := &TreeNode{isLeaf: false, order: curr.order}
			newRoot.keys = []string{key}
			newRoot.children = []int64{oldPage, rightPage}
			encBuf := t.pager.getEncodeBuffer()
			t.pager.writePage(newRootPage, encodeNode(newRoot, encBuf))
			t.pager.putEncodeBuffer(encBuf)
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
		encBuf := t.pager.getEncodeBuffer()
		t.pager.writePage(leftPage, encodeNode(curr, encBuf))
		t.pager.putEncodeBuffer(encBuf)
		encBuf = t.pager.getEncodeBuffer()
		t.pager.writePage(rightPage, encodeNode(rightNode, encBuf))
		t.pager.putEncodeBuffer(encBuf)

		*superseded = append(*superseded, oldPage)

		if oldPage == t.root || len(path) < 2 {
			newRootPage := t.allocatePage()
			newRoot := &TreeNode{isLeaf: false, order: curr.order}
			newRoot.keys = []string{key}
			newRoot.children = []int64{leftPage, rightPage}
			encBuf := t.pager.getEncodeBuffer()
			t.pager.writePage(newRootPage, encodeNode(newRoot, encBuf))
			t.pager.putEncodeBuffer(encBuf)
			t.root = newRootPage
		} else {
			parentPage := path[len(path)-2]
			newPath := path[:len(path)-1]
			t.insertIntoParent(parentPage, newPath, key, leftPage, rightPage, oldPage, superseded)
		}
	}
}

func (t *TreeRoot) insertIntoParent(parentPage int64, path []int64, key string, left int64, right int64, oldChild int64, superseded *[]int64) {
	buf, err := t.pager.readPage(parentPage)
	if err != nil {
		panic(err)
	}
	parent := decodeNode(buf, t.order)

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
		encBuf := t.pager.getEncodeBuffer()
		t.pager.writePage(newParent, encodeNode(parent, encBuf))
		t.pager.putEncodeBuffer(encBuf)

		t.updateParentLinks(path, parentPage, newParent, superseded)
	}
}

func (t *TreeRoot) updateParentLinks(path []int64, oldPage int64, newPage int64, superseded *[]int64) {
	if len(path) <= 1 || oldPage == t.root {
		t.root = newPage
		return
	}

	parentPage := path[len(path)-2]
	buf, err := t.pager.readPage(parentPage)
	if err != nil {
		panic(err)
	}
	parent := decodeNode(buf, t.order)

	for i := 0; i < len(parent.children); i++ {
		if parent.children[i] == oldPage {
			parent.children[i] = newPage
			break
		}
	}

	encBuf := t.pager.getEncodeBuffer()
	t.pager.writePage(parentPage, encodeNode(parent, encBuf))
	t.pager.putEncodeBuffer(encBuf)

	t.updateParentLinks(path[:len(path)-1], parentPage, parentPage, superseded)
}
