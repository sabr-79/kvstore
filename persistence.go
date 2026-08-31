package main

import (
	"container/list"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"sync"
)

type RaftSnapshot struct {
	CurrentTerm int
	VotedFor    string
	Log         []LogEntry
}

type Metadata struct {
	rootpage int64
	maxpage  int64
	freelist []int64
}

type Pager struct {
	file      *os.File
	pagesize  int
	order     int
	mu        sync.Mutex
	cache     map[int64]*list.Element
	lru       *list.List
	maxBytes  int64
	usedBytes int64
	// reduce GC pressure
	encodePool *sync.Pool
}

type pageEntry struct {
	node  *TreeNode
	dirty bool
	page  int64
}

func newPager(filepath string, pagesize, order int, maxBytes int64) *Pager {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil
	}
	return &Pager{
		file:      file,
		pagesize:  pagesize,
		order:     order,
		cache:     make(map[int64]*list.Element),
		lru:       list.New(),
		maxBytes:  maxBytes,
		usedBytes: 0,
		encodePool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, pagesize)
			},
		},
	}
}

func persist(node *RaftNode) {
	filename := fmt.Sprintf("raft_%s.state", node.id)
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println("error creating file")
		return
	}
	encode := gob.NewEncoder(file)
	var current = RaftSnapshot{CurrentTerm: node.currentTerm, VotedFor: node.votedFor, Log: node.log}
	err = encode.Encode(&current)
	if err != nil {
		fmt.Println("error encoding state")
	}
	file.Sync()
	file.Close()

}

func persistLog(node *RaftNode) {
	node.mu.Lock()
	if len(node.pendingLog) == 0 {
		node.mu.Unlock()
		return
	}
	batch := node.pendingLog
	node.pendingLog = nil
	node.mu.Unlock()
	filename := fmt.Sprintf("raft_%s.log", node.id)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}

	encode := gob.NewEncoder(file)
	for _, entry := range batch {
		if err := encode.Encode(&entry); err != nil {
			panic(err)
		}
	}
	if err := file.Sync(); err != nil {
		panic(err)
	}
	file.Close()
}

func readPersistFile(node *RaftNode) {

	filename := fmt.Sprintf("raft_%s.state", node.id)

	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("no such file")
		return
	}

	decode := gob.NewDecoder(file)
	var recovered RaftSnapshot
	err = decode.Decode(&recovered)
	if err != nil {
		fmt.Println("error decoding file")
	}
	node.votedFor = recovered.VotedFor
	node.currentTerm = recovered.CurrentTerm

	logfile, err := os.Open(fmt.Sprintf("raft_%s.log", node.id))
	if err != nil {
		return
	}

	decode = gob.NewDecoder(logfile)
	node.log = nil
	var entry LogEntry
	for {
		err := decode.Decode(&entry)
		if err != nil {
			break
		}
		node.log = append(node.log, entry)
	}

	file.Close()
	logfile.Close()

}

func encodeNode(n *TreeNode, buf []byte) []byte {
	if n.isLeaf {
		buf[0] = 1
	} else {
		buf[0] = 0
	}
	binary.BigEndian.PutUint16(buf[1:3], uint16(len(n.keys)))
	binary.BigEndian.PutUint64(buf[3:11], uint64(n.nextPage))

	slotStart := 11
	dataBottom := len(buf)

	if n.isLeaf {
		for i := 0; i < len(n.keys); i++ {
			keyBytes := []byte(n.keys[i])
			valBytes := []byte(n.values[i])

			payloadSize := 2 + len(keyBytes) + 2 + len(valBytes)
			dataBottom -= payloadSize

			currPos := dataBottom
			binary.BigEndian.PutUint16(buf[currPos:currPos+2], uint16(len(keyBytes)))
			currPos += 2
			copy(buf[currPos:currPos+len(keyBytes)], keyBytes)
			currPos += len(keyBytes)

			binary.BigEndian.PutUint16(buf[currPos:currPos+2], uint16(len(valBytes)))
			currPos += 2
			copy(buf[currPos:currPos+len(valBytes)], valBytes)

			binary.BigEndian.PutUint16(buf[slotStart+(i*2):slotStart+(i*2)+2], uint16(dataBottom))
		}
	} else {
		childStart := 11
		for i := 0; i < len(n.children); i++ {
			binary.BigEndian.PutUint64(buf[childStart+(i*8):childStart+(i*8)+8], uint64(n.children[i]))
		}

		slotStart = childStart + (len(n.children) * 8)

		for i := 0; i < len(n.keys); i++ {
			keyBytes := []byte(n.keys[i])
			payloadSize := 2 + len(keyBytes)
			dataBottom -= payloadSize

			binary.BigEndian.PutUint16(buf[dataBottom:dataBottom+2], uint16(len(keyBytes)))
			copy(buf[dataBottom+2:dataBottom+2+len(keyBytes)], keyBytes)

			binary.BigEndian.PutUint16(buf[slotStart+(i*2):slotStart+(i*2)+2], uint16(dataBottom))
		}
	}

	return buf
}

func decodeNode(data []byte, order int) *TreeNode {
	isLeaf := data[0] == 1
	keyCount := int(binary.BigEndian.Uint16(data[1:3]))
	nextPage := int64(binary.BigEndian.Uint64(data[3:11]))

	node := &TreeNode{
		isLeaf:   isLeaf,
		order:    order,
		nextPage: nextPage,
		keys:     make([]string, keyCount),
	}

	slotStart := 11

	if isLeaf {
		node.values = make([]string, keyCount)
		for i := 0; i < keyCount; i++ {
			offset := int(binary.BigEndian.Uint16(data[slotStart+(i*2) : slotStart+(i*2)+2]))

			keyLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
			node.keys[i] = string(data[offset+2 : offset+2+keyLen])
			offset += 2 + keyLen

			valLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
			node.values[i] = string(data[offset+2 : offset+2+valLen])
		}
	} else {
		childCount := keyCount + 1
		node.children = make([]int64, childCount)

		childStart := 11
		for i := 0; i < childCount; i++ {
			node.children[i] = int64(binary.BigEndian.Uint64(data[childStart+(i*8) : childStart+(i*8)+8]))
		}

		slotStart = childStart + (childCount * 8)
		for i := 0; i < keyCount; i++ {
			offset := int(binary.BigEndian.Uint16(data[slotStart+(i*2) : slotStart+(i*2)+2]))
			keyLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
			node.keys[i] = string(data[offset+2 : offset+2+keyLen])
		}
	}

	return node
}

func encodeMeta(meta *Metadata, pagesize int) []byte {
	buf := make([]byte, pagesize)
	binary.BigEndian.PutUint64(buf[0:8], uint64(meta.rootpage))
	binary.BigEndian.PutUint64(buf[8:16], uint64(meta.maxpage))
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(meta.freelist)))

	for i, pageNum := range meta.freelist {
		offset := 20 + (i * 8)
		binary.BigEndian.PutUint64(buf[offset:offset+8], uint64(pageNum))
	}
	return buf
}

func decodeMeta(buf []byte) *Metadata {
	meta := &Metadata{
		rootpage: int64(binary.BigEndian.Uint64(buf[0:8])),
		maxpage:  int64(binary.BigEndian.Uint64(buf[8:16])),
	}
	count := int(binary.BigEndian.Uint32(buf[16:20]))
	meta.freelist = make([]int64, count)
	for i := 0; i < count; i++ {
		offset := 20 + (i * 8)
		meta.freelist[i] = int64(binary.BigEndian.Uint64(buf[offset : offset+8]))
	}
	return meta
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
	buf := make([]byte, p.pagesize)
	offset := pageNum * int64(p.pagesize)
	_, err := p.file.ReadAt(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("readPage %d: %w", pageNum, err)
	}
	return buf, nil
}

func (p *Pager) writePage(pageNum int64, data []byte) {
	offset := pageNum * int64(p.pagesize)
	if _, err := p.file.WriteAt(data, offset); err != nil {
		panic(fmt.Errorf("writePage %d: %w", pageNum, err))
	}
}

func (p *Pager) flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, elem := range p.cache {
		entry := elem.Value.(*pageEntry)
		if entry.dirty {
			p.flushNode(entry)
		}
	}
	return p.syncFile()
}

func (p *Pager) close() error {
	if err := p.flush(); err != nil {
		return err
	}
	return p.file.Close()
}

func (n *TreeNode) estimatedSize() int64 {
	size := int64(64)
	for _, k := range n.keys {
		size += int64(len(k)) + 16
	}
	for _, v := range n.values {
		size += int64(len(v)) + 16
	}
	size += int64(len(n.children) * 8)
	return size
}

func (p *Pager) evictToFit(needed int64) {
	for p.usedBytes+needed > p.maxBytes && p.lru.Len() > 0 {
		e := p.lru.Back()
		entry := e.Value.(*pageEntry)
		if entry.dirty {
			p.flushNode(entry)
		}
		delete(p.cache, entry.page)
		p.lru.Remove(e)
		p.usedBytes -= entry.node.estimatedSize()
	}
}

func (p *Pager) flushNode(entry *pageEntry) {
	buf := p.getEncodeBuffer()
	encodeNode(entry.node, buf)
	offset := entry.page * int64(p.pagesize)
	if _, err := p.file.WriteAt(buf, offset); err != nil {
		panic(err)
	}
	p.putEncodeBuffer(buf)
	entry.dirty = false
}
func (p *Pager) loadNode(pageNum int64) *TreeNode {
	p.mu.Lock()
	if elem, ok := p.cache[pageNum]; ok {
		p.lru.MoveToFront(elem)
		entry := elem.Value.(*pageEntry)
		p.mu.Unlock()
		return entry.node
	}
	p.mu.Unlock()

	buf := make([]byte, p.pagesize)
	offset := pageNum * int64(p.pagesize)
	_, err := p.file.ReadAt(buf, offset)
	if err != nil {
		panic(fmt.Errorf("read page %d: %w", pageNum, err))
	}
	node := decodeNode(buf, p.order)

	p.mu.Lock()
	defer p.mu.Unlock()

	if elem, ok := p.cache[pageNum]; ok {
		p.lru.MoveToFront(elem)
		return elem.Value.(*pageEntry).node
	}

	nodeSize := node.estimatedSize()
	p.evictToFit(nodeSize)

	entry := &pageEntry{node: node, dirty: false, page: pageNum}
	elem := p.lru.PushFront(entry)
	p.cache[pageNum] = elem
	p.usedBytes += nodeSize
	return node
}

func (p *Pager) writeNode(pageNum int64, node *TreeNode) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if elem, ok := p.cache[pageNum]; ok {
		entry := elem.Value.(*pageEntry)
		oldSize := entry.node.estimatedSize()
		newSize := node.estimatedSize()
		p.usedBytes += newSize - oldSize
		entry.node = node
		entry.dirty = true
		p.lru.MoveToFront(elem)

		if p.usedBytes > p.maxBytes {
			for p.usedBytes > p.maxBytes && p.lru.Len() > 1 {
				back := p.lru.Back()
				if back == elem {
					p.lru.MoveToFront(elem)
					continue
				}

				entryBack := back.Value.(*pageEntry)
				if entryBack.dirty {
					p.flushNode(entryBack)
				}
				delete(p.cache, entryBack.page)
				p.lru.Remove(back)
				p.usedBytes -= entryBack.node.estimatedSize()
			}
		}
		return
	}

	nodeSize := node.estimatedSize()
	p.evictToFit(nodeSize)
	entry := &pageEntry{node: node, dirty: true, page: pageNum}
	elem := p.lru.PushFront(entry)
	p.cache[pageNum] = elem
	p.usedBytes += nodeSize
}

func (p *Pager) getEncodeBuffer() []byte {
	return p.encodePool.Get().([]byte)
}

func (p *Pager) putEncodeBuffer(buf []byte) {
	p.encodePool.Put(buf)
}

func nodeEncodedSize(n *TreeNode) int {
	size := 11
	if n.isLeaf {
		size += len(n.keys) * 2
		for i := 0; i < len(n.keys); i++ {
			size += 2 + len(n.keys[i])
			size += 2 + len(n.values[i])
		}
	} else {
		size += len(n.children) * 8
		size += len(n.keys) * 2
		for i := 0; i < len(n.keys); i++ {
			size += 2 + len(n.keys[i])
		}
	}
	return size
}
