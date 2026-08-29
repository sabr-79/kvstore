package main

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
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
	node.log = recovered.Log

	file.Close()

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

func (p *Pager) getEncodeBuffer() []byte {
	return p.encodePool.Get().([]byte)
}

func (p *Pager) putEncodeBuffer(buf []byte) {
	p.encodePool.Put(buf)
}
