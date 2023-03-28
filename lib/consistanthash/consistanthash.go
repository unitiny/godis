package consistanthash

import (
	"hash/crc32"
	"sort"
)

type HashFunc func(data []byte) uint32

type NodeMap struct {
	hashFunc    HashFunc
	nodeHashs   []int
	nodeHashMap map[int]string
}

func NewNodeMap(hashFunc HashFunc) *NodeMap {
	nodeMap := &NodeMap{
		hashFunc:    hashFunc,
		nodeHashMap: make(map[int]string),
	}
	if nodeMap.hashFunc == nil {
		nodeMap.hashFunc = crc32.ChecksumIEEE
	}
	return nodeMap
}

func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashs) == 0
}

func (m *NodeMap) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}

		hash := int(m.hashFunc([]byte(key)))
		m.nodeHashs = append(m.nodeHashs, hash)
		m.nodeHashMap[hash] = key
	}
	sort.Ints(m.nodeHashs)
}

func (m *NodeMap) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := int(m.hashFunc([]byte(key)))
	idx := sort.Search(len(m.nodeHashs), func(i int) bool {
		return m.nodeHashs[i] >= hash
	})

	// 闭环
	if idx == len(m.nodeHashs) {
		idx = 0
	}
	return m.nodeHashMap[m.nodeHashs[idx]]
}
