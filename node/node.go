package node

import (
	"errors"
	"unsafe"
)

var (
	ErrNonexistentIndex = errors.New("node has no matching edge")
)

// Node : radix tree node
// stores data about a node and its edges.
type Node struct {
	isLeaf uint8       // indicates whether this node has an associated value
	edges  [256]uint64 // possile indicies to next child nodes
	offset uint64      // reference to offset of data
	size   uint64      // reference to size of data
}

func New() *Node {
	return &Node{}
}

func (n *Node) NextIndex(b byte) uint64 {
	return n.edges[int(b)]
}

// Serialize : serialize a node to a byteslice
func Serialize(n *Node) []byte {
	data := make([]byte, 4096)

	data[0] = *(*byte)(unsafe.Pointer(&n.isLeaf))

	// has to be a better way to do this?
	edges := *(*[2048]byte)(unsafe.Pointer(&n.edges))
	for i := 1; i < 2049; i++ {
		data[i] = edges[i-1]
	}

	offset := *(*[8]byte)(unsafe.Pointer(&n.offset))
	for i := 4080; i < 4088; i++ {
		data[i] = offset[i-4080]
	}

	size := *(*[8]byte)(unsafe.Pointer(&n.size))
	for i := 4088; i < 4096; i++ {
		data[i] = size[i-4088]
	}

	return data
}

// Deserialize : deserialize from a byteslice to a Node
func Deserialize(data []byte) *Node {
	return &Node{
		isLeaf: *(*uint8)(unsafe.Pointer(&data[0])),
		edges:  *(*[256]uint64)(unsafe.Pointer(&data[1])),
		offset: *(*uint64)(unsafe.Pointer(&data[4080])),
		size:   *(*uint64)(unsafe.Pointer(&data[4088])),
	}
}
