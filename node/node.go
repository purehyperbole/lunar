package node

import (
	"errors"
	"unsafe"
)

var (
	// ErrNonexistentIndex : the provided node has no matching edge
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

// New : returns a new node
func New() *Node {
	return &Node{}
}

// Next : returns the index of the next radix node by character
func (n *Node) Next(b byte) uint64 {
	return n.edges[int(b)]
}

// SetNext : sets the index of the next radix node by character
func (n *Node) SetNext(b byte, index uint64) {
	n.edges[int(b)] = index
}

// Serialize : serialize a node to a byteslice
func Serialize(n *Node) []byte {
	data := make([]byte, 4096)

	data[0] = *(*byte)(unsafe.Pointer(&n.isLeaf))

	edges := *(*[2048]byte)(unsafe.Pointer(&n.edges))
	copy(data[1:], edges[:])

	offset := *(*[8]byte)(unsafe.Pointer(&n.offset))
	copy(data[4080:], offset[:])

	size := *(*[8]byte)(unsafe.Pointer(&n.size))
	copy(data[4088:], size[:])

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
