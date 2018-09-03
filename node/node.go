package node

import (
	"errors"
	"unsafe"
)

const (
	// NodeSize : the allocated size of the node
	NodeSize = 1 << 12
	// MaxPrefix : the largest prefix size supported
	MaxPrefix = 128
)

var (
	// ErrNonexistentIndex : the provided node has no matching edge
	ErrNonexistentIndex = errors.New("node has no matching edge")
)

// Node : radix tree node
// stores data about a node and its edges.
type Node struct {
	NodeOffset int64      // not persisted, used to indicate the node offset of the node
	leaf       uint8      // indicates whether this node has an associated value
	plen       uint8      // prefix length
	prefix     [128]byte  // key prefix is used when a key has more than one unique character
	edges      [256]int64 // possile indicies to next child nodes
	offset     int64      // reference to offset of data
	size       int64      // reference to size of data
	txid       uint64     // transaction id that created/updated the node's data
	prevtxid   uint64     // previous transaction id that updated the node's data
}

// New : returns a new node
func New() *Node {
	return &Node{}
}

// Edges : returns all edge offsets to child nodes
func (n *Node) Edges() [256]int64 {
	return n.edges
}

// Next : returns the index of the next radix node by character
func (n *Node) Next(b byte) int64 {
	return n.edges[int(b)]
}

// SetNext : sets the index of the next radix node by character
func (n *Node) SetNext(b byte, index int64) {
	n.edges[int(b)] = index
}

// Empty : returns true if there is no data associated with this node
func (n *Node) Empty() bool {
	// may be faster to assign information on write rather than looping
	if n.leaf == 1 {
		return false
	}

	return !n.Children()
}

// Children : returns true if the node has associated child edges
func (n *Node) Children() bool {
	for i := 0; i < len(n.edges); i++ {
		if n.edges[i] != 0 {
			return true
		}
	}

	return false
}

// Leaf : returns true if node has associated data
func (n *Node) Leaf() bool {
	return n.leaf == 1
}

// Prefix : returns the node's prefix. returns nil if no prefix is defined
func (n *Node) Prefix() []byte {
	if n.plen == 0 {
		return nil
	}

	return n.prefix[:n.plen]
}

// HasPrefix : returns true if a node has a prefix
func (n *Node) HasPrefix() bool {
	return n.plen > 0
}

// PrefixSize : returns the size of the nodes prefix
func (n *Node) PrefixSize() int {
	return int(n.plen)
}

// Size : returns size of associated node data
func (n *Node) Size() int64 {
	return n.size
}

// Offset : returns offset index of associated node data
func (n *Node) Offset() int64 {
	return n.offset
}

// Txid : returns the transaction id that created/updated the nodes data
func (n *Node) Txid() uint64 {
	return n.txid
}

// PrevTxid : returns the transaction id that created/updated the nodes data
func (n *Node) PrevTxid() uint64 {
	return n.prevtxid
}

// SetLeaf : returns true if node has associated data
func (n *Node) SetLeaf(leaf bool) {
	if leaf {
		n.leaf = 1
	} else {
		n.leaf = 0
	}
}

// SetSize : sets size of associated node data
func (n *Node) SetSize(size int64) {
	n.size = size
}

// SetOffset : sets offset index of associated node data
func (n *Node) SetOffset(offset int64) {
	n.offset = offset
}

// SetPrefix : sets the prefix of the node
func (n *Node) SetPrefix(prefix []byte) {
	n.plen = uint8(len(prefix))
	copy(n.prefix[:], prefix)
}

// SetTxid : sets the id of the last transaction that updated the node
func (n *Node) SetTxid(txid uint64) {
	n.txid = txid
}

// SetPrevTxid : sets the id of the last transaction that updated the node
func (n *Node) SetPrevTxid(txid uint64) {
	n.prevtxid = txid
}

// Serialize : serialize a node to a byteslice
func Serialize(n *Node) []byte {
	data := make([]byte, 4096)

	data[0] = *(*byte)(unsafe.Pointer(&n.leaf))
	data[1] = *(*byte)(unsafe.Pointer(&n.plen))

	prefix := *(*[128]byte)(unsafe.Pointer(&n.prefix))
	copy(data[2:], prefix[:])

	edges := *(*[2048]byte)(unsafe.Pointer(&n.edges))
	copy(data[130:], edges[:])

	offset := *(*[8]byte)(unsafe.Pointer(&n.offset))
	copy(data[4064:], offset[:])

	size := *(*[8]byte)(unsafe.Pointer(&n.size))
	copy(data[4072:], size[:])

	txid := *(*[8]byte)(unsafe.Pointer(&n.txid))
	copy(data[4080:], txid[:])

	return data
}

// Deserialize : deserialize from a byteslice to a Node
func Deserialize(data []byte) *Node {
	return &Node{
		leaf:   *(*uint8)(unsafe.Pointer(&data[0])),
		plen:   *(*uint8)(unsafe.Pointer(&data[1])),
		prefix: *(*[128]byte)(unsafe.Pointer(&data[2])),
		edges:  *(*[256]int64)(unsafe.Pointer(&data[130])),
		offset: *(*int64)(unsafe.Pointer(&data[4064])),
		size:   *(*int64)(unsafe.Pointer(&data[4072])),
		txid:   *(*uint64)(unsafe.Pointer(&data[4080])),
	}
}
