package radix

import (
	"errors"

	"github.com/purehyperbole/lunar/node"
	"github.com/purehyperbole/lunar/table"
)

var (
	// ErrNotFound : returned when a given key is not found
	ErrNotFound = errors.New("node key not found")
)

// Radix : an uncompressed radix tree utilizing a persistent underlying table for storage
type Radix struct {
	t     *table.Table
	nodes int
}

// New : creates a new radix tree backed by a persistent table
func New(table *table.Table) *Radix {
	return &Radix{table, 0}
}

// Lookup : returns the index and size for a particular key
// if the key isn't found, an error will be returned
func (r *Radix) Lookup(key []byte) (*node.Node, error) {
	n, _, err := r.LookupWithOffset(key)
	return n, err
}

// LookupWithOffset : returns the index and size for a particular key
// if the key isn't found, an error will be returned
func (r *Radix) LookupWithOffset(key []byte) (*node.Node, int64, error) {
	var next int64

	n, err := r.root()
	if err != nil {
		return nil, -1, err
	}

	for i := 0; i < len(key); i++ {
		next = n.Next(key[i])

		if next == 0 {
			return nil, -1, ErrNotFound
		}

		ndata, err := r.t.Read(node.NodeSize, next)
		if err != nil {
			return nil, -1, err
		}

		n = node.Deserialize(ndata)
	}

	return n, next, nil
}

// Insert : adds a key to the radix tree
func (r *Radix) Insert(key []byte, size, offset int64) error {
	var next int64

	n, err := r.root()
	if err != nil {
		return err
	}

	for i := 0; i < len(key); i++ {
		var ndata []byte

		if n.Next(key[i]) == 0 {
			n, next, err = r.createnode(n, next, key[i])
			if err != nil {
				return err
			}

			r.nodes++
			continue
		}

		next = n.Next(key[i])

		ndata, err = r.t.Read(node.NodeSize, next)
		if err != nil {
			return err
		}

		n = node.Deserialize(ndata)
	}

	n.SetSize(size)
	n.SetOffset(offset)

	ndata := node.Serialize(n)

	return r.t.Write(ndata, next)
}

// Modify : overwrites a node at a given offset
func (r *Radix) Modify(n *node.Node, offset int64) error {
	ndata := node.Serialize(n)

	return r.t.Write(ndata, offset)
}

// Delete : delete a key from the radix tree
// returns the size and offset of freed space
func (r *Radix) Delete(key []byte) (int64, int64, error) {
	return -1, -1, nil
}

// Close : close the underlying table
func (r *Radix) Close() {
	r.t.Close()
}

// Nodes : the amount of allocated nodes
func (r *Radix) Nodes() int {
	return r.nodes
}

func (r *Radix) root() (*node.Node, error) {
	// create root node if it doesn't exist
	if r.t.Free.Empty() {
		n := node.New()
		ndata := node.Serialize(n)

		_, err := r.t.Free.Reserve(int64(len(ndata)))
		if err != nil {
			return nil, err
		}

		return n, r.t.Write(ndata, int64(len(ndata)))
	}

	ndata, err := r.t.Read(node.NodeSize, 0)
	if err != nil {
		return nil, err
	}

	n := node.Deserialize(ndata)

	return n, nil
}

func (r *Radix) createnode(cn *node.Node, ci int64, c byte) (*node.Node, int64, error) {
	// create new node and assign it space
	n := node.New()
	ndata := node.Serialize(n)

	next, err := r.t.Free.Reserve(int64(len(ndata)))
	if err != nil {
		return nil, 0, err
	}

	// write new node
	err = r.t.Write(ndata, next)
	if err != nil {
		return nil, 0, err
	}

	// update current node with offset to new node
	cn.SetNext(c, next)

	ndata = node.Serialize(cn)

	return n, next, r.t.Write(ndata, ci)
}
