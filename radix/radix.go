package radix

import (
	"errors"
	"fmt"

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
func New(path string) (*Radix, error) {
	t, err := table.New(path)
	if err != nil {
		return nil, err
	}

	return &Radix{t, 0}, nil
}

// Lookup : returns the index and size for a particular key
// if the key isn't found, an error will be returned
func (r *Radix) Lookup(key []byte) (*node.Node, error) {
	n, err := r.root()
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(key); i++ {
		next := n.Next(key[i])
		if next == 0 {
			return nil, ErrNotFound
		}

		ndata, err := r.t.Read(next, node.NodeSize)
		if err != nil {
			return nil, err
		}

		n = node.Deserialize(ndata)
	}

	return n, nil
}

// Add : adds a key to the radix tree
func (r *Radix) Add(key []byte, size, offset int64) error {
	var next int64

	n, err := r.root()
	if err != nil {
		return err
	}

	for i := 0; i < len(key); i++ {
		var ndata []byte

		if n.Next(key[i]) == 0 {
			fmt.Printf("didnt find character: %s\n", string(key[i]))
			fmt.Println(next)
			n, next, err = r.createnode(n, next, key[i])
			if err != nil {
				return err
			}

			r.nodes++
			continue
		}

		next = n.Next(key[i])

		ndata, err = r.t.Read(offset, node.NodeSize)
		if err != nil {
			return err
		}

		n = node.Deserialize(ndata)
	}

	n.SetSize(size)
	n.SetOffset(offset)

	ndata := node.Serialize(n)

	err = r.t.Write(ndata, next)
	if err != nil {
		return err
	}

	return nil
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

	ndata, err := r.t.Read(0, node.NodeSize)
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
	fmt.Println(cn.Next(c))
	ndata = node.Serialize(cn)
	err = r.t.Write(ndata, ci)
	if err != nil {
		return nil, 0, err
	}

	fmt.Printf("current: %d\n", ci)
	fmt.Printf("next: %d\n", next)

	return n, next, nil
}
