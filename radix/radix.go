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
	t *table.Table
}

// Lookup : returns the index and size for a particular key
// if the key isn't found, an error will be returned
func (r *Radix) Lookup(key []byte) (*node.Node, error) {
	n, err := r.root()
	if err != nil {
		return nil, err
	}

	fmt.Println(n.Size())

	for {
		break
	}

	return nil, ErrNotFound
}

// Add : adds a key to the radix tree
func (r *Radix) Add(key []byte, size, offset int64) error {
	return nil
}

func (r *Radix) root() (*node.Node, error) {
	ndata, err := r.t.Read(0, node.NodeSize)
	if err != nil {
		return nil, err
	}

	n := node.Deserialize(ndata)

	return n, nil
}
