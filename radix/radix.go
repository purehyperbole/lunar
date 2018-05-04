package radix

import (
	"errors"
	"fmt"
	"strings"

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
	return &Radix{table, 1}
}

// Lookup : returns the index and size for a particular key
// if the key isn't found, an error will be returned
func (r *Radix) Lookup(key []byte) (*node.Node, error) {
	n, pos, _, err := r.lookup(key)
	if err != nil {
		return nil, err
	}

	if len(key) > pos {
		return nil, ErrNotFound
	}

	return n, nil
}

func (r *Radix) lookup(key []byte) (*node.Node, int, int, error) {
	var i, dv int
	var next int64

	n, err := r.root()
	if err != nil {
		return nil, 0, 0, err
	}

	for n.Next(key[i]) != 0 {
		next = n.Next(key[i])
		if next == 0 {
			return nil, 0, 0, ErrNotFound
		}
		i++

		ndata, err := r.t.Read(node.NodeSize, next)
		if err != nil {
			return nil, 0, 0, err
		}

		n = node.Deserialize(ndata)

		if n.HasPrefix() {
			dv = divergence(n.Prefix(), key[i:])

			if len(n.Prefix()) > dv {
				n.NodeOffset = next
				return n, i, dv, nil
			}

			i = i + dv
		}

		// if we've found the key, break the loop
		if i == len(key) {
			break
		}
	}

	n.NodeOffset = next

	return n, i, dv, nil
}

// Insert : adds a key to the radix tree
func (r *Radix) Insert(key []byte) (*node.Node, error) {
	n, i, dv, err := r.lookup(key)
	if err != nil && err != ErrNotFound {
		return nil, err
	}

	switch {
	// found keys prefix differs : split
	case n.HasPrefix() && n.PrefixSize() > dv:
		n, err = r.splitnode(n, dv, key[i:])
	// key matches : update node!
	case i == len(key):
		return n, nil
	// found key and its prefix is a sub key : create
	case n.HasPrefix() && dv == n.PrefixSize() || i < len(key):
		n, err = r.createnode(n, key[i:])
	}

	return n, err
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
func (r *Radix) Close() error {
	return r.t.Close()
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

func (r *Radix) createnode(parent *node.Node, prefix []byte) (*node.Node, error) {
	var n *node.Node
	var err error

	// create new node(s) and assign it space
	// if the prefix exceeds 128 bytes, slit it
	for _, pfx := range splitprefix(prefix) {
		n = node.New()

		if len(pfx) > 1 {
			n.SetPrefix(pfx[1:])
		}

		ndata := node.Serialize(n)

		n.NodeOffset, err = r.t.Free.Reserve(node.NodeSize)
		if err != nil {
			return nil, err
		}

		// write new node
		err = r.t.Write(ndata, n.NodeOffset)
		if err != nil {
			return nil, err
		}

		// update current node with offset to new node
		parent.SetNext(pfx[0], n.NodeOffset)

		ndata = node.Serialize(parent)

		err = r.t.Write(ndata, parent.NodeOffset)
		if err != nil {
			return nil, err
		}

		parent = n
		r.nodes++
	}

	return n, nil
}

func (r *Radix) splitnode(parent *node.Node, dv int, prefix []byte) (*node.Node, error) {
	var err error

	// new split node
	nn := node.New()
	nn.NodeOffset = parent.NodeOffset

	// allocate space for existing existing node
	parent.NodeOffset, err = r.t.Free.Reserve(node.NodeSize)
	if err != nil {
		return nil, err
	}

	// set parent as edge on new node
	nn.SetPrefix(parent.Prefix()[:dv])
	nn.SetNext(parent.Prefix()[dv], parent.NodeOffset)

	// update existing node's prefix
	if parent.PrefixSize() > dv+1 {
		parent.SetPrefix(parent.Prefix()[dv+1:])
	} else {
		parent.SetPrefix(nil)
	}

	// write existing node
	ndata := node.Serialize(parent)

	err = r.t.Write(ndata, parent.NodeOffset)
	if err != nil {
		return nil, err
	}

	// replace the parent node if the prefix is smaller than the found node
	if len(prefix) == dv {
		return r.twowaysplit(nn)
	}

	return r.threewaysplit(nn, dv, prefix)
}

func (r *Radix) twowaysplit(n *node.Node) (*node.Node, error) {
	ndata := node.Serialize(n)

	err := r.t.Write(ndata, n.NodeOffset)
	if err != nil {
		return nil, err
	}

	r.nodes++

	return n, nil
}

func (r *Radix) threewaysplit(n *node.Node, dv int, prefix []byte) (*node.Node, error) {
	n, err := r.createnode(n, prefix[dv:])
	if err != nil {
		return nil, err
	}

	r.nodes++

	return n, nil
}

// Graphviz : returns a graphviz formatted string of all the nodes in the tree
// this should only be run on trees with relatively few nodes
func (r *Radix) Graphviz() (string, error) {
	gvoutput := []string{"digraph G {"}
	gvzc := 0

	root, err := r.root()
	if err != nil {
		return "", err
	}

	err = r.graphviz(&gvoutput, &gvzc, "[-1] ROOT", root)
	if err != nil {
		return "", err
	}

	gvoutput = append(gvoutput, "}")

	return fmt.Sprint(strings.Join(gvoutput, "\n")), nil
}

func (r *Radix) graphviz(gvoutput *[]string, gvzc *int, previous string, n *node.Node) error {
	for i, e := range n.Edges() {
		if e != 0 {
			(*gvzc)++

			ndata, err := r.t.Read(node.NodeSize, e)
			if err != nil {
				return err
			}

			n := node.Deserialize(ndata)

			(*gvoutput) = append((*gvoutput), fmt.Sprintf("  \"%s\" -> \"[%d] %s\" [label=\"%s\"]", previous, *gvzc, string(n.Prefix()), string(byte(i))))

			err = r.graphviz(gvoutput, gvzc, fmt.Sprintf("[%d] %s", *gvzc, string(n.Prefix())), n)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// returns shared and divergent characters respectively
func divergence(prefix, key []byte) int {
	var i int

	for i < len(key) && i < len(prefix) {
		if key[i] != prefix[i] {
			break
		}
		i++
	}

	return i
}

func splitprefix(prefix []byte) [][]byte {
	var p []byte

	pfxs := make([][]byte, 0, len(prefix)/node.MaxPrefix+1)

	for len(prefix) >= node.MaxPrefix {
		p, prefix = prefix[:node.MaxPrefix], prefix[node.MaxPrefix:]
		pfxs = append(pfxs, p)
	}

	if len(prefix) > 0 {
		pfxs = append(pfxs, prefix[:len(prefix)])
	}

	return pfxs
}
