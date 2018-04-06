package lunar

import (
	"errors"
	"fmt"
	"os"

	"github.com/purehyperbole/lunar/node"
	"github.com/purehyperbole/lunar/table"
)

// DB : Database
type DB struct {
	index *table.Table
	data  *table.Table
}

// Open : open a database table and index, will create both if they dont exist
func Open(path string) (*DB, error) {
	idxpath := path + ".idx"

	idxt, dbt, err := setup(idxpath, path)
	if err != nil {
		return nil, err
	}

	return &DB{
		index: idxt,
		data:  dbt,
	}, nil
}

// Get : get an item by key
func (db *DB) Get(key string) ([]byte, error) {
	return nil, nil
}

// Set : set an item by key and value
func (db *DB) Set(key string, value []byte) error {
	return nil
}

func setup(indexpath, datapath string) (*table.Table, *table.Table, error) {
	idxpe := exists(indexpath)
	datpe := exists(datapath)

	if !idxpe && !datpe {
		return nil, nil, errors.New("missing index or database file")
	}

	idxt, err := table.New(indexpath)
	if err != nil {
		return nil, nil, err
	}

	dbt, err := table.New(datapath)
	if err != nil {
		return nil, nil, err
	}

	if idxpe {
		return idxt, dbt, loadfreelists(idxt, dbt)
	}

	return idxt, dbt, nil
}

func loadfreelists(index, data *table.Table) error {
	nodes := index.Size() / node.NodeSize

	fmt.Printf("nodes: %d\n", nodes)

	// Assume all space is allocated, free it later
	index.Free.Reserve(index.Size())

	// wont work for data :/
	data.Free.Reserve(data.Size())

	for i := 0; i < int(nodes); i++ {
		offset := int64(i) * node.NodeSize
		ndata, err := index.Read(offset, node.NodeSize)
		if err != nil {
			return err
		}

		n := node.Deserialize(ndata)

		if n.Empty() {
			index.Free.Release(node.NodeSize, offset)
		} else {
			// data.Free.Release
		}
	}

	return nil
}

func exists(path string) bool {
	_, err := os.Stat(path)
	if err == os.ErrNotExist {
		return false
	}

	return true
}
