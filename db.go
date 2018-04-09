package lunar

import (
	"github.com/purehyperbole/lunar/node"
	"github.com/purehyperbole/lunar/radix"
	"github.com/purehyperbole/lunar/table"
)

// DB : Database
type DB struct {
	index *radix.Radix
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
		index: radix.New(idxt),
		data:  dbt,
	}, nil
}

// Get : get an item by key
func (db *DB) Get(key string) ([]byte, error) {
	k := []byte(key)

	n, err := db.index.Lookup(k)
	if err != nil {
		return nil, err
	}

	return db.data.Read(n.Size(), n.Offset())
}

// Set : set an item by key and value
func (db *DB) Set(key string, value []byte) error {
	k := []byte(key)

	n, off, err := db.index.LookupWithOffset(k)
	if err != nil && err != radix.ErrNotFound {
		return err
	}

	if n != nil {
		return db.update(n, off, k, value)
	}

	return db.create(k, value)
}

func (db *DB) create(key, value []byte) error {
	sz := int64(len(value))

	off, err := db.data.Free.Reserve(sz)
	if err != nil {
		return err
	}

	err = db.data.Write(value, off)
	if err != nil {
		return err
	}

	return db.index.Insert(key, sz, off)
}

func (db *DB) update(n *node.Node, offset int64, key, value []byte) error {
	sz := int64(len(value))

	if n.Size() != sz {
		db.data.Free.Release(n.Size(), n.Offset())

		off, err := db.data.Free.Reserve(sz)
		if err != nil {
			return err
		}

		n.SetOffset(off)
		n.SetSize(sz)
	}

	err := db.data.Write(value, n.Offset())
	if err != nil {
		return err
	}

	return db.index.Modify(n, offset)
}
