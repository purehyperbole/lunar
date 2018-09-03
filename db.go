package lunar

import (
	"sync/atomic"

	"github.com/purehyperbole/lunar/node"
	"github.com/purehyperbole/lunar/radix"
	"github.com/purehyperbole/lunar/table"
)

// DB : Database
type DB struct {
	index *radix.Radix
	data  *table.Table
	wlock *WriteLock
	tx    uint64
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
		wlock: NewWriteLock(),
	}, nil
}

// Close : unmaps and closes data and index files
// TODO : implement msync to ensure that data is flushed before closing!
func (db *DB) Close() error {
	err := db.index.Close()
	if err != nil {
		return err
	}

	return db.data.Close()
}

// View : creates a readonly transaction
func (db *DB) View(tx func(*Tx) error) error {
	t := NewTransaction(db, true)
	return tx(t)
}

// Update : creates a writable transaction
func (db *DB) Update(tx func(*Tx) error) error {
	t := NewTransaction(db, false)
	return tx(t)
}

// Get : get a value by key
func (db *DB) Get(key []byte) ([]byte, error) {
	n, err := db.index.Lookup(key)
	if err != nil {
		return nil, err
	}

	if n.Size() == 0 && n.Offset() == 0 {
		return nil, radix.ErrNotFound
	}

	return db.data.Read(n.Size(), n.Offset())
}

// Set : set value by key
func (db *DB) Set(key, value []byte) error {
	k := []byte(key)

	n, err := db.index.Insert(k)
	if err != nil && err != radix.ErrNotFound {
		return err
	}

	return db.update(n, k, value)
}

// Gets : get a value by string key
func (db *DB) Gets(key string) ([]byte, error) {
	return db.Get([]byte(key))
}

// Sets : set a value by string key
func (db *DB) Sets(key string, value []byte) error {
	return db.Set([]byte(key), value)
}

func (db *DB) snapshot() *DB {
	return &DB{
		index: db.index.Snapshot(),
		data:  db.data,
	}
}

func (db *DB) update(n *node.Node, key, value []byte) error {
	sz := int64(len(value))

	off, err := db.data.Free.Reserve(sz)
	if err != nil {
		return err
	}

	n.SetOffset(off)
	n.SetSize(sz)

	err = db.data.Write(value, n.Offset())
	if err != nil {
		return err
	}

	return db.index.Modify(n, n.NodeOffset)
}

func (db *DB) newtxid() uint64 {
	atomic.AddUint64(&db.tx, 1)
	return atomic.LoadUint64(&db.tx)
}
