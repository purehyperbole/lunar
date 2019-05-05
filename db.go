package lunar

import (
	"errors"

	"github.com/purehyperbole/lunar/table"
	"github.com/purehyperbole/rad"
)

// DB : Database
type DB struct {
	index *rad.Radix
	data  *table.Table
}

type entry struct {
	offset int64
	size   int64
}

var (
	ErrNotFound = errors.New("key not found")
)

// Open : open a database table and index, will create both if they dont exist
func Open(path string) (*DB, error) {
	dbt, err := setup(path)
	if err != nil {
		return nil, err
	}

	return &DB{
		index: rad.New(),
		data:  dbt,
	}, nil
}

// Close : unmaps and closes data and index files
// TODO : implement msync to ensure that data is flushed before closing!
func (db *DB) Close() error {
	return db.data.Close()
}

// Get : get a value by key
func (db *DB) Get(key []byte) ([]byte, error) {
	v := db.index.Lookup(key)

	entry, ok := v.(*entry)

	if v == nil || !ok {
		return nil, ErrNotFound
	}

	return db.data.Read(entry.size, entry.offset)
}

// Set : set value by key
func (db *DB) Set(key, value []byte) error {
	sz := int64(len(value))

	off, err := db.data.Free.Reserve(sz)
	if err != nil {
		return err
	}

	err = db.data.Write(value, off)
	if err != nil {
		return err
	}

	db.index.MustInsert(key, &entry{
		size:   sz,
		offset: off,
	})

	return nil
}

// Gets : get a value by string key
func (db *DB) Gets(key string) ([]byte, error) {
	return db.Get([]byte(key))
}

// Sets : set a value by string key
func (db *DB) Sets(key string, value []byte) error {
	return db.Set([]byte(key), value)
}
