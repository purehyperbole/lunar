package lunar

import (
	"errors"

	"github.com/purehyperbole/lunar/header"
	"github.com/purehyperbole/lunar/table"
	"github.com/purehyperbole/rad"
)

// DB Database
type DB struct {
	index      *rad.Radix
	data       *table.Table
	compaction bool // compaction on file open
}

type entry struct {
	offset int64
	size   int64
}

var (
	ErrNotFound = errors.New("key not found")
)

// Open open a database table and index, will create both if they dont exist
func Open(path string, opts ...func(*DB) error) (*DB, error) {
	var db DB

	for _, opt := range opts {
		err := opt(&db)
		if err != nil {
			return nil, err
		}
	}

	return &db, db.setup(path)
}

// Close unmaps and closes data and index files
func (db *DB) Close() error {
	return db.data.Close()
}

// Get get a value by key
func (db *DB) Get(key []byte) ([]byte, error) {
	v := db.index.Lookup(key)

	entry, ok := v.(*entry)

	if v == nil || !ok {
		return nil, ErrNotFound
	}

	data, err := db.data.Read(entry.size, entry.offset)
	if err != nil {
		return nil, err
	}

	h := header.Deserialize(data[:header.HeaderSize])

	return data[h.DataOffset():], nil
}

// Set set value by key
func (db *DB) Set(key, value []byte) error {
	var h header.Header
	h.SetKeySize(int64(len(key)))
	h.SetDataSize(int64(len(value)))

	data := make([]byte, h.TotalSize())
	copy(data[0:], header.Serialize(&h))
	copy(data[header.HeaderSize:], key)
	copy(data[h.DataOffset():], value)

	off, err := db.data.Write(data)
	if err != nil {
		return err
	}

	db.index.MustInsert(key, &entry{
		size:   h.TotalSize(),
		offset: off,
	})

	return nil
}

// Gets get a value by string key
func (db *DB) Gets(key string) ([]byte, error) {
	return db.Get([]byte(key))
}

// Sets set a value by string key
func (db *DB) Sets(key string, value []byte) error {
	return db.Set([]byte(key), value)
}
