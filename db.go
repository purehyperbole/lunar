package lunar

import "github.com/purehyperbole/lunar/table"

// DB : Database
type DB struct {
	index *table.Table
	data  *table.Table
}

// Get : get an item by key
func (d *DB) Get(key string) ([]byte, error) {
	return nil, nil
}

// Set : set an item by key and value
func (d *DB) Set(key string, value []byte) error {
	return nil
}
