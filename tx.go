package lunar

import "github.com/purehyperbole/lunar/header"

// Tx stores all state of a transaction
type Tx struct {
	db       *DB
	txid     uint64
	readonly bool
}

// NewTx creates a new transaction
func NewTx(db *DB, txid uint64, readonly bool) *Tx {
	return &Tx{
		db:       db,
		txid:     txid,
		readonly: readonly,
	}
}

func (tx *Tx) Get(key []byte) ([]byte, error) {
	v := tx.db.index.Lookup(key)

	entry, ok := v.(*entry)
	if v == nil || !ok {
		return nil, ErrNotFound
	}

	data, err := tx.db.data.Read(entry.size, entry.offset)
	if err != nil {
		return nil, err
	}

	h := header.Deserialize(data[:header.HeaderSize])

	return data[h.DataOffset():], nil
}

func (tx *Tx) Set(key, value []byte) error {

}

func (tx *Tx) Commit() error {

}

func (tx *Tx) Rollback() error {

}
