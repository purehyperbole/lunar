package lunar

import (
	"bytes"
	"errors"

	"github.com/purehyperbole/lunar/header"
	"github.com/purehyperbole/lunar/node"
	"github.com/purehyperbole/lunar/radix"
)

var (
	// ErrTxReadOnly : the transaction is a readonly transaction
	ErrTxReadOnly = errors.New("cannot write in a readonly transaction")
	// ErrTxWriteConflict : the transaction is attempting to write to a node based on a stale read
	ErrTxWriteConflict = errors.New("write conlict detected")
)

// Tx : lunar transaction
type Tx struct {
	txid     uint64
	db       *DB
	reads    []read
	writes   []write
	readonly bool
}

type write struct {
	key    []byte
	node   *node.Node
	header *header.Header
	size   int64
	offset int64
}

type read struct {
	offset int64
}

// NewTransaction : creates a new transaction
func NewTransaction(db *DB, readonly bool) *Tx {
	tx := &Tx{
		txid:     db.newtxid(),
		db:       db,
		reads:    make([]read, 0),
		writes:   make([]write, 0),
		readonly: readonly,
	}

	return tx
}

// Get : get a value by key
func (tx *Tx) Get(key []byte) ([]byte, error) {
	// check if key has been writen to in this transaction before lookup
	if !tx.readonly {
		for i := 0; i < len(tx.writes); i++ {
			if bytes.Equal(tx.writes[i].key, key) {
				return tx.read(tx.writes[i].size, tx.writes[i].offset)
			}
		}
	}

	n, err := tx.db.index.Lookup(key)
	if err != nil {
		return nil, err
	}

	if n.Size() == 0 && n.Offset() == 0 {
		return nil, radix.ErrNotFound
	}

	if !tx.readonly {
		tx.addread(n.Offset())
	}

	return tx.read(n.Size(), n.Offset())
}

// Set : set value by key
func (tx *Tx) Set(key, value []byte) error {
	if tx.readonly {
		return ErrTxReadOnly
	}

	// create header and merge with data
	data := tx.createheader(value)
	sz := int64(len(data))

	off, err := tx.db.data.Free.Reserve(sz)
	if err != nil {
		return err
	}

	// write data and record offset and size
	err = tx.db.data.Write(data, off)
	if err != nil {
		return err
	}

	tx.addwrite(key, sz, off)

	return nil
}

// Gets : get a value by string key
func (tx *Tx) Gets(key string) ([]byte, error) {
	return tx.Get([]byte(key))
}

// Sets : set a value by string key
func (tx *Tx) Sets(key string, value []byte) error {
	return tx.Set([]byte(key), value)
}

// TODO : find a better solution to commits use of index.Insert
// this insert will keep unused index nodes around
// in the event rollback is called. This is done to simplify
// the checking logic and reduce the amount of locking of index
// nodes and avoids having to re-read index data multiple times

// Commit : commits the transaction
func (tx *Tx) Commit() error {
	pl := tx.db.data.PageLock()

	// TODO: validate reads; before or after writes, remove duplicates where data was read and written?

	for i := 0; i < len(tx.writes); i++ {
		// insert index nodes and lookup previous data versions
		n, err := tx.db.index.Insert(tx.writes[i].key)
		if err != nil {
			return err
		}

		// update new data header to point to old version
		hdr := header.Header{}
		hdr.SetXmin(tx.txid)
		hdr.SetPrevious(n.Offset())

		data := header.Serialize(&hdr)

		err = tx.db.data.Write(data, tx.writes[i].offset)
		if err != nil {
			return err
		}

		tx.writes[i].node = n

		// lock old data versions
		pl.Lock(n.Offset(), false)
		defer pl.Unlock(n.Offset(), false)
	}

	for i := 0; i < len(tx.writes); i++ {
		w := tx.writes[i]

		// skip checking newly inserted keys
		if !w.node.Leaf() {
			continue
		}

		// read old version data header
		data, err := tx.db.data.Read(header.HeaderSize, w.node.Offset())
		if err != nil {
			return nil
		}

		w.header = header.Deserialize(data)

		// check xmax hasn't been updated
		if w.header.Xmax() == 0 && w.header.Xmin() < tx.txid {
			return ErrTxWriteConflict
		}
	}

	for i := 0; i < len(tx.writes); i++ {
		w := tx.writes[i]

		// update old version xmax
		w.header.SetXmax(tx.txid)
		data := header.Serialize(w.header)

		err := tx.db.data.Write(data, w.node.Offset())
		if err != nil {
			return err
		}

		// finalize index insert/update
		w.node.SetSize(w.size)
		w.node.SetOffset(w.offset)

		err = tx.db.index.WriteUnlock(w.node)
		if err != nil {
			return err
		}
	}

	return nil
}

// Rollback : rolls back the transaction
func (tx *Tx) Rollback() error {
	for _, w := range tx.writes {
		tx.db.data.Free.Release(w.size, w.offset)
	}

	return nil
}

func (tx *Tx) read(size, offset int64) ([]byte, error) {
	pl := tx.db.data.PageLock()

	pl.Lock(offset, true)
	defer pl.Unlock(offset, true)

	// TODO : ensure transaction is reading the correct version of data
	data, err := tx.db.data.Read(size, offset)
	if err != nil {
		return nil, err
	}

	return data[header.HeaderSize:], nil
}

func (tx *Tx) createheader(value []byte) []byte {
	var hdr header.Header
	hdr.SetXmin(tx.txid)

	return header.Prepend(&hdr, value)
}

func (tx *Tx) addread(offset int64) {
	tx.reads = append(tx.reads, read{offset: offset})
}

func (tx *Tx) addwrite(key []byte, size, offset int64) {
	tx.writes = append(tx.writes, write{key: key, size: size, offset: offset})
}
