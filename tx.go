package lunar

import (
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
	snapshot *DB
	readonly bool
}

type allocation struct {
	Size   int64
	Offset int64
}

// NewTransaction : creates a new transaction
func NewTransaction(db *DB, readonly bool) *Tx {
	tx := &Tx{
		db:       db,
		snapshot: db.snapshot(),
		readonly: readonly,
	}

	if !readonly {
		tx.txid = db.newtxid()
	}

	return tx
}

// Get : get a value by key
func (tx *Tx) Get(key []byte) ([]byte, error) {
	n, err := tx.snapshot.index.Lookup(key)
	if err != nil {
		return nil, err
	}

	if n.Size() == 0 && n.Offset() == 0 {
		return nil, radix.ErrNotFound
	}

	// TODO : ensure transaction is reading the correct version of data
	data, err := tx.db.data.Read(n.Size(), n.Offset())
	if err != nil {
		return nil, err
	}

	return data[header.HeaderSize:], nil
}

// Set : set value by key
func (tx *Tx) Set(key, value []byte) error {
	if tx.readonly {
		return ErrTxReadOnly
	}

	k := []byte(key)

	n, err := tx.snapshot.index.Insert(k)
	if err != nil && err != radix.ErrNotFound {
		return err
	}

	return tx.update(n, k, value)
}

// Gets : get a value by string key
func (tx *Tx) Gets(key string) ([]byte, error) {
	return tx.Get([]byte(key))
}

// Sets : set a value by string key
func (tx *Tx) Sets(key string, value []byte) error {
	return tx.Set([]byte(key), value)
}

// Commit : commits the transaction
func (tx *Tx) Commit() error {
	wc := tx.snapshot.index.WriteCache()

	// lock all nodes we're going to write to
	for offset := range wc {
		// TODO : implement mvcc xmin and xmax checks
		tx.db.wlock.Lock(offset)
		defer tx.db.wlock.Unlock(offset)
	}

	for offset := range wc {
		// compare nodes written to snapshot (n) with whats currently persisted (pn)
		pn, err := tx.db.index.Read(offset)
		if err != nil {
			return err
		}

		pdata, err := tx.db.data.Read(header.HeaderSize, pn.Offset())
		if err != nil {
			return nil
		}

		phdr := header.Deserialize(pdata)

		// check tx id hasn't changed
		if phdr.Xmax() == 0 && phdr.Xmin() < tx.txid {
			return ErrTxWriteConflict
		}
	}

	// update the nodes transaction id and write them to disk
	for _, n := range wc {
		// write index data
		err := tx.db.index.Write(n)
		if err != nil {
			return err
		}
	}

	return nil
}

// Rollback : rolls back the transaction
func (tx *Tx) Rollback() error {
	// TODO : track and free data writen to data file & reserved space on index
	wc := tx.snapshot.index.WriteCache()

	for _, n := range wc {
		tx.db.data.Free.Release(n.Size(), n.Offset())
	}

	return nil
}

func (tx *Tx) update(n *node.Node, key, value []byte) error {
	// TODO : release allocated space when all transactions using that data have completed

	// create header and merge with data
	var hdr *header.Header
	hdr.SetXmin(tx.txid)
	hdr.SetPrevious(n.Offset())

	data := header.Prepend(hdr, value)
	sz := int64(len(data))

	err := tx.updatexmax(n)
	if err != nil {
		return err
	}

	off, err := tx.db.data.Free.Reserve(sz)
	if err != nil {
		return err
	}

	n.SetOffset(off)
	n.SetSize(sz)

	err = tx.db.data.Write(data, n.Offset())
	if err != nil {
		return err
	}

	return tx.snapshot.index.Write(n)
}

func (tx *Tx) updatexmax(n *node.Node) error {
	data, err := tx.db.data.Read(header.HeaderSize, n.Offset())
	if err != nil {
		return err
	}

	hdr := header.Deserialize(data)
	hdr.SetXmax(tx.txid)

	data = header.Serialize(hdr)

	return tx.db.data.Write(data, n.Offset())
}
