package lunar

import (
	"bytes"
	"errors"

	"github.com/purehyperbole/lunar/header"
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
	key     []byte
	size    int64
	offset  int64
	psize   int64
	poffset int64
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

	return tx.write(key, value)
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
	pl := tx.db.data.PageLock()
	headers := make(map[int64]*header.Header)

	// TODO: validate reads; before or after writes, remove duplicates where data was read and written?

	// lookup previous data versions
	for i := 0; i < len(tx.writes); i++ {
		n, err := tx.db.index.Lookup(tx.writes[i].key)
		if err == nil {
			tx.writes[i].psize = n.Size()
			tx.writes[i].poffset = n.Offset()
		} else if err != radix.ErrNotFound {
			return err
		}
	}

	// lock previous data values
	for _, w := range tx.writes {
		pl.Lock(w.poffset, false)
		defer pl.Unlock(w.poffset, false)
	}

	// read and validate old data version headers
	for _, w := range tx.writes {
		// if theres no previous version, dont check data
		if w.psize == 0 && w.poffset == 0 {
			continue
		}

		// read old version data header
		data, err := tx.db.data.Read(header.HeaderSize, w.poffset)
		if err != nil {
			return nil
		}

		hdr := header.Deserialize(data)

		// check xmax hasn't been updated
		if hdr.Xmax() == 0 && hdr.Xmin() < tx.txid {
			return ErrTxWriteConflict
		}

		// update old version xmax and store for writing when all headers have been verified
		hdr.SetXmax(tx.txid)
		headers[w.poffset] = hdr

		// update new data header to point to previous version
		var nhdr header.Header
		nhdr.SetXmin(tx.txid)
		nhdr.SetPrevious(w.poffset)

		nhdata := header.Serialize(&nhdr)

		err = tx.db.data.Write(nhdata, w.offset)
		if err != nil {
			return err
		}
	}

	// write old version headers
	for offset, hdr := range headers {
		data := header.Serialize(hdr)

		err := tx.db.data.Write(data, offset)
		if err != nil {
			return err
		}
	}

	// update indexes to point to new nodes
	for _, w := range tx.writes {
		n, err := tx.db.index.Insert(w.key)
		if err != nil {
			return err
		}

		n.SetSize(w.size)
		n.SetOffset(w.offset)

		err = tx.db.index.WriteUnlock(n)
		if err != nil {
			return err
		}
	}

	return nil
}

// Rollback : rolls back the transaction
func (tx *Tx) Rollback() error {
	// TODO : track and free data writen to data file & reserved space on index

	for _, w := range tx.writes {
		tx.db.data.Free.Release(w.size, w.offset)
	}

	return nil
}

func (tx *Tx) validateread(offset int64) error {
	/*
		pl.Lock(r.offset, true)
		defer pl.Unlock(r.offset, true)
	*/
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

func (tx *Tx) write(key, value []byte) error {
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
