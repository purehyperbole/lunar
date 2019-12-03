package lunar

import (
	"errors"
	"os"

	"github.com/purehyperbole/lunar/header"
	"github.com/purehyperbole/lunar/table"
	"github.com/purehyperbole/rad"
)

func (db *DB) setup(datapath string) error {
	var err error
	var rt *table.Table

	db.index = rad.New()

	if db.compaction && exists(datapath) {
		backup := datapath + ".backup"

		if exists(backup) {
			return errors.New("could not backup data file")
		}

		err = os.Rename(datapath, backup)
		if err != nil {
			return err
		}

		rt, err = table.New(backup)
	} else {
		rt, err = table.New(datapath)
	}

	if err != nil {
		return err
	}

	db.data, err = table.New(datapath)
	if err != nil {
		return err
	}

	if exists(datapath) {
		return db.reload(rt, db.data)
	}

	return nil
}

func (db *DB) reload(rt, wt *table.Table) error {
	var pos int64

	dsz := rt.Size()

	for {
		if pos+header.HeaderSize > dsz {
			return nil
		}

		// read header
		data, err := rt.Read(header.HeaderSize, pos)
		if err != nil {
			return err
		}

		h := header.Deserialize(data)

		if h.KeySize() < 1 {
			if !db.compaction {
				wt.SetPosition(pos)
			}
			break
		}

		// skip old records
		if db.compaction && h.Xmax() > 0 {
			continue
		}

		// get key from data
		key := make([]byte, h.KeySize())

		kd, err := rt.Read(h.KeySize(), pos+header.HeaderSize)
		if err != nil {
			return err
		}

		copy(key, kd)

		np := pos

		if db.compaction {
			data, err := rt.Read(h.TotalSize(), pos)
			if err != nil {
				return err
			}

			np, err = wt.Write(data)
			if err != nil {
				return err
			}
		}

		db.index.Insert(key, &entry{
			size:   h.TotalSize(),
			offset: np,
		})

		pos = pos + h.TotalSize()
	}

	db.data = wt

	return nil
}

func exists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		return false
	}

	return true
}
