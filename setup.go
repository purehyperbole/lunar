package lunar

import (
	"os"

	"github.com/purehyperbole/lunar/header"
	"github.com/purehyperbole/lunar/table"
	"github.com/purehyperbole/rad"
)

func setup(datapath string) (*rad.Radix, *table.Table, error) {
	r := rad.New()

	dbt, err := table.New(datapath)
	if err != nil {
		return nil, nil, err
	}

	if exists(datapath) {
		return r, dbt, reload(r, dbt)
	}

	return r, dbt, nil
}

func reload(r *rad.Radix, t *table.Table) error {
	var pos int64
	var err error

	defer func() {
		if err == nil {
			_, err = t.Free.Reserve(pos)
		}
	}()

	for {
		if pos+header.HeaderSize > t.Size() {
			return nil
		}

		data, err := t.Read(header.HeaderSize, pos)
		if err != nil {
			return err
		}

		h := header.Deserialize(data)

		if h.KeySize() < 1 {
			return nil
		}

		key := make([]byte, h.KeySize())

		kd, err := t.Read(h.KeySize(), pos+header.HeaderSize)
		if err != nil {
			return err
		}

		copy(key, kd)

		r.Insert(key, &entry{
			size:   h.TotalSize(),
			offset: pos,
		})

		pos = pos + h.TotalSize()
	}

	return err
}

func exists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		return false
	}

	return true
}
