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
		//return dbt, loadfreelists(dbt)
	}

	return r, dbt, nil
}

func reload(r *rad.Radix, t *table.Table) error {
	var pos int64

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

		key, err := t.Read(h.KeySize(), pos+header.HeaderSize)
		if err != nil {
			return nil
		}

		r.Insert(key, &entry{
			size:   h.TotalSize(),
			offset: pos,
		})

		pos = pos + h.TotalSize()
	}
	return nil
}

/*
func loadfreelists(data *table.Table) error {
	nodes := index.Size() / node.NodeSize

	for i := 0; i < int(nodes); i++ {
		offset := int64(i) * node.NodeSize
		ndata, err := index.Read(node.NodeSize, offset)

		if err != nil {
			return err
		}

		n := node.Deserialize(ndata)

		if !n.Empty() {
			index.Free.Allocate(node.NodeSize, offset)
			if n.Size() > 0 {
				data.Free.Allocate(n.Size(), n.Offset())
			}
		}
	}

	return nil
}
*/

func exists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		return false
	}

	return true
}
