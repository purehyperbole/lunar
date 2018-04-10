package lunar

import (
	"errors"
	"os"

	"github.com/purehyperbole/lunar/node"
	"github.com/purehyperbole/lunar/table"
)

func setup(indexpath, datapath string) (*table.Table, *table.Table, error) {
	idxpe := exists(indexpath)
	datpe := exists(datapath)

	if !idxpe && datpe || idxpe && !datpe {
		return nil, nil, errors.New("missing index or database file")
	}

	idxt, err := table.New(indexpath)
	if err != nil {
		return nil, nil, err
	}

	dbt, err := table.New(datapath)
	if err != nil {
		return nil, nil, err
	}

	if idxpe {
		return idxt, dbt, loadfreelists(idxt, dbt)
	}

	return idxt, dbt, nil
}

func loadfreelists(index, data *table.Table) error {
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

func exists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		return false
	}

	return true
}
