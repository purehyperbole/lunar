package lunar

import (
	"os"

	"github.com/purehyperbole/lunar/table"
)

func setup(datapath string) (*table.Table, error) {
	dbt, err := table.New(datapath)
	if err != nil {
		return nil, err
	}

	if exists(datapath) {
		//return dbt, loadfreelists(dbt)
	}

	return dbt, nil
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
