package radix

import (
	"os"
	"testing"

	"github.com/purehyperbole/lunar/table"
	"github.com/stretchr/testify/assert"
)

func TestRadixInsertAndLookup(t *testing.T) {
	tbl, err := table.New("test.index")
	assert.Nil(t, err)
	assert.NotNil(t, tbl)

	// clean file
	defer func() {
		tbl.Close()
		os.Remove("test.index")
	}()

	r := New(tbl)
	assert.NotNil(t, r)

	err = r.Insert([]byte("test1234"), 1024, 0)
	assert.Nil(t, err)
	assert.Equal(t, 8, r.nodes)

	n, err := r.Lookup([]byte("test1234"))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n.Offset())
	assert.Equal(t, int64(1024), n.Size())

	err = r.Insert([]byte("test5678"), 1024, 0)
	assert.Nil(t, err)
	assert.Equal(t, 12, r.nodes)

}
