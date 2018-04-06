package radix

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRadixAddAndLookup(t *testing.T) {
	r, err := New("test.index")

	assert.Nil(t, err)
	assert.NotNil(t, r)

	err = r.Add([]byte("test1234"), 1024, 0)
	assert.Nil(t, err)
	assert.Equal(t, 8, r.nodes)

	n, err := r.Lookup([]byte("test1234"))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), n.Offset())
	assert.Equal(t, int64(1024), n.Size())

	err = r.Add([]byte("test5678"), 1024, 0)
	assert.Nil(t, err)
	assert.Equal(t, 12, r.nodes)

	// clean file
	r.t.Close()
	os.Remove("test.index")
}
