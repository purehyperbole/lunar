package table

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFreeListReserve(t *testing.T) {
	f := NewFreeList(1 << 30)

	// reserve one 4kb block
	offset, err := f.Reserve(1 << 12)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), offset)

	// reserve another one 4kb block
	offset, err = f.Reserve(1 << 12)
	assert.Nil(t, err)
	assert.Equal(t, int64(4096), offset)

	f.Release(4096, 0)

	offset, err = f.Reserve(1 << 12)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), offset)
}

func TestFreeListRelease(t *testing.T) {
	f := NewFreeList(1 << 30)

	// reserve four 4kb blocks
	for i := 0; i < 4; i++ {
		f.Reserve(1 << 12)
	}

	f.Release(4096, 4096)

	// next alloc should return freed block as offset
	offset, err := f.Reserve(1 << 12)
	assert.Nil(t, err)
	assert.Equal(t, int64(4096), offset)
}

func TestFreeListFreagmentation(t *testing.T) {
	f := NewFreeList(1 << 30)

	// reserve four 4kb blocks
	for i := 0; i < 5; i++ {
		f.Reserve(1 << 12)
	}

	f.Release(20480, 0)

	var links int

	n := f.root

	for n != nil {
		n = n.next
		links++
	}

	assert.Equal(t, 1, links)
	assert.Equal(t, int64(1<<30), f.root.size)
	assert.Equal(t, int64(0), f.root.offset)
}

func TestFreeListFreagmentation2(t *testing.T) {
	f := NewFreeList(1 << 30)

	// reserve four 4kb blocks
	for i := 0; i < 9; i++ {
		f.Reserve(1 << 12)
	}

	f.Release(4096, 8192)
	f.Release(4096, 20480)

	f.Release(36864, 0)

	var links int

	n := f.root

	for n != nil {
		fmt.Printf("link: %d\n", links)
		fmt.Printf("size: %d\n", n.size)
		fmt.Printf("offset: %d\n", n.offset)
		fmt.Printf("------------\n")
		n = n.next
		links++
	}
}
