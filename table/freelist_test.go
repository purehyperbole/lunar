package table

import (
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

func TestFreeListAllocation(t *testing.T) {
	f := NewFreeList(1 << 30)

	// single allocaton
	err := f.Allocate(4096, 10240)
	assert.Nil(t, err)

	assert.Equal(t, int64(0), f.root.offset)
	assert.Equal(t, int64(10240), f.root.size)
	assert.Equal(t, int64(14336), f.root.next.offset)
	assert.Equal(t, int64(1<<30-14336), f.root.next.size)

	// allocate between two spaces
	err = f.Allocate(4096, 4194304)
	assert.Nil(t, err)

	assert.Equal(t, int64(0), f.root.offset)
	assert.Equal(t, int64(10240), f.root.size)
	assert.Equal(t, int64(14336), f.root.next.offset)
	assert.Equal(t, int64(4179968), f.root.next.size)
	assert.Equal(t, int64(4198400), f.root.next.next.offset)
	assert.Equal(t, int64(1069529088), f.root.next.next.size)

	// allocate two sections next to eachother
	err = f.Allocate(4096, 14336)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), f.root.offset)
	assert.Equal(t, int64(10240), f.root.size)
	assert.Equal(t, int64(18432), f.root.next.offset)
	assert.Equal(t, int64(4175872), f.root.next.size)
	assert.Equal(t, int64(4198400), f.root.next.next.offset)
	assert.Equal(t, int64(1069529088), f.root.next.next.size)
}

/*
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
*/
