package node

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func testuint64tobytes(x *uint64) []byte {
	return (*[8]byte)(unsafe.Pointer(x))[:]
}

func testBuildBytes() []byte {
	data := make([]byte, 4096)

	data[0] = byte(1)
	data[1] = byte(8)
	pfix := []byte("test1234")

	for i := 0; i < len(pfix); i++ {
		data[2+i] = pfix[i]
	}

	var scratch []byte

	for i := 0; i < 256; i++ {
		x := uint64(i)
		scratch = append(scratch, testuint64tobytes(&x)...)
	}

	copy(data[130:], scratch[:])

	var scratch2 []byte
	o := uint64(102400)
	s := uint64(4096)
	scratch2 = append(scratch2, (*[8]byte)(unsafe.Pointer(&o))[:]...)
	scratch2 = append(scratch2, (*[8]byte)(unsafe.Pointer(&s))[:]...)

	copy(data[4064:], scratch2[:])

	return data
}

func testBuildNode(n *Node) {
	for i := 0; i < 255; i++ {
		n.edges[i] = int64(i)
	}
}

func TestSerialize(t *testing.T) {
	node := Node{
		leaf:   1,
		plen:   0,
		offset: 102400,
		size:   4096,
	}

	testBuildNode(&node)

	data := Serialize(&node)

	assert.Len(t, data, 4096)
	assert.Equal(t, int8(1), int8(data[0]))
	assert.Equal(t, int8(0), int8(data[1]))
}

func TestDeserialize(t *testing.T) {
	data := testBuildBytes()

	node := Deserialize(data)

	assert.Equal(t, true, node.Leaf())
	assert.Equal(t, []byte("test1234"), node.Prefix())
	assert.Equal(t, int64(102400), node.Offset())
	assert.Equal(t, int64(4096), node.Size())
	assert.Equal(t, int64(104), node.Next([]byte("h")[0]))
}

func TestNext(t *testing.T) {
	key := []byte("k")[0]

	node := Node{
		leaf:   1,
		offset: 102400,
		size:   4096,
	}

	node.SetNext(key, 1024)
	offset := node.Next(key)
	assert.Equal(t, int64(1024), offset)
}

func TestEmpty(t *testing.T) {
	node := Node{
		leaf:   0,
		offset: 0,
		size:   0,
	}

	assert.True(t, node.Empty())

	node = Node{
		leaf:   1,
		offset: 0,
		size:   1024,
	}

	assert.False(t, node.Empty())

	node = Node{
		leaf:   0,
		offset: 0,
		size:   0,
	}

	node.SetNext([]byte("t")[0], 1024)

	assert.False(t, node.Empty())
}
