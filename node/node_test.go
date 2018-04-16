package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerialize(t *testing.T) {
	node := Node{
		isLeaf: 1,
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
		isLeaf: 1,
		offset: 102400,
		size:   4096,
	}

	node.SetNext(key, 1024)
	offset := node.Next(key)
	assert.Equal(t, int64(1024), offset)
}

func TestEmpty(t *testing.T) {
	node := Node{
		isLeaf: 0,
		offset: 0,
		size:   0,
	}

	assert.True(t, node.Empty())

	node = Node{
		isLeaf: 0,
		offset: 0,
		size:   1024,
	}

	assert.False(t, node.Empty())

	node = Node{
		isLeaf: 0,
		offset: 0,
		size:   0,
	}

	node.SetNext([]byte("t")[0], 1024)

	assert.False(t, node.Empty())
}
