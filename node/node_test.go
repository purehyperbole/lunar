package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSerialize(t *testing.T) {
	node := Node{
		isLeaf: 1,
		offset: 102400,
		size:   4096,
	}

	testBuildNode(&node)

	data := Serialize(&node)

	assert.Len(t, data, 4096)
	assert.Equal(t, uint8(1), uint8(data[0]))
	assert.Equal(t, uint8(1), uint8(data[0]))
}

func TestDeserialize(t *testing.T) {
	data := testBuildBytes()

	node := Deserialize(data)

	assert.Equal(t, uint8(1), node.isLeaf)
	assert.Equal(t, uint64(102400), node.offset)
	assert.Equal(t, uint64(4096), node.size)
	assert.Equal(t, uint64(104), node.Next([]byte("h")[0]))
}
