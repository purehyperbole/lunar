package header

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func testBuildBytes() []byte {
	data := make([]byte, 32)

	var scratch []byte
	xmin := uint64(2)
	xmax := uint64(15)
	previous := int64(8192)
	scratch = append(scratch, (*[8]byte)(unsafe.Pointer(&xmin))[:]...)
	scratch = append(scratch, (*[8]byte)(unsafe.Pointer(&xmax))[:]...)
	scratch = append(scratch, (*[8]byte)(unsafe.Pointer(&previous))[:]...)

	copy(data[0:], scratch[:])

	return data
}

func TestSerialize(t *testing.T) {
	hdr := Header{
		xmin:     0,
		xmax:     5,
		previous: 4096,
	}

	data := Serialize(&hdr)

	assert.Len(t, data, 32)
	assert.Equal(t, uint64(0), *(*uint64)(unsafe.Pointer(&data[0])))
	assert.Equal(t, uint64(5), *(*uint64)(unsafe.Pointer(&data[8])))
	assert.Equal(t, int64(4096), *(*int64)(unsafe.Pointer(&data[16])))
}

func TestDeserialize(t *testing.T) {
	data := testBuildBytes()

	hdr := Deserialize(data)

	assert.Equal(t, uint64(2), hdr.Xmin())
	assert.Equal(t, uint64(15), hdr.Xmax())
	assert.Equal(t, int64(8192), hdr.Previous())
}
