package node

import "unsafe"

func testuint64tobytes(x *uint64) []byte {
	return (*[8]byte)(unsafe.Pointer(x))[:]
}

func testBuildBytes() []byte {
	data := make([]byte, 4096)

	data[0] = byte(1)
	var scratch []byte

	for i := 0; i < 256; i++ {
		x := uint64(i)
		scratch = append(scratch, testuint64tobytes(&x)...)
	}

	copy(data[1:], scratch[:])

	var scratch2 []byte
	o := uint64(102400)
	s := uint64(4096)
	scratch2 = append(scratch2, (*[8]byte)(unsafe.Pointer(&o))[:]...)
	scratch2 = append(scratch2, (*[8]byte)(unsafe.Pointer(&s))[:]...)

	copy(data[4080:], scratch2[:])

	return data
}

func testBuildNode(n *Node) {
	for i := 0; i < 255; i++ {
		n.edges[i] = int64(i)
	}
}
