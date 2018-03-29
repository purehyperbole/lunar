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

	for i := 0; i < len(scratch); i++ {
		data[i+1] = scratch[i]
	}

	var scratch2 []byte
	o := uint64(102400)
	s := uint64(4096)
	scratch2 = append(scratch2, (*[8]byte)(unsafe.Pointer(&o))[:]...)
	scratch2 = append(scratch2, (*[8]byte)(unsafe.Pointer(&s))[:]...)

	for i := 0; i < 16; i++ {
		data[i+4080] = scratch2[i]
	}

	return data
}

func testBuildNode(n *Node) {
	for i := 0; i < 255; i++ {
		n.edges[i] = uint64(i)
	}
}
