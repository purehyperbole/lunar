package node

import (
	"fmt"
	"testing"
	"unsafe"
)

// use byte as array index?

func TestSerialize(t *testing.T) {
	/*
			node := New()

				for i := 0; i < 255; i++ {
					node.edges[i] = uint64(i)
				}

				// 104
				str := "h"

				idx := node.NextIndex([]byte(str)[0])
				fmt.Println(idx)

				data := Serialize(node)

				assert.Len(t, data, 4096)

		x := Node{
			isLeaf: 1,
		}

		for i := 0; i < 255; i++ {
			x.edges[i] = uint64(i)
		}

		s := Serialize(&x)
		d := Deserialize(s)
		fmt.Println(d.isLeaf)
		for _, b := range d.edges {
			fmt.Println(b)
		}
	*/

	data := make([]byte, 4096)

	data[0] = byte(1)
	var scratch []byte

	for i := 0; i < 256; i++ {
		x := uint64(i)
		scratch = append(scratch, (*[8]byte)(unsafe.Pointer(&x))[:]...)
	}

	for i := 0; i < len(scratch); i++ {
		data[i+1] = scratch[i]
	}

	n := Deserialize(data)
	fmt.Println(n.isLeaf)
	for _, c := range n.edges {
		fmt.Println(c)
	}

	//assert.Equal(t, len(tc.Changelog), len(cl))

}
