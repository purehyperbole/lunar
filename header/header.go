package header

import (
	"fmt"
	"strings"
	"unsafe"
)

const (
	// HeaderSize : the allocated size of the header
	HeaderSize = 1 << 5
)

// Header : data header stores
// info about a given data value
type Header struct {
	xmin    uint64 // transaction id that created the node's data
	xmax    uint64 // transaction id that updated/deleted the node's data
	psize   int64  // size of the previous version of this data, including header
	poffset int64  // offset of the previous version of this data
}

// Xmin : returns the transaction if of the node that created the data
func (h *Header) Xmin() uint64 {
	return h.xmin
}

// Xmax : returns the transaction if of the node that updated or deleted the data
func (h *Header) Xmax() uint64 {
	return h.xmax
}

// Previous : returns the size and offset of the previous version's data
func (h *Header) Previous() (int64, int64) {
	return h.psize, h.poffset
}

// HasPrevious : returns true if there is a previous version of the data
func (h *Header) HasPrevious() bool {
	return h.psize != 0
}

// SetXmin : sets the transaction if of the node that created the data
func (h *Header) SetXmin(txid uint64) {
	h.xmin = txid
}

// SetXmax : sets the transaction if of the node that updated or deleted the data
func (h *Header) SetXmax(txid uint64) {
	h.xmax = txid
}

// SetPrevious : sets the offset of the previous version's data
func (h *Header) SetPrevious(size, offset int64) {
	h.psize = size
	h.poffset = offset
}

// Serialize : serialize a node to a byteslice
func Serialize(h *Header) []byte {
	data := make([]byte, 32)

	xmin := *(*[8]byte)(unsafe.Pointer(&h.xmin))
	copy(data[0:], xmin[:])

	xmax := *(*[8]byte)(unsafe.Pointer(&h.xmax))
	copy(data[8:], xmax[:])

	psize := *(*[8]byte)(unsafe.Pointer(&h.psize))
	copy(data[16:], psize[:])

	poffset := *(*[8]byte)(unsafe.Pointer(&h.poffset))
	copy(data[24:], poffset[:])

	return data
}

// Deserialize : deserialize from a byteslice to a Node
func Deserialize(data []byte) *Header {
	return &Header{
		xmin:    *(*uint64)(unsafe.Pointer(&data[0])),
		xmax:    *(*uint64)(unsafe.Pointer(&data[8])),
		psize:   *(*int64)(unsafe.Pointer(&data[16])),
		poffset: *(*int64)(unsafe.Pointer(&data[24])),
	}
}

// Prepend : prepends header information to data
func Prepend(h *Header, data []byte) []byte {
	hdr := Serialize(h)
	// may be more performant to write the header seperately
	// as this append creates a copy of the data
	hdr = append(hdr, data...)
	return hdr
}

// Print : prints header information to stdout
func Print(h *Header) {
	output := []string{"{"}

	output = append(output, fmt.Sprintf("	Xmin: %d", h.xmin))
	output = append(output, fmt.Sprintf("	Xmax: %d", h.xmax))
	output = append(output, fmt.Sprintf("	Previous Version Size: %d", h.psize))
	output = append(output, fmt.Sprintf("	Previous Version Offset: %d", h.poffset))

	output = append(output, "}")

	fmt.Println(strings.Join(output, "\n"))
}
