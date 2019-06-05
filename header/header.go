package header

import (
	"fmt"
	"strings"
	"unsafe"
)

const (
	// HeaderSize the allocated size of the header
	HeaderSize = 48
)

// Header data header stores
// info about a given data value
type Header struct {
	xmin    uint64 // transaction id that created the node's data
	xmax    uint64 // transaction id that updated/deleted the node's data
	psize   int64  // size of the previous version of this data, including header
	poffset int64  // offset of the previous version of this data
	size    int64  // size of current data
	ksize   int64  // size of the current key
}

// Xmin returns the transaction if of the node that created the data
func (h *Header) Xmin() uint64 {
	return h.xmin
}

// Xmax returns the transaction if of the node that updated or deleted the data
func (h *Header) Xmax() uint64 {
	return h.xmax
}

// DataSize returns the size of the values data
func (h *Header) DataSize() int64 {
	return h.size
}

// KeySize returns the size of the tuples key
func (h *Header) KeySize() int64 {
	return h.ksize
}

// TotalSize returns the total size of header + data
func (h *Header) TotalSize() int64 {
	return HeaderSize + h.ksize + h.size
}

// DataOffset returs the offset that the data starts at
func (h *Header) DataOffset() int64 {
	return HeaderSize + h.ksize
}

// Previous returns the size and offset of the previous version's data
func (h *Header) Previous() (int64, int64) {
	return h.psize, h.poffset
}

// HasPrevious returns true if there is a previous version of the data
func (h *Header) HasPrevious() bool {
	return h.psize != 0
}

// SetXmin sets the transaction if of the node that created the data
func (h *Header) SetXmin(txid uint64) {
	h.xmin = txid
}

// SetXmax sets the transaction if of the node that updated or deleted the data
func (h *Header) SetXmax(txid uint64) {
	h.xmax = txid
}

// SetPrevious sets the offset of the previous version's data
func (h *Header) SetPrevious(size, offset int64) {
	h.psize = size
	h.poffset = offset
}

// SetDataSize sets the size of the keys data
func (h *Header) SetDataSize(size int64) {
	h.size = size
}

// SetKeySize sets the size of the tuples key
func (h *Header) SetKeySize(size int64) {
	h.ksize = size
}

// Serialize serialize a node to a byteslice
func Serialize(h *Header) []byte {
	data := make([]byte, 48)

	xmin := *(*[8]byte)(unsafe.Pointer(&h.xmin))
	copy(data[0:], xmin[:])

	xmax := *(*[8]byte)(unsafe.Pointer(&h.xmax))
	copy(data[8:], xmax[:])

	psize := *(*[8]byte)(unsafe.Pointer(&h.psize))
	copy(data[16:], psize[:])

	poffset := *(*[8]byte)(unsafe.Pointer(&h.poffset))
	copy(data[24:], poffset[:])

	size := *(*[8]byte)(unsafe.Pointer(&h.size))
	copy(data[32:], size[:])

	ksize := *(*[8]byte)(unsafe.Pointer(&h.ksize))
	copy(data[40:], ksize[:])

	return data
}

// Deserialize deserialize from a byteslice to a Node
func Deserialize(data []byte) *Header {
	return &Header{
		xmin:    *(*uint64)(unsafe.Pointer(&data[0])),
		xmax:    *(*uint64)(unsafe.Pointer(&data[8])),
		psize:   *(*int64)(unsafe.Pointer(&data[16])),
		poffset: *(*int64)(unsafe.Pointer(&data[24])),
		size:    *(*int64)(unsafe.Pointer(&data[32])),
		ksize:   *(*int64)(unsafe.Pointer(&data[40])),
	}
}

// Prepend prepends header information to data
func Prepend(h *Header, data []byte) []byte {
	hdr := Serialize(h)
	// may be more performant to write the header seperately
	// as this append creates a copy of the data
	hdr = append(hdr, data...)
	return hdr
}

// Print prints header information to stdout
func Print(h *Header) {
	output := []string{"{"}

	output = append(output, fmt.Sprintf("	Xmin: %d", h.xmin))
	output = append(output, fmt.Sprintf("	Xmax: %d", h.xmax))
	output = append(output, fmt.Sprintf("	Previous Version Size: %d", h.psize))
	output = append(output, fmt.Sprintf("	Previous Version Offset: %d", h.poffset))

	output = append(output, "}")

	fmt.Println(strings.Join(output, "\n"))
}
