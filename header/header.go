package header

import "unsafe"

const (
	// HeaderSize : the allocated size of the header
	HeaderSize = 1 << 12
)

// Header : data header stores
// info about a given data value
type Header struct {
	xmin     uint64 // transaction id that created the node's data
	xmax     uint64 // transaction id that updated/deleted the node's data
	previous int64  // offset of the previous version of this data
}

// Xmin : returns the transaction if of the node that created the data
func (h *Header) Xmin() uint64 {
	return h.xmin
}

// Xmax : returns the transaction if of the node that updated or deleted the data
func (h *Header) Xmax() uint64 {
	return h.xmax
}

// Previous : returns the offset of the previous version's data
func (h *Header) Previous() int64 {
	return h.previous
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
func (h *Header) SetPrevious(offset int64) {
	h.previous = offset
}

// Serialize : serialize a node to a byteslice
func Serialize(h *Header) []byte {
	data := make([]byte, 32)

	xmin := *(*[8]byte)(unsafe.Pointer(&h.xmin))
	copy(data[0:], xmin[:])

	xmax := *(*[8]byte)(unsafe.Pointer(&h.xmax))
	copy(data[8:], xmax[:])

	previous := *(*[8]byte)(unsafe.Pointer(&h.previous))
	copy(data[16:], previous[:])

	return data
}

// Deserialize : deserialize from a byteslice to a Node
func Deserialize(data []byte) *Header {
	return &Header{
		xmin:     *(*uint64)(unsafe.Pointer(&data[0])),
		xmax:     *(*uint64)(unsafe.Pointer(&data[8])),
		previous: *(*int64)(unsafe.Pointer(&data[16])),
	}
}

// Prepend : prepends header information to data
func Prepend(h *Header, data []byte) []byte {
	hdr := Serialize(h)
	hdr = append(hdr, data...)
	return hdr
}
