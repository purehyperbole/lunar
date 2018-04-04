package table

type Table struct {
	buffer []byte
}

func New() *Table {
	return &Table{
		buffer: make([]byte, 0),
	}
}

func (t *Table) Read(offset int64, data []byte) {

}

func (t *Table) Write(offset int64, data []byte) {
	copy(t.buffer[offset:], data)
}

// func (t *Table) allocate(blocks)
