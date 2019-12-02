package table

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	testpath := "test.db"

	db, err := New(testpath)
	require.Nil(t, err)
	require.NotNil(t, db)

	err = db.Close()
	require.Nil(t, err)
}

func TestWrite(t *testing.T) {
	data := []byte("test1234")
	comparison := make([]byte, len(data))

	db, err := New("test.db")
	require.Nil(t, err)

	offset, err := db.Write(data)
	require.Nil(t, err)
	assert.Equal(t, int64(0), offset)

	mapping := (*mmap)(db.mapping)

	// check mapping
	assert.Equal(t, data, mapping.mapping[:len(data)])

	// check file
	db.fd.ReadAt(comparison, 0)
	assert.Equal(t, data, comparison)

	// clean file
	os.Remove(db.fd.Name())
}

func TestRead(t *testing.T) {
	data := []byte("test4567")

	db, err := New("test.db")
	require.Nil(t, err)

	_, err = db.Write(data)
	require.Nil(t, err)

	comparison, err := db.Read(int64(len(data)), 0)
	require.Nil(t, err)
	assert.Equal(t, data, comparison)

	// clean file
	os.Remove(db.fd.Name())
}

func TestConcurrentWrite(t *testing.T) {
	var wg sync.WaitGroup

	db, err := New("test.db")
	require.Nil(t, err)

	defer os.Remove(db.fd.Name())

	values := [][]byte{
		[]byte("one"),
		[]byte("two"),
		[]byte("three"),
		[]byte("four"),
		[]byte("five"),
		[]byte("six"),
		[]byte("seven"),
		[]byte("eight"),
	}

	wg.Add(8)

	for i := 0; i < 8; i++ {
		go func(w int) {
			for x := 0; x < 10000000; x++ {
				_, err := db.Write(values[w])

				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}(i)
	}

	wg.Wait()
}
