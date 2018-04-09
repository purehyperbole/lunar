package table

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	testpath := "test.db"

	db, err := New(testpath)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Close()
	assert.Nil(t, err)
}

func TestWrite(t *testing.T) {
	data := []byte("test1234")
	comparison := make([]byte, len(data))

	db, err := New("test.db")
	assert.Nil(t, err)

	err = db.Write(data, 0)
	assert.Nil(t, err)

	// check mapping
	assert.Equal(t, data, db.mapping[:len(data)])

	// check file
	db.fd.ReadAt(comparison, 0)
	assert.Equal(t, data, comparison)

	// clean file
	os.Remove(db.fd.Name())
}

func TestRead(t *testing.T) {
	data := []byte("test4567")

	db, err := New("test.db")
	assert.Nil(t, err)

	err = db.Write(data, 0)
	assert.Nil(t, err)

	comparison, err := db.Read(int64(len(data)), 0)
	assert.Nil(t, err)
	assert.Equal(t, data, comparison)

	// clean file
	os.Remove(db.fd.Name())
}
