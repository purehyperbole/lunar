package lunar

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func cleanup(db *DB) {
	db.Close()
	os.Remove("test.db")
	os.Remove("test.db.idx")
}

func TestDBOpen(t *testing.T) {
	// open new
	db, err := Open("test.db")
	defer cleanup(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	dstat, derr := os.Stat("test.db")
	istat, ierr := os.Stat("test.db.idx")

	assert.Nil(t, derr)
	assert.Nil(t, ierr)
	assert.Equal(t, int64(1<<16), dstat.Size())
	assert.Equal(t, int64(1<<16), istat.Size())

	assert.Nil(t, db.Close())

	// open existing
	db, err = Open("test.db")
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDBSet(t *testing.T) {
	db, err := Open("test.db")
	defer cleanup(db)

	assert.Nil(t, err)

	// insert new key & retrieve
	err = db.Sets("test-key", []byte("test"))

	assert.Nil(t, err)

	data, err := db.Gets("test-key")

	assert.Nil(t, err)
	assert.Equal(t, []byte("test"), data)

	// update existing key
	err = db.Sets("test-key", []byte("test-1234"))

	assert.Nil(t, err)

	data, err = db.Gets("test-key")

	assert.Nil(t, err)
	assert.Equal(t, []byte("test-1234"), data)
}

func TestDBGet(t *testing.T) {
	db, err := Open("test.db")
	defer cleanup(db)

	assert.Nil(t, err)

	// get a nonexistant key
	data, err := db.Gets("test-key")

	assert.NotNil(t, err)
	assert.Nil(t, data)

	// get an existing key
	err = db.Sets("test-key", []byte("test-1234"))

	assert.Nil(t, err)

	data, err = db.Gets("test-key")

	assert.Nil(t, err)
	assert.Equal(t, []byte("test-1234"), data)

	// test persistence
	err = db.Close()

	assert.Nil(t, err)

	db, err = Open("test.db")
	defer cleanup(db)

	assert.Nil(t, err)

	data, err = db.Gets("test-key")

	assert.Nil(t, err)
	assert.Equal(t, []byte("test-1234"), data)
}
