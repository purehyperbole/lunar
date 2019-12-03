package lunar

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	require.Nil(t, err)
	assert.NotNil(t, db)

	dstat, derr := os.Stat("test.db")

	assert.Nil(t, derr)
	assert.Equal(t, int64(1<<16), dstat.Size())
	assert.Nil(t, db.Close())

	// open existing
	db, err = Open("test.db")
	require.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDBSet(t *testing.T) {
	db, err := Open("test.db")
	defer cleanup(db)

	require.Nil(t, err)

	// insert new key & retrieve
	err = db.Sets("test-key", []byte("test"))

	require.Nil(t, err)

	data, err := db.Gets("test-key")

	require.Nil(t, err)
	assert.Equal(t, []byte("test"), data)

	// update existing key
	err = db.Sets("test-key", []byte("test-1234"))

	require.Nil(t, err)

	data, err = db.Gets("test-key")

	require.Nil(t, err)
	assert.Equal(t, []byte("test-1234"), data)
}

func TestDBGet(t *testing.T) {
	db, err := Open("test.db")
	defer cleanup(db)

	require.Nil(t, err)

	// get a nonexistant key
	data, err := db.Gets("test-key")
	require.NotNil(t, err)
	assert.Nil(t, data)

	// get an existing key
	err = db.Sets("test-key", []byte("test-1234"))
	require.Nil(t, err)

	data, err = db.Gets("test-key")
	require.Nil(t, err)
	assert.Equal(t, []byte("test-1234"), data)

	// test persistence
	err = db.Close()
	require.Nil(t, err)

	db, err = Open("test.db")
	defer cleanup(db)
	require.Nil(t, err)

	err = db.Sets("test-4567", []byte("test-4567"))
	require.Nil(t, err)

	data, err = db.Gets("test-4567")
	require.Nil(t, err)
	assert.Equal(t, []byte("test-4567"), data)

	data, err = db.Gets("test-key")
	require.Nil(t, err)
	assert.Equal(t, []byte("test-1234"), data)
}

func TestPersistence(t *testing.T) {
	db, err := Open("test.db")
	defer cleanup(db)
	defer os.Remove("test.db.backup")

	require.Nil(t, err)

	err = db.Sets("test-key", []byte("test"))
	require.Nil(t, err)

	err = db.Sets("test-key-2", []byte("test-1"))
	require.Nil(t, err)

	err = db.Sets("test-key-2", []byte("test-2"))
	require.Nil(t, err)

	pos := db.data.Position()

	db.Close()

	// test reopen

	db, err = Open("test.db")
	require.Nil(t, err)

	assert.Equal(t, pos, db.data.Position())

	data, err := db.Gets("test-key")
	require.Nil(t, err)
	assert.Equal(t, []byte("test"), data)

	data, err = db.Gets("test-key-2")
	require.Nil(t, err)
	assert.Equal(t, []byte("test-2"), data)

	db.Close()

	// with compaction
	db, err = Open("test.db", Compact(true))
	require.Nil(t, err)

	fmt.Println(db.data.Position())

	data, err = db.Gets("test-key")
	require.Nil(t, err)
	assert.Equal(t, []byte("test"), data)

	data, err = db.Gets("test-key-2")
	require.Nil(t, err)
	assert.Equal(t, []byte("test-2"), data)
}
