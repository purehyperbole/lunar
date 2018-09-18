package lunar

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var proceed = true

func errcheck(t *testing.T, errs []chan error, timeout time.Duration) error {
	for _, e := range errs {
		select {
		case err := <-e:
			if err != nil {
				return err
			}
		case <-time.After(timeout):
			return errors.New("timeout reached")
		}
	}
	return nil
}

func TestTXConcurrentReads(t *testing.T) {
	var errs []chan error

	// open new
	db, err := Open("tx-cro.db")
	defer cleanup(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Update(func(tx *Tx) error {
		return tx.Set([]byte("test"), []byte("1"))
	})

	assert.Nil(t, err)

	for i := 0; i < 5; i++ {
		ec := make(chan error)
		errs = append(errs, ec)

		go func(e chan error) {
			err := db.View(func(tx *Tx) error {
				data, err := tx.Get([]byte("test"))
				assert.Nil(t, err)
				assert.Equal(t, "1", string(data))
				return err
			})

			e <- err
		}(ec)
	}

	// loops over error channels and asserts a failure
	errcheck(t, errs, time.Millisecond*100)
}
func TestTXConcurrentReadAndWrites(t *testing.T) {

	// open new
	db, err := Open("tx-crw.db")
	defer cleanup(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	txa := make(chan bool)
	txb := make(chan bool)

	wg := sync.WaitGroup{}
	wg.Add(2)

	err = db.Update(func(tx *Tx) error {
		return tx.Set([]byte("test"), []byte("1"))
	})

	assert.NotNil(t, db)

	// read only transaction starts and signals to the write tranasaction
	// when to update the kv pair
	go func(txa, txb chan bool, wg *sync.WaitGroup) {
		err := db.View(func(tx *Tx) error {
			data, err := tx.Get([]byte("test"))
			assert.Nil(t, err)
			assert.Equal(t, "1", string(data))

			// tell write tx to update and wait for it to complete
			txb <- proceed
			<-txa

			data, err = tx.Get([]byte("test"))
			assert.Nil(t, err)
			assert.Equal(t, "1", string(data))

			return err
		})

		assert.Nil(t, err)
		wg.Done()
	}(txa, txb, &wg)

	go func(txa, txb chan bool, wg *sync.WaitGroup) {
		err := db.Update(func(tx *Tx) error {
			// block until read only tx has read once
			<-txb

			return tx.Set([]byte("test"), []byte("2"))
		})

		assert.Nil(t, err)

		txa <- proceed

		wg.Done()
	}(txa, txb, &wg)

	wg.Wait()
}
