package mdbx

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/log"
	dbx "github.com/torquem-ch/mdbx-go/mdbx"
)

type Database struct {
	fn  string // filename for reporting
	env *dbx.Env

	quitLock sync.Mutex // Mutex protecting the quit channel access

	onClose func() error
	onDrop  func()
}

// New returns a wrapped LevelDB object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
func New(path string, close func() error, drop func()) (*Database, error) {
	env, err := dbx.NewEnv()
	if err != nil {
		log.Error("Cannot create mdbx environment", "err", err)
		return nil, err
	}
	if err := env.SetGeometry(-1, -1, 1024*1024, -1, -1, 4096); err != nil {
		log.Error("Cannot set mdbx mapsize", "err", err)
		return nil, err
	}
	if err := env.Open(path, 0, 0664); err != nil {
		log.Error("Cannot open mdbx environment", "err", err)
		return nil, err
	}
	// Assemble the wrapper with all the registered metrics
	ldb := Database{
		fn:      path,
		env:     env,
		onClose: close,
		onDrop:  drop,
	}
	return &ldb, nil
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (db *Database) Close() error {
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	if db.env == nil {
		panic("already closed")
	}

	ldb := db.env
	db.env = nil

	if db.onClose != nil {
		if err := db.onClose(); err != nil {
			return err
		}
		db.onClose = nil
	}
	ldb.Close()
	return nil
}

// Drop whole database.
func (db *Database) Drop() {
	if db.env != nil {
		panic("Close database first!")
	}
	if db.onDrop != nil {
		db.onDrop()
	}
}

// Has retrieves if a key is present in the key-value store.
func (db *Database) Has(key []byte) (bool, error) {
	if err := db.env.View(func(txn *dbx.Txn) error {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		_, err = txn.Get(dbi, key)
		if err != nil {
			return fmt.Errorf("get: %v", err)
		}
		return nil
	}); err != nil {
		return false, err
	}
	return true, nil
}

// Get retrieves the given key if it's present in the key-value store.
func (db *Database) Get(key []byte) ([]byte, error) {
	var val []byte
	if err := db.env.View(func(txn *dbx.Txn) error {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}
		val, err = txn.Get(dbi, key)
		if err != nil {
			return fmt.Errorf("get: %v", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return val, nil
}

// Put inserts the given value into the key-value store.
func (db *Database) Put(key []byte, value []byte) error {
	if err := db.env.Update(func(txn *dbx.Txn) (err error) {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}

		err = txn.Put(dbi, key, value, dbx.NoOverwrite)
		if err != nil {
			return fmt.Errorf("put: %v", err)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// Delete removes the key from the key-value store.
func (db *Database) Delete(key []byte) error {
	if err := db.env.Update(func(txn *dbx.Txn) (err error) {
		dbi, err := txn.OpenRoot(0)
		if err != nil {
			return err
		}

		err = txn.Del(dbi, key, nil)
		if err != nil {
			return fmt.Errorf("delete: %v", err)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (db *Database) Stat(property string) (string, error) {
	stat, err := db.env.Stat()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%#v", stat), nil
}

// Path returns the path to the database directory.
func (db *Database) Path() string {
	return db.fn
}
