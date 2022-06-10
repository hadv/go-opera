package mdbx

import (
	"context"
	"fmt"
	"sync"

	"github.com/Fantom-foundation/go-opera/integration/kv"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/google/btree"
	dbx "github.com/torquem-ch/mdbx-go/mdbx"
)

type Database struct {
	fn string // filename for reporting
	db kv.RwDB

	quitLock sync.Mutex // Mutex protecting the quit channel access

	onClose func() error
	onDrop  func()
}

// New returns a wrapped LevelDB object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
func New(path string, close func() error, drop func()) (*Database, error) {
	opts := NewMDBX(log.New())
	opts = opts.Path(path)
	db, err := opts.Open()
	if err != nil {
		return nil, fmt.Errorf("Cannot open database")
	}
	// Assemble the wrapper with all the registered metrics
	ldb := Database{
		fn:      path,
		db:      db,
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

	if db.db == nil {
		panic("already closed")
	}

	ldb := db.db
	db.db = nil

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
	if db.db != nil {
		panic("Close database first!")
	}
	if db.onDrop != nil {
		db.onDrop()
	}
}

// Has retrieves if a key is present in the key-value store.
func (db *Database) Has(key []byte) (bool, error) {
	tx, err := db.db.BeginRo(context.Background())
	defer tx.Rollback()
	if err != nil {
		return false, err
	}
	return tx.Has("", key)
}

// Get retrieves the given key if it's present in the key-value store.
func (db *Database) Get(key []byte) ([]byte, error) {
	tx, err := db.db.BeginRo(context.Background())
	defer tx.Rollback()
	if err != nil {
		return nil, err
	}
	return tx.GetOne("", key)
}

// Put inserts/updates the given value into the key-value store.
func (db *Database) Put(key []byte, value []byte) error {
	tx, err := db.db.BeginRw(context.Background())
	defer tx.Rollback()
	if err != nil {
		return err
	}
	if err := tx.Put("", key, value); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// Delete removes the key from the key-value store.
func (db *Database) Delete(key []byte) error {
	tx, err := db.db.BeginRw(context.Background())
	defer tx.Rollback()
	if err != nil {
		return err
	}
	if err := tx.Delete("", key, nil); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// Stat returns a particular internal stat of the database.
func (db *Database) Stat(property string) (string, error) {
	return "", nil
}

// Compact mdbx itself was already compact db then don't need to do any more compacting
func (db *Database) Compact(start []byte, limit []byte) error {
	return nil
}

// Path returns the path to the database directory.
func (db *Database) Path() string {
	return db.fn
}

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (db *Database) NewBatch() kvdb.Batch {
	tx, err := db.db.BeginRw(context.Background())
	if err != nil {
		return nil
	}
	return &batch{
		kv.NewBatch(tx, nil),
	}
}

// batch is a write-only leveldb batch that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type batch struct {
	*kv.Batch
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(key, value []byte) error {
	return b.Put(key, value)
}

// Delete inserts the key removal into the batch for later committing.
func (b *batch) Delete(key []byte) error {
	return b.Delete(key)
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return b.BatchSize()
}

// Write flushes any accumulated data to disk.
func (b *batch) Write() error {
	return b.Commit()
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	b.Close()
}

// Replay replays the batch contents.
func (b *batch) Replay(w kvdb.Writer) (err error) {
	b.Iterator().Ascend(func(i btree.Item) bool {
		mi := i.(*kv.Item)
		if len(mi.Value()) == 0 {
			err = w.Delete(mi.Key())
		} else {
			err = w.Put(mi.Key(), mi.Value())
		}
		if err != nil {
			return false
		}
		return true
	})
	return err
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
// TODO: able to set key prefix for mdbx cursor or not
func (db *Database) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	tx, err := db.db.BeginRo(context.Background())
	if err != nil {
		return nil
	}
	cur, err := tx.Cursor("")
	cur.Seek(start)
	if err != nil {
		return nil
	}
	iter := iterator{tx, cur, nil}
	return &iter

}

func (it *iterator) Next() bool {
	_, _, err := it.Cursor.Next()
	if err != nil {
		it.accumulate(err)
		return false
	}
	return true
}

func (it *iterator) Error() error {
	return it.err
}

func (it *iterator) Key() []byte {
	key, _, err := it.Current()
	if err != nil {
		it.accumulate(err)
		return nil
	}
	return key
}

func (it *iterator) Value() []byte {
	_, val, err := it.Current()
	if err != nil {
		it.accumulate(err)
		return nil
	}
	return val
}

func (it *iterator) Release() {
	it.Cursor.Close()
	it.tx.Rollback()
	it.err = nil
}

func (it *iterator) accumulate(err error) {
	// don't accumulate the is not found error
	if dbx.IsNotFound(err) {
		return
	}
	if it.err == nil {
		it.err = err
	} else {
		it.err = fmt.Errorf("%w "+err.Error(), it.err)
	}
}

type iterator struct {
	tx kv.Tx
	kv.Cursor
	err error
}
