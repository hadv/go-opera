package mdbx

import (
	"container/list"
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

type snapshotElement struct {
	seq uint64
	ref int
	e   *list.Element
}

// Snapshot is a DB snapshot.
type MbdxSnapshot struct {
	db       *MdbxKV
	elem     *snapshotElement
	mu       sync.RWMutex
	released bool
}

func (snap *MbdxSnapshot) String() string {
	return fmt.Sprintf("mbdx.Snapshot{%d}", snap.elem.seq)
}

// Get gets the value for the given key. It returns ErrNotFound if
// the DB does not contains the key.
//
// The caller should not modify the contents of the returned slice, but
// it is safe to modify the contents of the argument after Get returns.
func (snap *MbdxSnapshot) Get(key []byte) (value []byte, err error) {
	snap.mu.RLock()
	defer snap.mu.RUnlock()
	if snap.released {
		err = ErrSnapshotReleased
		return
	}
	tx, err := snap.db.BeginRo(context.Background())
	if err != nil {
		return
	}
	defer tx.Rollback()

	return tx.GetOne("", key)
}

// Has returns true if the DB does contains the given key.
//
// It is safe to modify the contents of the argument after Get returns.
func (snap *MbdxSnapshot) Has(key []byte) (ret bool, err error) {
	snap.mu.RLock()
	defer snap.mu.RUnlock()
	if snap.released {
		err = ErrSnapshotReleased
		return
	}
	tx, err := snap.db.BeginRo(context.Background())
	if err != nil {
		return
	}
	defer tx.Rollback()

	return tx.Has("", key)
}

// NewIter returns an iterator that is unpositioned (Iterator.Valid() will
// return false). The iterator can be positioned via a call to SeekGE,
// SeekLT, First or Last.
func (snap *MbdxSnapshot) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	if snap.db == nil {
		panic(ErrClosed)
	}
	tx, err := snap.db.BeginRo(context.Background())
	if err != nil {
		return nil
	}
	iter, err := tx.Cursor("")
	iter.Seek(start)
	return &iterator{tx, iter, err}
}

// Release releases the snapshot. This will not release any returned
// iterators, the iterators would still be valid until released or the
// underlying DB is closed.
//
// Other methods should not be called after the snapshot has been released.
func (snap *MbdxSnapshot) Release() {
	snap.mu.Lock()
	defer snap.mu.Unlock()

	if !snap.released {
		// Clear the finalizer.
		runtime.SetFinalizer(snap, nil)

		snap.released = true
		snap.db.releaseSnapshot(snap.elem)
		atomic.AddInt32(&snap.db.aliveSnaps, -1)
		snap.db = nil
		snap.elem = nil
	}
}
