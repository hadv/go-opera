package kv

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/ethereum/go-ethereum/log"
	"github.com/google/btree"
)

var ErrStopped = errors.New("stopped")

type Batch struct {
	puts  *btree.BTree
	db    RwTx
	quit  <-chan struct{}
	clean func()
	item  Item
	mu    sync.RWMutex
	size  int
}

type Item struct {
	key   []byte
	value []byte
}

func (i *Item) Less(than btree.Item) bool {
	o := than.(*Item)
	return bytes.Compare(i.key, o.key) < 0
}

func (i *Item) Key() []byte {
	return i.key
}

func (i *Item) Value() []byte {
	return i.value
}

func NewBatch(tx RwTx, quit <-chan struct{}) *Batch {
	clean := func() {}
	if quit == nil {
		ch := make(chan struct{})
		clean = func() { close(ch) }
		quit = ch
	}
	return &Batch{
		db:    tx,
		puts:  btree.New(32),
		quit:  quit,
		clean: clean,
	}
}

func (b *Batch) Put(key []byte, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	item := &Item{key, value}
	i := b.puts.ReplaceOrInsert(item)
	b.size += int(unsafe.Sizeof(item)) + len(key) + len(value)
	if i != nil {
		old := i.(*Item)
		b.size -= (int(unsafe.Sizeof(old)) + len(old.key) + len(old.value))
	}
	return nil
}

func (b *Batch) Delete(k []byte) error {
	return b.Put(k, nil)
}

func (b *Batch) BatchSize() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.size
}

func (b *Batch) doCommit(tx RwTx) error {
	var c RwCursor
	var innerErr error
	var isEndOfBucket bool
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	count := 0
	total := float64(b.puts.Len())

	b.puts.Ascend(func(i btree.Item) bool {
		mi := i.(*Item)
		if c != nil {
			c.Release()
		}
		var err error
		c, err = tx.RwCursor("")
		if err != nil {
			innerErr = err
			return false
		}
		firstKey, _, err := c.Seek(mi.key)
		if err != nil {
			innerErr = err
			return false
		}
		isEndOfBucket = firstKey == nil
		if isEndOfBucket {
			if len(mi.value) > 0 {
				if err := c.Append(mi.key, mi.value); err != nil {
					innerErr = err
					return false
				}
			}
		} else if len(mi.value) == 0 {
			if err := c.Delete(mi.key, nil); err != nil {
				innerErr = err
				return false
			}
		} else {
			if err := c.Put(mi.key, mi.value); err != nil {
				innerErr = err
				return false
			}
		}

		count++

		select {
		default:
		case <-logEvery.C:
			progress := fmt.Sprintf("%.1fM/%.1fM", float64(count)/1_000_000, total/1_000_000)
			log.Info("Write to db", "progress", progress)
			tx.CollectMetrics()
		case <-b.quit:
			innerErr = ErrStopped
			return false
		}
		return true
	})
	tx.CollectMetrics()
	return innerErr
}

func (b *Batch) Iterator() *btree.BTree {
	return b.puts
}

func (b *Batch) Commit() error {
	if b.db == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.doCommit(b.db); err != nil {
		return err
	}

	b.puts.Clear(false /* addNodesToFreelist */)
	b.size = 0
	b.clean()
	return nil
}

func (b *Batch) Rollback() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.puts.Clear(false)
	b.size = 0
	b.clean()
}

func (b *Batch) Close() {
	b.Rollback()
}
