package evmstore_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/Fantom-foundation/go-opera/gossip"
	"github.com/Fantom-foundation/go-opera/gossip/evmstore"
	"github.com/Fantom-foundation/go-opera/integration"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/memorydb"
	"github.com/Fantom-foundation/lachesis-base/utils/cachescale"
	"github.com/stretchr/testify/require"
)

const (
	FILE_CONTENT = `Lorem ipsum dolor sit amet, consectetur adipiscing elit. 
		Nunc finibus ultricies interdum. Nulla porttitor arcu a tincidunt mollis. Aliquam erat volutpat. 
		Maecenas eget ligula mi. Maecenas in ligula non elit fringilla consequat. 
		Morbi non imperdiet odio. Integer viverra ligula a varius tempor. 
		Duis ac velit vel augue faucibus tincidunt ut ac nisl. Nulla sed magna est. 
		Etiam quis nunc in elit ultricies pulvinar sed at felis. 
		Suspendisse fringilla lectus vel est hendrerit pulvinar. 
		Vivamus nec lorem pharetra ligula pulvinar blandit in quis nunc. 
		Cras id eros fermentum mauris tristique faucibus. 
		Praesent vehicula lectus nec ipsum sollicitudin tempus. Nullam et massa velit.`
)

func cachedStore() *evmstore.Store {
	cfg := evmstore.LiteStoreConfig()

	return evmstore.NewStore(memorydb.New(), cfg)
}

func nonCachedStore() *evmstore.Store {
	cfg := evmstore.StoreConfig{}

	return evmstore.NewStore(memorydb.New(), cfg)
}

func newStore(chainDir string) *gossip.Store {
	chaindataDir := path.Join(chainDir, "chaindata")
	os.MkdirAll(chaindataDir, 0700)
	rawProducer := integration.DBProducer(chaindataDir, cachescale.Identity)
	dbs := &integration.DummyFlushableProducer{rawProducer}
	return gossip.NewStore(dbs, gossip.DefaultStoreConfig(cachescale.Identity))
}

func TestEvmStore(t *testing.T) {
	chainDir, _ := ioutil.TempDir("", "evmstore")
	defer os.RemoveAll(chainDir)
	store := newStore(chainDir)
	require.NotNil(t, store)
	data := bytes.Repeat([]byte(FILE_CONTENT), 20000)
	evm := store.EvmStore()
	for i := 0; i < 100; i++ {
		evm.EvmDb.Put([]byte(fmt.Sprintf("test%d", i)), data)
	}
	fmt.Println(sizeOfDir(chainDir))

	for i := 0; i < 50; i++ {
		evm.EvmDb.Delete([]byte(fmt.Sprintf("test%d", i)))
	}
	fmt.Println(sizeOfDir(chainDir))

	for i := 100; i < 150; i++ {
		evm.EvmDb.Put([]byte(fmt.Sprintf("test%d", i)), data)
	}
	fmt.Println(sizeOfDir(chainDir))

	for i := 150; i < 250; i++ {
		evm.EvmDb.Put([]byte(fmt.Sprintf("test%d", i)), data)
	}
	fmt.Println(sizeOfDir(chainDir))

	for i := 150; i < 200; i++ {
		evm.EvmDb.Delete([]byte(fmt.Sprintf("test%d", i)))
	}
	fmt.Println(sizeOfDir(chainDir))
}

func TestEvmStoreWithBatch(t *testing.T) {
	chainDir, _ := ioutil.TempDir("", "evmstore")
	defer os.RemoveAll(chainDir)
	store := newStore(chainDir)
	batch := store.EvmStore().EvmDb.NewBatch()
	defer batch.Reset()
	value := bytes.Repeat([]byte(FILE_CONTENT), 20000)
	for i := 0; i < 100; i++ {
		batch.Put([]byte(fmt.Sprintf("test%d", i)), value)
		if batch.ValueSize() > kvdb.IdealBatchSize {
			batch.Write()
			batch.Reset()
		}
	}
	batch.Write()
	batch.Reset()
	fmt.Println(sizeOfDir(chainDir))

	for i := 0; i < 50; i++ {
		batch.Delete([]byte(fmt.Sprintf("test%d", i)))
		if batch.ValueSize() > kvdb.IdealBatchSize {
			batch.Write()
			batch.Reset()
		}
	}
	batch.Write()
	batch.Reset()
	fmt.Println(sizeOfDir(chainDir))

	for i := 100; i < 150; i++ {
		batch.Put([]byte(fmt.Sprintf("test%d", i)), value)
		if batch.ValueSize() > kvdb.IdealBatchSize {
			batch.Write()
			batch.Reset()
		}
	}
	batch.Write()
	batch.Reset()
	fmt.Println(sizeOfDir(chainDir))

	for i := 150; i < 250; i++ {
		batch.Put([]byte(fmt.Sprintf("test%d", i)), value)
		if batch.ValueSize() > kvdb.IdealBatchSize {
			batch.Write()
			batch.Reset()
		}
	}
	batch.Write()
	batch.Reset()
	fmt.Println(sizeOfDir(chainDir))
}

func sizeOfDir(dir string) (size int64) {
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fmt.Println("datadir walk", "path", path, "err", err)
			return filepath.SkipDir
		}

		if info.IsDir() {
			return nil
		}

		dst, err := filepath.EvalSymlinks(path)
		if err == nil && dst != path {
			size += sizeOfDir(dst)
		} else {
			size += info.Size()
		}

		return nil
	})

	if err != nil {
		fmt.Println("datadir walk", "path", dir, "err", err)
	}

	return
}
