package evmstore_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/Fantom-foundation/go-opera/gossip/evmstore"
	"github.com/Fantom-foundation/go-opera/integration"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/memorydb"
	"github.com/Fantom-foundation/lachesis-base/utils/cachescale"
	"github.com/ethereum/go-ethereum/cmd/utils"
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

	return evmstore.NewStore(memorydb.NewProducer(""), cfg)
}

func nonCachedStore() *evmstore.Store {
	cfg := evmstore.StoreConfig{}

	return evmstore.NewStore(memorydb.NewProducer(""), cfg)
}

func newStore(chainDir string, cc integration.DBsCacheConfig, dc integration.DBsConfig) *evmstore.Store {
	cfg := evmstore.LiteStoreConfig()
	integration.MakeDBDirs(chainDir)
	genesisProducers, _ := integration.SupportedDBs(chainDir, cc)
	dbs, _ := integration.MakeDirectMultiProducer(genesisProducers, dc.Routing)
	return evmstore.NewStore(dbs, cfg)
}

func TestEvmStore(t *testing.T) {
	t.Run("leveldb", func(t *testing.T) {
		testEvmStore(t, integration.Ldb1RuntimeDBsCacheConfig(cachescale.Identity.U64, uint64(utils.MakeDatabaseHandles())), integration.DefaultDBsConfig(cachescale.Identity.U64, uint64(utils.MakeDatabaseHandles())))
	})

	// t.Run("pebble", func(t *testing.T) {
	// 	testEvmStore(t, integration.Pbl1RuntimeDBsCacheConfig(cachescale.Identity.U64, uint64(utils.MakeDatabaseHandles())), integration.Pbl1DBsConfig(cachescale.Identity.U64, uint64(utils.MakeDatabaseHandles())))
	// })
}

func TestEvmStoreWithBatch(t *testing.T) {
	t.Run("leveldb", func(t *testing.T) {
		testEvmStoreWithBatch(t, integration.Ldb1RuntimeDBsCacheConfig(cachescale.Identity.U64, uint64(utils.MakeDatabaseHandles())), integration.DefaultDBsConfig(cachescale.Identity.U64, uint64(utils.MakeDatabaseHandles())))
	})

	// t.Run("pebble", func(t *testing.T) {
	// 	testEvmStoreWithBatch(t, integration.Pbl1RuntimeDBsCacheConfig(cachescale.Identity.U64, uint64(utils.MakeDatabaseHandles())), integration.Pbl1DBsConfig(cachescale.Identity.U64, uint64(utils.MakeDatabaseHandles())))
	// })
}

func testEvmStore(t *testing.T, cc integration.DBsCacheConfig, dc integration.DBsConfig) {
	chainDir, _ := ioutil.TempDir("", "evmstore")
	defer os.RemoveAll(chainDir)
	store := newStore(chainDir, cc, dc)
	data := bytes.Repeat([]byte(FILE_CONTENT), 22000)
	for i := 0; i < 100; i++ {
		store.EvmDb.Put([]byte(fmt.Sprintf("test%d", i)), data)
	}
	var cp1 int64
	fmt.Sscanf(fmt.Sprintf(store.EvmDb.Stat("disk.size")), "Size(B):%d", &cp1)
	for i := 0; i < 50; i++ {
		store.EvmDb.Delete([]byte(fmt.Sprintf("test%d", i)))
	}
	var cp2 int64
	fmt.Sscanf(fmt.Sprintf(store.EvmDb.Stat("disk.size")), "Size(B):%d", &cp2)
	require.Equal(t, cp1, cp2)

	for i := 100; i < 150; i++ {
		store.EvmDb.Put([]byte(fmt.Sprintf("test%d", i)), data)
	}

	var cp3 int64
	fmt.Sscanf(fmt.Sprintf(store.EvmDb.Stat("disk.size")), "Size(B):%d", &cp3)
	fmt.Println(cp2)
	fmt.Println(cp3)

	for i := 150; i < 250; i++ {
		store.EvmDb.Put([]byte(fmt.Sprintf("test%d", i)), data)
	}
	var cp4 int64
	fmt.Sscanf(fmt.Sprintf(store.EvmDb.Stat("disk.size")), "Size(B):%d", &cp4)
	require.Greater(t, cp4, cp3)
	fmt.Println(cp4)

	for i := 150; i < 200; i++ {
		store.EvmDb.Delete([]byte(fmt.Sprintf("test%d", i)))
	}
	var cp5 int64
	fmt.Sscanf(fmt.Sprintf(store.EvmDb.Stat("disk.size")), "Size(B):%d", &cp5)
	require.Equal(t, cp4, cp5)
}

func testEvmStoreWithBatch(t *testing.T, cc integration.DBsCacheConfig, dc integration.DBsConfig) {
	chainDir, _ := ioutil.TempDir("", "evmstore")
	defer os.RemoveAll(chainDir)
	store := newStore(chainDir, cc, dc)
	batch := store.EvmDb.NewBatch()
	defer batch.Reset()
	data := bytes.Repeat([]byte(FILE_CONTENT), 22000)
	for i := 0; i < 100; i++ {
		batch.Put([]byte(fmt.Sprintf("test%d", i)), data)
		if batch.ValueSize() > kvdb.IdealBatchSize {
			batch.Write()
			batch.Reset()
		}
	}
	batch.Write()
	batch.Reset()

	var cp1 int64
	fmt.Sscanf(fmt.Sprintf(store.EvmDb.Stat("disk.size")), "Size(B):%d", &cp1)
	for i := 0; i < 50; i++ {
		batch.Delete([]byte(fmt.Sprintf("test%d", i)))
		if batch.ValueSize() > kvdb.IdealBatchSize {
			batch.Write()
			batch.Reset()
		}
	}
	batch.Write()
	batch.Reset()

	var cp2 int64
	fmt.Sscanf(fmt.Sprintf(store.EvmDb.Stat("disk.size")), "Size(B):%d", &cp2)
	require.Equal(t, cp1, cp2)

	for i := 100; i < 150; i++ {
		batch.Put([]byte(fmt.Sprintf("test%d", i)), data)
		if batch.ValueSize() > kvdb.IdealBatchSize {
			batch.Write()
			batch.Reset()
		}
	}
	batch.Write()
	batch.Reset()

	var cp3 int64
	fmt.Sscanf(fmt.Sprintf(store.EvmDb.Stat("disk.size")), "Size(B):%d", &cp3)
	fmt.Println(cp2)
	fmt.Println(cp3)

	for i := 150; i < 250; i++ {
		batch.Put([]byte(fmt.Sprintf("test%d", i)), data)
		if batch.ValueSize() > kvdb.IdealBatchSize {
			batch.Write()
			batch.Reset()
		}
	}
	batch.Write()
	batch.Reset()
	var cp4 int64
	fmt.Sscanf(fmt.Sprintf(store.EvmDb.Stat("disk.size")), "Size(B):%d", &cp4)
	fmt.Println(cp4)
}
