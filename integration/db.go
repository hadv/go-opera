package integration

import (
	"io/ioutil"
	"path"
	"strings"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/leveldb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/multidb"
	"github.com/Fantom-foundation/lachesis-base/utils/fmtfilter"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/Fantom-foundation/go-opera/gossip"
)

type DBsConfig struct {
	Routing RoutingConfig
	Cache   DBsCacheConfig
}

type DBCacheConfig struct {
	Cache   uint64
	Fdlimit uint64
}

type DBsCacheConfig struct {
	Table map[string]DBCacheConfig
}

func DefaultDBsConfig(scale func(uint64) uint64, fdlimit uint64) DBsConfig {
	return DBsConfig{
		Routing: DefaultRoutingConfig(),
		Cache:   DefaultDBsCacheConfig(scale, fdlimit),
	}
}

func DefaultDBsCacheConfig(scale func(uint64) uint64, fdlimit uint64) DBsCacheConfig {
	return DBsCacheConfig{
		Table: map[string]DBCacheConfig{
			"gossip": {
				Cache:   scale(128 * opt.MiB),
				Fdlimit: fdlimit*128/148 + 1,
			},
			"lachesis": {
				Cache:   scale(4 * opt.MiB),
				Fdlimit: fdlimit*4/148 + 1,
			},
			"lachesis-%d": {
				Cache:   scale(8 * opt.MiB),
				Fdlimit: fdlimit*8/148 + 1,
			},
			"gossip-%d": {
				Cache:   scale(8 * opt.MiB),
				Fdlimit: fdlimit*8/148 + 1,
			},
			"": {
				Cache:   opt.MiB,
				Fdlimit: 16,
			},
		},
	}
}

func SupportedDBs(chaindataDir string, cfg DBsCacheConfig) (map[multidb.TypeName]kvdb.IterableDBProducer, error) {
	if chaindataDir == "inmemory" || chaindataDir == "" {
		chaindataDir, _ = ioutil.TempDir("", "opera-tmp")
	}
	cacher, err := dbCacheFdlimit(cfg)
	if err != nil {
		return nil, err
	}
	return map[multidb.TypeName]kvdb.IterableDBProducer{
		"leveldb": leveldb.NewProducer(path.Join(chaindataDir, "leveldb"), cacher),
	}, nil
}

func dbCacheFdlimit(cfg DBsCacheConfig) (func(string) (int, int), error) {
	fmts := make([]func(req string) (string, error), 0, len(cfg.Table))
	fmtsCaches := make([]DBCacheConfig, 0, len(cfg.Table))
	exactTable := make(map[string]DBCacheConfig, len(cfg.Table))
	// build scanf filters
	for name, cache := range cfg.Table {
		if !strings.ContainsRune(name, '%') {
			exactTable[name] = cache
		} else {
			fn, err := fmtfilter.CompileFilter(name, name)
			if err != nil {
				return nil, err
			}
			fmts = append(fmts, fn)
			fmtsCaches = append(fmtsCaches, cache)
		}
	}
	return func(name string) (int, int) {
		// try exact match
		if cache, ok := cfg.Table[name]; ok {
			return int(cache.Cache), int(cache.Fdlimit)
		}
		// try regexp
		for i, fn := range fmts {
			if _, err := fn(name); err == nil {
				return int(fmtsCaches[i].Cache), int(fmtsCaches[i].Fdlimit)
			}
		}
		// default
		return int(cfg.Table[""].Cache), int(cfg.Table[""].Fdlimit)
	}, nil
}

func dropAllDBs(producer kvdb.IterableDBProducer) {
	names := producer.Names()
	for _, name := range names {
		db, err := producer.OpenDB(name)
		if err != nil {
			continue
		}
		_ = db.Close()
		db.Drop()
	}
}

func isInterrupted(rawProducers map[multidb.TypeName]kvdb.IterableDBProducer) bool {
	for _, producer := range rawProducers {
		names := producer.Names()
		for _, name := range names {
			db, err := producer.OpenDB(name)
			if err != nil {
				return false
			}
			flushID, err := db.Get(FlushIDKey)
			_ = db.Close()
			if err != nil {
				return false
			}
			if flushID == nil {
				return true
			}
		}
	}
	return false
}

func isEmpty(rawProducers map[multidb.TypeName]kvdb.IterableDBProducer) bool {
	for _, producer := range rawProducers {
		if len(producer.Names()) > 0 {
			return false
		}
	}
	return true
}

func dropAllDBsIfInterrupted(rawProducers map[multidb.TypeName]kvdb.IterableDBProducer) bool {
	if isInterrupted(rawProducers) {
		for _, producer := range rawProducers {
			dropAllDBs(producer)
		}
		return true
	}
	return isEmpty(rawProducers)
}

type GossipStoreAdapter struct {
	*gossip.Store
}

func (g *GossipStoreAdapter) GetEvent(id hash.Event) dag.Event {
	e := g.Store.GetEvent(id)
	if e == nil {
		return nil
	}
	return e
}

type DummyFlushableProducer struct {
	kvdb.IterableDBProducer
}

func (p *DummyFlushableProducer) NotFlushedSizeEst() int {
	return 0
}

func (p *DummyFlushableProducer) Flush(_ []byte) error {
	return nil
}
