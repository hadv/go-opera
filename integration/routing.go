package integration

import (
	"fmt"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/cachedproducer"
	"github.com/Fantom-foundation/lachesis-base/kvdb/flushable"
	"github.com/Fantom-foundation/lachesis-base/kvdb/multidb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/skipkeys"
)

type RoutingConfig struct {
	Table map[string]multidb.Route
}

func DefaultRoutingConfig() RoutingConfig {
	return RoutingConfig{
		Table: map[string]multidb.Route{
			"": {
				Type: "leveldb",
			},
		},
	}
}

func MakeFlushableMultiProducer(rawProducers map[multidb.TypeName]kvdb.IterableDBProducer, cfg RoutingConfig) (kvdb.FullDBProducer, error) {
	flushables := make(map[multidb.TypeName]kvdb.FullDBProducer)
	var flushID []byte
	var err error
	for typ, producer := range rawProducers {
		existingDBs := producer.Names()
		flushableDB := flushable.NewSyncedPool(producer, FlushIDKey)
		flushID, err = flushableDB.Initialize(existingDBs, flushID)
		if err != nil {
			return nil, fmt.Errorf("failed to open existing databases: %v", err)
		}
		flushables[typ] = cachedproducer.WrapAll(flushableDB)
	}

	return makeMultiProducer(flushables, cfg)
}

func MakeRawMultiProducer(rawProducers map[multidb.TypeName]kvdb.IterableDBProducer, cfg RoutingConfig) (kvdb.FullDBProducer, error) {
	flushables := make(map[multidb.TypeName]kvdb.FullDBProducer)
	var flushID []byte
	var err error
	for typ, producer := range rawProducers {
		existingDBs := producer.Names()
		dbs := flushable.NewSyncedPool(producer, FlushIDKey)
		flushID, err = dbs.Initialize(existingDBs, flushID)
		if err != nil {
			return nil, fmt.Errorf("failed to open existing databases: %v", err)
		}
		flushables[typ] = cachedproducer.WrapAll(&DummyFlushableProducer{producer})
	}

	return makeMultiProducer(flushables, cfg)
}

func makeMultiProducer(producers map[multidb.TypeName]kvdb.FullDBProducer, cfg RoutingConfig) (kvdb.FullDBProducer, error) {
	multi, err := multidb.NewProducer(producers, cfg.Table, TablesKey)
	if err != nil {
		return nil, fmt.Errorf("failed to construct multidb: %v", err)
	}

	err = multi.Verify()
	if err != nil {
		return nil, fmt.Errorf("incompatible chainstore DB layout: %v. Try to use 'db migrate' to recover", err)
	}
	return skipkeys.WrapAllProducer(multi, MetadataPrefix), nil
}
