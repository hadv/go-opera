package integration

import (
	"fmt"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/cachedproducer"
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
				Type: "pebble-fsh",
			},
			"lachesis": {
				Type:  "pebble-fsh",
				Name:  "main",
				Table: ">",
			},
			"gossip": {
				Type: "pebble-fsh",
				Name: "main",
			},
			"evm": {
				Type: "pebble-fsh",
				Name: "main",
			},
			"gossip/e": {
				Type: "pebble-fsh",
				Name: "events",
			},
			"evm/M": {
				Type: "pebble-drc",
				Name: "evm-data",
			},
			"evm-logs": {
				Type: "pebble-fsh",
				Name: "evm-logs",
			},
			"gossip-%d": {
				Type:  "leveldb-fsh",
				Name:  "epoch-%d",
				Table: "G",
			},
			"lachesis-%d": {
				Type:   "leveldb-fsh",
				Name:   "epoch-%d",
				Table:  "L",
				NoDrop: true,
			},
		},
	}
}

func MakeMultiProducer(rawProducers map[multidb.TypeName]kvdb.IterableDBProducer, scopedProducers map[multidb.TypeName]kvdb.FullDBProducer, cfg RoutingConfig) (kvdb.FullDBProducer, error) {
	cachedProducers := make(map[multidb.TypeName]kvdb.FullDBProducer)
	var flushID []byte
	var err error
	for typ, producer := range scopedProducers {
		flushID, err = producer.Initialize(rawProducers[typ].Names(), flushID)
		if err != nil {
			return nil, fmt.Errorf("failed to open existing databases: %v", err)
		}
		cachedProducers[typ] = cachedproducer.WrapAll(producer)
	}

	p, err := makeMultiProducer(cachedProducers, cfg)
	return p, err
}

func MakeDirectMultiProducer(rawProducers map[multidb.TypeName]kvdb.IterableDBProducer, cfg RoutingConfig) (kvdb.FullDBProducer, error) {
	dproducers := map[multidb.TypeName]kvdb.FullDBProducer{}
	for typ, producer := range rawProducers {
		dproducers[typ] = &DummyScopedProducer{producer}
	}
	return MakeMultiProducer(rawProducers, dproducers, cfg)
}

func makeMultiProducer(scopedProducers map[multidb.TypeName]kvdb.FullDBProducer, cfg RoutingConfig) (kvdb.FullDBProducer, error) {
	multi, err := multidb.NewProducer(scopedProducers, cfg.Table, TablesKey)
	if err != nil {
		return nil, fmt.Errorf("failed to construct multidb: %v", err)
	}

	err = multi.Verify()
	if err != nil {
		return nil, fmt.Errorf("incompatible chainstore DB layout: %v. Try to use 'db migrate' to recover", err)
	}
	return skipkeys.WrapAllProducer(multi, MetadataPrefix), nil
}
