package makefakegenesis

import (
	"crypto/ecdsa"
	"math/big"
	"time"

	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/inter/pos"
	"github.com/Fantom-foundation/lachesis-base/kvdb/memorydb"
	"github.com/Fantom-foundation/lachesis-base/lachesis"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/Fantom-foundation/go-opera/evmcore"
	"github.com/Fantom-foundation/go-opera/integration/makegenesis"
	"github.com/Fantom-foundation/go-opera/inter"
	"github.com/Fantom-foundation/go-opera/inter/drivertype"
	"github.com/Fantom-foundation/go-opera/inter/iblockproc"
	"github.com/Fantom-foundation/go-opera/inter/ier"
	"github.com/Fantom-foundation/go-opera/inter/validatorpk"
	"github.com/Fantom-foundation/go-opera/opera"
	"github.com/Fantom-foundation/go-opera/opera/contracts/driver"
	"github.com/Fantom-foundation/go-opera/opera/contracts/driver/drivercall"
	"github.com/Fantom-foundation/go-opera/opera/contracts/driverauth"
	"github.com/Fantom-foundation/go-opera/opera/contracts/evmwriter"
	"github.com/Fantom-foundation/go-opera/opera/contracts/netinit"
	netinitcall "github.com/Fantom-foundation/go-opera/opera/contracts/netinit/netinitcalls"
	"github.com/Fantom-foundation/go-opera/opera/contracts/sfc"
	"github.com/Fantom-foundation/go-opera/opera/genesis"
	"github.com/Fantom-foundation/go-opera/opera/genesis/gpos"
	"github.com/Fantom-foundation/go-opera/opera/genesisstore"
)

var (
	FakeGenesisTime = inter.Timestamp(1608600000 * time.Second)
)

// FakeKey gets n-th fake private key.
func FakeKey(n idx.ValidatorID) *ecdsa.PrivateKey {
	return evmcore.FakeKey(int(n))
}

func FakeGenesisStore(num idx.Validator, balance, stake *big.Int) *genesisstore.Store {
	return FakeGenesisStoreWithRules(num, balance, stake, opera.FakeNetRules())
}

func FakeGenesisStoreWithRules(num idx.Validator, balance, stake *big.Int, rules opera.Rules) *genesisstore.Store {
	return FakeGenesisStoreWithRulesAndStart(num, balance, stake, rules, 2, 1)
}

func FakeGenesisStoreWithRulesAndStart(num idx.Validator, balance, stake *big.Int, rules opera.Rules, epoch idx.Epoch, block idx.Block) *genesisstore.Store {
	builder := makegenesis.NewGenesisBuilder(memorydb.NewProducer(""))

	// add balance for some test accounts
	b, _ := hexutil.DecodeBig("0x123450000000000000000")
	builder.AddBalance(common.HexToAddress("cf49fda3be353c69b41ed96333cd24302da4556f"), b)
	builder.AddBalance(common.HexToAddress("0161e041aad467a890839d5b08b138c1e6373072"), b)
	builder.AddBalance(common.HexToAddress("87da6a8c6e9eff15d703fc2773e32f6af8dbe301"), b)
	builder.AddBalance(common.HexToAddress("b97de4b8c857e4f6bc354f226dc3249aaee49209"), b)
	builder.AddBalance(common.HexToAddress("c5065c9eeebe6df2c2284d046bfc906501846c51"), b)

	validators := GetFakeValidators(num)

	// add balances to validators
	var delegations []drivercall.Delegation
	for _, val := range validators {
		builder.AddBalance(val.Address, balance)
		delegations = append(delegations, drivercall.Delegation{
			Address:            val.Address,
			ValidatorID:        val.ID,
			Stake:              stake,
			LockedStake:        new(big.Int),
			LockupFromEpoch:    0,
			LockupEndTime:      0,
			LockupDuration:     0,
			EarlyUnlockPenalty: new(big.Int),
			Rewards:            new(big.Int),
		})
	}

	// deploy essential contracts
	// pre deploy NetworkInitializer
	builder.SetCode(netinit.ContractAddress, netinit.GetContractBin())
	// pre deploy NodeDriver
	builder.SetCode(driver.ContractAddress, driver.GetContractBin())
	// pre deploy NodeDriverAuth
	builder.SetCode(driverauth.ContractAddress, driverauth.GetContractBin())
	// pre deploy SFC
	builder.SetCode(sfc.ContractAddress, sfc.GetContractBin())
	// set non-zero code for pre-compiled contracts
	builder.SetCode(evmwriter.ContractAddress, []byte{0})

	// pre deploy test contracts
	b, _ = hexutil.DecodeBig("0x9999999999999999999999999999999")
	contractAddress := common.HexToAddress("0000000000000000000000000000000000000315")
	builder.AddBalance(contractAddress, b)
	builder.SetCode(contractAddress, hexutil.MustDecode("0x60606040526000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff168063ef2769ca1461003e575b610000565b3461000057610078600480803573ffffffffffffffffffffffffffffffffffffffff1690602001909190803590602001909190505061007a565b005b8173ffffffffffffffffffffffffffffffffffffffff166108fc829081150290604051809050600060405180830381858888f1935050505015610106578173ffffffffffffffffffffffffffffffffffffffff167f30a3c50752f2552dcc2b93f5b96866280816a986c0c0408cb6778b9fa198288f826040518082815260200191505060405180910390a25b5b50505600a165627a7a72305820637991fabcc8abad4294bf2bb615db78fbec4edff1635a2647d3894e2daf6a610029"))

	contractAddress = common.HexToAddress("0000000000000000000000000000000000000314")
	builder.SetCode(contractAddress, hexutil.MustDecode("0x60606040526000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff168063a223e05d1461006a578063abd1a0cf1461008d578063abfced1d146100d4578063e05c914a14610110578063e6768b451461014c575b610000565b346100005761007761019d565b6040518082815260200191505060405180910390f35b34610000576100be600480803573ffffffffffffffffffffffffffffffffffffffff169060200190919050506101a3565b6040518082815260200191505060405180910390f35b346100005761010e600480803573ffffffffffffffffffffffffffffffffffffffff169060200190919080359060200190919050506101ed565b005b346100005761014a600480803590602001909190803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610236565b005b346100005761017960048080359060200190919080359060200190919080359060200190919050506103c4565b60405180848152602001838152602001828152602001935050505060405180910390f35b60005481565b6000600160008373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019081526020016000205490505b919050565b80600160008473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020819055505b5050565b7f6031a8d62d7c95988fa262657cd92107d90ed96e08d8f867d32f26edfe85502260405180905060405180910390a17f47e2689743f14e97f7dcfa5eec10ba1dff02f83b3d1d4b9c07b206cbbda66450826040518082815260200191505060405180910390a1817fa48a6b249a5084126c3da369fbc9b16827ead8cb5cdc094b717d3f1dcd995e2960405180905060405180910390a27f7890603b316f3509577afd111710f9ebeefa15e12f72347d9dffd0d65ae3bade81604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390a18073ffffffffffffffffffffffffffffffffffffffff167f7efef9ea3f60ddc038e50cccec621f86a0195894dc0520482abf8b5c6b659e4160405180905060405180910390a28181604051808381526020018273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019250505060405180910390a05b5050565b6000600060008585859250925092505b935093509390505600a165627a7a72305820aaf842d0d0c35c45622c5263cbb54813d2974d3999c8c38551d7c613ea2bc1170029"))
	builder.SetStorage(contractAddress,
		common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000"),
		common.HexToHash("0x1234"))
	builder.SetStorage(contractAddress,
		common.HexToHash("0x6661e9d6d8b923d5bbaab1b96e1dd51ff6ea2a93520fdc9eb75d059238b8c5e9"),
		common.HexToHash("0x01"))

	builder.SetCurrentEpoch(ier.LlrIdxFullEpochRecord{
		LlrFullEpochRecord: ier.LlrFullEpochRecord{
			BlockState: iblockproc.BlockState{
				LastBlock: iblockproc.BlockCtx{
					Idx:     block - 1,
					Time:    FakeGenesisTime,
					Atropos: hash.Event{},
				},
				FinalizedStateRoot:    hash.Hash{},
				EpochGas:              0,
				EpochCheaters:         lachesis.Cheaters{},
				CheatersWritten:       0,
				ValidatorStates:       make([]iblockproc.ValidatorBlockState, 0),
				NextValidatorProfiles: make(map[idx.ValidatorID]drivertype.Validator),
				DirtyRules:            nil,
				AdvanceEpochs:         0,
			},
			EpochState: iblockproc.EpochState{
				Epoch:             epoch - 1,
				EpochStart:        FakeGenesisTime,
				PrevEpochStart:    FakeGenesisTime - 1,
				EpochStateRoot:    hash.Zero,
				Validators:        pos.NewBuilder().Build(),
				ValidatorStates:   make([]iblockproc.ValidatorEpochState, 0),
				ValidatorProfiles: make(map[idx.ValidatorID]drivertype.Validator),
				Rules:             rules,
			},
		},
		Idx: epoch - 1,
	})

	var owner common.Address
	if num != 0 {
		owner = validators[0].Address
	}

	blockProc := makegenesis.DefaultBlockProc()
	genesisTxs := GetGenesisTxs(epoch-2, validators, builder.TotalSupply(), delegations, owner)
	err := builder.ExecuteGenesisTxs(blockProc, genesisTxs)
	if err != nil {
		panic(err)
	}

	return builder.Build(genesis.Header{
		GenesisID:   builder.CurrentHash(),
		NetworkID:   rules.NetworkID,
		NetworkName: rules.Name,
	})
}

func txBuilder() func(calldata []byte, addr common.Address) *types.Transaction {
	nonce := uint64(0)
	return func(calldata []byte, addr common.Address) *types.Transaction {
		tx := types.NewTransaction(nonce, addr, common.Big0, 1e10, common.Big0, calldata)
		nonce++
		return tx
	}
}

func GetGenesisTxs(sealedEpoch idx.Epoch, validators gpos.Validators, totalSupply *big.Int, delegations []drivercall.Delegation, driverOwner common.Address) types.Transactions {
	buildTx := txBuilder()
	internalTxs := make(types.Transactions, 0, 15)
	// initialization
	calldata := netinitcall.InitializeAll(sealedEpoch, totalSupply, sfc.ContractAddress, driverauth.ContractAddress, driver.ContractAddress, evmwriter.ContractAddress, driverOwner)
	internalTxs = append(internalTxs, buildTx(calldata, netinit.ContractAddress))
	// push genesis validators
	for _, v := range validators {
		calldata := drivercall.SetGenesisValidator(v)
		internalTxs = append(internalTxs, buildTx(calldata, driver.ContractAddress))
	}
	// push genesis delegations
	for _, delegation := range delegations {
		calldata := drivercall.SetGenesisDelegation(delegation)
		internalTxs = append(internalTxs, buildTx(calldata, driver.ContractAddress))
	}
	return internalTxs
}

func GetFakeValidators(num idx.Validator) gpos.Validators {
	validators := make(gpos.Validators, 0, num)

	for i := idx.ValidatorID(1); i <= idx.ValidatorID(num); i++ {
		key := FakeKey(i)
		addr := crypto.PubkeyToAddress(key.PublicKey)
		pubkeyraw := crypto.FromECDSAPub(&key.PublicKey)
		validators = append(validators, gpos.Validator{
			ID:      i,
			Address: addr,
			PubKey: validatorpk.PubKey{
				Raw:  pubkeyraw,
				Type: validatorpk.Types.Secp256k1,
			},
			CreationTime:     FakeGenesisTime,
			CreationEpoch:    0,
			DeactivatedTime:  0,
			DeactivatedEpoch: 0,
			Status:           0,
		})
	}

	return validators
}
