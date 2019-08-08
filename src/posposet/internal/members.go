package internal

import (
	"sort"

	"github.com/Fantom-foundation/go-lachesis/src/hash"
	"github.com/Fantom-foundation/go-lachesis/src/inter"
	"github.com/Fantom-foundation/go-lachesis/src/inter/idx"
)

// MembersCount in top set.
const MembersCount = 30

type (
	// Members of super-frame with stake.
	Members map[hash.Peer]inter.Stake
)

// Add appends item.
func (mm *Members) Add(addr hash.Peer, stake inter.Stake) {
	(*mm)[addr] = stake
}

func (mm Members) sortedArray() members {
	array := make(members, 0, len(mm))
	for n, s := range mm {
		array = append(array, member{
			Addr:  n,
			Stake: s,
		})
	}
	sort.Sort(array)
	return array
}

// Top gets top subset.
func (mm Members) Top() Members {
	top := mm.sortedArray()

	if len(top) > MembersCount {
		top = top[:MembersCount]
	}

	res := make(Members)
	for _, m := range top {
		res.Add(m.Addr, m.Stake)
	}

	return res
}

// Idxs gets deterministic total order of members.
func (mm Members) Idxs() map[hash.Peer]idx.Member {
	idxs := make(map[hash.Peer]idx.Member, len(mm))
	for i, m := range mm.sortedArray() {
		idxs[m.Addr] = idx.Member(i)
	}
	return idxs
}

// Quorum limit of members.
func (mm Members) Quorum() inter.Stake {
	return mm.TotalStake()*2/3 + 1
}

// TotalStake of members.
func (mm Members) TotalStake() (sum inter.Stake) {
	for _, s := range mm {
		sum += s
	}
	return
}

// StakeOf member.
func (mm Members) StakeOf(n hash.Peer) inter.Stake {
	return mm[n]
}

// ToWire converts to protobuf message.
func (mm Members) ToWire() map[string]uint64 {
	w := make(map[string]uint64)

	for n, s := range mm {
		w[n.Hex()] = uint64(s)
	}

	return w
}

// WireToMembers converts from protobuf message.
func WireToMembers(w map[string]uint64) Members {
	if w == nil {
		return nil
	}

	mm := make(Members, len(w))
	for hex, amount := range w {
		addr := hash.HexToPeer(hex)
		mm[addr] = inter.Stake(amount)
	}

	return mm
}