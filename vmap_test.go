package swisstable

// VMap is a self validating map. It wraps a swisstable.Map and validates
// varies aspects of its operation, including during iteration where
// it validates whether or not a key is allowed to be seen zero times,
// exactly once, or multiple times due to add/deletes during the iteration.
//
// It is intended to work well with fuzzing. See autogenfuzzchain_test.go for an example.
//
// It was extracted from TestMap_RangeAddDelete, and currently overlaps with it.
// TODO: use a vMap in TestMap_RangeAddDelete.
// TODO: maybe move to internal. (currently reaching into implementation to set hashFunc)

import (
	"fmt"
	"sort"
	"testing"
)

type OpType byte

const (
	GetOp OpType = iota
	SetOp
	DeleteOp
	LenOp
	RangeOp

	BulkGetOp // must be first bulk op, after non-bulk ops
	BulkSetOp
	BulkDeleteOp

	OpTypeCount
)

// func (op OpType) isBulkOp() bool {
// 	return op%OpTypeCount >= GetBulkOp
// }

type Op struct {
	OpType OpType

	// used only if Op is not bulk Op
	Key Key

	// used only if Op is bulk op
	Keys Keys

	// used during a Range to specify when to do this op,
	// not used if this Op is not used in a Range
	RangeIndex uint16
}

func (o Op) String() string {
	t := o.OpType % OpTypeCount
	switch {
	case t < BulkGetOp:
		return fmt.Sprintf("{Op: %v Key: %v}", t, o.Key)
	case t < OpTypeCount:
		return fmt.Sprintf("{Op: %v Keys: %v RangeIndex: %v}", t, o.Keys, o.RangeIndex)
	default:
		return fmt.Sprintf("{Op: unknown %v}", o.OpType)
	}
}

type Keys struct {
	Start, End, Stride uint8 // [Start, End) - start inclusive, end exclusive
}

// Vmap is a self-validating wrapper around Map
type Vmap struct {
	// swisstable.Map under test
	m *Map

	// repeat any operations on our Map to a mirrored runtime map
	mirror map[Key]Value
}

// TODO: add testing.T
func NewVmap(capacity byte, start []Key) *Vmap {
	vm := &Vmap{}
	vm.m = New(int(capacity))

	// override the seed to make repeatable and consistent with an earlier value
	vm.m.seed = 42

	// make more reproducible, and also lumpier with a worse hash
	vm.m.hashFunc = identityHash

	vm.mirror = make(map[Key]Value)

	return vm
}

// TODO: maybe add testing.T
// TODO: don't think I need return values?
func (vm *Vmap) Get(k Key) (v Value, ok bool) {
	// TODO: consolidate or remove the debugVmap printlns
	if debugVmap {
		println("Get key:", k)
	}
	got, gotOk := vm.m.Get(k)
	want, wantOk := vm.mirror[k]
	if want != got || gotOk != wantOk {
		panic(fmt.Sprintf("Map.Get(%v) = %v, %v. want = %v, %v", k, got, gotOk, want, wantOk))
	}
	return
}

func (vm *Vmap) Set(k Key, v Value) {
	// TODO: could validate presence/absence vs. mirror in Set and Delete,
	// but eventually Get hopefully will evacuate, so probably better not to call here.
	if debugVmap {
		println("Set key:", k)
	}
	vm.m.Set(k, v)
	vm.mirror[k] = v
}

func (vm *Vmap) Delete(k Key) {
	if debugVmap {
		println("Delete key:", k)
	}
	vm.m.Delete(k)
	delete(vm.mirror, k)
}

// TODO: maybe add testing.T
func (vm *Vmap) Len() int {
	got := vm.m.Len()
	want := len(vm.mirror)
	if want != got {
		panic(fmt.Sprintf("Map.Len() = %v, want %v", got, want))
	}

	return vm.m.Len()
}

// Bulk operations

func (vm *Vmap) GetBulk(list Keys) (values []Value, oks []bool) {
	for _, key := range keySlice(list) {
		vm.Get(key)
	}
	return nil, nil
}

func (vm *Vmap) SetBulk(list Keys) {
	for _, key := range keySlice(list) {
		vm.Set(key, Value(key))
	}
}

func (vm *Vmap) DeleteBulk(list Keys) {
	for _, key := range keySlice(list) {
		vm.Delete(key)
	}
}

func (vm *Vmap) Range(ops []Op) {
	// we fix up RangeIndex to make the values useful more often
	for i := range ops {
		if ops[i].RangeIndex > 5001 {
			ops[i].RangeIndex = 0
		}
	}

	sort.SliceStable(ops, func(i, j int) bool {
		return ops[i].RangeIndex < ops[j].RangeIndex
	})

	// Create somes sets to dynamically track validity of keys that appear in a range.
	// allowed tracks start + added - deleted; these keys allowed but not required.
	allowed := newKeySet(nil)
	// mustSee tracks start - deleted; these are keys we are required to see at some point.
	mustSee := newKeySet(nil)
	// add the starting keys
	for k := range vm.mirror {
		allowed.add(k)
		mustSee.add(k)
	}

	// seen is used to verify no unexpected dups, and at end, to verify mustSee.
	seen := newKeySet(nil)

	// Also dynamically track if key X is added, deleted, and then re-added during iteration,
	// which means it is legal per Go spec to be seen again in the iteration.
	// Example with stdlib map repeating keys during iter: https://go.dev/play/p/RN-v8rmQmeE
	deleted := newKeySet(nil)
	addedAfterDeleted := newKeySet(nil)

	trackSet := func(k Key) {
		// update our trackers for a Set op during the range.
		allowed.add(k)
		if deleted.contains(k) {
			addedAfterDeleted.add(k)
			deleted.remove(k)
		}
	}

	trackDelete := func(k Key) {
		// update our trackers for a Delete op during the range.
		allowed.remove(k)
		mustSee.remove(k) // we are no longer required to see this. Fine if we saw it earlier.
		deleted.add(k)
		if addedAfterDeleted.contains(k) {
			addedAfterDeleted.remove(k)
		}
	}

	var rangeIndex uint16
	vm.m.Range(func(key Key, value Value) bool {
		// TODO: maybe add env var for equiv of:
		// println("iteration:", rangeIndex, "key:", key)

		seen.add(key)

		for len(ops) > 0 {
			op := ops[0]
			if op.RangeIndex != rangeIndex {
				break
			}

			switch op.OpType % OpTypeCount {
			case GetOp:
				if debugVmap {
					println("range case GetOp key:", op.Key)
				}
				vm.Get(op.Key)
			case SetOp:
				if debugVmap {
					println("range case SetOp key:", op.Key)
				}
				vm.Set(op.Key, Value(op.Key))
				trackSet(op.Key)
			case DeleteOp:
				if debugVmap {
					println("range case DeleteOp key:", op.Key)
				}
				vm.Delete(op.Key)
				trackDelete(op.Key)
			case LenOp:
				vm.Len()
			case RangeOp:
				// Ignore.
				// We could allow this, but naive approach might allow O(n^2) or worse behavior
			case BulkGetOp:
				for _, key := range keySlice(op.Keys) {
					if debugVmap {
						println("range case BulkGetOp key:", key)
					}
					vm.Get(key)
				}
			case BulkSetOp:
				for _, key := range keySlice(op.Keys) {
					if debugVmap {
						println("range case BulkSetOp key:", key)
					}
					vm.Set(key, Value(key))
					trackSet(key)
				}
			case BulkDeleteOp:
				for _, key := range keySlice(op.Keys) {
					if debugVmap {
						println("range case BulkDeleteOp key:", key)
					}
					vm.Delete(key)
					trackDelete(key)
				}
			default:
				panic("unexpected OpType")
			}

			ops = ops[1:]
		}
		rangeIndex++
		return true // keep iterating
	})

	for _, key := range mustSee.elems() {
		if !seen.contains(key) {
			panic(fmt.Sprintf("Map.Range() expected key %v not seen", key))
		}
	}
}

// keySlice converts from start/end/stride to a []Key
func keySlice(list Keys) []Key {
	// we fix up start/end to make the values useful more often
	start, end := int(list.Start), int(list.End)
	switch {
	case start > end:
		start, end = end, start
	case start == end:
		return nil
	}

	var stride int
	switch {
	case list.Stride < 128:
		// prefer stride of 1
		stride = 1
	default:
		stride = int(list.Stride%8) + 1
	}

	var res []Key
	for i := start; i < end; i += stride {
		res = append(res, Key(i))
	}
	return res
}

// some simple test functions

func TestValidatingMap_Range(t *testing.T) {
	tests := []struct {
		name string
		ops  []Op
	}{
		{
			name: "",
			ops: []Op{
				{
					OpType:     GetOp,
					Key:        1,
					Keys:       Keys{},
					RangeIndex: 0,
				},
				{
					OpType:     GetOp,
					Key:        2,
					Keys:       Keys{},
					RangeIndex: 0,
				},
				{
					OpType:     SetOp,
					Key:        3,
					Keys:       Keys{},
					RangeIndex: 2, // should happen last
				},
				{
					OpType:     55,
					Key:        4,
					Keys:       Keys{},
					RangeIndex: 0,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("ops: %v", tt.ops)
			vm := NewVmap(100, nil)
			vm.m.Set(100, 100)
			vm.m.Set(101, 101)
			vm.m.Set(102, 102)
			vm.Range(tt.ops)
			// TODO: maybe delete this test, or add a want here
		})
	}
}

const debugVmap = false
