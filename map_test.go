package swisstable

import (
	"flag"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
)

var longTestFlag = flag.Bool("long", false, "run long benchmarks")
var coldMemTestFlag = flag.Float64("coldmem", 512, "memory in MB to use for cold memory tests. should be substantially larger than L3 cache.")

// TODO: 1000 is probably reasonable
var repFlag = flag.Int("rep", 200, "number of repetitions for some tests that are randomized")

func TestMap_Get(t *testing.T) {
	tests := []struct {
		name string
		keys []Key
	}{
		{"one key", []Key{1}},
		{"small, with one grow", list(0, 20, 1)},
		{"small, with multiple grows", list(0, 111, 1)}, // from fuzzing
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf(tt.name), func(t *testing.T) {
			m := New(10)
			m.hashFunc = identityHash

			for _, k := range tt.keys {
				m.Set(Key(k), Value(k))
			}

			gotLen := m.Len()
			if gotLen != len(tt.keys) {
				t.Errorf("Map.Len() = %d, want %d", gotLen, len(tt.keys))
			}

			for _, k := range tt.keys {
				gotV, gotOk := m.Get(k)
				if gotV != Value(k) || !gotOk {
					t.Errorf("Map.Get(%v) = %v, %v. want = %v, true", k, gotV, gotOk, k)
				}
			}

			notPresent := Key(1e12)
			gotV, gotOk := m.Get(notPresent)
			if gotV != 0 || gotOk {
				t.Errorf("Map.Get(notPresent) = %v, %v. want = 0, false", gotV, gotOk)
			}
		})
	}
}

func TestMap_Range(t *testing.T) {
	tests := []struct {
		name  string
		elems map[Key]Value
	}{
		{
			"three elements",
			map[Key]Value{
				1:   2,
				8:   8,
				1e6: 1e10,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New(256) // TODO: confirm this is probably 512 underlying table length?

			for key, value := range tt.elems {
				m.Set(key, value)
				gotV, gotOk := m.Get(key)
				if !gotOk {
					t.Errorf("Map.Get() gotOk = %v, want true", gotOk)
				}
				if gotV != value {
					t.Errorf("Map.Get() gotV = %v, want %v", gotV, value)
				}
			}
			got := make(map[Key]Value)
			m.Range(func(key Key, value Value) bool {
				// validate we don't see the same key twice
				_, ok := got[key]
				if ok {
					dumpFixedTables(m)
					t.Errorf("Map.Range() key %v seen before", key)
				}
				got[key] = value
				return true
			})
			// validate our returned key/values match what we put in
			if diff := cmp.Diff(tt.elems, got); diff != "" {
				t.Errorf("Map.Range() result mismatch (-want +got):\n%s", diff)
			}
			gotLen := m.Len()
			if gotLen != len(tt.elems) {
				t.Errorf("Map.Len() gotV = %v, want %v", gotLen, len(tt.elems))
			}
		})
	}
}

func TestMap_Delete(t *testing.T) {
	tests := []struct {
		name            string
		capacity        int
		disableResizing bool
		insert          int
		deleteFront     int
		deleteBack      int
	}{
		{
			name:            "small, delete one",
			disableResizing: false,
			capacity:        256,
			insert:          2,
			deleteFront:     1,
			deleteBack:      0,
		},
		{
			name:            "small, delete one after resizing",
			disableResizing: false,
			capacity:        10,
			insert:          20, // this forces a resize
			deleteFront:     0,
			deleteBack:      0,
		},
		{
			name:            "delete ten after resizing",
			disableResizing: false,
			capacity:        256,
			insert:          510, // this forces a resize
			deleteFront:     0,
			deleteBack:      10,
		},
		{
			name:            "delete ten force fill",
			disableResizing: true,
			capacity:        256,
			insert:          510, // this is close to full
			deleteFront:     0,
			deleteBack:      10,
		},
		{
			name:            "delete all after resizing",
			disableResizing: false,
			capacity:        256,
			insert:          511, // this forces a resize
			deleteFront:     256,
			deleteBack:      256,
		},
		{
			name:            "delete all force fill",
			disableResizing: true,
			capacity:        256,
			insert:          511, // this is a force fill, leaving one empty slot
			deleteFront:     256,
			deleteBack:      256,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New(tt.capacity)
			want := make(map[Key]Value)

			for i := 0; i < tt.insert; i++ {
				m.Set(Key(i), Value(i))
				want[Key(i)] = Value(i)
			}

			// // Delete a non-existent key
			m.Delete(-1)
			delete(want, -1)

			// Delete requested keys
			for i := 0; i < tt.deleteFront; i++ {
				m.Delete(Key(i))
				delete(want, Key(i))
			}
			for i := tt.insert - tt.deleteBack; i < tt.insert; i++ {
				m.Delete(Key(i))
				delete(want, Key(i))
			}

			got := make(map[Key]Value)
			m.Range(func(key Key, value Value) bool {
				// validate we don't see the same key twice
				_, ok := got[key]
				if ok {
					t.Errorf("Map.Range() key %v seen twice", key)
				}
				got[key] = value
				return true
			})

			if diff := cmp.Diff(want, got); diff != "" {
				t.Logf("slots: %v", m.current.slots)
				t.Errorf("Map.Range() result mismatch (-want +got):\n%s", diff)
			}
			gotLen := m.Len()
			if gotLen != len(want) {
				t.Errorf("Map.Len() gotV = %v, want %v", gotLen, len(want))
			}
		})
	}
}

// TODO: force example of an *allowed* repeat key from a range, such as:
//    https://go.dev/play/p/y8kvkPoNCv_H
// We can't quite create that same pattern with the current TestMap_RangeAddDelete,
// including the bulk add doesn't happen after all the preceding add/deletes.

func TestMap_RangeAddDelete(t *testing.T) {
	tests := []struct {
		name          string
		repeatAllowed bool // allow repeated key, such as if add X, del X, then add X while iterating
		capacity      int
		start         []Key
		del           []Key
		add           []Key
		addBulk       []Key // can be set up to trigger resize in middle of loop if desired
		addBulk2      []Key
		bulkIndex     int // loop index in Map range to do the addBulk
	}{
		{
			name:          "small",
			repeatAllowed: true, // this pattern could in theory trigger repeat key
			capacity:      16,
			start:         []Key{1, 2, 3, 4},
			del:           []Key{3, 4},
			add:           []Key{5, 6, 4, 7},
			addBulk:       nil,
			addBulk2:      nil,
			bulkIndex:     0,
		},
		{
			name:          "small with one grow",
			repeatAllowed: false,
			capacity:      8, // will be table len of 16
			start:         []Key{1, 2, 3, 4},
			del:           nil,
			add:           nil,
			addBulk:       list(5, 15, 1),
			addBulk2:      nil,
			bulkIndex:     0,
		},
		{
			name:          "small with two grows",
			repeatAllowed: false,
			capacity:      8, // will be table len of 16
			start:         []Key{1, 2, 3, 4},
			del:           nil,
			add:           nil,
			addBulk:       list(5, 30, 1),
			addBulk2:      nil,
			bulkIndex:     0,
		},
		{
			name:          "small, start iter mid-grow then grow",
			repeatAllowed: false,
			capacity:      8, // will be table len of 16
			start:         list(0, 53, 1),
			del:           nil,
			add:           nil,
			addBulk:       list(64, 128, 1),
			addBulk2:      nil,
			bulkIndex:     0,
		},
		{
			name:          "medium",
			repeatAllowed: true, // this pattern could in theory trigger repeat key
			capacity:      650,
			start:         list(0, 500, 1),
			del:           list(10, 400, 1),
			add:           list(500, 650, 1),
			addBulk:       list(10, 400, 1),
			addBulk2:      []Key{},
			bulkIndex:     400,
		},
		{
			name:          "medium, start iter mid-grow then grow",
			repeatAllowed: false,
			capacity:      8,               // will be table len of 16
			start:         list(0, 417, 1), // trigger growth at 416
			del:           nil,
			add:           list(512, 950, 1),
			addBulk:       nil,
			addBulk2:      nil,
			bulkIndex:     415,
		},
		{
			name:          "medium, start iter mid-grow, overlapping writes during iter", // from fuzzing
			repeatAllowed: false,
			capacity:      48,
			start:         list(48, 102, 1), // 54 elems, grow starts at 52
			del:           nil,
			add:           nil,
			addBulk:       list(11, 119, 1), // 108 elems, some overlapping
			addBulk2:      nil,
			bulkIndex:     0,
		},
		{
			name:          "medium, two bulks adds",
			repeatAllowed: true, // this pattern could in theory trigger repeat key
			capacity:      650,
			start:         list(0, 300, 1),
			del:           list(0, 299, 1),
			add:           nil,
			addBulk:       list(1000, 1300, 1),
			addBulk2:      list(0, 300, 1),
			bulkIndex:     256 + 8,
		},
		{
			name:          "medium, no del",
			repeatAllowed: false,
			capacity:      650,
			start:         list(0, 500, 1),
			del:           nil,
			add:           list(500, 650, 1),
			addBulk:       list(10, 400, 1),
			addBulk2:      nil,
			bulkIndex:     400,
		},
		{
			name:          "medium, no add overlaps del",
			repeatAllowed: false,
			capacity:      650,
			start:         list(0, 500, 1),
			del:           list(10, 400, 1), // no add overlaps with what we delete
			add:           list(500, 650, 1),
			addBulk:       list(500, 800, 1),
			addBulk2:      nil,
			bulkIndex:     400,
		},
	}

	for _, tt := range tests {
		tt := tt
		for _, startCap := range []int{tt.capacity, 10, 20, 40, 52, 53, 54, 100, 1000} {
			t.Run(fmt.Sprintf("%s, start cap %d", tt.name, startCap), func(t *testing.T) {
				t.Parallel()
				for rep := 0; rep < *repFlag; rep++ {
					// Create the Map under test.
					m := New(startCap)
					m.seed = uintptr(rep)
					// TODO:
					// m.hashFunc = identityHash
					// m.hashFunc = zeroHash
					// // TODO: TEMP. get into subtest name
					switch rep {
					case 0:
						m.hashFunc = identityHash
					case 1:
						// do this second (worse perf, even further from reality than identityHash)
						m.hashFunc = zeroHash
					default:
						m.hashFunc = hashUint64 // real hash
					}

					for _, key := range tt.start {
						m.Set(key, Value(key))
					}

					// Create some sets to dynamically track validity of keys that appear in a range
					allowed := newKeySet(tt.start) // tracks start + added - deleted; these keys allowed but not required
					mustSee := newKeySet(tt.start) // tracks start - deleted; these are keys we are required to see at some point
					seen := newKeySet(nil)         // use to verify no dups, and at end, used to verify mustSee
					// Also dynamically track if key X is added, deleted, and then re-added during iteration,
					// which means it is legal per Go spec to be seen again in the iteration.
					// Example with stdlib map repeating keys during iter: https://go.dev/play/p/RN-v8rmQmeE
					deleted := newKeySet(nil)
					addedAfterDeleted := newKeySet(nil)

					// during loop, verify no duplicate keys and we only see allowed keys.
					// after loop, verify that we saw everything that we were required to see.
					i := 0
					m.Range(func(key Key, value Value) bool {
						if seen.contains(key) {
							if !tt.repeatAllowed {
								t.Fatalf("Map.Range() key %v seen twice, unexpected for this test", key)
							}
							// Even though this pattern is generally allowed to have repeats,
							// verify this specific key has been added, then deleted, then added,
							// which means it is legal to see it later in the iteration after
							// being re-added.
							if !addedAfterDeleted.contains(key) {
								t.Fatalf("Map.Range() key %v seen twice and was not re-added after being deleted", key)
							}
						}
						seen.add(key)

						if !allowed.contains(key) {
							t.Fatalf("Map.Range() key %v seen but not allowed (e.g., might have been deleted, or never added)", key)
						}

						// Delete one key, if requested
						if i < len(tt.del) {
							k := tt.del[i]
							m.Delete(k)
							allowed.remove(k)
							mustSee.remove(k) // We are no longer required to see this... It's ok if we saw it earlier
							deleted.add(k)
							if addedAfterDeleted.contains(k) {
								addedAfterDeleted.remove(k)
							}
						}

						set := func(k Key, v Value) {
							m.Set(k, v) // TODO: not checking values. maybe different test?
							allowed.add(k)
							if deleted.contains(k) {
								addedAfterDeleted.add(k)
								deleted.remove(k)
							}
						}
						// Add one key, if requested
						if i < len(tt.add) {
							set(tt.add[i], Value(i+1e6))
						}
						// Bulk add keys, if requested
						if i == tt.bulkIndex {
							for _, k := range tt.addBulk {
								set(k, Value(i+1e9))
							}
							for _, k := range tt.addBulk2 {
								set(k, Value(i+1e12))
							}
						}
						i++
						return true
					})

					for _, key := range mustSee.elems() {
						if !seen.contains(key) {
							dumpFixedTables(m)
							t.Fatalf("Map.Range() expected key %v not seen. table size: %d grows: %d",
								key, m.elemCount, m.resizeGenerations)
						}
					}

					if !tt.repeatAllowed && addedAfterDeleted.len() > 0 {
						// TODO: is this still working? Verfied once
						// repeatAllowed could be inferred in theory,
						// but keep it as extra sanity check and to be more explicit on expectations
						t.Fatal("repeatAllowed incorrectly set to false")
					}
				}
			})
		}
	}
}

// TestMap_IterGrowAndDelete is modeled after TestIterGrowAndDelete
// from runtime/map_test.go.
func TestMap_IterGrowAndDelete(t *testing.T) {
	m := New(16) // will resize
	for i := 0; i < 100; i++ {
		m.Set(Key(i), Value(i))
	}
	growflag := true
	m.Range(func(key Key, value Value) bool {
		if growflag {
			// grow the table
			for i := 100; i < 1000; i++ {
				m.Set(Key(i), Value(i))
			}
			// delete all odd keys
			for i := 1; i < 1000; i += 2 {
				m.Delete(Key(i))
			}
			growflag = false
		} else {
			if key&1 == 1 {
				t.Errorf("odd value returned %d", key)
			}
		}
		return true
	})
}

func TestMap_StoredKeys(t *testing.T) {
	// TODO: probably make helper?
	list := func(start, end Key) []Key {
		var res []Key
		for i := start; i < end; i++ {
			res = append(res, i)
		}
		return res
	}

	storedKeys := func(m *Map) []Key {
		// reach into the implementation to return
		// keys in stored order
		if m.old != nil {
			panic("unexpectedly growing")
		}
		var keys []Key
		for i := range m.current.control {
			if isStored(m.current.control[i]) {
				keys = append(keys, m.current.slots[i].Key)
			}
		}
		return keys
	}

	tests := []struct {
		name     string
		capacity int
		start    []Key
		del      []Key
		add      []Key
		want     []Key
	}{
		{
			name:     "delete key, add different key, 1 group",
			capacity: 8, // ends up with 16 slots
			start:    []Key{0, 1, 2, 3},
			del:      []Key{2},
			add:      []Key{42}, // the slot that had 2 is replaced with 42
			want:     []Key{0, 1, 42, 3},
		},
		{
			name:     "delete key 1st group, add different key, 2 groups",
			capacity: 16,          // ends up with 32 slots
			start:    list(0, 20), // [0, 20)
			del:      []Key{2},
			add:      []Key{42}, // the DELETED slot that had 2 is replaced with 42
			want:     append([]Key{0, 1, 42}, list(3, 20)...),
		},
		{
			name:     "delete key 1st group, set key present in 2nd group",
			capacity: 16,          // ends up with 32 slots
			start:    list(0, 20), // [0, 20)
			del:      []Key{2},
			add:      []Key{19}, // should end up with single 19, still in the second group
			want:     append([]Key{0, 1}, list(3, 20)...),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create the Map under test.
			m := New(tt.capacity)

			// Reach into the implementation to force a terrible hash func,
			// which lets us more predictably place elems.
			hashToZero := func(k Key, seed uintptr) uint64 {
				// could do something like: return uint64(k << m.current.h2Shift)
				return 0
			}
			m.hashFunc = hashToZero

			// Apply our operations
			for _, key := range tt.start {
				m.Set(key, Value(key))
			}
			for _, key := range tt.del {
				m.Delete(key)
			}
			for _, key := range tt.add {
				m.Set(key, Value(key))
			}

			got := storedKeys(m)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Logf("got: %v", got)
				t.Errorf("stored keys mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMap_ForceFill(t *testing.T) {
	tests := []struct {
		elem KV
	}{
		{KV{Key: 1, Value: 2}},
		{KV{Key: 8, Value: 8}},
		{KV{Key: 1e6, Value: 1e10}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("get key %d", tt.elem.Key), func(t *testing.T) {
			size := 10_000
			m := New(size)
			m.disableResizing = true

			// TODO: this is true for sparsehash, but not our swisstable,
			// which sizes the underlying table slices to roundPow2(1/0.8) times the requested capacity.
			// TODO: also, might no longer be true for sparestable, either.

			// TODO: reach in to disable growth?
			// We reach into the implementation to see what full means.
			underlyingTableLen := len(m.current.slots)
			t.Logf("setting %d elements in table with underlying size %d", underlyingTableLen-1, underlyingTableLen)

			// Force the underlying table to fill up the map so that it only has one empty slot left,
			// without any resizing. This helps verify our triangular numbers are correct and
			// we cycle properly.  We also do this in a loop (so we set the same values repeatedly)
			// in order to slightly stress things a bit more.
			for i := 0; i < 100; i++ {
				for j := 1000; j < 1000+underlyingTableLen-1; j++ {
					m.Set(Key(j), Value(j))
				}
			}

			// Confirm it is nearly 100% full, with only room for one more
			gotLen := m.Len()
			if gotLen != underlyingTableLen-1 {
				t.Errorf("Map.Len gotLen = %v, want %v", gotLen, underlyingTableLen-1)
			}

			missingKey := Key(1e12)
			gotV, gotOk := m.Get(missingKey)
			if gotOk {
				t.Errorf("Map.Get(missingKey) gotOk = %v, want false", gotOk)
			}
			if gotV != 0 {
				t.Errorf("Map.Get(missingKey) gotV = %v, want %v", gotV, 0)
			}

			// Set one more value, which should make our table 100% full,
			// and confirm we can get it back.
			m.Set(tt.elem.Key, tt.elem.Value)
			gotV, gotOk = m.Get(tt.elem.Key)
			if !gotOk {
				t.Errorf("Map.Get(%d) gotOk = %v, want true", tt.elem.Key, gotOk)
			}
			if gotV != tt.elem.Value {
				t.Errorf("Map.Get(%d) gotV = %v, want %v", tt.elem.Key, gotV, tt.elem.Value)
			}

			// Confirm it is 100% full according to the public API.
			gotLen = m.Len()
			if gotLen != underlyingTableLen {
				t.Errorf("Map.Len gotLen = %v, want %v", gotLen, underlyingTableLen)
			}
			// Reach in to the impl and to confirm that it is indeed seem to be 100% full
			for i := 0; i < len(m.current.control); i++ {
				if m.current.control[i] == emptySentinel {
					t.Fatalf("control byte %d is empty", i)
				}
			}
			for i := 0; i < len(m.current.slots); i++ {
				if m.current.slots[i].Key == 0 || m.current.slots[i].Value == 0 {
					// We set everything to non-zero values above.
					t.Fatalf("element at index %d has key or value that is still 0: key = %d value = %d",
						i, m.current.slots[i].Key, m.current.slots[i].Value)
				}
			}
		})
	}
}

func Test_StatusByte(t *testing.T) {
	// probably/hopefully overkill
	b := byte(0)
	if isEvacuated(b) || isChainEvacuated(b) || curHasDisplaced(b) {
		t.Errorf("statusByte unexpectedly set")
	}

	got := setEvacuated(b)
	if !isEvacuated(got) {
		t.Errorf("isEvacuated() = false, got = %v", got)
	}
	if isChainEvacuated(got) {
		t.Errorf("isChainEvacuated() = true")
	}
	if curHasDisplaced(got) {
		t.Errorf("curHasDisplaced() = true")
	}

	got = setChainEvacuated(b)
	if isEvacuated(got) {
		t.Errorf("isEvacuated() = true")
	}
	if !isChainEvacuated(got) {
		t.Errorf("isChainEvacuated() = false, got = %v", got)
	}
	if curHasDisplaced(got) {
		t.Errorf("curHasDisplaced() = true")
	}

	got = setCurHasDisplaced(b)
	if isEvacuated(got) {
		t.Errorf("isEvacuated() = true")
	}
	if isChainEvacuated(got) {
		t.Errorf("isChainEvacuated() = true")
	}
	if !curHasDisplaced(got) {
		t.Errorf("curHasDisplaced() = false, got = %v", got)
	}
}

func BenchmarkMatchByte(b *testing.B) {
	buffer := make([]byte, 16)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = MatchByte(42, buffer)
	}
}

func BenchmarkFillGrow_Swiss(b *testing.B) {
	bms := almostGrowPointMapSizes([]int{
		1 << 10,
		1 << 20,
		1 << 23,
	})
	if !*longTestFlag {
		bms = []benchmark{
			{"map size 1000000", 1_000_000},
		}
	}
	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				m := New(10)
				for j := Key(0); j < Key(bm.mapElements); j++ {
					m.Set(j, Value(j))
				}
			}
		})
	}
}

func BenchmarkFillGrow_Std(b *testing.B) {
	bms := almostGrowPointMapSizes([]int{
		1 << 10,
		1 << 20,
		1 << 23,
	})
	if !*longTestFlag {
		bms = []benchmark{
			{"map size 1000000", 1_000_000},
		}
	}
	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				m := make(map[int64]int64, 10)
				for j := int64(0); j < int64(bm.mapElements); j++ {
					m[j] = j
				}
			}
		})
	}
}

func BenchmarkFillPresize_Swiss(b *testing.B) {
	bms := almostGrowPointMapSizes([]int{
		1 << 10,
		1 << 20,
		1 << 23,
	})
	if !*longTestFlag {
		bms = []benchmark{
			{"map size 1000000", 1_000_000},
		}
	}
	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				m := New(bm.mapElements)
				for j := Key(0); j < Key(bm.mapElements); j++ {
					m.Set(j, Value(j))
				}
			}
		})
	}
}

func BenchmarkFillPresize_Std(b *testing.B) {
	bms := almostGrowPointMapSizes([]int{
		1 << 10,
		1 << 20,
		1 << 23,
	})
	if !*longTestFlag {
		bms = []benchmark{
			{"map size 1000000", 1_000_000},
		}
	}
	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				m := make(map[int64]int64, bm.mapElements)
				for j := int64(0); j < int64(bm.mapElements); j++ {
					m[j] = j
				}
			}
		})
	}
}

// TODO: probably change over to sinkKey, sinkValue, and use map[Key]Value as the runtime maps
var sinkUint uint64
var sinkInt int64
var sinkValue Value
var sinkBool bool

func BenchmarkGetHitHot_Swiss(b *testing.B) {
	hotKeyCount := 20
	lookupEachKey := 50

	bms := almostGrowPointMapSizes([]int{
		1 << 10,
		1 << 20,
		1 << 23,
	})
	if !*longTestFlag {
		bms = []benchmark{
			{"map size 1000000", 1_000_000},
		}
	}

	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			// Fill the map under test
			m := New(bm.mapElements)
			for i := Key(0); i < Key(bm.mapElements); i++ {
				m.Set(i, Value(i))
			}

			// Generate random hot keys repeated N times then shuffled
			var hotKeys []Key
			for i := 0; i < hotKeyCount; i++ {
				hotKeys = append(hotKeys, Key(rand.Intn(bm.mapElements)))
			}
			var gets []Key
			for i := 0; i < hotKeyCount; i++ {
				k := hotKeys[i]
				for j := 0; j < lookupEachKey; j++ {
					gets = append(gets, k)
				}
			}
			rand.Shuffle(len(gets), func(i, j int) {
				gets[i], gets[j] = gets[j], gets[i]
			})

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for _, key := range gets {
					v, b := m.Get(key)
					sinkInt = int64(v)
					sinkBool = b
				}
			}
		})
	}
}

func BenchmarkGetHitHot_Std(b *testing.B) {
	hotKeyCount := 20
	lookupEachKey := 50

	bms := almostGrowPointMapSizes([]int{
		1 << 10,
		1 << 20,
		1 << 23,
	})
	if !*longTestFlag {
		bms = []benchmark{
			{"map size 1000000", 1_000_000},
		}
	}

	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			// Fill the map under test
			m := make(map[int64]int64, bm.mapElements)
			for i := 0; i < bm.mapElements; i++ {
				m[int64(i)] = int64(i)
			}

			// Generate random hot keys repeated N times then shuffled
			var hotKeys []int64
			for i := 0; i < hotKeyCount; i++ {
				hotKeys = append(hotKeys, int64(rand.Intn(bm.mapElements)))
			}
			var gets []int64
			for i := 0; i < hotKeyCount; i++ {
				k := hotKeys[i]
				for j := 0; j < lookupEachKey; j++ {
					gets = append(gets, k)
				}
			}
			rand.Shuffle(len(gets), func(i, j int) {
				gets[i], gets[j] = gets[j], gets[i]
			})

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for _, key := range gets {
					sinkInt, sinkBool = m[key]
				}
			}
		})
	}
}

func BenchmarkGetMissHot_Swiss(b *testing.B) {
	hotKeyCount := 20
	lookupEachKey := 50

	bms := almostGrowPointMapSizes([]int{
		1 << 10,
		1 << 20,
		1 << 23,
	})
	if !*longTestFlag {
		bms = []benchmark{
			{"map size 1000000", 1_000_000},
		}
	}

	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			// Fill the map under test
			m := New(bm.mapElements)
			for i := Key(0); i < Key(bm.mapElements); i++ {
				m.Set(i, Value(i))
			}

			// Generate keys that don't exist, repeated N times then shuffled
			var missKeys []Key
			for i := 0; i < hotKeyCount; i++ {
				missKeys = append(missKeys, Key(i+(1<<40)))
			}
			var gets []Key
			for i := 0; i < hotKeyCount; i++ {
				k := missKeys[i]
				for j := 0; j < lookupEachKey; j++ {
					gets = append(gets, k)
				}
			}
			rand.Shuffle(len(gets), func(i, j int) {
				gets[i], gets[j] = gets[j], gets[i]
			})

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for _, key := range gets {
					v, b := m.Get(key)
					sinkInt = int64(v)
					sinkBool = b
				}
			}
		})
	}
}

func BenchmarkGetMissHot_Std(b *testing.B) {
	hotKeyCount := 20
	lookupEachKey := 50

	bms := almostGrowPointMapSizes([]int{
		1 << 10,
		1 << 20,
		1 << 23,
	})
	if !*longTestFlag {
		bms = []benchmark{
			{"map size 1000000", 1_000_000},
		}
	}

	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			// Fill the map under test
			m := make(map[int64]int64, bm.mapElements)
			for i := 0; i < bm.mapElements; i++ {
				m[int64(i)] = int64(i)
			}

			// Generate keys that don't exist, repeated N times then shuffled
			var missKeys []int64
			for i := 0; i < hotKeyCount; i++ {
				missKeys = append(missKeys, int64(i+(1<<40)))
			}
			var gets []int64
			for i := 0; i < hotKeyCount; i++ {
				k := missKeys[i]
				for j := 0; j < lookupEachKey; j++ {
					gets = append(gets, k)
				}
			}
			rand.Shuffle(len(gets), func(i, j int) {
				gets[i], gets[j] = gets[j], gets[i]
			})

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for _, key := range gets {
					sinkInt, sinkBool = m[key]
				}
			}
		})
	}
}

// BenchmarkGetAllStartCold_Std creates many maps so that they are
// cold at the start. It is intended to be run with -benchtime=1x.
func BenchmarkGetAllStartCold_Std(b *testing.B) {
	bms := almostGrowPointMapSizes([]int{
		1 << 10,
		1 << 20,
		1 << 23,
	})
	if !*longTestFlag {
		bms = []benchmark{
			{"map size 1000000", 1_000_000},
		}
	}

	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			minMem := *coldMemTestFlag * (1 << 20)

			// we don't use overhead to keep the count of maps consistent
			// across different implementations
			mapMem := float64(bm.mapElements) * 16
			mapCnt := int(math.Ceil(minMem / mapMem))

			keys := make([]int64, bm.mapElements)
			for i := int64(0); i < int64(len(keys)); i++ {
				keys[i] = i
			}

			b.Logf("creating %d maps with %.1f MB of data. %d total keys", mapCnt, float64(mapCnt)*mapMem/(1<<20), mapCnt*bm.mapElements)
			maps := make([]map[int64]int64, mapCnt)
			for i := 0; i < mapCnt; i++ {
				m := make(map[int64]int64, bm.mapElements)
				for j := int64(0); j < int64(bm.mapElements); j++ {
					m[j] = j
				}
				maps[i] = m
			}

			// Shuffle the keys after we have placed them in the maps.
			// Otherwise, we could favor early entrants in a given bucket when reading below.
			rand.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
			})

			getKeys := func(m map[int64]int64, ratio float64) {
				count := int(ratio * float64(bm.mapElements))
				for _, k := range keys {
					if count == 0 {
						break
					}
					count--
					sinkInt, sinkBool = m[k]
				}
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// We keep the same order of maps and keys in their respective slices
				// so that any given map or key is more likely to be cold by the time we
				// cycle back around if b.N is > 1. In practice, b.N seems to usually be 1.
				for _, m := range maps {
					getKeys(m, 1.0)
				}
			}
		})
	}
}

// BenchmarkGetAllStartCold_Swiss creates many maps so that they are
// cold at the start. It is intended to be run with -benchtime=1x.
func BenchmarkGetAllStartCold_Swiss(b *testing.B) {
	bms := almostGrowPointMapSizes([]int{
		1 << 10,
		1 << 20,
		1 << 23,
	})
	if !*longTestFlag {
		bms = []benchmark{
			{"map size 1000000", 1_000_000},
		}
	}

	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			minMem := *coldMemTestFlag * (1 << 20)

			// we don't use overhead to keep the count of maps consistent
			// across different implementations
			mapMem := float64(bm.mapElements) * 16
			mapCnt := int(math.Ceil(minMem / mapMem))

			keys := make([]Key, bm.mapElements)
			for i := 0; i < len(keys); i++ {
				keys[i] = Key(i)
			}

			b.Logf("creating %d maps with %.1f MB of data. %d total keys", mapCnt, float64(mapCnt)*mapMem/(1<<20), mapCnt*bm.mapElements)
			maps := make([]*Map, mapCnt)
			for i := 0; i < mapCnt; i++ {
				m := New(bm.mapElements)
				for j := 0; j < bm.mapElements; j++ {
					m.Set(Key(j), Value(j))
				}
				maps[i] = m
			}

			// Shuffle the keys after we have placed them in the maps.
			// Otherwise, we could favor early entrants in a given bucket when reading below.
			rand.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
			})

			getKeys := func(m *Map, ratio float64) {
				count := int(ratio * float64(bm.mapElements))
				for _, k := range keys {
					if count == 0 {
						break
					}
					count--
					v, b := m.Get(Key(k))
					sinkValue = v
					sinkBool = b
				}
			}

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				// We keep the same order of maps and keys in their respective slices
				// so that any given map or key is more likely to be cold by the time we
				// cycle back around if b.N is > 1. In practice, b.N seems to usually be 1.
				for _, m := range maps {
					getKeys(m, 1.0)
				}
			}
		})
	}
}

//go:noinline
func iterStd(m map[int64]int64) int64 {
	var ret int64
	for _, a := range m {
		ret += a
	}
	return ret
}

func BenchmarkRange_Std(b *testing.B) {
	// From https://github.com/golang/go/issues/51410, but with int64 rather than strings.
	// That should mean the hashing impact is less here.
	minSize := 51 // was 50
	maxSize := 58 // was 60
	for size := minSize; size < maxSize; size++ {
		b.Run(fmt.Sprintf("map_size_%d", size), func(b *testing.B) {
			m := make(map[int64]int64)
			for i := 0; i < size; i++ {
				m[int64(i)] = int64(i)
			}
			var x int64
			for i := 0; i < b.N; i++ {
				x += iterStd(m)
			}
		})
	}
}

//go:noinline
func iterSwiss(m *Map) int64 {
	var ret int64
	m.Range(func(key Key, value Value) bool {
		ret += int64(value)
		return true
	})
	return ret
}

func BenchmarkRange_Swiss(b *testing.B) {
	// From https://github.com/golang/go/issues/51410, but with int64 rather than strings.
	// That should mean the hashing impact is less here.
	minSize := 51 // was 50
	maxSize := 58 // was 60
	for size := minSize; size < maxSize; size++ {
		b.Run(fmt.Sprintf("map_size_%d", size), func(b *testing.B) {
			m := New(10)
			for i := 0; i < size; i++ {
				m.Set(Key(i), Value(i))
			}
			var x int64
			for i := 0; i < b.N; i++ {
				x += iterSwiss(m)
			}
		})
	}
}

type benchmark struct {
	name        string
	mapElements int
}

// coarseMapSizes returns a []benchmark with large steps from 1K to 100M elements
func coarseMapSizes() []benchmark {
	const (
		mapSizeCoarseLow    = 1_000
		mapSizeCoarseHigh   = 100_000_000 // 16 bytes * 1e8 = 1.6 GB of data, plus overhead
		mapSizeCoarseFactor = 1.75
	)

	var bms []benchmark
	mapSize := mapSizeCoarseLow
	for {
		mapSize = min(mapSize, mapSizeCoarseHigh)
		bms = append(bms, benchmark{fmt.Sprintf("map size %d", mapSize), mapSize})
		if mapSize == mapSizeCoarseHigh {
			break
		}
		mapSize = int(float64(mapSize) * mapSizeCoarseFactor)
	}
	return bms
}

func almostGrowPointMapSizes(pow2s []int) []benchmark {
	var bms []benchmark
	for _, size := range pow2s {
		if size&(size-1) != 0 || size == 0 {
			panic(fmt.Sprintf("bad test setup, size %d is not power of 2", size))
		}
		growPoint := (size * 13 / 2 / 8) + 1 // TODO: centralize

		s1 := int(0.8 * float64(growPoint))
		s2 := int(1.2 * float64(growPoint))

		bms = append(bms, benchmark{name: fmt.Sprintf("map size %d", s1), mapElements: s1})
		bms = append(bms, benchmark{name: fmt.Sprintf("map size %d", s2), mapElements: s2})
	}
	return bms
}

// fineMapSizes returns a []benchmark with smaller steps from 400K elements to 2.4M elements
func fineMapSizes() []benchmark {
	const (
		mapSizeFineLow  = 400_000
		mapSizeFineHigh = 2_400_000
		mapSizeFineStep = 50_000
	)

	var bms []benchmark
	mapSize := mapSizeFineLow
	for {
		mapSize = min(mapSize, mapSizeFineHigh)
		bms = append(bms, benchmark{fmt.Sprintf("map size %d", mapSize), mapSize})
		if mapSize == mapSizeFineHigh {
			break
		}
		mapSize += mapSizeFineStep
	}
	return bms
}

func sweepMapSizes() []benchmark {
	const (
		mapSizeSweepLow    = 800_000
		mapSizeSweepHigh   = 4_400_000
		mapSizeSweepFactor = 1.01
	)

	var bms []benchmark
	mapSize := mapSizeSweepLow
	for {
		mapSize = min(mapSize, mapSizeSweepHigh)
		bms = append(bms, benchmark{fmt.Sprintf("map size %d", mapSize), mapSize})
		if mapSize == mapSizeSweepHigh {
			break
		}
		// fmt.Println("mapSize", mapSize)
		// fmt.Println("nextWorstCase", nextWorstCase)
		nextMapSize := int(float64(mapSize) * mapSizeSweepFactor)

		// insert the expected worse case for memory usage at the size just before a resize would be triggered.
		// also include the two points immediately around that, where the point just before is the
		// expected best case for best case for memory usage.
		nextWorstCase := (roundPow2(mapSize) * 13 / 2 / 8) + 1
		if mapSize < nextWorstCase && nextWorstCase < nextMapSize {
			bms = append(bms, benchmark{fmt.Sprintf("map size %d", nextWorstCase-1), nextWorstCase - 1})
			bms = append(bms, benchmark{fmt.Sprintf("map size %d", nextWorstCase), nextWorstCase})
			bms = append(bms, benchmark{fmt.Sprintf("map size %d", nextWorstCase+1), nextWorstCase + 1})
		}
		mapSize = nextMapSize
	}
	return bms
}

func roundPow2(n int) int {
	if n == 0 {
		return 0
	}
	return 1 << (64 - bits.LeadingZeros64(uint64(n-1)))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// zeroHash is a terrible hash function that is reproducible
// and can help trigger corner cases.
func zeroHash(k Key, seed uintptr) uint64 {
	// could do something like: return uint64(k << m.current.h2Shift)
	return 0
}

// identityHash is another terrible hash function, but not as bad
// as zeroHash.
func identityHash(k Key, seed uintptr) uint64 {
	return uint64(k)
}

func dumpFixedTables(m *Map) {
	tables := []struct {
		name string
		t    *fixedTable
	}{
		{"current", &m.current},
		{"old", m.old},
	}
	for _, t := range tables {
		fmt.Println("\n===", t.name, "===")
		if t.t == nil {
			fmt.Println("table is nil")
			return
		}
		for i := range t.t.slots {
			if i%16 == 0 {
				fmt.Println()
				fmt.Println(t.name, "group", i/16)
				fmt.Println("-----")
			}
			fmt.Printf("%08b %v\n", t.t.control[i], t.t.slots[i])
		}
	}
}

// list returns a slice of of keys based on start (inclusive), end (exclusive), and stride
func list(start, end, stride Key) []Key {
	var res []Key
	for i := start; i < end; i += stride {
		res = append(res, i)
	}
	return res
}

// keysAndValues collects keys and values from a Map into a runtime map
// for use in testing and fuzzing.
// It panics if the same key is observed twice while iterating over the keys.
func keysAndValues(m *Map) map[Key]Value {
	res := make(map[Key]Value)
	m.Range(func(key Key, value Value) bool {
		// validate we don't see the same key twice
		_, ok := res[key]
		if ok {
			panic(fmt.Sprintf("Map.Range() key %v seen before", key))
		}
		res[key] = value
		return true
	})
	return res
}

// keySet is a simple set to aid with valiation
type keySet struct {
	m map[Key]struct{}
}

func newKeySet(elems []Key) *keySet {
	s := &keySet{}
	s.m = make(map[Key]struct{})
	for _, k := range elems {
		s.add(k)
	}
	return s
}

func (s *keySet) add(k Key) {
	s.m[k] = struct{}{}
}

func (s *keySet) remove(k Key) {
	delete(s.m, k)
}

func (s *keySet) contains(k Key) bool {
	_, ok := s.m[k]
	return ok
}

func (s *keySet) len() int {
	return len(s.m)
}

func (s *keySet) elems() []Key {
	var keys []Key
	for key := range s.m {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func Test_fixedTable_reconstructHash(t *testing.T) {
	tests := []struct {
		capacity int // must be power of 2
	}{
		{16}, {1 << 6}, {1 << 17},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("capacity %d", tt.capacity), func(t *testing.T) {
			table := newFixedTable(tt.capacity)
			for i := 0; i < 100; i++ {
				hash := hashUint64(Key(0), uintptr(i))
				group := hash & table.groupMask
				h2 := table.h2(hash)

				usefulPortionMask := (1 << (table.h2Shift + 7)) - 1
				usefulHash := hash & uint64(usefulPortionMask)

				if got := table.reconstructHash(h2, group); got != usefulHash {
					t.Fatalf("fixedTable.reconstructHash() = 0x%X, want 0x%X", got, usefulHash)
				}
			}
		})
	}
}
