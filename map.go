package swisstable

import (
	"fmt"
	"math/bits"
	"reflect"
	"unsafe"
)

/*
-----------------
// old propsed API (mostly based off of HTTP header naming):

MVP

// probably use functional opts. Capacity is a hint.
New(opts)

// Set sets the value associated with key.
func (m *Map) Set(key K, value V)

// Get gets the value associated with the given key.
func (m *Map) Get(key K) (value V, ok bool)

Len()

probably:

// Delete deletes the value associated with key.
func (m *Map) Delete(key K)

MVP needs some type of abilty to iterate: Keys() or Range() or ...

consider:

Cap() ?
helpful to know if need to call Clip or Grow


consider (from exp/slices):

Clip() // pefectly resizes
Grow() // capacity hint, approximate, don't panic on wrong size though exp/slices does panic

consider some type of range:

// probably model after sync.map?
// Range is sync.map. typeutil I think uses Iterate
// needs to track if a Range is live during a Set:
//   if new slot taken, and no resize -- fine
//   if new slot taken, and resize -- need to create aux storage for new elements, and copy in at end of Range
// Delete is fine, hopefully -- next iteration might find that something is deleted
//   though supporting this dynamically might mean more complexity? or might fall out naturally?
// see https://pkg.go.dev/sync#Map.Range
func (m *Map) Range(f func(key K, value V) bool)

consider (from exp/maps):
// Keys returns the keys of the map m.
// The keys will be in an indeterminate order.
func Keys[M ~map[K]V, K comparable, V any](m M) []K {

// Values returns the values of the map m.
// The values will be in an indeterminate order.
func Values[M ~map[K]V, K comparable, V any](m M) []V {

// Clear removes all entries from m, leaving it empty.
func Clear[M ~map[K]V, K comparable, V any](m M) {

====================================
====================================
====================================
// misc older / stale notes below


// could consider:
// SetMultiple sets the value associated with key.
// but not needed if have a Resize or Cap because can do:
//   m.Resize(len(m) + 342)
//   then add 342 entries
//   not quite as effecient, because variation in where they go.
func (Header) SetMultiple
  calc hashes

// Len returns the count of items
Len

Keys

Iterate

func (m *Map) Iterate(f func(key types.Type, value interface{}))
Iterate calls function f on each entry in the map in unspecified order.

If f should mutate the map, Iterate provides the same guarantees as Go maps: if f deletes a map entry that Iterate has not yet reached, f will not be invoked for it, but if f inserts a map entry that Iterate has not yet reached, whether or not f will be invoked for it is unspecified.


Resize(capacity int)
// Capacity gives a hint.
// Internal:
//    if < current size, probably use exact size, no spare.
//    if > current size, give some spare, e.g. +1

Options:
 CapacityHint int

*/

// TODO: placeholder Key Value prior to generics.
type Key int64
type Value int64
type KV struct {
	Key   Key
	Value Value
}

type hashFunc func(k Key) uint64

type Map struct {
	control   []byte
	table     []KV
	groupMask uint64
	hashFunc  hashFunc
	elemCount int
	// resizeTable *sparsetable.Table

	// TODO:
	// stats
	gets                     int64
	getTopHashFalsePositives int64
	getExtraGroups           int64
}

// capacity is a hint, and "at least"
func New(capacity int) *Map {

	// tableLength will be roughly 2x the user suggested
	// capacity, rounded up to a power of 2.
	tableLength := calcTableLength(capacity)

	if debug {
		fmt.Println("new: underlying table length", tableLength)
	}
	// TODO: not using capacity in our make calls. Probably right for straight swisstable impl?
	control := make([]byte, tableLength)
	table := make([]KV, tableLength)
	return &Map{
		control: control,
		table:   table,
		// 16 control bytes per group
		groupMask: (uint64(tableLength) / 16) - 1,
		hashFunc:  hashUint64,
	}
}

func (m *Map) Get(k Key) (v Value, ok bool) {
	m.gets++ // stats

	h := m.hashFunc(k)
	group := h & m.groupMask
	topHash := uint8((h >> 56) & 0xff)
	// TODO: empty/deleted/etc
	if topHash == 0 {
		topHash++
	}

	// firstBucket := bucket
	var probeCount uint64

	// Do quadratic probing.
	// This loop will terminate because (a) incrementing by
	// triangluar numbers will hit every slot in a power of 2 sized table
	// and (b) we always enforce at least some empty slots by resizing when needed.
	for {
		bitmask, ok := MatchByte(topHash, m.control[group*16:])
		if debug {
			if !ok {
				panic("short control byte slice")
			}
		}
		if bitmask != 0 {
			// We have at least one hit on topHash
			for {
				index := bits.TrailingZeros32(bitmask)
				if debug {
					fmt.Println("get: match on topHash:", index)
				}
				kv := m.table[int(group*16)+index]
				if kv.Key == k {
					return kv.Value, true
				}
				m.getTopHashFalsePositives++ // stats

				// continue to look. infrequent with 7 bit topHash.
				bitmask &= ^(1 << index)
				if bitmask == 0 {
					break
				}
			}
		}

		// No matching topHash, or we had a matching topHash
		// but failed to find an equal key in loop just above.
		// Check if this group is full or has at least one empty slot.
		// TODO: terminology: element, slot, bucket, group, ...
		// probably: element, group, slot (with is empty or non-empty position within table)
		emptyBitmask, ok := MatchByte(0, m.control[group*16:])
		if debug {
			if !ok {
				panic("short control byte slice")
			}
		}
		emptyIndex := bits.TrailingZeros32(emptyBitmask)
		if emptyIndex < 16 {
			// TODO: stop on empty. I think that is valid with no delete?
			return 0, false
		}

		// This group is full, so continue on to the next group.
		// We don't do quadratic probing within a group, but we do
		// quadratic probing across groups.
		// Continue our quadratic probing across groups, using triangular numbers.
		m.getExtraGroups++ // stats
		probeCount++
		group = (group + probeCount) & m.groupMask
		if debug {
			fmt.Println("get: CONTINUE to group:", group)
		}

		// but need to update group logic

		// TODO: remove sanity check
		// if bucket != (firstBucket+triangleNum(probeCount))&m.bucketMask {
		// 	panic("impossible")
		// }
		// if probeCount == uint64(m.table.Len()) {
		// 	panic("impossible probeCount")
		// }
	}
}

func (m *Map) Set(k Key, v Value) {
	h := m.hashFunc(k)
	group := h & m.groupMask
	topHash := uint8((h >> 56) & 0xff)
	// TODO: empty/deleted/etc
	if topHash == 0 {
		// TODO: reserve 0 for empty for now
		topHash++
	}

	// firstBucket := bucket
	var probeCount uint64
	// Do quadratic probing.
	// This loop will terminate for same reasons as Get loop.
	for {
		bitmask, ok := MatchByte(topHash, m.control[group*16:])
		if !ok {
			if debug {
				panic("short control byte slice")
			}
		}

		if bitmask != 0 {
			// We have at least one hit on topHash
			for {
				index := bits.TrailingZeros32(bitmask)
				if debug {
					fmt.Println("set: match on topHash: group:", group, "index:", index)
				}
				kv := m.table[int(group*16)+index]
				if kv.Key == k {
					// update existing key
					if debug {
						fmt.Println("set: updating existing key: group:", group, "index:", index)
					}
					m.control[int(group*16)+index] = topHash
					m.table[int(group*16)+index] = KV{Key: k, Value: v}
					return
				}
				if debug {
					fmt.Println("set: false collision on topHash: group:", group, "index:", index)
				}

				// continue to look. infrequent with 7 bit topHash.
				bitmask &= ^(1 << index)
				if bitmask == 0 {
					break
				}
			}
		}

		// No matching topHash, or we had a matching topHash
		// but failed to find an equal key in loop just above.
		// Either way, find next empty slot and add our new value.
		// TODO: for now, empty is 0
		emptyBitmask, ok := MatchByte(0, m.control[group*16:])
		if debug {
			if !ok {
				panic("short control byte slice")
			}
		}
		emptyIndex := bits.TrailingZeros32(emptyBitmask)
		if emptyIndex < 16 {
			if debug {
				fmt.Println("set: updating empty slot: group:", group, "index:", emptyIndex)
			}
			m.control[int(group*16)+emptyIndex] = topHash
			m.table[int(group*16)+emptyIndex] = KV{Key: k, Value: v}
			m.elemCount++
			return
		}
		// TODO: keep probing. Also, resize makes this less common.
		if debug {
			fmt.Println("set: FULL group:", group, "index:", emptyIndex)
		}

		// TODO: handle resize
		// We don't do quadratic probing within a group, but we do
		// quadratic probing across groups.
		// Continue our quadratic probing across groups, using triangular numbers.
		probeCount++
		group = (group + probeCount) & m.groupMask

		// TODO: remove sanity check
		// if bucket != (firstBucket+triangleNum(probeCount))&m.bucketMask {
		// 	panic("impossible")
		// }
		// TODO: remove
		if debug {
			if probeCount == uint64(len(m.table)/16) {
				panic(fmt.Sprintf("impossible: probeCount: %d groups: %d underlying table len: %d",
					probeCount, len(m.table)/16, len(m.table)))
			}
		}
	}
}

// Number of elements stored in Map
// Should track this explicitly.
// TODO: currently not O(1)
func (m *Map) Len() int {
	// TODO: remove sanity check
	// var count int
	// for i := 0; i < m.table.Len(); i++ {
	// 	if m.table.IsAssigned(i) {
	// 		count++
	// 	}
	// }
	// if count != m.elemCount {
	// 	panic(fmt.Errorf("elemCount %d does not match IsAssigned count %d", m.elemCount, count))
	// }
	return m.elemCount
}

func triangleNum(n uint64) uint64 {
	return n * (n + 1) / 2
}

// calcTableLength returns the length to use
// for the underlying sparsetable to support
// capacityHint stored map elements.
// We aim for the underlying sparsetable to start
// roughly 50% empty, and the sparsetable must be
// a power of 2 length, which allows simple bucket indexing
// and other benefits.
// The underlying sparsetable cannot change its own size,
// so if we grow past its size and it becomes too full,
// we resize via a new sparsetable with 2x the length.
func calcTableLength(capacityHint int) int {
	// TODO: old comments. review, delete.
	// Could do something like:
	// start with +4 in every group (with is enough for ~85% of them by end)
	// remember capacity
	//   once insertCnt > 0.95 * capacity, right size to +2 current size
	//   on average, each one will get ~1.6 more if ending up with 32 (0.05*32 = 1.6)

	// We want at most 80% load factor, rounded up to a power of 2.
	// Our current minimum sparsetable size is 64.
	// (If we have for example just 1 value, though, we don't pay for the space
	// for values for the other 63 -- the value storage is sparse).
	tableLength := int(float64(capacityHint) / 0.8)
	pow2 := 64
	// TODO: clip max
	for tableLength > pow2 {
		pow2 = pow2 << 1
	}
	tableLength = pow2

	// sanity check power of 2
	if tableLength&(tableLength-1) != 0 || tableLength == 0 {
		panic("impossible")
	}
	return tableLength
}

func hashUint64(k Key) uint64 {
	// TODO: need to randomize initial hash (currently always 0).
	// TODO: could consider something like:
	// h := uint64(memhash64(unsafe.Pointer(&k), 0))
	h := uint64(memhash(unsafe.Pointer(&k), 0, uintptr(8)))
	return h
}

func hashString(k string) uint64 {
	// could consider runtime.strhash
	hdr := (*reflect.StringHeader)(unsafe.Pointer(&k))
	h := uint64(memhash(unsafe.Pointer(hdr.Data), 0, uintptr(hdr.Len)))
	return h
}

//go:linkname memhash runtime.memhash
//go:noescape
func memhash(p unsafe.Pointer, seed, s uintptr) uintptr

const debug = false
