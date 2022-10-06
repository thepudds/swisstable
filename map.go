package swisstable

import (
	"fmt"
	"math/bits"
	"runtime"
	"unsafe"
)

// Basic terminology:
// 		map: overall data structure, internally organized into groups.
//		group: a set of 16 contiguous positions that can be examined in parallel.
// 		position: index within the overall linear table. Each position has a slot & control byte.
//		slot: stores one key/value.
//		control byte: metadata about a particular slot, including whether empty, deleted, or has a stored value.
//		offset: index within a group.
//		H1: hash(key) % group count. Corresponds to the natural (non-displaced) group for a given key.
//		H2: 7 additional bits from hash(key). Stored in control byte.
// 		count: number of live key/values. Returned via Len.
// 		table size: len(slots).
//
// Individual positions can be EMPTY, DELETED, or STORED (containing a key/value).
//
// In addition, internally there is a fixedTable type that is a non-resizable Swisstable.
// Map manages a current fixedTable, and when doing incremental growth, an old fixedTable.
// During write operations to Map (Set/Delete), the old fixedTable is gradually
// evacuated to the current fixedTable.
//
// Incremental growth without invalidating iterators presents some challenges, including
// because a Swisstable can mark control bytes as EMPTY or DELETED to ensure probing chains
// across groups are correctly followed to find any displaced elements. This must be
// properly navigated when juggling an old and new table.
//
// The basic approach is to maintain an immutable old once growth starts, along with
// some growth status bytes that are live for the duration of the growth, with one
// byte per group. (This can be collapsed down to fewer bits, but we use a full byte for now).
// Even with the extra growth status bytes, this still uses less memory than the runtime map,
// which allocates extra overflow buckets that exceed the size of the growth status bytes
// even for small key/values.
//
// If an iterator starts mid-growth, it walks both the old and new table, taking care
// not to emit the same key twice. If growth completes, the iterator continues to walk
// the old and new tables it started with. In both cases, it checks the live tables if needed to
// get the live golden data. It attempts to avoid re-hashing in some cases by reconstructing
// the hash from the group and 7-bits of stored h2. See the Range method for details.
// (I think it re-hashes less than runtime map iterator. TODO: confirm).

// Key, Value, and KV define our key and value types.
// TODO: these are placeholder types for performance testing prior to using generics.
type Key int64
type Value int64
type KV struct {
	Key   Key
	Value Value
}

type hashFunc func(k Key, seed uintptr) uint64

// Control byte special values.
// If the high bit is 1, it is a special sentinel value of EMPTY or DELETED.
// If the high bit is 0, there is a STORED entry in the corresponding
// slot in the table, and the next 7 bits are the h2 values. (This is called 'FULL'
// in the original C++ swisstable implementation, but we call it STORED).
// TODO: consider flipping meaning of first bit, possibly with 0x00 for empty and 0x7F for deleted?
const emptySentinel = 0b1111_1111
const deletedSentinel = 0b1000_0000

// Map is a map, supporting Set, Get, Delete, Range and Len.
// It is implemented via a modified Swisstable.
// Unlike the original C++ Swisstable implementation,
// Map supports incremental resizing without invalidating iterators.
type Map struct {
	// Internally, a Map manages one or two fixedTables to store key/values. Normally,
	// it manages one fixedTable. While growing, it manages two fixedTables.

	// current is a fixedTable containing the element array and metadata for the active fixedTable.
	// Write operations (Set/Delete) on Map go to current.
	current fixedTable

	// old is only used during incremental growth.
	// When growth starts, we move current to old, and no longer write or delete key/values in old,
	// but instead gradually evacuate old to new on write operations (Set/Delete).
	// Get and Range handle finding the correct "golden" data in either current or old.
	old *fixedTable

	// growStatus tracks what has happened on a group by group basis.
	// To slightly simplify, currently each group gets a byte. TODO: could collapse that down to few bits.
	growStatus []byte

	sweepCursor uint64

	// elemCount tracks the live count of key/values, and is returned by Len.
	elemCount int

	// when resizeThreshold is passed, we need to resize
	// TODO: need to track DELETED count as well for resizing or compacting
	resizeThreshold int

	// currently for testing, we purposefully fill beyond the resizeThreshold.
	// TODO: remove
	disableResizing bool

	// Our hash function, which generates a 64-bit hash
	hashFunc hashFunc
	seed     uintptr

	// Flags tracking state.
	// TODO: collapse down to single flag variable
	// TODO: could use these flags to indicate OK to clear during evac
	// haveIter    bool
	// haveOldIter bool

	// Internal stats to help observe behavior.
	// TODO: eventually remove stats, not actively tracking some
	gets                int
	getH2FalsePositives int
	getExtraGroups      int
	resizeGenerations   int
}

// New returns a *Map that is ready to use.
// capacity is a hint, and "at least".
func New(capacity int) *Map {
	// tableSize will be roughly 1/0.8 x user suggested capacity,
	// rounded up to a power of 2.
	// TODO: for now, should probably make capcity be at least 16 (group size)
	// to temporarily simplify handling small maps (where small here is < 16).
	tableSize := calcTableSize(capacity)

	current := *newFixedTable(tableSize)

	// TODO: for now, use same fill factor as the runtime map to
	// make it easier to compare performance across different sizes.
	resizeThreshold := (tableSize * 13) / 16 // TODO: centralize
	return &Map{
		current:         current,
		hashFunc:        hashUint64,
		seed:            uintptr(fastrand())<<32 | uintptr(fastrand()),
		resizeThreshold: resizeThreshold,
	}
}

// fixedTable does not support resizing.
type fixedTable struct {
	control []byte
	slots   []KV
	// groupCount int // TODO: consider using this, but maybe instead compare groupMask?
	groupMask uint64
	h2Shift   uint8

	// track our count of deletes, which we use when determining when to resize
	// TODO: dropping deletes without resize, or same size grow
	// if zero, we can skip some logic in some operations
	// TODO: check if that is a perf win
	deleteCount int
}

// TODO: pick a key/value layout. Within the slots our current layout is KV|KV|KV|KV|..., vs.
// the runtime's layout uses unsafe to access K|K|K|K|...|V|V|V|V|... per 8-elem bucket. That is more compact
// if K & V are not aligned, but equally compact if they are aligned.
// If we ignore alignment, our current layout might have better cache behavior
// given high confidence that loading a key for example for lookup means you are about
// to access the adjacent value (which for typical key sizes would be in same or adjacent cache line).
// Folly F14 layout though is probably better overall than runtime layout or our current layout.
// (F14FastMap picks between values inline vs. values packed in a contiguous array based on entry size:
//    https://github.com/facebook/folly/blob/main/folly/container/F14.md#f14-variants )

func (m *Map) Get(k Key) (v Value, ok bool) {
	h := m.hashFunc(k, m.seed)

	if m.old == nil || isChainEvacuated(m.growStatus[h&m.old.groupMask]) {
		// We are either not growing, which is the simple case, and we
		// can just look in m.current, or we are growing but we have
		// recorded that any keys with the natural group of this key
		// have already been moved to m.current, which also means we
		// can just look in m.current.
		kv, _, _ := m.find(&m.current, k, h)
		if kv != nil {
			return kv.Value, true
		}
		return zeroValue(), false
	}

	// We are growing.
	// TODO: maybe extract to findGrowing or similar. Would be nice to do midstack inlining for common case.
	oldNatGroup := h & m.old.groupMask
	oldNatGroupEvac := isEvacuated(m.growStatus[oldNatGroup])
	table := &m.current
	if !oldNatGroupEvac {
		// The key has never been written/deleted in current since this grow started
		// (because we always move the natural group when writing/deleting a key while growing).
		table = m.old
	}
	kv, _, _ := m.find(table, k, h)
	if kv != nil {
		// Hit
		return kv.Value, true
	}
	if !oldNatGroupEvac {
		// Miss in old, and the key has never been written/deleted in current since grow started,
		// so this is a miss for the overall map.
		return zeroValue(), false
	}

	// We had a miss in current, and the old natural group was evacuated,
	// but it is not yet conclusive if we have an overall miss. For example,
	// perhaps a displaced key in old was moved to current and later deleted, or
	// perhaps a displaced key was never moved to current and the golden copy is still in old.
	// Side note: for any mid-growth map, the majority of groups are one of (a) not yet evacuated, or
	// (b) evacuated and this Get is for a non-displaced key (because most keys are not displaced),
	// so the work we did above handled that majority of groups.
	// Now we do more work for less common cases.

	oldKv, oldDisplGroup, _ := m.find(m.old, k, h)
	if oldNatGroup == oldDisplGroup {
		// We already know from above that this group was evacuated,
		// which means if there was a prior matching key in this group,
		// it would have been evacuated to current.
		// Given it is not in current now, this is a miss for the overall map.
		return zeroValue(), false
	}
	if oldKv != nil && !isEvacuated(m.growStatus[oldDisplGroup]) {
		// Hit for the overall map. This is a group with a displaced matching key, and
		// we've never written/deleted this key since grow started,
		// so golden copy is in old.
		// (This is example of us currently relying on always evacuating displaced key
		// on write/delete).
		// TODO: no non-fuzzing test hits this. might require longer probe chain. the fuzzing might hit.
		return oldKv.Value, true
	}
	// Miss. The displaced group was evacuated to current, but current doesn't have the key
	return zeroValue(), false
}

// find searches the fixedTable for a key.
// For a hit, group is the location of the key, and offset is the location within the group.
// For a miss, group is the last probed group.
func (m *Map) find(t *fixedTable, k Key, h uint64) (kv *KV, group uint64, offset int) {
	// TODO: likely giving up some of performance by sharing find between Get and Delete
	group = h & t.groupMask
	h2 := t.h2(h)

	// TODO: could try hints to elim some bounds check below with additional masking? maybe:
	// controlLenMask := len(m.current.control) - 1
	// slotsLenMask := len(m.current.slots) - 1

	var probeCount uint64

	// Do quadratic probing.
	// This loop will terminate because (1) incrementing by
	// triangluar numbers will hit every slot in a power of 2 sized table
	// and (2) we always enforce at least some empty slots by resizing when needed.
	for {
		pos := group * 16
		controlBytes := t.control[pos:]
		bitmask, ok := MatchByte(h2, controlBytes)
		if debug && !ok {
			panic("short control byte slice")
		}
		for bitmask != 0 {
			// We have at least one hit on h2
			offset = bits.TrailingZeros32(bitmask)
			kv := &t.slots[int(pos)+offset]
			if kv.Key == k {
				return kv, group, offset
			}
			// TODO: is this right? The test coverage hits this, but
			// getting lower than expected false positives in benchmarks, maybe?
			// (but current benchmarks might have more conservative fill currently?)
			// m.getH2FalsePositives++ // stats.

			// continue to look. infrequent with 7 bit h2.
			// clear the bit we just checked.
			bitmask &^= 1 << offset
		}

		// No matching h2, or we had a matching h2
		// but failed to find an equal key in loop just above.
		// Check if this group is full or has at least one empty slot.
		// TODO: call it H1 and H2, removing h2 term
		// TODO: can likely skip getting the offset below and just test bitmask > 0
		emptyBitmask, ok := MatchByte(emptySentinel, t.control[group*16:])
		if debug && !ok {
			panic("short control byte slice")
		}

		// If we have any EMPTY positions, we know the key we were
		// looking to find was never displaced outside this group
		// by quadratic probing during Set and hence can we stop now at this group
		// (most often the key's natural group).
		if emptyBitmask != 0 {
			return nil, group, offset
		}

		// This group is full or contains STORED/DELETE without any EMPTY,
		// so continue on to the next group.
		// We don't do quadratic probing within a group, but we do
		// quadratic probing across groups.
		// Continue our quadratic probing across groups, using triangular numbers.
		// TODO: rust implementation uses a ProbeSeq and later C++ also has a probe seq; could consider something similar
		// m.getExtraGroups++ // stats
		probeCount++
		group = (group + probeCount) & t.groupMask
		if debug && probeCount >= uint64(len(t.slots)/16) {
			panic(fmt.Sprintf("impossible: probeCount: %d groups: %d underlying table len: %d", probeCount, len(t.slots)/16, len(t.slots)))
		}
	}
}

// Set sets k and v within the map.
func (m *Map) Set(k Key, v Value) {
	// Write the element, incrementing element count if needed and moving if needed.
	m.set(k, v, 1, true)
}

// set sets k and v within the map, returning group and the probe count.
// elemIncr indicates if we should increment elementCount when populating
// a free slot. A zero enables us to use set when evacuating,
// which does not change the number of elements.
// moveIfNeeded indicates if we should do move operations if currently growing.
func (m *Map) set(k Key, v Value, elemIncr int, moveIfNeeded bool) {
	h := m.hashFunc(k, m.seed)
	group := h & m.current.groupMask
	h2 := m.current.h2(h)

	if moveIfNeeded && m.old != nil {
		// We are growing. Move groups if needed
		m.moveGroups(group, k, h)
	}

	var probeCount uint64
	// Do quadratic probing.
	// This loop will terminate for same reasons as find loop.
	for {
		bitmask, ok := MatchByte(h2, m.current.control[group*16:])
		if debug && !ok {
			panic("short control byte slice")
		}

		for bitmask != 0 {
			// We have at least one hit on h2
			offset := bits.TrailingZeros32(bitmask)
			pos := int(group*16) + offset
			kv := m.current.slots[pos]
			if kv.Key == k {
				// update the existing key. Note we don't increment the elem count because we are replacing.
				m.current.control[pos] = h2
				m.current.slots[pos] = KV{Key: k, Value: v}
				// Track if we have any displaced elements in current while growing. This is rare.
				// TODO: This might not be a net perf win.
				if m.old != nil && probeCount != 0 {
					oldGroup := group & m.old.groupMask
					m.growStatus[oldGroup] = setCurHasDisplaced(m.growStatus[oldGroup])
				}
				return
			}

			// continue to look. infrequent with 7 bit h2.
			// clear the bit we just checked.
			bitmask &^= 1 << offset
		}

		// No matching h2, or we had a matching h2
		// but failed to find an equal key in loop just above.
		// See if this is the end of our probe chain, which is indicated
		// by the presence of an EMPTY slot.
		emptyBitmask := matchEmpty(m.current.control[group*16:])
		if emptyBitmask != 0 {
			// We've reached the end of our probe chain without finding
			// a match on an existing key.
			if m.elemCount+m.current.deleteCount >= m.resizeThreshold && !m.disableResizing {
				// Double our size
				m.startResize()

				// Also set the key we are working on, then we are done.
				// (Simply re-using Set here causes tiny bit of extra work when resizing;
				// we could instead let findFirstEmptyOrDeleted below handle it,
				// but we would need to at least recalc h2).
				// This is our first modification in our new table,
				// and we want to move the group(s) that correspond to this key.
				m.set(k, v, 1, true)
				return
			}

			var offset int
			if m.current.deleteCount == 0 || probeCount == 0 {
				// If we've never used a DELETED tombstone in this fixedTable,
				// the first group containing usable space is this group with its EMPTY slot,
				// which might be at the end of a probe chain, and we can use it now.
				// If instead we have DELETED somewhere but we have not just now probed beyond
				// the natural group, we can use an EMPTY slot in the natural group.
				// Either way, set the entry in this group using its first EMPTY slot.
				// TODO: double-check this is worthwhile given this
				// is an optimization that might not be in the C++ implementation?
				offset = bits.TrailingZeros32(emptyBitmask)
			} else {
				// We know there is room in the group we are on,
				// but we might have passed a usable DELETED slot during our
				// probing, so we rewind to this key's natural group and
				// probe forward from there,
				// and use the first EMPTY or DELETED slot found.
				group, offset = m.current.findFirstEmptyOrDeleted(h)
			}

			// update empty or deleted slot
			pos := int(group*16) + offset
			if m.current.control[pos] == deletedSentinel {
				m.current.deleteCount--
			}
			m.current.control[pos] = h2
			m.current.slots[pos] = KV{Key: k, Value: v}
			m.elemCount += elemIncr
			// Track if we have any displaced elements in current while growing. This is rare.
			if m.old != nil && probeCount != 0 {
				oldGroup := group & m.old.groupMask
				m.growStatus[oldGroup] = setCurHasDisplaced(m.growStatus[oldGroup])
			}
			return
		}

		// We did not find an available slot.
		// We don't do quadratic probing within a group, but we do
		// quadratic probing across groups.
		// Continue our quadratic probing across groups, using triangular numbers.
		probeCount++
		group = (group + probeCount) & m.current.groupMask

		if debug && probeCount >= uint64(len(m.current.slots)/16) {
			panic(fmt.Sprintf("impossible: probeCount: %d groups: %d underlying table len: %d", probeCount, len(m.current.slots)/16, len(m.current.slots)))
		}
	}
}

// startResize creates a new fixedTable with doubled table size,
// then copies the elements from the old table to the new table,
// leaving the new table as a ready-to-use current.
func (m *Map) startResize() {
	// prepare for a new, larger and initially empty current.
	m.resizeThreshold = m.resizeThreshold << 1
	newTableSize := len(m.current.control) << 1

	// place current in old, and create a new current
	m.old = &fixedTable{}
	*m.old = m.current
	m.current = *newFixedTable(newTableSize)

	// get ready to track our grow operation
	m.growStatus = make([]byte, len(m.old.control))
	m.sweepCursor = 0

	// TODO: temp stat for now
	m.resizeGenerations++
}

// moveGroups takes a group in current along with a
// key that is triggering the move. It only expects to be called
// while growing. It moves up to three groups:
//   1. the natural group for this key
//   2. the group this key is located in if it is displaced in old from its natural group
//   3. incrementally move from the front, including to ensure we finish and don't miss any groups
func (m *Map) moveGroups(group uint64, k Key, h uint64) {
	allowedMoves := 2

	// First, if the natural group for this key has not been moved, move it
	oldNatGroup := group & m.old.groupMask
	if !isEvacuated(m.growStatus[oldNatGroup]) {
		m.moveGroup(oldNatGroup)
		allowedMoves--
	}

	if !isChainEvacuated(m.growStatus[oldNatGroup]) {
		// Walk the chain that started at the natural group, moving any unmoved groups as we go.
		// If we move the complete chain, we mark the natural group as ChainEvacuated with moveChain.
		// The first group we'll visit is the one after the natural group (probeCount of 1).
		var chainEnd bool
		allowedMoves, chainEnd = m.moveChain(oldNatGroup, 1, allowedMoves)

		// We walked the chain as far we could.
		if !chainEnd {
			// Rare case.
			// Our key might be displaced from its natural group in old,
			// and we did not complete the chain, so we might not have
			// reached the actual group with the key.
			// We rely elsewhere (such as in Get) upon always moving the actual group
			// containing the key when an existing key is Set/Deleted.
			// Find the key. Note that we don't need to recompute the hash.
			kv, oldDisplGroup, _ := m.find(m.old, k, h)
			if kv != nil && oldDisplGroup != oldNatGroup {
				if !isEvacuated(m.growStatus[oldDisplGroup]) {
					// Not moved yet, so move it.
					// TODO: non-fuzzing tests don't hit this. fuzzing hasn't reached this branch either (so far).
					m.moveGroup(oldDisplGroup)
					allowedMoves-- // Can reach -1 here. Rare, should be ok.
				}
			}
		}
	}

	stopCursor := uint64(len(m.old.control)) / 16
	if stopCursor > m.sweepCursor+1000 {
		stopCursor = m.sweepCursor + 1000
	}
	for m.sweepCursor < stopCursor {
		// Walk up to N groups looking for something to move and/or to mark ChainEvacuated.
		// The sweepCursor group is marked ChainEvacuated if we evac through the end of the chain.
		// The majority of the time, sweepCursor is a singleton chain or is otherwise the end of a chain.
		if !isChainEvacuated(m.growStatus[m.sweepCursor]) {
			allowedMoves, _ = m.moveChain(m.sweepCursor, 0, allowedMoves)
		}
		if isChainEvacuated(m.growStatus[m.sweepCursor]) {
			m.sweepCursor++
			continue
		}
		if allowedMoves <= 0 {
			break
		}
	}

	// Check if we are now done
	if m.sweepCursor >= (uint64(len(m.old.control)) / 16) {
		// Done growing!
		// TODO: we have some test coverage of this, but would be nice to have more explicit test
		// TODO: maybe extract a utility func
		m.old = nil
		m.growStatus = nil
		m.sweepCursor = 0
	}
}

// moveChain walks a probe chain that starts at a natural group, moving unmoved groups.
// The probeCount parameter allows it to begin in the middle of a walk.
// moveChain returns the number of remaining allowedMoves and a bool indicating
// if the end of chain has been reached.
// Each moved group is marked as being evacuated, and if a chain is completely
// evacuated, the starting natural group is marked ChainEvacuated.
func (m *Map) moveChain(oldNatGroup uint64, probeCount uint64, allowedMoves int) (int, bool) {
	g := (oldNatGroup + probeCount) & m.old.groupMask

	for allowedMoves > 0 {
		if !isEvacuated(m.growStatus[g]) {
			// Evacute.
			m.moveGroup(g)
			allowedMoves--
		}
		if matchEmpty(m.old.control[g*16:]) != 0 {
			// Done with the chain. Record that.
			m.growStatus[oldNatGroup] = setChainEvacuated(m.growStatus[oldNatGroup])
			// chainEnd is true
			return allowedMoves, true
		}
		probeCount++
		g = (g + probeCount) & m.old.groupMask
	}
	return allowedMoves, false
}

// moveGroup takes a group in old, and moves it to current.
// It only moves that group, and does not cascade to other groups
// (even if moving the group writes displaced elements to other groups).
func (m *Map) moveGroup(group uint64) {
	for offset, b := range m.old.control[group*16 : group*16+16] {
		if isStored(b) {
			// TODO: cleanup
			kv := m.old.slots[group*16+uint64(offset)]

			// We are re-using the set mechanism to write to
			// current, but we don't want cascading moves of other groups
			// based on this write, so moveIfNeeded is false.
			// TODO: m.set does a little more work than strictly required,
			// including we know key is not present in current yet, so could avoid MatchByte(h2) and
			// some other logic.
			m.set(kv.Key, kv.Value, 0, false)
		}
	}
	// Mark it evacuated.
	m.growStatus[group] = setEvacuated(m.growStatus[group])

	if matchEmpty(m.old.control[group*16:]) != 0 {
		// The probe chain starting at this group ends at this group,
		// so we can also mark it ChainEvacuated.
		m.growStatus[group] = setChainEvacuated(m.growStatus[group])
	}
}

func (m *Map) Delete(k Key) {
	// TODO: make a 'delete' with moveIfNeeded

	h := m.hashFunc(k, m.seed)
	group := h & m.current.groupMask
	if m.old != nil {
		// We are growing. Move groups if needed
		// TODO: don't yet have a test that hits this (Delete while growing)
		m.moveGroups(group, k, h)
	}

	kv, group, offset := m.find(&m.current, k, h)
	if kv == nil {
		return
	}

	// Mark existing key as deleted or empty.
	// In the common case we can set this position back to empty.
	var sentinel byte = emptySentinel

	// However, we need to check if there are any EMPTY positions in this group
	emptyBitmask, ok := MatchByte(emptySentinel, m.current.control[group*16:])
	if debug && !ok {
		panic("short control byte slice")
	}
	if emptyBitmask == 0 {
		// We must use a DELETED tombstone because there are no remaining
		// positions marked EMPTY (which means there might have been displacement
		// past this group in the past by quadratic probing, and hence we use tombstones to make
		// sure we follow any displacement chain properly in any future operations).
		sentinel = deletedSentinel
		m.current.deleteCount++
	}

	pos := int(group*16) + offset
	m.current.control[pos] = sentinel
	// TODO: for a pointer, would want to set nil. could do with 'zero' generics func.
	m.current.slots[pos] = KV{}
	m.elemCount--
}

// matchEmptyOrDeleted checks if the first 16 bytes of controlBytes has
// any empty or deleted sentinels, returning a bitmask of the corresponding offsets.
// TODO: can optimize this via SSE (e.g., check high bit via _mm_movemask_epi8 or similar).
func matchEmptyOrDeleted(controlBytes []byte) uint32 {
	emptyBitmask, ok := MatchByte(emptySentinel, controlBytes)
	deletedBitmask, ok2 := MatchByte(deletedSentinel, controlBytes)
	if debug && !(ok && ok2) {
		panic("short control byte slice")
	}
	return emptyBitmask | deletedBitmask
}

// matchEmpty checks if the first 16 bytes of controlBytes has
// any empty sentinels, returning a bitmask of the corresponding offsets.
func matchEmpty(controlBytes []byte) uint32 {
	emptyBitmask, ok := MatchByte(emptySentinel, controlBytes)
	if debug && !ok {
		panic("short control byte slice")
	}
	return emptyBitmask
}

func (m *Map) Range(f func(key Key, value Value) bool) {
	// We iterate over snapshots of old and current tables, looking up
	// the golden data in the live tables as needed. It might be that the live
	// tables have a different value, or the live tables might have deleted the key,
	// both of which we must respect at the moment we emit a key/value during iteration.
	// However, we are not obligated to iterate over all the keys in the
	// live tables -- we are allowed to emit a key added after iteration start, but
	// are not required to do so.
	//
	// When iterating over our snapshot of old, we emit all keys encountered that are
	// still present in the live tables. We then iterate over our snapshot of current,
	// but skip any key present in the immutable old snapshot to avoid duplicates.
	//
	// In some cases, we can emit without a lookup, but in other cases we need to do a
	// lookup in another table. We have some logic to minimize rehashing. While iterating
	// over old, we typically need to rehash keys in evacuated groups, but while iterating
	// over current, the common case is we do not need to rehash even to do a lookup.
	//
	// A Set or Delete is allowed during an iteration (e.g., a Set within the user's code
	// invoked by Range might cause growth to start or finish), but not concurrently.
	// For example, iterating while concurrently calling Set from another goroutine
	// would be a user-level data race (similar to runtime maps).
	//
	// TODO: clean up comments and add better intro.
	// TODO: make an iter struct, with a calling sequence like iterstart and iternext

	// Begin by storing some snapshots of our tables.
	// For example, another m.old could appear later if a
	// new grow starts after this iterator starts.
	// We want to iterate over the old that we started with.
	// Note that old is immutable once we start growing.
	// TODO: maybe gather these, such as:
	// type iter struct { old, growStatus, current, oldPos, curPos, ... }
	old := m.old
	growStatus := m.growStatus

	// A new m.current can also be created mid iteration, so snapshot
	// it as well so that we can iterate over the current we started with.
	cur := m.current
	curControl := m.current.control[:] // TODO: maybe not needed, and/or collapse these?
	curSlots := m.current.slots[:]     // TODO: same

	// Below, we pick a random starting group and starting offset within that group.
	r := (uint64(fastrand()) << 32) | uint64(fastrand())
	if m.seed == 0 || m.seed == 42 {
		// TODO: currently forcing repeatability for some tests, including fuzzing, but eventually remove
		r = 0
	}

	// Now, iterate over our snapshot of old.
	if old != nil {
		for i, group := 0, r&old.groupMask; i < len(old.control)/16; i, group = i+1, (group+1)&old.groupMask {
			offsetMask := uint64(0x0F)
			for j, offset := 0, (r>>61)&offsetMask; j < 16; j, offset = j+1, (offset+1)&offsetMask {
				pos := group*16 + offset
				// Iterate over control bytes individually for now.
				// TODO: consider 64-bit check of control bytes or SSE operations (e.g., _mm_movemask_epi8).
				if isStored(old.control[pos]) {
					k := old.slots[pos].Key

					// We don't need to worry about displacements here when checking
					// evacuation status. (We are iterating over each control byte, wherever they have landed).
					if !isEvacuated(growStatus[pos/16]) {
						// Not evac. Because we always move both a key's natural group
						// and the key's displaced group for any Set or Delete, not evac means
						// we know nothing in this group has ever
						// been written or deleted in current, which means
						// the key/value here in old are the golden data,
						// which we use now. (If grow had completed, or if there
						// have been multiple generations of growing, our snapshot
						// of old will have everything evacuated).
						// TODO: current non-fuzzing tests don't hit this. fuzzing does ;-)
						cont := f(k, old.slots[pos].Value)
						if !cont {
							return
						}
						continue
					}

					// Now we handle the evacuated case. This key at one time was moved to current.
					// Check where the golden data resides now, and emit the live key/value if they still exist.
					// TODO: could probably do less work, including avoiding lookup/hashing in same cases

					if cur.groupMask == m.current.groupMask || m.old == nil {
						// We still in the same grow as when the iter started,
						// or that grow is finished and we are not in the middle
						// of a different grow, so we don't need to look in m.old
						// (because this elem is already evacuated, or m.old doesn't exist),
						// and hence can just look in m.current.
						kv, _, _ := m.find(&m.current, k, m.hashFunc(k, m.seed))
						if kv != nil {
							cont := f(kv.Key, kv.Value)
							if !cont {
								return
							}
						}
						continue
					}

					// We are in in the middle of a grow that is different from the grow at iter start.
					// In other words, m.old is now a "new" old.
					// Do a full Get, which looks in the live m.current or m.old as needed.
					v, ok := m.Get(k)
					if !ok {
						// Group was evacuated, but key not there now, so we don't emit anything
						continue
					}
					// Key exists in live m.current, or possibly live m.old. Emit that copy.
					// TODO: for floats, handle -0 vs. +0 (https://go.dev/play/p/mCN_sddUlG9)
					cont := f(k, v)
					if !cont {
						return
					}
					continue
				}
			}
		}
	}

	// No old, or we've reached the end of old.
	// We now iterate over our snapshot of current, but we will skip anything present in
	// the immutable old because it would have been already processed above.
	loopMask := uint64(len(curControl)/16 - 1)
	for i, group := 0, r&loopMask; i < len(curControl)/16; i, group = i+1, (group+1)&loopMask {
		offsetMask := uint64(0x0F)
		for j, offset := 0, (r>>61)&offsetMask; j < 16; j, offset = j+1, (offset+1)&offsetMask {
			pos := group*16 + offset
			if isStored(curControl[pos]) {
				curGroup := uint64(pos / 16)
				k := curSlots[pos].Key

				if old != nil {
					// We are about to look in old, but first, compute the hash for this key (frequently cheaply).
					var h uint64
					if !curHasDisplaced(growStatus[curGroup&old.groupMask]) {
						// During a grow, we track when a group contains a displaced element.
						// The group we are on does not have any displaced elemenets, which means
						// we can reconstruct the useful portion of the hash from the group and h2
						// This could help with cases like https://go.dev/issue/51410 when a map
						// is in a growing state for an extended period.
						// TODO: check cost and if worthwhile
						h = cur.reconstructHash(curControl[pos], curGroup)
					} else {
						// Rare that a group in current would have displaced elems during a grow,
						// but it means we must recompute the hash from scratch
						h = m.hashFunc(k, m.seed)
					}

					// Look in old
					kv, _, _ := m.find(old, k, h)
					if kv != nil {
						// This key exists in the immutable old, so already handled above in our loop over old
						continue
					}
				}

				// The key was not in old or there is no old. If the key is still live, we will emit it.
				// Start by checking if m.current is the same as the snapshot of current we are iterating over.
				if cur.groupMask == m.current.groupMask {
					// They are the same, so we can simply emit from the snapshot
					cont := f(k, curSlots[pos].Value)
					if !cont {
						return
					}
					continue
				}

				// Additional grows have happened since we started, so we need to check m.current and
				// possibly a new m.old if needed, which is all handled by Get
				// TODO: could pass in reconstructed hash here as well, though this is a rarer case compared to
				// writes stopping and a map being "stuck" in the same growing state forever or long time.
				v, ok := m.Get(k)
				if !ok {
					// key not there now, so we don't emit anything
					continue
				}
				// Key exists in live current, or possibly live old. Emit.
				// TODO: for floats, handle -0 vs. +0
				cont := f(k, v)
				if !cont {
					return
				}
				continue
			}
		}
	}
}

// isStored reports whether controlByte indicates a stored value.
// If leading bit is 0, it means there is a valid value in the corresponding
// slot in the table. (The next 7 bits are the h2 values).
// TODO: maybe isStored -> hasStored or similar?
func isStored(controlByte byte) bool {
	return controlByte&(1<<7) == 0
}

// isEvacuated reports whether the group corresponding to statusByte
// has been moved from old to new.
// Note: this is just for the elements stored in that group in old,
// and does not mean all elements dispalced fro mthat group have been evacuated.
// TODO: collapse these flags down into fewer bits rather than using a full byte
// TODO: maybe make a type
func isEvacuated(statusByte byte) bool {
	return statusByte&(1<<0) != 0
}

func setEvacuated(statusByte byte) byte {
	return statusByte | (1 << 0)
}

// isChainEvacuated is similar to isEvacuated, but reports whether the group
// corresponding to statusByte has been moved from old to new
// along with any probe chains that orginate from that group.
// A group that does not have any chains originating from it can have isChainEvacuated true.
func isChainEvacuated(statusByte byte) bool {
	return statusByte&(1<<1) != 0
}

func setChainEvacuated(statusByte byte) byte {
	return statusByte | (1 << 1)
}

// curHasDisplaced indicates the group in current has displaced elements.
// It is only tracked during grow operations, and therefore is
// only very rarely set. If we are mid-grow, it means current was recently
// doubled in size and has not yet had enough elems added to complete the grow.
// TODO: verify this is a performance win for range
// TODO: consider oldHasDisplaced, but might be less of a win
// (additional book keeping, likely higher mispredictions than curHasDisplaced, ...).
func curHasDisplaced(statusByte byte) bool {
	return statusByte&(1<<2) != 0
}

func setCurHasDisplaced(statusByte byte) byte {
	return statusByte | (1 << 2)
}

// Number of elements stored in Map
// Should track this explicitly.
func (m *Map) Len() int {
	return m.elemCount
}

// newFixedTable returns a *newFixedTable that is ready to use.
// A fixedTable can be copied.
func newFixedTable(tableSize int) *fixedTable {
	// TODO: not using capacity in our make calls. Probably reasonable for straight swisstable impl?

	if tableSize&(tableSize-1) != 0 || tableSize == 0 {
		panic(fmt.Sprintf("table size %d is not power of 2", tableSize))
	}

	slots := make([]KV, tableSize)
	control := make([]byte, tableSize)
	// Initialize all control bytes to empty
	// TODO: consider using 0x00 for empty, or unroll, or set these with unsafe, or...
	// A simple loop here is ~15% of time to construct a large capacity empty table.
	for i := range control {
		control[i] = emptySentinel
	}

	return &fixedTable{
		control: control,
		slots:   slots,
		// 16 control bytes per group, table length is power of 2
		groupMask: (uint64(tableSize) / 16) - 1,
		// h2Shift gives h2 as the next 7 bits just above the group mask.
		// (It is not the top 7 bits, which is what runtime map uses).
		// TODO: small sanity of h2Shift; maybe make test: https://go.dev/play/p/DjmN7O4YrWI
		h2Shift: uint8(bits.TrailingZeros(uint(tableSize / 16))),
	}
}

func (t *fixedTable) findFirstEmptyOrDeleted(h uint64) (group uint64, offset int) {
	group = h & t.groupMask

	// Do quadratic probing.
	var probeCount uint64
	for {
		bitmask := matchEmptyOrDeleted(t.control[group*16:])
		if bitmask != 0 {
			// We have at least one hit
			offset = bits.TrailingZeros32(bitmask)
			return group, offset
		}

		// No matching empty or delete control byte.
		// Keep probing to next group. (It's a bug if the whole table
		// does not contain any empty or deleted positions).
		probeCount++
		group = (group + probeCount) & t.groupMask
		if debug && probeCount >= uint64(len(t.slots)/16) {
			panic(fmt.Sprintf("impossible: probeCount: %d groups: %d underlying table len: %d", probeCount, len(t.slots)/16, len(t.slots)))
		}
	}
}

// h2 returns the 7 bits immediately above the bits covered by the table's groupMask
func (t *fixedTable) h2(h uint64) uint8 {
	// TODO: does an extra mask here elim a shift check in the generated code?
	return uint8((h >> uint64(t.h2Shift)) & 0x7f)
}

// reconstructHash reconstructs the bits of the original hash covered by
// the table's groupMask plus an additional 7 bits. In other words, it reconstructs
// the bits that we use elsewhere for h2 and the group (h1). It assumes
// controlByte contains the h2 (that is, that it corresponds to a stored position).
// TODO: runtime map might be able to use this approach?
func (t *fixedTable) reconstructHash(controlByte byte, group uint64) uint64 {
	return group | ((uint64(controlByte) & 0x7F) << uint64(t.h2Shift))
}

// calcTableSize returns the length to use
// for the storage slices to support
// capacityHint stored map elements.
func calcTableSize(capacityHint int) int {
	// For now, follow Go maps with max of 6.5 entries per 8 elem buckets,
	// which is 81.25% max load factor, rounded up to a power of 2.
	// Our current minimum size is 16.
	tableSize := int(float64(capacityHint) / (6.5 / 8))
	pow2 := 16
	// TODO: clip max
	for tableSize > pow2 {
		pow2 = pow2 << 1
	}
	tableSize = pow2

	// sanity check power of 2
	if tableSize&(tableSize-1) != 0 || tableSize == 0 {
		panic("impossible")
	}
	return tableSize
}

func zeroKey() Key {
	return Key(0)
}

func zeroValue() Value {
	return Value(0)
}

func hashUint64(k Key, seed uintptr) uint64 {
	// earlier: uint64(memhash(unsafe.Pointer(&k), seed, uintptr(8)))
	return uint64(memhash64(unsafe.Pointer(&k), seed))
}

func hashString(s string, seed uintptr) uint64 {
	return uint64(strhash(unsafe.Pointer(&s), seed))
}

//go:linkname memhash runtime.memhash
//go:noescape
func memhash(p unsafe.Pointer, seed, s uintptr) uintptr

//go:linkname memhash64 runtime.memhash64
//go:noescape
func memhash64(p unsafe.Pointer, seed uintptr) uintptr

//go:linkname strhash runtime.strhash
//go:noescape
func strhash(p unsafe.Pointer, h uintptr) uintptr

// TODO: fastrand64 did not initially work
//go:linkname fastrand runtime.fastrand
func fastrand() uint32

func init() {
	if runtime.GOARCH != "amd64" {
		// the assembly is only amd64 without a pure Go fallback yet.
		// also, we are ignoring 32-bit in several places.
		panic("only amd64 is supported")
	}
}

const debug = false
