package swisstable

import (
	"flag"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

var longTestFlag = flag.Bool("long", false, "run long benchmarks")

func TestMap_Set(t *testing.T) {
	tests := []struct {
		elem KV
	}{
		{KV{Key: 1, Value: 2}},
		{KV{Key: 3, Value: 4}},
		{KV{Key: 8, Value: 1e9}},
		{KV{Key: 1e6, Value: 1e10}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("set key %d", tt.elem.Key), func(t *testing.T) {
			m := New(256)

			m.Set(tt.elem.Key, tt.elem.Value)

			gotLen := m.Len()
			if gotLen != 1 {
				t.Errorf("Map.Len() == %d, want 1", gotLen)
			}
		})
	}
}

func TestMap_Get(t *testing.T) {
	tests := []struct {
		elem KV
	}{
		{KV{Key: 1, Value: 2}},
		{KV{Key: 8, Value: 8}},
		{KV{Key: 1e6, Value: 1e10}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("get key %d", tt.elem.Key), func(t *testing.T) {
			m := New(256)

			m.Set(tt.elem.Key, tt.elem.Value)
			gotV, gotOk := m.Get(tt.elem.Key)
			if !gotOk {
				t.Errorf("Map.Get() gotOk = %v, want true", gotOk)
			}
			if gotV != tt.elem.Value {
				t.Errorf("Map.Get() gotV = %v, want %v", gotV, tt.elem.Value)
			}

			gotV, gotOk = m.Get(1e12)
			if gotOk {
				t.Errorf("Map.Get() gotOk = %v, want false", gotOk)
			}
			if gotV != 0 {
				t.Errorf("Map.Get() gotV = %v, want %v", gotV, 0)
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

			// TODO: this is true for sparsehash, but not our swisstable,
			// which sizes the underlying table slices to roundPow2(1/0.8) times the requested capacity.
			// TODO: also, might no longer be true for sparestable, either.

			// TODO: reach in to disable growth?
			// We reach into the implementation to see what full means.
			underlyingTableLen := len(m.table)
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
			for i := 0; i < len(m.control); i++ {
				// TODO: 0 currently means empty
				if m.control[i] == 0 {
					t.Fatalf("control byte %d is empty", i)
				}
			}
			for i := 0; i < len(m.table); i++ {
				if m.table[i].Key == 0 || m.table[i].Value == 0 {
					// We set everything to non-zero values above.
					t.Fatalf("element at index %d has key or value that is still 0: key = %d value = %d",
						i, m.table[i].Key, m.table[i].Value)
				}
			}

		})
	}
}

var newBenchmarks = []benchmark{
	{"map size 1000000", 1_000_000},
	{"map size 2000000", 2_000_000},
	{"map size 3000000", 3_000_000},
	{"map size 4000000", 4_000_000},
	{"map size 5000000", 5_000_000},
	{"map size 6000000", 6_000_000},
	{"map size 7000000", 7_000_000},
	{"map size 8000000", 8_000_000},
	{"map size 9000000", 9_000_000},
	{"map size 10000000", 10_000_000},
	{"map size 20000000", 20_000_000},
	{"map size 30000000", 30_000_000},
	{"map size 40000000", 40_000_000},
	{"map size 50000000", 50_000_000},
	{"map size 60000000", 60_000_000},
	{"map size 70000000", 70_000_000},
	{"map size 80000000", 80_000_000},
	{"map size 90000000", 90_000_000},
	{"map size 100000000", 100_000_000},
}

func BenchmarkMatchByte(b *testing.B) {
	buffer := make([]byte, 16)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = MatchByte(42, buffer)
	}

}

func BenchmarkNew_Int64_Std(b *testing.B) {
	bms := newBenchmarks
	if !*longTestFlag {
		bms = []benchmark{
			{"map size 1000000", 1_000_000},
		}
	}

	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				testA = make(map[int64]*int64, bm.mapElements)
			}
			b.StopTimer()
			runtime.GC()
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			b.ReportMetric(float64(memStats.HeapAlloc)/float64(16*bm.mapElements), "overhead")
			b.ReportMetric(float64(memStats.HeapAlloc), "heap:bytes")

			// the nil reduces highwater mark -- don't have 2 in mem at once
			testA = nil
		})
	}
}

// TODO: make test driven, pick some values, match outpu from others
func BenchmarkNew_Int64_Swisstable(b *testing.B) {
	bms := newBenchmarks
	if !*longTestFlag {
		bms = []benchmark{
			{"map size 1000000", 1_000_000},
		}
	}
	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				testB = New(bm.mapElements)
			}
			b.StopTimer()
			runtime.GC()
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			b.ReportMetric(float64(memStats.HeapAlloc)/float64(16*bm.mapElements), "overhead")
			b.ReportMetric(float64(memStats.HeapAlloc), "heap:bytes")

			// the nil reduces highwater mark -- don't have 2 in mem at once
			testB = nil

			// currently:
			// 201458656/(1024*1024)=192
			// 11534336*17.5=201458656
			// 1.5 extra per slot is 96 extra per 64 (64*1.5)
			// We are half capacity... so len is 2x capacity, so that's 1 byte accounted for...
			// with mystery of 0.5 per 128 slots (I think) or 0.25 per __ ... or rethink!
			// for this test, B/op is shows same as live heap because all the allocation happens in the loop.
			// for start empty and add 1M, they hopefully show a difference.
			/* could instead use:
			https://pkg.go.dev/runtime/metrics

			/memory/classes/heap/objects:bytes
			Memory occupied by live objects and dead objects that have
			not yet been marked free by the garbage collector.
			*/

			/* If we request a capacity of 10M elements of 16 byte total payload (keys + values = 16 bytes),
			that currently works out to:

			no overhead bytes:  160000000 (152.6 MB)

			requested cap:       10000000
			actual cap:          10485760
			table size:          16777216 (16.000 M)
			load factor:           62.50%
			groups:                262144

			data bytes:         167772160 (95.24%)
			group bytes:          8388608 (4.76%)
			total bytes:        176160768 (100.00%) (168.0 MB)

			capacity overhead:      1.05x
			group overhead:         1.05x
			total overhead:         1.10x
			*/
		})
	}
}

func BenchmarkNewSweep_Int64_Std(b *testing.B) {
	bms := sweepMapSizes()

	if !*longTestFlag {
		b.Skip()
	}

	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				testA = make(map[int64]*int64, bm.mapElements)
			}

			b.StopTimer()
			runtime.GC()
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			b.ReportMetric(float64(memStats.HeapAlloc)/float64(16*bm.mapElements), "overhead")
			b.ReportMetric(float64(memStats.HeapAlloc), "heap:bytes")

			// the nil reduces highwater mark -- don't have 2 in mem at once
			testA = nil
		})
	}
}

func BenchmarkNewSweep_Int64_Swisstable(b *testing.B) {
	bms := sweepMapSizes()
	if !*longTestFlag {
		b.Skip()
	}

	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				testB = New(bm.mapElements)
			}

			b.StopTimer()
			runtime.GC()
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			b.ReportMetric(float64(memStats.HeapAlloc)/float64(16*bm.mapElements), "overhead")
			b.ReportMetric(float64(memStats.HeapAlloc), "heap:bytes")

			// the nil reduces highwater mark -- don't have 2 in mem at once
			testA = nil
		})
	}
}

// older:

// ---- INSERT ----
// Roughly 1.89x slower for 1M
// Roughly 1.639x slower for 1K

// FIRST CUT

// BenchmarkMapsInt64KeysInsert-4   	      15	  72443760 ns/op	   10462 B/op	       1 allocs/op
// BenchmarkMapsInt64KeysInsert-4   	      15	  74837213 ns/op	   10462 B/op	       1 allocs/op
// 1x capacity:
// BenchmarkSparsemapInsert-4   	       5	     232332120 ns/op	 4194304 B/op	    3276 allocs/op
// 2x capacity:
// BenchmarkSparsemapInsert-4   	       7	     148209329 ns/op	  814080 B/op	     636 allocs/op
// >2x (?) capacity:
// BenchmarkSparsemapInsert-4   	       7	     185569914 ns/op	       0 B/op	       0 allocs/op

/*
INSERT

// insert repeated benchmark
go test -count=20 -benchmem -run=^$ -bench ^BenchmarkSparse.*Insert1K$ github.com/thpudds/sparsehash > sparse-1K-insert.txt

// insert pprof
go test -benchtime=20s -benchmem -cpuprofile sparse-profile-1k-insert-20s.out -run=^$ -bench ^BenchmarkSparse.*Insert1K$ github.com/thpudds/sparsehash

benchstat std-really-1M-insert.txt
name                     time/op
MapsInt64KeysInsert1M-4  72.4ms ± 3%

benchstat sparse-1M-insert.txt
name                 time/op
SparsemapInsert1M-4  137ms ± 4%


benchstat std-1K-insert.txt
name                     time/op
MapsInt64KeysInsert1K-4  23.0µs ± 4%


benchstat sparse-1K-insert.txt
name                 time/op
SparsemapInsert1K-4  37.7µs ± 3%

*/

// ---- LOOKUP ----
// Roughly 1.5x slower for 1K
// Roughly 2.0x slower for 1M

// BenchmarkMapsInt64KeysLookup-4   	      18	  63500650 ns/op	       0 B/op	       0 allocs/op
// BenchmarkSparsemapLookup-4   	       8	     133696150 ns/op	       0 B/op	       0 allocs/op

// 1.54 slower for small (1k)
// was: 1.67x slower for small (1K)

// benchstat std-1K.txt
// name                   time/op
// MapsInt64KeysLookup-4  19.9µs ± 4%

// benchstat sparse-1K.txt
// name               time/op
// SparsemapLookup-4  33.2µs ± 9%

// benchstat sparse-1K-pos-simpl.txt
// name               time/op
// SparsemapLookup-4  30.8µs ± 1%

// 10% improvement for pos-simpl:
// benchstat sparse-1K.txt sparse-1K-pos-simpl.txt
// name               old time/op    new time/op    delta
// SparsemapLookup-4    33.2µs ± 9%    30.8µs ± 1%  -7.27%  (p=0.000 n=17+17)

// ==> 33 nanoseconds

// -----------------------------

// 2x slower for 1M

// benchstat std-1M.txt
// name                   time/op
// MapsInt64KeysLookup-4  63.9ms ± 2%

// benchstat sparse-1M.txt
// name               time/op
// SparsemapLookup-4  128ms ± 4%

// ==> 128 nanoseconds
// L2 access is ~7 nanoseconds
// RAM access is ~100 nanoseconds

// pprof:
// go test -benchtime=20s -benchmem -run=^$ -cpuprofile sparse-profile-small-20s.out -bench ^BenchmarkSparse.*Lookup$ github.com/thpudds/sparsehash

/*

INSERT pprof

small:

$ go tool pprof sparse-profile-1k-insert-20s.out
Type: cpu
Time: Feb 2, 2022 at 3:01pm (EST)
Duration: 24.84s, Total samples = 12.78s (51.46%)
Entering interactive mode (type "help" for commands, "o" for options)
(pprof) top 20
Showing nodes accounting for 12.56s, 98.28% of 12.78s total
Dropped 27 nodes (cum <= 0.06s)
Showing top 20 nodes out of 24
      flat  flat%   sum%        cum   cum%
     3.71s 29.03% 29.03%      3.71s 29.03%  aeshashbody
     2.48s 19.41% 48.44%     12.13s 94.91%  github.com/thpudds/sparsehash.(*Map).Set
     2.31s 18.08% 66.51%      3.29s 25.74%  github.com/thpudds/sparsehash/internal/sparsetable.(*Table).Get
	 1.42s 11.11% 77.62%      1.91s 14.95%  github.com/thpudds/sparsehash/internal/sparsetable.(*Table).Set
     0.89s  6.96% 84.59%      0.89s  6.96%  github.com/thpudds/sparsehash/internal/sparsetable.(*sparsegroup).isAssigned (inline)
     0.50s  3.91% 88.50%      0.50s  3.91%  github.com/thpudds/sparsehash/internal/sparsetable.(*sparsegroup).posToOffset
     0.47s  3.68% 92.18%      4.39s 34.35%  github.com/thpudds/sparsehash.hashUint64
     0.37s  2.90% 95.07%     12.50s 97.81%  github.com/thpudds/sparsehash.BenchmarkSparsemapInsert1K
     0.21s  1.64% 96.71%      0.21s  1.64%  runtime.memhash

1M: (biggest chunk of time is where we first touch the group.Values slice, i.e., cold)

(pprof) top 15
Showing nodes accounting for 19.06s, 97.24% of 19.60s total
Dropped 69 nodes (cum <= 0.10s)
Showing top 15 nodes out of 25
      flat  flat%   sum%        cum   cum%
    11.47s 58.52% 58.52%     14.53s 74.13%  github.com/thpudds/sparsehash/internal/sparsetable.(*Table).Get
     2.99s 15.26% 73.78%      2.99s 15.26%  github.com/thpudds/sparsehash/internal/sparsetable.(*sparsegroup).isAssigned (inline)
     1.70s  8.67% 82.45%      1.70s  8.67%  aeshashbody
     1.34s  6.84% 89.29%     18.78s 95.82%  github.com/thpudds/sparsehash.(*Map).Set
     0.66s  3.37% 92.65%      0.94s  4.80%  github.com/thpudds/sparsehash/internal/sparsetable.(*Table).Set
     0.27s  1.38% 94.03%      0.27s  1.38%  github.com/thpudds/sparsehash/internal/sparsetable.(*sparsegroup).posToOffset (partial-inline)
     0.23s  1.17% 95.20%      0.23s  1.17%  runtime.stdcall1


GET pprof

$ go tool pprof sparse-profile-small-20s-simplify-postooffset-2.out
Type: cpu
Time: Feb 2, 2022 at 2:31pm (EST)
Duration: 25.32s, Total samples = 13.24s (52.28%)
Entering interactive mode (type "help" for commands, "o" for options)
(pprof) top20
Showing nodes accounting for 12.96s, 97.89% of 13.24s total
Dropped 35 nodes (cum <= 0.07s)
Showing top 20 nodes out of 25
      flat  flat%   sum%        cum   cum%
     4.43s 33.46% 33.46%      4.43s 33.46%  aeshashbody
     3.05s 23.04% 56.50%      4.31s 32.55%  github.com/thpudds/sparsehash/internal/sparsetable.(*Table).Get
     2.67s 20.17% 76.66%     12.25s 92.52%  github.com/thpudds/sparsehash.(*Map).Get
     0.87s  6.57% 83.23%      0.87s  6.57%  github.com/thpudds/sparsehash/internal/sparsetable.(*sparsegroup).isAssigned (inline)
     0.59s  4.46% 87.69%      5.22s 39.43%  github.com/thpudds/sparsehash.hashUint64
     0.49s  3.70% 91.39%     12.74s 96.22%  github.com/thpudds/sparsehash.BenchmarkSparsemapLookup
     0.37s  2.79% 94.18%      0.37s  2.79%  github.com/thpudds/sparsehash/internal/sparsetable.(*sparsegroup).posToOffset
     0.20s  1.51% 95.69%      0.20s  1.51%  runtime.memhash
     0.17s  1.28% 96.98%      0.17s  1.28%  runtime.stdcall1
     0.05s  0.38% 97.36%      0.09s  0.68%  runtime/pprof.(*profMap).lookup


*/

var testA map[int64]*int64
var testB *Map
var sinkUint uint64
var sinkBool bool

func BenchmarkAdd1M_Int64_Std(b *testing.B) {
	mapElements := 1_000_000

	m := make(map[uint64]uint64, mapElements)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < mapElements; k++ {
			m[uint64(k)] = uint64(k)
		}
	}
}

func BenchmarkAdd1M_Int64_Swisstable(b *testing.B) {
	mapElements := 1_000_000

	m := New(mapElements)
	b.ReportAllocs()
	b.ResetTimer()

	// Note this does not report the final mem usage.
	for i := 0; i < b.N; i++ {
		for k := 0; k < mapElements; k++ {
			m.Set(Key(k), Value(k))
		}
	}
}
func BenchmarkAdd1K_Int64_Std(b *testing.B) {
	mapElements := 1_000

	m := make(map[uint64]uint64, mapElements)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < mapElements; k++ {
			m[uint64(k)] = uint64(k)
		}
	}
}

func BenchmarkAdd1K_Int64_Swisstable(b *testing.B) {
	mapElements := 1_000

	m := New(mapElements)
	b.ReportAllocs()
	b.ResetTimer()

	// Note this does not report the final mem usage.
	for i := 0; i < b.N; i++ {
		for k := 0; k < mapElements; k++ {
			m.Set(Key(k), Value(k))
		}
	}
}

func BenchmarkGet1K_Int64_1KStd(b *testing.B) {
	mapElements := 1_000

	m := make(map[uint64]uint64, mapElements)
	for k := 0; k < mapElements; k++ {
		m[uint64(k)] = uint64(k)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < mapElements; k++ {
			sinkUint, sinkBool = m[uint64(k)]
		}
	}
}

func BenchmarkGet1K_Int64_1KSwisstable(b *testing.B) {
	mapElements := 1_000

	m := New(mapElements)
	for k := 0; k < mapElements; k++ {
		m.Set(Key(k), Value(k))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for k := 0; k < mapElements; k++ {
			v, b := m.Get(Key(k))
			sinkUint = uint64(v)
			sinkBool = b
		}
	}
}

// 400K -> 2.2M
// 100K
// 3 each

func BenchmarkGet1KWarm_Int64_1MStd(b *testing.B) {
	hotKeyCount := 20
	lookupLoop := 50

	var bms []benchmark
	if !*longTestFlag {
		bms = []benchmark{
			{"map size 1000", 1_000},
			{"map size 1000000", 1_000_000},
		}
	} else {
		bms = coarseMapSizes()
		bms = append(bms, fineMapSizes()...)
	}

	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			m := make(map[uint64]uint64, bm.mapElements)
			for i := 0; i < bm.mapElements; i++ {
				m[uint64(i)] = uint64(i)
			}

			var hotKeys []uint64
			for i := 0; i < hotKeyCount; i++ {
				hotKeys = append(hotKeys, uint64(rand.Intn(bm.mapElements)))
				sinkUint, sinkBool = m[uint64(i)]
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for j := 0; j < lookupLoop; j++ {
					for k := range hotKeys {
						sinkUint, sinkBool = m[uint64(k)]
					}
				}
			}
		})
	}
}

type benchmark struct {
	name        string
	mapElements int
}

func BenchmarkGet1K_Hit_Hot_16byte_Swisstable(b *testing.B) {
	const (
		hotKeyCount = 20
		lookupLoop  = 50
	)

	var bms []benchmark
	if !*longTestFlag {
		bms = []benchmark{
			{"map size 1000", 1_000},
			{"map size 1000000", 1_000_000},
		}
	} else {
		bms = coarseMapSizes()
		bms = append(bms, fineMapSizes()...)
	}

	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			m := New(bm.mapElements)
			for i := 0; i < bm.mapElements; i++ {
				m.Set(Key(i), Value(i))
			}

			var hotKeys []uint64
			for i := 0; i < hotKeyCount; i++ {
				hotKeys = append(hotKeys, uint64(rand.Intn(bm.mapElements)))
				v, b := m.Get(Key(i))
				sinkUint = uint64(v)
				sinkBool = b
			}

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				for j := 0; j < lookupLoop; j++ {
					for k := range hotKeys {
						v, b := m.Get(Key(k))
						sinkUint = uint64(v)
						sinkBool = b
					}
				}
			}
			b.StopTimer()
			// TODO: remove stats output
			b.Logf("stats: gets: %d extra groups: %d tophash false pos: %d",
				m.gets, m.getExtraGroups, m.getTopHashFalsePositives)
		})
	}
}

func BenchmarkGet1K_Hit_Cold_Swisstable(b *testing.B) {
	var bms []benchmark
	if !*longTestFlag {
		bms = []benchmark{
			{"map size 1000", 1_000},
			{"map size 10000", 10_000},
			{"map size 100000", 100_000},
			{"map size 1000000", 1_000_000},
		}
	} else {
		bms = coarseMapSizes()
		bms = append(bms, fineMapSizes()...)
	}

	for _, bm := range bms {
		b.Run(bm.name, func(b *testing.B) {
			minMem := 512.0 * (1 << 20)

			// we don't use overhead to keep the count of maps consistent
			// across different implemenations
			mapMem := float64(bm.mapElements) * 16
			mapCnt := int(math.Ceil(minMem / mapMem))

			keys := make([]Key, bm.mapElements)
			for i := 0; i < len(keys); i++ {
				keys[i] = Key(i)
			}
			rand.Shuffle(len(keys), func(i, j int) {
				keys[i], keys[j] = keys[j], keys[i]
			})

			b.Logf("creating %d maps with %.1f KB of data. %d total keys", mapCnt, float64(mapCnt)*mapMem/1024, mapCnt*bm.mapElements)
			maps := make([]*Map, mapCnt)
			for i := 0; i < mapCnt; i++ {
				m := New(bm.mapElements)
				for j := 0; j < bm.mapElements; j++ {
					m.Set(Key(j), Value(j))
				}
				maps[i] = m
			}
			rand.Shuffle(len(maps), func(i, j int) {
				maps[i], maps[j] = maps[j], maps[i]
			})

			b.ReportAllocs()
			b.ResetTimer()

			start := time.Now()
			for i := 0; i < b.N; i++ {
				// Exhaustively look up all keys
				for _, k := range keys {
					for _, m := range maps {
						// force no op:
						// _ = k
						// _ = m

						// force key reuse:
						// k = 0

						// do real work
						v, b := m.Get(Key(k))
						sinkUint = uint64(v)
						sinkBool = b
					}
				}
				// Done with this i for b.N.
				// Get new keys, and shuffle our maps.
				// TODO: Could sufficient to just pick random starting position for the maps,
				// but shuffle might be fast enough - shuffle 1M takes ~35ms.
				// b.StopTimer()
				// // TODO: no keys
				// // keys = randomKeys(coldKeyCount, bm.mapElements-coldKeyCount)
				// // TODO: maybe not needed to shuffle maps again?
				// rand.Shuffle(len(maps), func(i, j int) {
				// 	maps[i], maps[j] = maps[j], maps[i]
				// })
				// b.StartTimer()
				end := time.Since(start)
				b.ReportMetric(float64(end.Nanoseconds())/(float64(b.N)*float64(len(keys)*len(maps))), "ns/get")
			}
		})
	}
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
