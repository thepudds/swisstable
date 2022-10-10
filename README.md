# swisstable

This is a Go map implementation that preserves the semantics of the Go runtime map, including doing incremental growth without invalidating iterators.

### Sample Benchmarks

old is the runtime map, new is this swisstable implementation.

```
name                      old time/op    new time/op    delta
FillGrow/664-4            63.3µs ± 1%    58.9µs ± 2%   -6.87%  (p=0.000 n=20+20)
FillGrow/999-4             112µs ± 3%     105µs ± 2%   -5.77%  (p=0.000 n=20+20)
FillGrow/681575-4          141ms ± 4%     111ms ± 3%  -21.37%  (p=0.000 n=20+19)
FillGrow/1022362-4         262ms ± 4%     207ms ± 4%  -20.95%  (p=0.000 n=20+20)
FillGrow/5452596-4         1.51s ± 5%     1.24s ± 4%  -17.81%  (p=0.000 n=18+19)
FillGrow/8178894-4         2.72s ± 3%     2.22s ± 2%  -18.54%  (p=0.000 n=20+18)

FillPresize/664-4         29.3µs ± 2%    22.5µs ± 3%  -23.22%  (p=0.000 n=20+20)
FillPresize/999-4         45.0µs ± 4%    36.8µs ± 4%  -18.12%  (p=0.000 n=20+20)
FillPresize/681575-4      80.6ms ± 7%    66.1ms ± 7%  -17.93%  (p=0.000 n=20+20)
FillPresize/1022362-4      117ms ± 5%     122ms ± 6%   +4.54%  (p=0.000 n=20+20)
FillPresize/5452596-4      761ms ±15%     866ms ± 3%  +13.80%  (p=0.000 n=19+20)
FillPresize/8178894-4      1.12s ± 7%     1.43s ± 4%  +27.42%  (p=0.000 n=20+20)

GetHitHot/664-4           20.7µs ±15%    16.3µs ±12%  -21.28%  (p=0.000 n=20+18)
GetHitHot/999-4           19.5µs ±15%    16.5µs ± 9%  -15.62%  (p=0.000 n=20+19)
GetHitHot/681575-4        21.5µs ± 8%    17.8µs ±11%  -17.44%  (p=0.000 n=20+20)
GetHitHot/1022362-4       19.8µs ±11%    16.7µs ±11%  -15.32%  (p=0.000 n=19+19)
GetHitHot/5452596-4       21.4µs ±13%    17.0µs ±10%  -20.66%  (p=0.000 n=20+19)
GetHitHot/8178894-4       19.8µs ±17%    17.0µs ± 9%  -14.04%  (p=0.000 n=20+20)

GetMissHot/664-4          17.2µs ±23%    21.6µs ±19%  +25.54%  (p=0.000 n=20+20)
GetMissHot/999-4          15.7µs ±12%    20.2µs ±13%  +28.61%  (p=0.000 n=20+20)
GetMissHot/681575-4       17.7µs ±20%    22.6µs ±23%  +27.68%  (p=0.000 n=20+20)
GetMissHot/1022362-4      15.5µs ± 5%    19.6µs ±10%  +26.30%  (p=0.000 n=20+18)
GetMissHot/5452596-4      17.8µs ±12%    20.9µs ±19%  +17.80%  (p=0.000 n=20+19)
GetMissHot/8178894-4      15.3µs ± 6%    19.8µs ±11%  +28.92%  (p=0.000 n=19+20)

GetAllStartCold/664-4      1.10s ± 1%     0.87s ± 0%  -20.85%  (p=0.000 n=10+8)
GetAllStartCold/999-4      1.15s ± 1%     0.91s ± 1%  -20.34%  (p=0.000 n=10+10)
GetAllStartCold/681575-4   3.22s ± 3%     2.96s ± 1%   -8.04%  (p=0.000 n=9+10)
GetAllStartCold/1022362-4  3.25s ± 1%     3.44s ± 4%   +5.75%  (p=0.000 n=10+10)
GetAllStartCold/5452596-4  4.50s ± 2%     5.90s ± 3%  +31.27%  (p=0.000 n=10+10)
GetAllStartCold/8178894-4  4.77s ± 1%     6.82s ± 3%  +43.15%  (p=0.000 n=9+10)
[Geo mean]                                            -15.20%
```

There is an overview of the approach [here](https://github.com/golang/go/issues/54766#issuecomment-1270385441), and some comments on the current performance [here](https://github.com/golang/go/issues/54766#issuecomment-1270533454).

## Iteration

The current iterator implementation (which we will call "alternative 1") has the following high-level approach:

* Evacuation status is maintained during growth in a separate growth status slice. (This growth status slice uses memory, but it is not per element but rather per group info, and it uses less than the memory overhead of the overflow buckets used by the current runtime map, even for small keys).
* Iterators hold on to references to the current table and an immutable-during-growth old table. (The runtime iterators also hold on to references to tables, but the runtime's old buckets are not immutable).
* Iterators walk both the old and current tables, with de-duplication to avoid emitting the same key twice and checking the live tables when needed to emit the golden data. It has some logic to avoid some hashing while doing this, and I think it does less overall hashing during mid-move iteration than the runtime map iterator (but need to confirm the hashing frequency vs. the runtime a bit more).

### Some iteration alternatives:

* Alternative 2: similar to Alternative 1, but using atomics to do growth work during iteration and Get operations, which would have common cases of atomic loads and take advantage of the old table being immutable during growth. Set and Delete would not use atomics.

* Alternative 3: "iteration is moving, and always move chains". This only loops over the snapshot of the current table (and does not loop over the snapshot of hold), but would look back to old if a group is not evacuated. The basic case is emitting all elements from their natural group in the current snapshot. This uses atomics to do grow work during iteration and Get. Iteration would always move any probe chains found in old, which simplifies & improves the performance  of some cases. 

* Alternative 4: similar to Alternative 2, but without using atomics and without doing grow work during iteration and Get. The basic case is still emitting all elements from their natural group in the current snapshot, but instead of moving chains, it instead follows probe chains forward and hashes to determine the natural group.

