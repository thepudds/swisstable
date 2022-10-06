package swisstable

// Edit if desired. Code generated by "fzgen -chain .".

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/thepudds/fzgen/fuzzer"
)

func Fuzz_NewVmap_Chain(f *testing.F) {
	f.Fuzz(func(t *testing.T, data []byte) {
		var capacity byte
		fz := fuzzer.NewFuzzer(data)
		fz.Fill(&capacity)

		target := NewVmap(capacity, nil)

		steps := []fuzzer.Step{
			{
				Name: "Fuzz_ValidatingMap_Delete",
				Func: func(k Key) {
					target.Delete(k)
				},
			},
			{
				Name: "Fuzz_ValidatingMap_DeleteBulk",
				Func: func(list Keys) {
					target.DeleteBulk(list)
				},
			},
			{
				Name: "Fuzz_ValidatingMap_Get",
				Func: func(k Key) (Value, bool) {
					return target.Get(k)
				},
			},
			{
				Name: "Fuzz_ValidatingMap_GetBulk",
				Func: func(list Keys) ([]Value, []bool) {
					return target.GetBulk(list)
				},
			},
			{
				Name: "Fuzz_ValidatingMap_Len",
				Func: func() int {
					return target.Len()
				},
			},
			{
				Name: "Fuzz_ValidatingMap_Range",
				Func: func(ops []Op) {
					target.Range(ops)
				},
			},
			{
				Name: "Fuzz_ValidatingMap_Set",
				Func: func(k Key, v Value) {
					target.Set(k, v)
				},
			},
			{
				Name: "Fuzz_ValidatingMap_SetBulk",
				Func: func(list Keys) {
					target.SetBulk(list)
				},
			},
		}

		// Execute a specific chain of steps, with the count, sequence and arguments controlled by fz.Chain
		fz.Chain(steps)

		// Final validation.
		got := keysAndValues(target.m)
		if diff := cmp.Diff(target.mirror, got); diff != "" {
			t.Errorf("Fuzz_NewVmap_Chain target mismatch after steps completed (-want +got):\n%s", diff)
		}
	})
}
