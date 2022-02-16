package swisstable

import (
	"bytes"
	"testing"
)

func TestMatchByte(t *testing.T) {
	tests := []struct {
		name     string
		c        uint8
		buffer   []byte
		wantMask uint32
		wantOk   bool
	}{
		{
			"match 3",
			42,
			[]byte{42, 0, 0, 42, 42, 0, 17, 17, 0, 0, 0, 0, 0, 0, 0, 0},
			1<<0 | 1<<3 | 1<<4,
			true,
		},
		{
			"match 1 at end",
			42,
			[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42},
			1 << 15,
			true,
		},
		{
			"match 2 at start and end",
			42,
			[]byte{42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42},
			1<<0 | 1<<15,
			true,
		},
		{
			"match all",
			42,
			[]byte{42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42},
			1<<16 - 1,
			true,
		},
		{
			"match none - no match",
			255,
			[]byte{42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42},
			0,
			true,
		},
		{
			"match none - len short by 1",
			42,
			[]byte{42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			0,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMask, gotOk := MatchByte(tt.c, tt.buffer)
			if gotMask != tt.wantMask {
				t.Errorf("MatchByte() gotMask = %v, want %v", gotMask, tt.wantMask)
			}
			if gotOk != tt.wantOk {
				t.Errorf("MatchByte() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}

func TestMatchByteAlignment(t *testing.T) {
	tests := []struct {
		name     string
		c        uint8
		buffer   []byte
		wantMask uint32
		wantOk   bool
	}{
		{
			"match all",
			42,
			bytes.Repeat([]byte{42}, 10000),
			1<<16 - 1,
			true,
		},
		{
			"match none",
			255,
			bytes.Repeat([]byte{42}, 10000),
			0,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < len(tt.buffer)-16; i++ {
				buffer := tt.buffer[i : i+16]

				gotMask, gotOk := MatchByte(tt.c, buffer)
				if gotMask != tt.wantMask {
					t.Fatalf("MatchByte() offset %d gotMask = %v, want %v", i, gotMask, tt.wantMask)
				}
				if gotOk != tt.wantOk {
					t.Fatalf("MatchByte() offset %d gotOk = %v, want %v", i, gotOk, tt.wantOk)
				}
			}
		})
	}
}
