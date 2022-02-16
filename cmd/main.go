package main

import (
	"fmt"
	"math/bits"

	"github.com/thepudds/swisstable-wip"
)

func main() {
	c := uint8(42)
	buffer := []byte{42, 0, 42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0}
	// buffer := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	buffer = buffer[2:]
	// buffer = buffer[2:8]
	fmt.Println(len(buffer))
	res, ok := swisstable.MatchByte(c, buffer)
	if !ok {
		panic("not ok")
	}
	fmt.Println(res)
	zeros := bits.TrailingZeros32(res)
	if zeros == 32 {
		fmt.Println("no match")
	} else {
		for {
			index := bits.TrailingZeros32(res)
			fmt.Println("match:", index)
			res &= ^(1 << index)
			if res == 0 {
				break
			}
		}
	}

}
