package main

import (
	"fmt"
	"math/bits"
)

func main() {
	c := uint8(42)
	buffer := []byte{42, 0, 42, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0}
	// buffer := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	buffer = buffer[2:]
	fmt.Println(len(buffer))
	res := MatchByte(c, buffer)
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
