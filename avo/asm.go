// //go:build ignore
// // +build ignore

package main

import (
	. "github.com/mmcloughlin/avo/build"
	"github.com/mmcloughlin/avo/operand"
)

// func main() {
// 	TEXT("Set1", NOSPLIT, "func(c uint8) ")
// 	x := Load(Param("c"), XMM())
// 	PUNPCKLBW(x, x)
// 	// 	PUNPCKLWD(x, x)
// 	PSHUFD(x, x, operand.Imm(0))
// 	// Store(x, ReturnIndex(0))
// 	RET()
// 	Generate()
// }

// WORKS!
// func main() {
// 	TEXT("MatchByte", NOSPLIT, "func(c uint8, buffer []byte) uint32")
// 	// TEXT("MatchByte", NOSPLIT, "func(c uint8, buffer *byte) uint32")
// 	c := Load(Param("c"), GP32())
// 	ptr := Load(Param("buffer").Base(), GP64())
// 	// ptr := Load(Param("buffer"), GP64())
// 	x0, x1 := XMM(), XMM()
// 	result := GP32()
// 	PXOR(x1, x1)
// 	MOVD(c, x0)
// 	PSHUFB(x1, x0)
// 	// if !operand.IsM128(operand.Mem{Base: ptr}) {
// 	// 	panic("not m128")
// 	// }
// 	// Mem example from https://github.com/mmcloughlin/avo/blob/master/examples/fnv1a/asm.go#L32
// 	// also: https://github.com/mmcloughlin/avo/blob/master/examples/sum/asm.go
// 	PCMPEQB(operand.Mem{Base: ptr}, x0)
// 	PMOVMSKB(x0, result)
// 	Store(result, ReturnIndex(0))
// 	RET()
// 	Generate()
// }

func main2() {
	TEXT("MatchByte", NOSPLIT, "func(c uint8, buffer []byte) (mask uint32, ok bool)")
	// TEXT("MatchByte", NOSPLIT, "func(c uint8, buffer *byte) uint32")
	n := Load(Param("buffer").Len(), GP64())
	result := GP32()
	// ok := GP8()
	CMPQ(n, operand.Imm(16))
	JGE(operand.LabelRef("valid"))
	// XORB(ok, ok)
	// Store(ok, ReturnIndex(1))
	ok, err := ReturnIndex(1).Resolve()
	if err != nil {
		panic(err)
	}
	// TODO: return zero value ;=)
	XORL(result, result)
	Store(result, ReturnIndex(0))
	MOVB(operand.Imm(0), ok.Addr)
	RET()

	Label("valid")
	c := Load(Param("c"), GP32())
	ptr := Load(Param("buffer").Base(), GP64())

	// ptr := Load(Param("buffer"), GP64())
	x0, x1, x2 := XMM(), XMM(), XMM()
	PXOR(x1, x1)
	MOVD(c, x0)
	PSHUFB(x1, x0)
	// if !operand.IsM128(operand.Mem{Base: ptr}) {
	// 	panic("not m128")
	// }
	// Mem example from https://github.com/mmcloughlin/avo/blob/master/examples/fnv1a/asm.go#L32
	// also: https://github.com/mmcloughlin/avo/blob/master/examples/sum/asm.go
	// MOVOU is how MOVDQU is spelled in Go asm
	MOVOU(operand.Mem{Base: ptr}, x2)
	PCMPEQB(x2, x0)
	PMOVMSKB(x0, result)
	Store(result, ReturnIndex(0))
	// MOVB(operand.Imm(1), ok)
	// Store(ok, ReturnIndex(1))
	MOVB(operand.Imm(1), ok.Addr)
	RET()
	Generate()
}

/*
TEXT("Add", NOSPLIT, "func(x, y uint64) uint64")
Doc("Add adds x and y.")
x := Load(Param("x"), GP64())
y := Load(Param("y"), GP64())
ADDQ(x, y)
Store(y, ReturnIndex(0))
RET()
Generate()
*/
