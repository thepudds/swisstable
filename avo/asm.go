package main

import (
	. "github.com/mmcloughlin/avo/build"
	"github.com/mmcloughlin/avo/operand"
)

//go:generate go run . -out ../match_amd64.s -stubs ../match_stub.go -pkg swisstable

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

func main() {
	TEXT("MatchByte", NOSPLIT, "func(c uint8, buffer []byte) (mask uint32, ok bool)")
	// TEXT("MatchByte", NOSPLIT, "func(c uint8, buffer *byte) uint32")
	Comment("Get our input parameters")
	c := Load(Param("c"), GP32())
	ptr := Load(Param("buffer").Base(), GP64())
	n := Load(Param("buffer").Len(), GP64())
	result := GP32()

	Comment("Check len of our input slice, which must be at least 16")
	CMPQ(n, operand.Imm(16))
	JGE(operand.LabelRef("valid"))
	// XORB(ok, ok)
	// Store(ok, ReturnIndex(1))
	Comment("Input slice too short. Return 0, false")
	ok, err := ReturnIndex(1).Resolve()
	if err != nil {
		panic(err)
	}
	XORL(result, result)
	Store(result, ReturnIndex(0))
	MOVB(operand.Imm(0), ok.Addr)
	RET()

	Label("valid")
	Comment("Input slice is a valid length")

	// ptr := Load(Param("buffer"), GP64())
	Comment("Move c into an xmm register")
	x0, x1, x2 := XMM(), XMM(), XMM()
	MOVD(c, x0)
	Comment("Shuffle the value of c into every byte of another xmm register")
	PXOR(x1, x1)
	PSHUFB(x1, x0)
	// if !operand.IsM128(operand.Mem{Base: ptr}) {
	// 	panic("not m128")
	// }
	// Mem example from https://github.com/mmcloughlin/avo/blob/master/examples/fnv1a/asm.go#L32
	// also: https://github.com/mmcloughlin/avo/blob/master/examples/sum/asm.go
	// https://c9x.me/x86/html/file_module_x86_id_184.html
	//   When the source or destination operand is a memory operand, the operand may be unaligned on a 16-byte boundary
	//   without causing a general-protection exception (#GP) to be generated.
	Comment("Do an unaligned move of 16 bytes of input slice data to xmm register")
	Comment("MOVOU is how MOVDQU is spelled in Go asm")
	MOVOU(operand.Mem{Base: ptr}, x2)

	Comment("Find matching bytes with result in xmm register")
	PCMPEQB(x2, x0)

	Comment("Collapse matching bytes result down to an integer bitmask")
	PMOVMSKB(x0, result)

	Comment("Return bitmask, true")
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
