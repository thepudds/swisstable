package main

import (
	. "github.com/mmcloughlin/avo/build"
	"github.com/mmcloughlin/avo/operand"
)

//go:generate go run . -out ../match_amd64.s -stubs ../match_stub.go -pkg swisstable

func main() {
	// TODO: add more emitted comments. Mention ok is only false if short (<16 bytes), and >16 bytes allowed.
	// TODO: probably could have lighterweight signature (no ok, maybe direct pointer rather than slice, etc).
	// TODO: at least change signature to:
	//  func(b []byte, c byte) (bitmask uint32, ok bool)
	// similar to:
	//    func IndexAny(s []byte, chars string) int
	//    func IndexByte(b []byte, c byte) int
	TEXT("MatchByte", NOSPLIT, "func(c uint8, buffer []byte) (mask uint32, ok bool)")
	Comment("Get our input parameters")
	c := Load(Param("c"), GP32())
	ptr := Load(Param("buffer").Base(), GP64())
	n := Load(Param("buffer").Len(), GP64())
	result := GP32()

	Comment("Check len of our input slice, which must be at least 16")
	CMPQ(n, operand.Imm(16))
	JGE(operand.LabelRef("valid"))
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

	Comment("Move c into an xmm register")
	x0, x1, x2 := XMM(), XMM(), XMM()
	MOVD(c, x0)
	Comment("Shuffle the value of c into every byte of another xmm register")
	PXOR(x1, x1)
	PSHUFB(x1, x0)
	Comment("Do an unaligned move of 16 bytes of input slice data to xmm register")
	Comment("MOVOU is how MOVDQU is spelled in Go asm")
	MOVOU(operand.Mem{Base: ptr}, x2)

	Comment("Find matching bytes with result in xmm register")
	PCMPEQB(x2, x0)

	Comment("Collapse matching bytes result down to an integer bitmask")
	PMOVMSKB(x0, result)

	Comment("Return bitmask, true")
	Store(result, ReturnIndex(0))
	MOVB(operand.Imm(1), ok.Addr)
	RET()
	Generate()
}
