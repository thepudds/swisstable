// Code generated by command: go run asm.go -out ../match_amd64.s -stubs ../match_stub.go -pkg swisstable. DO NOT EDIT.

#include "textflag.h"

// func MatchByte(c uint8, buffer []byte) (mask uint32, ok bool)
// Requires: SSE2, SSSE3
TEXT ·MatchByte(SB), NOSPLIT, $0-37
	// Get our input parameters
	MOVBLZX c+0(FP), AX
	MOVQ    buffer_base+8(FP), CX
	MOVQ    buffer_len+16(FP), DX

	// Check len of our input slice, which must be at least 16
	CMPQ DX, $0x10
	JGE  valid

	// Input slice too short. Return 0, false
	XORL AX, AX
	MOVL AX, mask+32(FP)
	MOVB $0x00, ok+36(FP)
	RET

valid:
	// Input slice is a valid length
	// Move c into an xmm register
	MOVD AX, X0

	// Shuffle the value of c into every byte of another xmm register
	PXOR   X1, X1
	PSHUFB X1, X0

	// Do an unaligned move of 16 bytes of input slice data to xmm register
	// MOVOU is how MOVDQU is spelled in Go asm
	MOVOU (CX), X1

	// Find matching bytes with result in xmm register
	PCMPEQB X1, X0

	// Collapse matching bytes result down to an integer bitmask
	PMOVMSKB X0, AX

	// Return bitmask, true
	MOVL AX, mask+32(FP)
	MOVB $0x01, ok+36(FP)
	RET
