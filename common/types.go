package common

import (
	"encoding/hex"
	"fmt"
)

type ByteAddress [39]byte
type Address string
type Hash [32]byte

func (a Address) ToString() string {
	return string(a)
}

func (b *ByteAddress) ToAddress() Address {
	return Address(fmt.Sprintf("Q%s", hex.EncodeToString(b[:])))
}

func (h *Hash) ToString() string {
	return hex.EncodeToString(h[:])
}
