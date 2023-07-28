package xmss

import (
	"github.com/theQRL/qrl-rich-list-indexer/common"
	"github.com/theQRL/qrl-rich-list-indexer/misc"
)

func GetXMSSAddressFromPK(unsizedEPK []byte) common.Address {
	var ePK [ExtendedPKSize]byte
	copy(ePK[:], unsizedEPK)
	desc := NewQRLDescriptorFromExtendedPK(&ePK)

	if desc.GetAddrFormatType() != SHA256_2X {
		panic("Address format type not supported")
	}

	var address common.ByteAddress
	descBytes := desc.GetBytes()

	copy(address[:DescriptorSize], descBytes[:DescriptorSize])

	var hashedKey [32]uint8
	misc.SHA256(hashedKey[:], ePK[:])

	copy(address[DescriptorSize:], hashedKey[:])

	misc.SHA256(hashedKey[:], address[:DescriptorSize+32])
	copy(address[DescriptorSize+32:], hashedKey[len(hashedKey)-4:])

	return misc.ToStringAddress(address[:])
}
