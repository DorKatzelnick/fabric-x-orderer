/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"fmt"

	"github.com/hyperledger/fabric-protos-go-apiv2/common"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
)

/*
	type AssemblerSupport interface {
		identity.SignerSerializer
		Height() uint64
		SharedConfig() channelconfig.Orderer
		ChannelID() string
		WriteConfigBlock(block *common.Block, encodedMetadataValue []byte)
		WriteBlockSync(block *common.Block, encodedMetadataValue []byte)
	}
*/
type AssemblerSupportAdapter struct {
	assembler *Assembler
}

// todo - implement. add signer serializer to assembler.
func (a *AssemblerSupportAdapter) Sign(message []byte) ([]byte, error) {
	return nil, nil
}

// todo - implement. add signer serializer to assembler.
func (a *AssemblerSupportAdapter) Serialize() ([]byte, error) {
	return nil, nil
}

func (a *AssemblerSupportAdapter) Height() uint64 {
	return a.assembler.ledger.LedgerReader().Height()
}

func (a *AssemblerSupportAdapter) ChannelID() string {
	return a.assembler.assemblerNodeConfig.Bundle.ConfigtxValidator().ChannelID()
}

func (a *AssemblerSupportAdapter) SharedConfig() channelconfig.Orderer {
	conf, ok := a.assembler.assemblerNodeConfig.Bundle.OrdererConfig()
	if !ok {
		a.assembler.logger.Panic("orderer config not found in bundle")
		return nil
	}
	return conf
}

func (a *AssemblerSupportAdapter) WriteBlockSync(block *common.Block) {
	a.assembler.ledger.AppendBlock(block)
}

func (a *AssemblerSupportAdapter) WriteConfigBlock(block *common.Block) {
	a.assembler.ledger.AppendBlock(block)
}

// Block returns the block with the given number from the ledger,
// or nil if no such block exists.
func (a *AssemblerSupportAdapter) Block(number uint64) *common.Block {
	block, err := a.assembler.ledger.LedgerReader().RetrieveBlockByNumber(number)
	if err != nil {
		a.assembler.logger.Errorf("Failed to retrieve block %d: %v", number, err)
		return nil
	}
	return block
}

// LastConfigBlock returns the most recent (fabric) config block at or before the given block,
// or an error if it cannot be retrieved.
func (a *AssemblerSupportAdapter) LastConfigBlock(block *common.Block) (*common.Block, error) {
	lastConfigIndex, err := protoutil.GetLastConfigIndexFromBlock(block)
	if err != nil {
		return nil, fmt.Errorf("failed to get last config index from assebmler's last block: %s", err)
	}
	lastConfigBlock, err := a.assembler.ledger.LedgerReader().RetrieveBlockByNumber(lastConfigIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get last config block: %s", err)
	}
	return lastConfigBlock, nil
}
