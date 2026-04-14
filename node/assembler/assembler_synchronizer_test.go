/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package assembler

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/fabric-lib-go/bccsp"
	"github.com/hyperledger/fabric-lib-go/common/flogging"
	cb "github.com/hyperledger/fabric-protos-go-apiv2/common"
	ab "github.com/hyperledger/fabric-protos-go-apiv2/orderer"
	"github.com/hyperledger/fabric-x-common/common/channelconfig"
	"github.com/hyperledger/fabric-x-common/protoutil"
	"github.com/hyperledger/fabric-x-common/protoutil/identity"
	"github.com/hyperledger/fabric-x-orderer/common/deliverclient/blocksprovider"
	"github.com/hyperledger/fabric-x-orderer/common/types"
	"github.com/hyperledger/fabric-x-orderer/config"
	"github.com/hyperledger/fabric-x-orderer/node/comm"
	"github.com/hyperledger/fabric-x-orderer/testutil/tx"
	"github.com/stretchr/testify/require"
)

// TestAssemblerSynchronizer_SyncWithMultipleAssemblers creates several fake
// assemblers with pre-filled ledgers and asserts that a newly created
// assembler's synchronizer pulls the genesis and subsequent blocks from the
// cluster and writes them into its ledger.
func TestAssemblerSynchronizer_SyncWithMultipleAssemblers(t *testing.T) {
	// Build a small chain: genesis (0) and blocks 1..4
	// Create a valid ConfigEnvelope for genesis so ConfigFromBlock succeeds
	cfgEnv := &cb.ConfigEnvelope{Config: &cb.Config{}}
	genesis := tx.CreateConfigBlock(0, protoutil.MarshalOrPanic(cfgEnv))
	b1 := &cb.Block{Header: &cb.BlockHeader{Number: 1}}
	b2 := &cb.Block{Header: &cb.BlockHeader{Number: 2}}
	b3 := &cb.Block{Header: &cb.BlockHeader{Number: 3}}
	b4 := &cb.Block{Header: &cb.BlockHeader{Number: 4}}

	// Existing assemblers are simulated implicitly via endpoints
	endpoints := []string{"asm1", "asm2", "asm3", "asm4"}

	// Fake genesis fetcher returns the genesis block from all endpoints
	gff := &multiGenesisFetcherFactory{endpoints: endpoints, genesis: genesis}

	// Fake deliverer will push blocks 1..4 into the sync buffer
	fdf := &fakeBFTDelivererFactory{blocks: []*cb.Block{b1, b2, b3, b4}}

	// New assembler support starts empty
	newAsm := &fakeSupport{written: []*cb.Block{}}

	s := &AssemblerSynchronizer{
		Logger:              flogging.MustGetLogger("test.sync.multi"),
		SelfPartyID:         99,
		TargetHeight:        5, // fetch genesis (0) and blocks 1..4
		Support:             newAsm,
		CryptoProvider:      nil,
		ClusterDialer:       &comm.PredicateDialer{Config: comm.ClientConfig{}},
		LocalConfigCluster:  config.Cluster{},
		BlockPullerFactory:  gff,
		VerifierFactory:     &noopVerifierCreator{},
		BFTDelivererFactory: fdf,
	}

	err := s.Sync()
	require.NoError(t, err)

	// The new assembler should have written genesis + blocks 1..4
	require.Equal(t, 5, len(newAsm.written))
	require.EqualValues(t, uint64(0), newAsm.written[0].GetHeader().GetNumber())
	require.EqualValues(t, uint64(1), newAsm.written[1].GetHeader().GetNumber())
	require.EqualValues(t, uint64(2), newAsm.written[2].GetHeader().GetNumber())
	require.EqualValues(t, uint64(3), newAsm.written[3].GetHeader().GetNumber())
	require.EqualValues(t, uint64(4), newAsm.written[4].GetHeader().GetNumber())
}

// multiGenesisFetcherFactory returns the same genesis block for multiple endpoints.
type multiGenesisFetcherFactory struct {
	endpoints []string
	genesis   *cb.Block
}

func (f *multiGenesisFetcherFactory) CreateGenesisFetcher(myPartyID types.PartyID, support AssemblerSupport, baseDialer *comm.PredicateDialer, clusterConfig config.Cluster, bccsp bccsp.BCCSP, logger *flogging.FabricLogger) (GenesisFetcher, error) {
	// return a GenesisFetcher that reports genesis for each endpoint
	genmap := map[string]*cb.Block{}
	for _, ep := range f.endpoints {
		genmap[ep] = f.genesis
	}
	return &fakeGenesisFetcherMulti{genesisByEP: genmap}, nil
}

type fakeGenesisFetcherMulti struct{ genesisByEP map[string]*cb.Block }

func (f *fakeGenesisFetcherMulti) GenesisByEndpoints() (map[string]*cb.Block, error) {
	return f.genesisByEP, nil
}
func (f *fakeGenesisFetcherMulti) Close() {}

// fakeSupport implements AssemblerSupport for tests.
type fakeSupport struct {
	mu      sync.Mutex
	written []*cb.Block
}

func (f *fakeSupport) Sign(message []byte) ([]byte, error) { return nil, nil }
func (f *fakeSupport) Serialize() ([]byte, error)          { return []byte("fake"), nil }
func (f *fakeSupport) Height() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return uint64(len(f.written))
}
func (f *fakeSupport) SharedConfig() channelconfig.Orderer {
	return &fakeOrderer{}
}
func (f *fakeSupport) ChannelID() string { return "testchannel" }
func (f *fakeSupport) WriteConfigBlock(block *cb.Block) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.written = append(f.written, block)
}
func (f *fakeSupport) WriteBlockSync(block *cb.Block) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.written = append(f.written, block)
}

func (f *fakeSupport) Block(number uint64) *cb.Block {
	f.mu.Lock()
	defer f.mu.Unlock()
	if int(number) < len(f.written) {
		return f.written[number]
	}
	return nil
}

func (f *fakeSupport) LastConfigBlock(block *cb.Block) (*cb.Block, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if block != nil && protoutil.IsConfigBlock(block) {
		return block, nil
	}
	for i := len(f.written) - 1; i >= 0; i-- {
		if protoutil.IsConfigBlock(f.written[i]) {
			return f.written[i], nil
		}
	}
	return nil, fmt.Errorf("no config block available")
}

// fakeOrderer implements channelconfig.Orderer minimally for tests.
type fakeOrderer struct{}

func (f *fakeOrderer) ConsensusType() string                              { return "" }
func (f *fakeOrderer) ConsensusMetadata() []byte                          { return nil }
func (f *fakeOrderer) ConsensusState() ab.ConsensusType_State             { return 0 }
func (f *fakeOrderer) BatchSize() *ab.BatchSize                           { return &ab.BatchSize{} }
func (f *fakeOrderer) BatchTimeout() time.Duration                        { return 0 }
func (f *fakeOrderer) MaxChannelsCount() uint64                           { return 0 }
func (f *fakeOrderer) Consenters() []*cb.Consenter                        { return []*cb.Consenter{{}, {}, {}, {}} }
func (f *fakeOrderer) Organizations() map[string]channelconfig.OrdererOrg { return nil }
func (f *fakeOrderer) Capabilities() channelconfig.OrdererCapabilities    { return nil }

// fakeBFTDelivererFactory returns a deliverer that pushes predefined blocks into the block handler.
type fakeBFTDelivererFactory struct{ blocks []*cb.Block }

func (f *fakeBFTDelivererFactory) CreateBFTDeliverer(channelID string, blockHandler blocksprovider.BlockHandler, ledger blocksprovider.LedgerInfo, updatableBlockVerifier blocksprovider.UpdatableBlockVerifier, dialer blocksprovider.Dialer, orderersSourceFactory blocksprovider.OrdererConnectionSourceFactory, cryptoProvider bccsp.BCCSP, doneC chan struct{}, signer identity.SignerSerializer, deliverStreamer blocksprovider.DeliverStreamer, censorshipDetectorFactory blocksprovider.CensorshipDetectorFactory, endpointsExtractor blocksprovider.EndpointsExtractor, logger *flogging.FabricLogger, initialRetryInterval time.Duration, maxRetryInterval time.Duration, blockCensorshipTimeout time.Duration, maxRetryDuration time.Duration, maxRetryDurationExceededHandler blocksprovider.MaxRetryDurationExceededHandler) BFTBlockDeliverer {
	return &fakeBFTDeliverer{channelID: channelID, bh: blockHandler, blocks: f.blocks, stopC: make(chan struct{})}
}

type fakeBFTDeliverer struct {
	channelID string
	bh        blocksprovider.BlockHandler
	blocks    []*cb.Block
	stopC     chan struct{}
}

func (f *fakeBFTDeliverer) Initialize(channelConfig *cb.Config, selfPartyID types.PartyID) {}
func (f *fakeBFTDeliverer) DeliverBlocks() {
	for _, b := range f.blocks {
		_ = f.bh.HandleBlock(f.channelID, b)
	}
	<-f.stopC
}
func (f *fakeBFTDeliverer) Stop() { close(f.stopC) }

// TestAssemblerSynchronizer_SyncWithExistingBlocks verifies that an assembler
// that already has some blocks in its ledger only fetches the missing blocks
// and does not re-fetch genesis.
func TestAssemblerSynchronizer_SyncWithExistingBlocks(t *testing.T) {
	// Build a small chain: genesis (0) and blocks 1..4
	cfgEnv := &cb.ConfigEnvelope{Config: &cb.Config{}}
	genesis := tx.CreateConfigBlock(0, protoutil.MarshalOrPanic(cfgEnv))
	b1 := &cb.Block{Header: &cb.BlockHeader{Number: 1}}
	b2 := &cb.Block{Header: &cb.BlockHeader{Number: 2}}
	b3 := &cb.Block{Header: &cb.BlockHeader{Number: 3}}
	b4 := &cb.Block{Header: &cb.BlockHeader{Number: 4}}

	// endpoints and factories
	endpoints := []string{"asm1", "asm2", "asm3", "asm4"}
	gff := &multiGenesisFetcherFactory{endpoints: endpoints, genesis: genesis}

	// Fake deliverer will push only blocks 2..4, since 0 and 1 already exist
	fdf := &fakeBFTDelivererFactory{blocks: []*cb.Block{b2, b3, b4}}

	// New assembler support starts with genesis and block 1 already written
	newAsm := &fakeSupport{written: []*cb.Block{genesis, b1}}

	s := &AssemblerSynchronizer{
		Logger:              flogging.MustGetLogger("test.sync.existing"),
		SelfPartyID:         99,
		TargetHeight:        5, // want blocks 0..4
		Support:             newAsm,
		CryptoProvider:      nil,
		ClusterDialer:       &comm.PredicateDialer{Config: comm.ClientConfig{}},
		LocalConfigCluster:  config.Cluster{},
		BlockPullerFactory:  gff,
		VerifierFactory:     &noopVerifierCreator{},
		BFTDelivererFactory: fdf,
	}

	err := s.Sync()
	require.NoError(t, err)

	// The assembler should have genesis + blocks 1..4
	require.Equal(t, 5, len(newAsm.written))
	require.EqualValues(t, uint64(0), newAsm.written[0].GetHeader().GetNumber())
	require.EqualValues(t, uint64(1), newAsm.written[1].GetHeader().GetNumber())
	require.EqualValues(t, uint64(2), newAsm.written[2].GetHeader().GetNumber())
	require.EqualValues(t, uint64(3), newAsm.written[3].GetHeader().GetNumber())
	require.EqualValues(t, uint64(4), newAsm.written[4].GetHeader().GetNumber())
}
